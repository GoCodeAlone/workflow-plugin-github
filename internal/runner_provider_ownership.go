package internal

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const jitOwnershipJournalVersion = 1
const jitOwnershipJournalName = "jit-ownership.json"

type jitOwnershipJournal struct {
	Version int                        `json:"version"`
	Entries []jitOwnershipJournalEntry `json:"entries"`
}

type jitOwnershipJournalEntry struct {
	Organization      string    `json:"organization"`
	RunnerID          int64     `json:"runner_id"`
	State             string    `json:"state"`
	TokenHash         string    `json:"token_hash,omitempty"`
	ExpiresAt         time.Time `json:"expires_at"`
	CleanupAttempts   int       `json:"cleanup_attempts,omitempty"`
	LastCleanupStatus string    `json:"last_cleanup_status,omitempty"`
	LastCleanupAt     time.Time `json:"last_cleanup_at,omitempty"`
}

func (m *githubRunnerProviderModule) initializeJITOwnershipJournal() error {
	if strings.TrimSpace(m.config.StateDir) == "" {
		return nil
	}
	stateDir, err := filepath.Abs(m.config.StateDir)
	if err != nil {
		return fmt.Errorf("resolve state_dir: %w", err)
	}
	info, err := os.Lstat(stateDir)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(stateDir, 0o700); err != nil {
			return fmt.Errorf("create state_dir: %w", err)
		}
		info, err = os.Lstat(stateDir)
	}
	if err != nil {
		return fmt.Errorf("inspect state_dir: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state_dir must be a directory and not a symbolic link")
	}
	root, err := os.OpenRoot(stateDir)
	if err != nil {
		return fmt.Errorf("open state_dir root: %w", err)
	}
	keepRoot := false
	defer func() {
		if !keepRoot {
			_ = root.Close()
		}
	}()
	rootInfo, err := root.Stat(".")
	if err != nil || !os.SameFile(info, rootInfo) {
		return errors.New("state_dir changed while opening durable root")
	}
	if err := root.Chmod(".", 0o700); err != nil {
		return fmt.Errorf("protect state_dir: %w", err)
	}
	m.config.StateDir = stateDir
	journalInfo, err := root.Lstat(jitOwnershipJournalName)
	if errors.Is(err, os.ErrNotExist) {
		m.stateRoot = root
		keepRoot = true
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect journal: %w", err)
	}
	if !journalInfo.Mode().IsRegular() || journalInfo.Mode()&os.ModeSymlink != 0 {
		return errors.New("journal must be a regular file and not a symbolic link")
	}
	if err := root.Chmod(jitOwnershipJournalName, 0o600); err != nil {
		return fmt.Errorf("protect journal: %w", err)
	}
	data, err := root.ReadFile(jitOwnershipJournalName)
	if err != nil {
		return fmt.Errorf("read journal: %w", err)
	}
	var journal jitOwnershipJournal
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&journal); err != nil {
		return fmt.Errorf("decode journal: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("journal must contain exactly one JSON document")
		}
		return fmt.Errorf("decode journal trailing data: %w", err)
	}
	if journal.Version != jitOwnershipJournalVersion {
		return fmt.Errorf("journal version must be %d", jitOwnershipJournalVersion)
	}
	loaded := make(map[pendingJITKey]*pendingJITOwnership, len(journal.Entries))
	for index, entry := range journal.Entries {
		key, pending, err := pendingJITFromJournalEntry(entry)
		if err != nil {
			return fmt.Errorf("journal entries[%d]: %w", index, err)
		}
		if _, exists := loaded[key]; exists {
			return fmt.Errorf("journal entries[%d]: duplicate organization and runner_id", index)
		}
		loaded[key] = pending
	}
	m.stateRoot = root
	m.pendingJIT = loaded
	keepRoot = true
	m.pendingJITMu.Lock()
	for key, pending := range m.pendingJIT {
		m.scheduleJITOwnershipLocked(key, pending)
	}
	m.pendingJITMu.Unlock()
	return nil
}

func pendingJITFromJournalEntry(entry jitOwnershipJournalEntry) (pendingJITKey, *pendingJITOwnership, error) {
	organization, err := parseOrganization(entry.Organization)
	if err != nil {
		return pendingJITKey{}, nil, err
	}
	if entry.RunnerID <= 0 || entry.ExpiresAt.IsZero() {
		return pendingJITKey{}, nil, errors.New("runner_id and expires_at are required")
	}
	pending := &pendingJITOwnership{
		organization:      organization,
		expiresAt:         entry.ExpiresAt.UTC(),
		cleanupAttempts:   entry.CleanupAttempts,
		lastCleanupStatus: entry.LastCleanupStatus,
		lastCleanupAt:     entry.LastCleanupAt.UTC(),
	}
	switch entry.State {
	case "pending":
		hash, err := hex.DecodeString(entry.TokenHash)
		if err != nil || len(hash) != sha256.Size {
			return pendingJITKey{}, nil, errors.New("pending token_hash must be a SHA-256 digest")
		}
		copy(pending.tokenHash[:], hash)
	case "owned":
		if entry.TokenHash != "" {
			return pendingJITKey{}, nil, errors.New("owned entry must not retain token_hash")
		}
		pending.acknowledged = true
	case "deleting":
		if entry.TokenHash != "" {
			return pendingJITKey{}, nil, errors.New("deleting entry must not retain token_hash")
		}
		pending.deleting = true
	default:
		return pendingJITKey{}, nil, errors.New("state must be pending, owned, or deleting")
	}
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: entry.RunnerID}
	return key, pending, nil
}

func (m *githubRunnerProviderModule) persistJITOwnershipJournalLocked() error {
	if strings.TrimSpace(m.config.StateDir) == "" {
		return nil
	}
	if m.journalDirectorySyncPending {
		if err := m.syncJITOwnershipJournalDirectory(); err != nil {
			return fmt.Errorf("retry journal directory sync: %w", err)
		}
		m.journalDirectorySyncPending = false
	}
	journal := jitOwnershipJournal{Version: jitOwnershipJournalVersion, Entries: make([]jitOwnershipJournalEntry, 0, len(m.pendingJIT))}
	for key, pending := range m.pendingJIT {
		state := "pending"
		tokenHash := hex.EncodeToString(pending.tokenHash[:])
		if pending.acknowledged {
			state = "owned"
			tokenHash = ""
		}
		if pending.deleting {
			state = "deleting"
			tokenHash = ""
		}
		journal.Entries = append(journal.Entries, jitOwnershipJournalEntry{
			Organization:      pending.organization,
			RunnerID:          key.runnerID,
			State:             state,
			TokenHash:         tokenHash,
			ExpiresAt:         pending.expiresAt.UTC(),
			CleanupAttempts:   pending.cleanupAttempts,
			LastCleanupStatus: pending.lastCleanupStatus,
			LastCleanupAt:     pending.lastCleanupAt.UTC(),
		})
	}
	sort.Slice(journal.Entries, func(i, j int) bool {
		if journal.Entries[i].Organization != journal.Entries[j].Organization {
			return journal.Entries[i].Organization < journal.Entries[j].Organization
		}
		return journal.Entries[i].RunnerID < journal.Entries[j].RunnerID
	})
	data, err := json.MarshalIndent(journal, "", "  ")
	if err != nil {
		return fmt.Errorf("encode journal: %w", err)
	}
	data = append(data, '\n')
	randomSuffix := make([]byte, 16)
	if _, err := rand.Read(randomSuffix); err != nil {
		return fmt.Errorf("generate journal staging name: %w", err)
	}
	temporaryName := ".jit-ownership-" + hex.EncodeToString(randomSuffix)
	temporary, err := m.stateRoot.OpenFile(temporaryName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create journal staging file: %w", err)
	}
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = m.stateRoot.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("protect journal staging file: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		return fmt.Errorf("write journal staging file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync journal staging file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close journal staging file: %w", err)
	}
	if err := m.stateRoot.Rename(temporaryName, jitOwnershipJournalName); err != nil {
		return fmt.Errorf("replace journal: %w", err)
	}
	committed = true
	if err := m.syncJITOwnershipJournalDirectory(); err != nil {
		m.journalDirectorySyncPending = true
		return fmt.Errorf("%w: %v", errJITJournalDurabilityUncertain, err)
	}
	return nil
}

func (m *githubRunnerProviderModule) syncJITOwnershipJournalDirectory() error {
	if m.journalDirectorySync != nil {
		return m.journalDirectorySync()
	}
	return syncJITOwnershipJournalDirectoryPlatform(m.stateRoot, m.config.StateDir)
}

func (m *githubRunnerProviderModule) ensureJITOwnershipJournalWritable() error {
	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	return m.persistJITOwnershipJournalLocked()
}

func (m *githubRunnerProviderModule) jitOwnershipJournalPath() string {
	return filepath.Join(m.config.StateDir, jitOwnershipJournalName)
}

func (m *githubRunnerProviderModule) scheduleJITOwnershipLocked(key pendingJITKey, pending *pendingJITOwnership) {
	if m.stopped {
		return
	}
	delay := time.Until(pending.expiresAt)
	if delay < 0 {
		delay = 0
	}
	pending.timerGeneration++
	generation := pending.timerGeneration
	m.cleanupWG.Add(1)
	pending.timer = time.AfterFunc(delay, func() {
		defer m.cleanupWG.Done()
		m.expirePendingJIT(key, pending, generation)
	})
}

func (m *githubRunnerProviderModule) stopJITOwnershipTimerLocked(pending *pendingJITOwnership) {
	if pending.timer == nil {
		pending.timerGeneration++
		return
	}
	if pending.timer.Stop() {
		m.cleanupWG.Done()
	}
	pending.timer = nil
	pending.timerGeneration++
}

func (m *githubRunnerProviderModule) effectiveJITOwnershipTTL() time.Duration {
	if m.jitOwnershipTTL > 0 {
		return m.jitOwnershipTTL
	}
	return defaultJITOwnershipTTL
}

func (m *githubRunnerProviderModule) effectiveJITOwnedTTL() time.Duration {
	if m.jitOwnedTTL > 0 {
		return m.jitOwnedTTL
	}
	return defaultJITOwnedTTL
}

func (m *githubRunnerProviderModule) effectiveJITRetryTTL() time.Duration {
	if m.jitRetryTTL > 0 {
		return m.jitRetryTTL
	}
	return defaultJITOwnershipRetryTTL
}
