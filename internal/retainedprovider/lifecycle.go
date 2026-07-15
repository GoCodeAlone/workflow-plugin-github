package retainedprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	LifecycleJournalProtocolVersion = "retained-provider.lifecycle-transaction.v1"
	maxLifecycleSafetyEvents        = 16
	maxLifecycleDiagnosticEvents    = 33
)

type LifecycleOperation string

const (
	LifecycleInstall         LifecycleOperation = "install"
	LifecycleUninstall       LifecycleOperation = "uninstall"
	LifecycleRefresh         LifecycleOperation = "refresh"
	LifecycleRefreshRecovery LifecycleOperation = "refresh_recovery"
)

type LifecyclePhase string

const (
	LifecycleIntent    LifecyclePhase = "intent"
	LifecycleAdopting  LifecyclePhase = "adopting"
	LifecycleFencing   LifecyclePhase = "fencing"
	LifecycleFenced    LifecyclePhase = "fenced"
	LifecycleReady     LifecyclePhase = "ready"
	LifecycleReleasing LifecyclePhase = "releasing"
	LifecycleCommitted LifecyclePhase = "committed"
)

type LifecycleOutcome string

const (
	LifecycleCommit   LifecycleOutcome = "commit"
	LifecycleRollback LifecycleOutcome = "rollback"
)

type ProviderEffect string

const (
	ProviderChanged       ProviderEffect = "changed"
	ProviderUnchanged     ProviderEffect = "unchanged"
	ProviderNotApplicable ProviderEffect = "not_applicable"
)

type LifecycleIdentity struct {
	WorkerID    string `json:"worker_id"`
	ProfileID   string `json:"profile_id"`
	PluginID    string `json:"plugin_id"`
	ComponentID string `json:"component_id"`
}

func lifecycleIdentityFor(config Config) LifecycleIdentity {
	return LifecycleIdentity{
		WorkerID: config.WorkerID, ProfileID: config.ProfileID,
		PluginID: config.PluginID, ComponentID: config.ComponentID,
	}
}

func (identity LifecycleIdentity) Validate() error {
	for name, value := range map[string]string{
		"worker_id": identity.WorkerID, "profile_id": identity.ProfileID,
		"component_id": identity.ComponentID,
	} {
		if !safeIdentifierPattern.MatchString(value) {
			return fmt.Errorf("lifecycle identity %s is invalid", name)
		}
	}
	if identity.PluginID != GitHubPluginID {
		return errors.New("lifecycle identity plugin_id is invalid")
	}
	return nil
}

type LifecycleFileAttestation struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func (attestation LifecycleFileAttestation) Validate() error {
	if !filepath.IsAbs(attestation.Path) || containsControl(attestation.Path) {
		return errors.New("lifecycle attestation path is invalid")
	}
	if !digestPattern.MatchString(attestation.SHA256) {
		return errors.New("lifecycle attestation digest is invalid")
	}
	return nil
}

type LifecycleRecoveryAuthority struct {
	Config           Config                    `json:"config"`
	ComputeAgent     LifecycleFileAttestation  `json:"compute_agent"`
	SupervisorConfig LifecycleFileAttestation  `json:"supervisor_config"`
	Podman           LifecycleFileAttestation  `json:"podman"`
	Systemctl        LifecycleFileAttestation  `json:"systemctl"`
	Loginctl         LifecycleFileAttestation  `json:"loginctl"`
	AgentUnitBefore  LifecycleSystemdSignature `json:"agent_unit_before"`
}

type LifecycleSystemdSignature struct {
	Fragment         LifecycleFileAttestation   `json:"fragment"`
	DropIns          []LifecycleFileAttestation `json:"drop_ins,omitempty"`
	ExecStart        string                     `json:"exec_start"`
	EnvironmentFiles []LifecycleFileAttestation `json:"environment_files,omitempty"`
}

func (signature LifecycleSystemdSignature) Validate(home string) error {
	if err := signature.Fragment.Validate(); err != nil {
		return fmt.Errorf("validate lifecycle systemd fragment: %w", err)
	}
	if strings.TrimSpace(signature.ExecStart) == "" || len(signature.ExecStart) > 16*1024 || containsControl(signature.ExecStart) {
		return errors.New("lifecycle systemd ExecStart is invalid")
	}
	if len(signature.DropIns) > 64 || len(signature.EnvironmentFiles) > 64 {
		return errors.New("lifecycle systemd signature has too many inputs")
	}
	seen := map[string]struct{}{signature.Fragment.Path: {}}
	for label, attestations := range map[string][]LifecycleFileAttestation{
		"drop-in": signature.DropIns, "environment file": signature.EnvironmentFiles,
	} {
		for _, attestation := range attestations {
			if err := attestation.Validate(); err != nil {
				return fmt.Errorf("validate lifecycle systemd %s: %w", label, err)
			}
			if _, duplicate := seen[attestation.Path]; duplicate {
				return fmt.Errorf("lifecycle systemd signature contains a duplicate %s path", label)
			}
			seen[attestation.Path] = struct{}{}
		}
	}
	for path := range seen {
		if err := ValidateUserPath(home, path, false); err != nil {
			return fmt.Errorf("validate lifecycle systemd input path: %w", err)
		}
	}
	return nil
}

func (signature LifecycleSystemdSignature) Reattest() error {
	if err := reattestLifecycleFile("fragment", signature.Fragment); err != nil {
		return err
	}
	for _, attestation := range signature.DropIns {
		if err := reattestLifecycleFile("drop-in", attestation); err != nil {
			return err
		}
	}
	for _, attestation := range signature.EnvironmentFiles {
		if err := reattestLifecycleFile("environment file", attestation); err != nil {
			return err
		}
	}
	return nil
}

func reattestLifecycleFile(label string, attestation LifecycleFileAttestation) error {
	info, err := os.Lstat(attestation.Path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("lifecycle systemd %s is not a regular file", label)
	}
	if err := validateOwner(info); err != nil {
		return fmt.Errorf("lifecycle systemd %s ownership: %w", label, err)
	}
	digest, err := hashRegularFile(attestation.Path, false)
	if err != nil {
		return fmt.Errorf("re-attest lifecycle systemd %s: %w", label, err)
	}
	if digest != attestation.SHA256 {
		return fmt.Errorf("lifecycle systemd %s attestation mismatch", label)
	}
	return nil
}

func (authority LifecycleRecoveryAuthority) Validate(home string, identity LifecycleIdentity) error {
	if err := authority.Config.Validate(home); err != nil {
		return fmt.Errorf("validate lifecycle recovery config: %w", err)
	}
	if lifecycleIdentityFor(authority.Config) != identity {
		return errors.New("lifecycle recovery config identity mismatch")
	}
	if err := authority.ComputeAgent.Validate(); err != nil {
		return err
	}
	if authority.ComputeAgent.Path != authority.Config.ComputeAgentPath {
		return errors.New("lifecycle compute-agent attestation path mismatch")
	}
	if err := authority.SupervisorConfig.Validate(); err != nil {
		return err
	}
	if authority.SupervisorConfig.Path != authority.Config.SupervisorConfigPath {
		return errors.New("lifecycle supervisor config attestation path mismatch")
	}
	for _, executable := range []struct {
		label       string
		attestation LifecycleFileAttestation
		path        string
	}{
		{label: "podman", attestation: authority.Podman, path: authority.Config.PodmanPath},
		{label: "systemctl", attestation: authority.Systemctl, path: authority.Config.SystemctlPath},
		{label: "loginctl", attestation: authority.Loginctl, path: authority.Config.LoginctlPath},
	} {
		if err := executable.attestation.Validate(); err != nil {
			return fmt.Errorf("validate lifecycle %s attestation: %w", executable.label, err)
		}
		if executable.attestation.Path != executable.path {
			return fmt.Errorf("lifecycle %s attestation path mismatch", executable.label)
		}
	}
	if err := authority.AgentUnitBefore.Validate(home); err != nil {
		return err
	}
	return nil
}

type LifecycleProviderTransaction struct {
	TransactionID       string `json:"transaction_id"`
	ProfileID           string `json:"profile_id"`
	Digest              string `json:"digest"`
	LegacyJournalSHA256 string `json:"legacy_journal_sha256,omitempty"`
}

func (binding LifecycleProviderTransaction) Validate(identity LifecycleIdentity) error {
	if !safeIdentifierPattern.MatchString(binding.TransactionID) || binding.ProfileID != identity.ProfileID || !digestPattern.MatchString(binding.Digest) {
		return errors.New("lifecycle provider transaction binding is invalid")
	}
	if binding.LegacyJournalSHA256 != "" && !digestPattern.MatchString(binding.LegacyJournalSHA256) {
		return errors.New("lifecycle legacy provider journal binding is invalid")
	}
	return nil
}

type LifecycleUninstallPayload struct {
	Purge bool `json:"purge"`
}

type LifecycleUnchangedProvenance struct {
	Active        ImageSelection `json:"active"`
	Candidate     VerifiedUpdate `json:"candidate"`
	StableProbeAt time.Time      `json:"stable_probe_at,omitempty"`
}

type LifecycleManagedFileIntent struct {
	Path     string      `json:"path"`
	Present  bool        `json:"present"`
	Mode     os.FileMode `json:"mode,omitempty"`
	SHA256   string      `json:"sha256,omitempty"`
	Contents []byte      `json:"contents,omitempty"`
}

type lifecycleWiringExpectation uint8

const (
	lifecycleWiringMixed lifecycleWiringExpectation = iota + 1
	lifecycleWiringPre
	lifecycleWiringIntended
)

func (provenance LifecycleUnchangedProvenance) Validate(identity LifecycleIdentity, requireProbe bool) error {
	if err := provenance.Active.Validate(); err != nil {
		return fmt.Errorf("validate lifecycle unchanged active provenance: %w", err)
	}
	if err := provenance.Candidate.Validate(); err != nil {
		return fmt.Errorf("validate lifecycle unchanged candidate provenance: %w", err)
	}
	active := provenance.Active.Update
	if active.WorkerID != identity.WorkerID || active.PluginID != identity.PluginID || active.ComponentID != identity.ComponentID ||
		provenance.Candidate.WorkerID != identity.WorkerID || provenance.Candidate.PluginID != identity.PluginID || provenance.Candidate.ComponentID != identity.ComponentID {
		return errors.New("lifecycle unchanged provenance identity mismatch")
	}
	if active.SHA256 != provenance.Candidate.SHA256 {
		return errors.New("lifecycle unchanged provenance digest mismatch")
	}
	if requireProbe && provenance.StableProbeAt.IsZero() {
		return errors.New("lifecycle unchanged provenance requires a successful stable probe")
	}
	return nil
}

type LifecycleAuditKind string

const (
	AuditPhase    LifecycleAuditKind = "phase"
	AuditRecovery LifecycleAuditKind = "recovery"
	AuditError    LifecycleAuditKind = "error"
	AuditOverflow LifecycleAuditKind = "overflow"
)

type LifecycleAuditEvent struct {
	EventID        string             `json:"event_id"`
	Sequence       uint64             `json:"sequence"`
	Timestamp      time.Time          `json:"timestamp"`
	TransactionID  string             `json:"transaction_id"`
	WorkerID       string             `json:"worker_id"`
	Operation      LifecycleOperation `json:"operation"`
	Phase          LifecyclePhase     `json:"phase"`
	Kind           LifecycleAuditKind `json:"kind"`
	Outcome        LifecycleOutcome   `json:"outcome,omitempty"`
	ProviderEffect ProviderEffect     `json:"provider_effect,omitempty"`
	Purge          *bool              `json:"purge,omitempty"`
	Disposition    string             `json:"disposition,omitempty"`
	ErrorClass     string             `json:"error_class,omitempty"`
	Count          uint64             `json:"count,omitempty"`
	FirstSeen      time.Time          `json:"first_seen,omitempty"`
	LastSeen       time.Time          `json:"last_seen,omitempty"`
	Digest         string             `json:"digest,omitempty"`
	Offset         *int64             `json:"offset,omitempty"`
}

func (event LifecycleAuditEvent) Validate() error {
	if !safeIdentifierPattern.MatchString(event.EventID) || event.Sequence == 0 || event.Timestamp.IsZero() ||
		!safeIdentifierPattern.MatchString(event.TransactionID) || !safeIdentifierPattern.MatchString(event.WorkerID) {
		return errors.New("lifecycle audit event identity is invalid")
	}
	if !validLifecycleOperation(event.Operation) || !validLifecyclePhase(event.Phase) {
		return errors.New("lifecycle audit event operation or phase is invalid")
	}
	switch event.Kind {
	case AuditPhase:
		if (event.Outcome != "" && event.Outcome != LifecycleCommit && event.Outcome != LifecycleRollback) ||
			(event.ProviderEffect != ProviderChanged && event.ProviderEffect != ProviderUnchanged && event.ProviderEffect != ProviderNotApplicable) ||
			event.ErrorClass != "" || event.Disposition != "" || event.Count != 0 || !event.FirstSeen.IsZero() || !event.LastSeen.IsZero() {
			return errors.New("phase audit event outcome, provider effect, error_class, or diagnostic fields are invalid")
		}
		if event.Operation == LifecycleUninstall {
			if event.ProviderEffect != ProviderNotApplicable || event.Purge == nil {
				return errors.New("uninstall phase audit event requires purge intent")
			}
		} else if event.ProviderEffect == ProviderNotApplicable || event.Purge != nil {
			return errors.New("non-uninstall phase audit event has invalid provider effect or purge intent")
		}
	case AuditRecovery:
		if !safeIdentifierPattern.MatchString(event.Disposition) || event.Outcome != "" || event.ProviderEffect != "" || event.Purge != nil ||
			event.ErrorClass != "" || event.Count == 0 || event.FirstSeen.IsZero() || event.LastSeen.Before(event.FirstSeen) {
			return errors.New("recovery audit event disposition is invalid")
		}
	case AuditError, AuditOverflow:
		if !safeIdentifierPattern.MatchString(event.ErrorClass) || event.Count == 0 || event.FirstSeen.IsZero() || event.LastSeen.Before(event.FirstSeen) ||
			event.Outcome != "" || event.ProviderEffect != "" || event.Purge != nil || event.Disposition != "" {
			return errors.New("error audit event error_class or summary is invalid")
		}
		if event.Kind == AuditOverflow && event.ErrorClass != "other" {
			return errors.New("overflow audit event error_class must be other")
		}
	default:
		return errors.New("lifecycle audit event kind is invalid")
	}
	if event.Digest != "" && !digestPattern.MatchString(event.Digest) {
		return errors.New("lifecycle audit event digest is invalid")
	}
	if event.Offset != nil && *event.Offset < 0 {
		return errors.New("lifecycle audit event offset is invalid")
	}
	if (event.Digest == "") != (event.Offset == nil) {
		return errors.New("lifecycle audit event pending append metadata is incomplete")
	}
	return nil
}

type LifecycleAuditQueue struct {
	NextSequence uint64                `json:"next_sequence"`
	Safety       []LifecycleAuditEvent `json:"safety,omitempty"`
	Diagnostics  []LifecycleAuditEvent `json:"diagnostics,omitempty"`
}

func (queue *LifecycleAuditQueue) EnqueueDiagnostic(event LifecycleAuditEvent) error {
	if queue == nil {
		return errors.New("lifecycle audit queue is required")
	}
	if queue.NextSequence == 0 {
		queue.NextSequence = 1
	}
	for index := range queue.Diagnostics {
		current := &queue.Diagnostics[index]
		if current.Offset == nil && current.Kind == event.Kind && current.Phase == event.Phase && current.ErrorClass == event.ErrorClass {
			current.Count++
			current.LastSeen = event.Timestamp
			return current.Validate()
		}
	}
	if len(queue.Diagnostics) >= maxLifecycleDiagnosticEvents-1 {
		for index := range queue.Diagnostics {
			current := &queue.Diagnostics[index]
			if current.Offset == nil && current.Kind == AuditOverflow && current.ErrorClass == "other" {
				current.Count++
				current.LastSeen = event.Timestamp
				return current.Validate()
			}
		}
		if len(queue.Diagnostics) >= maxLifecycleDiagnosticEvents {
			return errors.New("lifecycle audit diagnostic queue is full")
		}
		event = LifecycleAuditEvent{
			EventID: event.EventID, Timestamp: event.Timestamp,
			TransactionID: event.TransactionID, WorkerID: event.WorkerID,
			Operation: event.Operation, Phase: event.Phase,
			Kind: AuditOverflow, ErrorClass: "other",
		}
	}
	event.Sequence = queue.NextSequence
	event.Count = 1
	event.FirstSeen = event.Timestamp
	event.LastSeen = event.Timestamp
	event.Outcome = ""
	event.ProviderEffect = ""
	if err := event.Validate(); err != nil {
		return err
	}
	queue.NextSequence++
	queue.Diagnostics = append(queue.Diagnostics, event)
	return nil
}

func (queue *LifecycleAuditQueue) EnqueueSafety(event LifecycleAuditEvent) error {
	if queue == nil {
		return errors.New("lifecycle audit queue is required")
	}
	if queue.NextSequence == 0 {
		queue.NextSequence = 1
	}
	if len(queue.Safety) >= maxLifecycleSafetyEvents {
		return errors.New("lifecycle audit safety queue is full")
	}
	event.Sequence = queue.NextSequence
	if err := event.Validate(); err != nil {
		return err
	}
	queue.NextSequence++
	queue.Safety = append(queue.Safety, event)
	return nil
}

func (queue LifecycleAuditQueue) Validate() error {
	if queue.NextSequence == 0 || len(queue.Safety) > maxLifecycleSafetyEvents || len(queue.Diagnostics) > maxLifecycleDiagnosticEvents {
		return errors.New("lifecycle audit queue bounds are invalid")
	}
	seen := map[uint64]struct{}{}
	for _, events := range [][]LifecycleAuditEvent{queue.Safety, queue.Diagnostics} {
		for _, event := range events {
			if err := event.Validate(); err != nil {
				return err
			}
			if _, duplicate := seen[event.Sequence]; duplicate || event.Sequence >= queue.NextSequence {
				return errors.New("lifecycle audit queue sequence is invalid")
			}
			seen[event.Sequence] = struct{}{}
		}
	}
	return nil
}

func lifecycleAuditPayload(event LifecycleAuditEvent) ([]byte, error) {
	event.Digest = ""
	event.Offset = nil
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("encode lifecycle audit event: %w", err)
	}
	return append(payload, '\n'), nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type LifecycleJournal struct {
	ProtocolVersion     string                        `json:"protocol_version"`
	TransactionID       string                        `json:"transaction_id"`
	Operation           LifecycleOperation            `json:"operation"`
	Phase               LifecyclePhase                `json:"phase"`
	Outcome             LifecycleOutcome              `json:"outcome,omitempty"`
	ProviderEffect      ProviderEffect                `json:"provider_effect"`
	Identity            LifecycleIdentity             `json:"identity"`
	Recovery            LifecycleRecoveryAuthority    `json:"recovery"`
	ProviderTransaction *LifecycleProviderTransaction `json:"provider_transaction,omitempty"`
	Unchanged           *LifecycleUnchangedProvenance `json:"unchanged,omitempty"`
	Uninstall           *LifecycleUninstallPayload    `json:"uninstall,omitempty"`
	Snapshots           []managedFileSnapshot         `json:"snapshots,omitempty"`
	WiringIntent        []LifecycleManagedFileIntent  `json:"wiring_intent,omitempty"`
	PreviousUnits       map[string]systemdUnitState   `json:"previous_units,omitempty"`
	Activation          systemdActivation             `json:"activation,omitempty"`
	AgentUnitIntended   *LifecycleSystemdSignature    `json:"agent_unit_intended,omitempty"`
	Audit               LifecycleAuditQueue           `json:"audit"`
	StartedAt           time.Time                     `json:"started_at"`
	UpdatedAt           time.Time                     `json:"updated_at"`
}

func (journal LifecycleJournal) Validate(home string, paths LifecyclePaths) error {
	if journal.ProtocolVersion != LifecycleJournalProtocolVersion || !safeIdentifierPattern.MatchString(journal.TransactionID) {
		return errors.New("lifecycle journal protocol or transaction id is invalid")
	}
	if !validLifecycleOperation(journal.Operation) || !validLifecyclePhase(journal.Phase) {
		return errors.New("lifecycle journal operation or phase is invalid")
	}
	if err := journal.Identity.Validate(); err != nil {
		return err
	}
	if err := journal.Recovery.Validate(home, journal.Identity); err != nil {
		return err
	}
	if LifecyclePathsFor(journal.Recovery.Config).Root != paths.Root || paths.LifecycleTransactionRoot(journal.TransactionID) == paths.Root {
		return errors.New("lifecycle journal path identity mismatch")
	}
	if err := journal.validateOperationEffect(); err != nil {
		return err
	}
	if err := journal.validateSnapshots(home, paths); err != nil {
		return err
	}
	if journal.ProviderTransaction != nil {
		if err := journal.ProviderTransaction.Validate(journal.Identity); err != nil {
			return err
		}
	}
	terminal := journal.Phase == LifecycleReady || journal.Phase == LifecycleReleasing || journal.Phase == LifecycleCommitted
	if terminal != (journal.Outcome != "") {
		return errors.New("lifecycle journal outcome is invalid for phase")
	}
	if journal.Outcome != "" && journal.Outcome != LifecycleCommit && journal.Outcome != LifecycleRollback {
		return errors.New("lifecycle journal outcome is invalid")
	}
	if terminal && journal.Outcome == LifecycleCommit && journal.ProviderEffect == ProviderChanged && journal.ProviderTransaction == nil {
		return errors.New("changed commit lifecycle requires provider transaction binding")
	}
	if journal.Outcome == LifecycleRollback && journal.ProviderTransaction != nil {
		return errors.New("rollback lifecycle provider transaction must be absent")
	}
	if journal.AgentUnitIntended != nil {
		if journal.Operation != LifecycleInstall && journal.Operation != LifecycleUninstall {
			return errors.New("refresh lifecycle must not contain an intended agent unit signature")
		}
		if err := journal.AgentUnitIntended.Validate(home); err != nil {
			return err
		}
	}
	if terminal && journal.Outcome == LifecycleCommit && (journal.Operation == LifecycleInstall || journal.Operation == LifecycleUninstall) && journal.AgentUnitIntended == nil {
		return errors.New("committed wiring lifecycle requires an intended agent unit signature")
	}
	if err := journal.Audit.Validate(); err != nil {
		return err
	}
	if len(journal.Audit.Safety)+journal.requiredSafetyReservation() > maxLifecycleSafetyEvents {
		return errors.New("lifecycle audit safety queue has insufficient reserved terminal capacity")
	}
	if journal.StartedAt.IsZero() || journal.UpdatedAt.Before(journal.StartedAt) {
		return errors.New("lifecycle journal timestamps are invalid")
	}
	return nil
}

func (journal LifecycleJournal) validateSnapshots(home string, paths LifecyclePaths) error {
	if journal.Operation != LifecycleInstall && journal.Operation != LifecycleUninstall {
		if len(journal.Snapshots) != 0 || len(journal.WiringIntent) != 0 || len(journal.PreviousUnits) != 0 || journal.Activation != (systemdActivation{}) {
			return errors.New("refresh lifecycle must not contain managed snapshots or systemd state")
		}
		return nil
	}
	allowedPaths := make(map[string]struct{}, len(managedInstallPaths(paths)))
	for _, path := range managedInstallPaths(paths) {
		allowedPaths[path] = struct{}{}
	}
	if len(journal.Snapshots) > len(allowedPaths) {
		return errors.New("lifecycle snapshots exceed managed paths")
	}
	transactionRoot := paths.LifecycleTransactionRoot(journal.TransactionID)
	snapshotRoot := filepath.Join(transactionRoot, "snapshots")
	seenPaths := make(map[string]struct{}, len(journal.Snapshots))
	seenBackups := make(map[string]struct{}, len(journal.Snapshots))
	for _, snapshot := range journal.Snapshots {
		if _, allowed := allowedPaths[snapshot.Path]; !allowed {
			return errors.New("lifecycle snapshots contain an unmanaged path")
		}
		if _, duplicate := seenPaths[snapshot.Path]; duplicate {
			return errors.New("lifecycle snapshots contain duplicate paths")
		}
		if _, duplicate := seenBackups[snapshot.Backup]; duplicate {
			return errors.New("lifecycle snapshots contain duplicate backups")
		}
		seenPaths[snapshot.Path] = struct{}{}
		seenBackups[snapshot.Backup] = struct{}{}
		if filepath.Dir(snapshot.Backup) != snapshotRoot {
			return errors.New("lifecycle snapshot must be inside the transaction root")
		}
		if snapshot.Existed {
			if snapshot.Mode != 0o600 && snapshot.Mode != 0o700 {
				return errors.New("lifecycle snapshot mode is invalid")
			}
			if !digestPattern.MatchString(snapshot.SHA256) {
				return errors.New("lifecycle snapshot digest is invalid")
			}
			requireBackup := journal.Phase != LifecycleCommitted
			if err := ValidateUserPath(home, snapshot.Backup, requireBackup); err != nil {
				return fmt.Errorf("validate lifecycle snapshot: %w", err)
			}
			info, err := os.Lstat(snapshot.Backup)
			if journal.Phase == LifecycleCommitted && errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != snapshot.Mode {
				return errors.New("lifecycle snapshot is not an owner-only regular file")
			}
			if err := validateOwner(info); err != nil {
				return fmt.Errorf("validate lifecycle snapshot owner: %w", err)
			}
			digest, err := hashRegularFile(snapshot.Backup, snapshot.Mode&0o100 != 0)
			if err != nil || digest != snapshot.SHA256 {
				return errors.New("lifecycle snapshot digest mismatch")
			}
		} else {
			if snapshot.Mode != 0 || snapshot.SHA256 != "" {
				return errors.New("absent lifecycle snapshot mode or digest is invalid")
			}
			if _, err := os.Lstat(snapshot.Backup); err == nil || !errors.Is(err, os.ErrNotExist) {
				return errors.New("absent lifecycle snapshot backup unexpectedly exists")
			}
		}
	}
	if err := journal.validateWiringIntent(paths); err != nil {
		return err
	}
	requiresComplete := journal.Phase == LifecycleFenced || journal.Outcome == LifecycleCommit
	if requiresComplete && len(journal.Snapshots) != len(allowedPaths) {
		return errors.New("fenced lifecycle requires complete snapshots")
	}
	if requiresComplete && len(journal.WiringIntent) != len(managedWiringPaths(paths)) {
		return errors.New("fenced lifecycle requires complete wiring intent")
	}
	for unit, state := range journal.PreviousUnits {
		if unit != providerServiceUnit && unit != refreshPathUnit && unit != refreshTimerUnit {
			return errors.New("lifecycle snapshots contain an unmanaged systemd unit")
		}
		if err := validateRestorableUnitState(state); err != nil {
			return fmt.Errorf("validate lifecycle previous unit %s: %w", unit, err)
		}
	}
	return nil
}

func (journal LifecycleJournal) validateWiringIntent(paths LifecyclePaths) error {
	allowed := make(map[string]struct{}, len(managedWiringPaths(paths)))
	for _, path := range managedWiringPaths(paths) {
		allowed[path] = struct{}{}
	}
	seen := make(map[string]struct{}, len(journal.WiringIntent))
	for _, intent := range journal.WiringIntent {
		if _, found := allowed[intent.Path]; !found {
			return errors.New("lifecycle wiring intent contains an unmanaged path")
		}
		if _, duplicate := seen[intent.Path]; duplicate {
			return errors.New("lifecycle wiring intent contains duplicate paths")
		}
		seen[intent.Path] = struct{}{}
		switch journal.Operation {
		case LifecycleInstall:
			if !intent.Present || intent.Mode != 0o600 || len(intent.Contents) == 0 || !digestPattern.MatchString(intent.SHA256) || digestBytes(intent.Contents) != intent.SHA256 {
				return errors.New("install lifecycle wiring intent digest or contents is invalid")
			}
		case LifecycleUninstall:
			if intent.Present || intent.Mode != 0 || intent.SHA256 != "" || len(intent.Contents) != 0 {
				return errors.New("uninstall lifecycle wiring intent must record absence")
			}
		}
	}
	return nil
}

func validateLifecycleWiringVector(journal LifecycleJournal, paths LifecyclePaths, expectation lifecycleWiringExpectation) error {
	if journal.Operation != LifecycleInstall && journal.Operation != LifecycleUninstall {
		if len(journal.WiringIntent) != 0 {
			return errors.New("refresh lifecycle unexpectedly contains wiring intent")
		}
		return nil
	}
	if expectation != lifecycleWiringMixed && expectation != lifecycleWiringPre && expectation != lifecycleWiringIntended {
		return errors.New("lifecycle wiring expectation is invalid")
	}
	snapshots := make(map[string]managedFileSnapshot, len(journal.Snapshots))
	for _, snapshot := range journal.Snapshots {
		snapshots[snapshot.Path] = snapshot
	}
	for _, intent := range journal.WiringIntent {
		snapshot, found := snapshots[intent.Path]
		if !found {
			return errors.New("lifecycle wiring path has no pre-state snapshot")
		}
		matchesPre, err := managedFileMatches(intent.Path, snapshot.Existed, snapshot.Mode, snapshot.SHA256)
		if err != nil {
			return fmt.Errorf("inspect lifecycle wiring pre-state: %w", err)
		}
		matchesIntended, err := managedFileMatches(intent.Path, intent.Present, intent.Mode, intent.SHA256)
		if err != nil {
			return fmt.Errorf("inspect lifecycle intended wiring: %w", err)
		}
		switch expectation {
		case lifecycleWiringMixed:
			if !matchesPre && !matchesIntended {
				return fmt.Errorf("lifecycle wiring %s matches neither pre-state nor intended state", intent.Path)
			}
		case lifecycleWiringPre:
			if !matchesPre {
				return fmt.Errorf("lifecycle wiring %s does not match pre-state", intent.Path)
			}
		case lifecycleWiringIntended:
			if !matchesIntended {
				return fmt.Errorf("lifecycle wiring %s does not match intended state", intent.Path)
			}
		}
	}
	return nil
}

func managedFileMatches(path string, present bool, mode os.FileMode, digest string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return !present, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, errors.New("managed lifecycle wiring must be a regular file")
	}
	if err := validateOwner(info); err != nil {
		return false, err
	}
	if !present || info.Mode().Perm() != mode {
		return false, nil
	}
	actual, err := hashRegularFile(path, mode&0o100 != 0)
	if err != nil {
		return false, err
	}
	return actual == digest, nil
}

func writeLifecycleJournal(home string, paths LifecyclePaths, journal LifecycleJournal) error {
	if err := validateLifecyclePathBoundary(home, paths); err != nil {
		return err
	}
	if err := journal.Validate(home, paths); err != nil {
		return err
	}
	if journal.Phase == LifecycleIntent {
		transactionRoot := paths.LifecycleTransactionRoot(journal.TransactionID)
		if err := mkdirAllDurable(transactionRoot, 0o700); err != nil {
			return fmt.Errorf("create lifecycle transaction root: %w", err)
		}
		if err := validateOwnedDirectory(transactionRoot); err != nil {
			return fmt.Errorf("validate lifecycle transaction root: %w", err)
		}
		if err := syncDirectory(paths.LifecycleTransactions); err != nil {
			return fmt.Errorf("sync lifecycle transactions: %w", err)
		}
	}
	if err := AtomicWriteJSON(paths.LifecycleJournal, journal); err != nil {
		return fmt.Errorf("write lifecycle journal: %w", err)
	}
	return nil
}

func readLifecycleJournal(home string, paths LifecyclePaths) (LifecycleJournal, bool, error) {
	if err := validateLifecyclePathBoundary(home, paths); err != nil {
		return LifecycleJournal{}, false, err
	}
	var journal LifecycleJournal
	if err := ReadStrictJSONFile(paths.LifecycleJournal, &journal); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LifecycleJournal{}, false, nil
		}
		return LifecycleJournal{}, false, err
	}
	if err := journal.Validate(home, paths); err != nil {
		return LifecycleJournal{}, false, err
	}
	return journal, true, nil
}

func drainLifecycleAudit(home string, paths LifecyclePaths, journal *LifecycleJournal) (returnErr error) {
	if journal == nil {
		return errors.New("lifecycle journal is required")
	}
	if err := validateLifecyclePathBoundary(home, paths); err != nil {
		return err
	}
	if err := mkdirAllDurable(filepath.Dir(paths.LifecycleAudit), 0o700); err != nil {
		return fmt.Errorf("create lifecycle audit directory: %w", err)
	}
	lock, err := AcquireInstallLock(paths.LifecycleAuditLock)
	if err != nil {
		return fmt.Errorf("acquire lifecycle audit lock: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()

	for len(journal.Audit.Safety) > 0 || len(journal.Audit.Diagnostics) > 0 {
		lane := &journal.Audit.Safety
		if len(*lane) == 0 || (len(journal.Audit.Diagnostics) > 0 && journal.Audit.Diagnostics[0].Sequence < (*lane)[0].Sequence) {
			lane = &journal.Audit.Diagnostics
		}
		event := (*lane)[0]
		payload, err := lifecycleAuditPayload(event)
		if err != nil {
			return err
		}
		file, err := openLifecycleAudit(paths.LifecycleAudit)
		if err != nil {
			return err
		}
		if event.Offset == nil {
			info, statErr := file.Stat()
			if statErr != nil {
				_ = file.Close()
				return fmt.Errorf("stat lifecycle audit: %w", statErr)
			}
			offset := info.Size()
			event.Offset = &offset
			event.Digest = digestBytes(payload)
			(*lane)[0] = event
			if err := writeLifecycleJournal(home, paths, *journal); err != nil {
				_ = file.Close()
				return err
			}
		}
		if event.Digest != digestBytes(payload) {
			_ = file.Close()
			return errors.New("lifecycle audit pending digest mismatch")
		}
		if err := validateLifecycleAuditOffset(file, *event.Offset); err != nil {
			_ = file.Close()
			return err
		}
		if _, err := file.Seek(*event.Offset, io.SeekStart); err != nil {
			_ = file.Close()
			return fmt.Errorf("seek lifecycle audit: %w", err)
		}
		tail, err := io.ReadAll(io.LimitReader(file, int64(len(payload)+1)))
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("read lifecycle audit tail: %w", err)
		}
		switch {
		case len(tail) == 0:
			if err := appendLifecycleAuditAt(file, *event.Offset, payload); err != nil {
				_ = file.Close()
				return err
			}
		case len(tail) < len(payload) && bytes.Equal(tail, payload[:len(tail)]):
			if err := appendLifecycleAuditAt(file, *event.Offset, payload); err != nil {
				_ = file.Close()
				return err
			}
		case len(tail) >= len(payload) && bytes.Equal(tail[:len(payload)], payload):
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return fmt.Errorf("sync completed lifecycle audit: %w", err)
			}
		default:
			_ = file.Close()
			return errors.New("lifecycle audit contains unrelated tail bytes")
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close lifecycle audit: %w", err)
		}
		*lane = append((*lane)[:0], (*lane)[1:]...)
		if err := writeLifecycleJournal(home, paths, *journal); err != nil {
			return err
		}
	}
	return nil
}

func openLifecycleAudit(path string) (*os.File, error) {
	return openLifecycleAuditWith(path, openNoFollowFile)
}

func openLifecycleAuditWith(path string, opener func(string, fs.FileMode) (*os.File, error)) (*os.File, error) {
	if opener == nil {
		return nil, errors.New("lifecycle audit opener is required")
	}
	if err := rejectNonRegularDestination(path); err != nil {
		return nil, err
	}
	file, err := opener(path, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lifecycle audit: %w", err)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		return nil, errors.New("lifecycle audit must be an owner-only regular file")
	}
	if err := validateOwner(info); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lifecycle audit ownership: %w", err)
	}
	return file, nil
}

func appendLifecycleAuditAt(file *os.File, offset int64, payload []byte) error {
	if err := validateLifecycleAuditOffset(file, offset); err != nil {
		return err
	}
	written, err := file.WriteAt(payload, offset)
	if err != nil {
		return fmt.Errorf("append lifecycle audit: %w", err)
	}
	if written != len(payload) {
		return io.ErrShortWrite
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync lifecycle audit: %w", err)
	}
	stored := make([]byte, len(payload))
	read, err := file.ReadAt(stored, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("verify lifecycle audit append: %w", err)
	}
	if read != len(payload) || !bytes.Equal(stored, payload) {
		return errors.New("lifecycle audit append verification failed")
	}
	return nil
}

func validateLifecycleAuditOffset(file *os.File, offset int64) error {
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat lifecycle audit offset: %w", err)
	}
	if info.Size() < offset {
		return errors.New("lifecycle audit is shorter than pending offset")
	}
	return nil
}

func (journal LifecycleJournal) validateOperationEffect() error {
	switch journal.Operation {
	case LifecycleUninstall:
		if journal.ProviderEffect != ProviderNotApplicable || journal.Uninstall == nil {
			return errors.New("uninstall lifecycle requires not_applicable provider effect and payload")
		}
	case LifecycleInstall, LifecycleRefresh, LifecycleRefreshRecovery:
		if journal.ProviderEffect != ProviderChanged && journal.ProviderEffect != ProviderUnchanged {
			return errors.New("install or refresh lifecycle rejects not_applicable provider effect")
		}
		if journal.Uninstall != nil {
			return errors.New("non-uninstall lifecycle contains uninstall payload")
		}
	default:
		return errors.New("lifecycle operation is invalid")
	}
	if journal.ProviderEffect != ProviderChanged && journal.ProviderTransaction != nil {
		return errors.New("unchanged or not_applicable lifecycle must not bind provider transaction")
	}
	if journal.ProviderEffect == ProviderUnchanged {
		if journal.Unchanged == nil {
			return errors.New("unchanged lifecycle requires unchanged provenance")
		}
		requireProbe := journal.Outcome == LifecycleCommit
		if err := journal.Unchanged.Validate(journal.Identity, requireProbe); err != nil {
			return err
		}
	} else if journal.Unchanged != nil {
		return errors.New("non-unchanged lifecycle must not contain unchanged provenance")
	}
	return nil
}

func (journal LifecycleJournal) requiredSafetyReservation() int {
	switch journal.Phase {
	case LifecycleIntent, LifecycleAdopting:
		return 5
	case LifecycleFencing:
		return 4
	case LifecycleFenced:
		return 3
	case LifecycleReady:
		return 2
	case LifecycleReleasing:
		return 1
	case LifecycleCommitted:
		return 0
	default:
		return maxLifecycleSafetyEvents + 1
	}
}

func validLifecycleOperation(operation LifecycleOperation) bool {
	return operation == LifecycleInstall || operation == LifecycleUninstall || operation == LifecycleRefresh || operation == LifecycleRefreshRecovery
}

func validLifecyclePhase(phase LifecyclePhase) bool {
	switch phase {
	case LifecycleIntent, LifecycleAdopting, LifecycleFencing, LifecycleFenced, LifecycleReady, LifecycleReleasing, LifecycleCommitted:
		return true
	default:
		return false
	}
}

func lifecycleHome(paths LifecyclePaths) string {
	return filepath.Dir(filepath.Dir(paths.Root))
}

func validateLifecyclePathBoundary(home string, paths LifecyclePaths) error {
	for name, path := range map[string]string{
		"journal":      paths.LifecycleJournal,
		"transactions": paths.LifecycleTransactions,
		"audit":        paths.LifecycleAudit,
		"audit lock":   paths.LifecycleAuditLock,
	} {
		if err := ValidateUserPath(home, path, false); err != nil {
			return fmt.Errorf("lifecycle %s path: %w", strings.TrimSpace(name), err)
		}
	}
	return nil
}

func (authority LifecycleRecoveryAuthority) Reattest() error {
	computeAgentDigest, err := hashRegularFile(authority.ComputeAgent.Path, true)
	if err != nil {
		return fmt.Errorf("re-attest lifecycle compute-agent: %w", err)
	}
	if computeAgentDigest != authority.ComputeAgent.SHA256 {
		return errors.New("lifecycle compute-agent attestation mismatch")
	}
	supervisorDigest, err := hashRegularFile(authority.SupervisorConfig.Path, false)
	if err != nil {
		return fmt.Errorf("re-attest lifecycle supervisor config: %w", err)
	}
	if supervisorDigest != authority.SupervisorConfig.SHA256 {
		return errors.New("lifecycle supervisor config attestation mismatch")
	}
	for _, executable := range []struct {
		label       string
		attestation LifecycleFileAttestation
	}{
		{label: "podman", attestation: authority.Podman},
		{label: "systemctl", attestation: authority.Systemctl},
		{label: "loginctl", attestation: authority.Loginctl},
	} {
		digest, err := hashHostExecutable(executable.attestation.Path)
		if err != nil {
			return fmt.Errorf("re-attest lifecycle %s: %w", executable.label, err)
		}
		if digest != executable.attestation.SHA256 {
			return fmt.Errorf("lifecycle %s attestation mismatch", executable.label)
		}
	}
	return nil
}

func lifecycleMaintenanceIdentity(journal LifecycleJournal) (id, reason string, err error) {
	if !safeIdentifierPattern.MatchString(journal.TransactionID) {
		return "", "", errors.New("lifecycle transaction has no maintenance identity")
	}
	switch journal.Operation {
	case LifecycleInstall:
		return journal.TransactionID, installMaintenanceReason, nil
	case LifecycleUninstall:
		return journal.TransactionID, uninstallMaintenanceReason, nil
	case LifecycleRefresh, LifecycleRefreshRecovery:
		return journal.TransactionID, refreshMaintenanceReason, nil
	default:
		return "", "", errors.New("lifecycle operation has no maintenance identity")
	}
}

func writeLifecycleTransition(home string, paths LifecyclePaths, journal *LifecycleJournal, phase LifecyclePhase, outcome LifecycleOutcome, now time.Time) error {
	if journal == nil {
		return errors.New("lifecycle journal is required")
	}
	next := *journal
	next.Phase = phase
	next.Outcome = outcome
	next.UpdatedAt = now.UTC()
	event := LifecycleAuditEvent{
		EventID: "event-" + fmt.Sprint(next.Audit.NextSequence), Timestamp: next.UpdatedAt,
		TransactionID: next.TransactionID, WorkerID: next.Identity.WorkerID,
		Operation: next.Operation, Phase: phase, Kind: AuditPhase,
		Outcome: outcome, ProviderEffect: next.ProviderEffect,
	}
	if next.Uninstall != nil {
		purge := next.Uninstall.Purge
		event.Purge = &purge
	}
	if err := next.Audit.EnqueueSafety(event); err != nil {
		return fmt.Errorf("enqueue lifecycle phase audit: %w", err)
	}
	if err := writeLifecycleJournal(home, paths, next); err != nil {
		return err
	}
	*journal = next
	return nil
}

func writeLifecycleDiagnostic(home string, paths LifecyclePaths, journal *LifecycleJournal, kind LifecycleAuditKind, value string, now time.Time) error {
	if journal == nil {
		return errors.New("lifecycle journal is required")
	}
	event := LifecycleAuditEvent{
		EventID: "event-" + fmt.Sprint(journal.Audit.NextSequence), Timestamp: now.UTC(),
		TransactionID: journal.TransactionID, WorkerID: journal.Identity.WorkerID,
		Operation: journal.Operation, Phase: journal.Phase, Kind: kind,
	}
	switch kind {
	case AuditError:
		event.ErrorClass = value
	case AuditRecovery:
		event.Disposition = value
	default:
		return errors.New("unsupported lifecycle diagnostic kind")
	}
	if err := journal.Audit.EnqueueDiagnostic(event); err != nil {
		return fmt.Errorf("enqueue lifecycle diagnostic audit: %w", err)
	}
	journal.UpdatedAt = now.UTC()
	if err := writeLifecycleJournal(home, paths, *journal); err != nil {
		return err
	}
	_ = drainLifecycleAudit(home, paths, journal)
	return nil
}

func newLifecycleJournal(config Config, operation LifecycleOperation, effect ProviderEffect, uninstall *LifecycleUninstallPayload, now time.Time) (LifecycleJournal, error) {
	home := lifecycleHome(LifecyclePathsFor(config))
	if err := config.Validate(home); err != nil {
		return LifecycleJournal{}, err
	}
	computeAgentDigest, err := hashRegularFile(config.ComputeAgentPath, true)
	if err != nil {
		return LifecycleJournal{}, fmt.Errorf("attest lifecycle compute-agent: %w", err)
	}
	supervisorDigest, err := hashRegularFile(config.SupervisorConfigPath, false)
	if err != nil {
		return LifecycleJournal{}, fmt.Errorf("attest lifecycle supervisor config: %w", err)
	}
	podmanDigest, err := hashHostExecutable(config.PodmanPath)
	if err != nil {
		return LifecycleJournal{}, fmt.Errorf("attest lifecycle podman: %w", err)
	}
	systemctlDigest, err := hashHostExecutable(config.SystemctlPath)
	if err != nil {
		return LifecycleJournal{}, fmt.Errorf("attest lifecycle systemctl: %w", err)
	}
	loginctlDigest, err := hashHostExecutable(config.LoginctlPath)
	if err != nil {
		return LifecycleJournal{}, fmt.Errorf("attest lifecycle loginctl: %w", err)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	identity := lifecycleIdentityFor(config)
	seed := strings.Join([]string{string(operation), identity.WorkerID, identity.ProfileID, now.UTC().Format(time.RFC3339Nano)}, "\x00")
	transactionDigest := sha256.Sum256([]byte(seed))
	return LifecycleJournal{
		ProtocolVersion: LifecycleJournalProtocolVersion,
		TransactionID:   string(operation) + "-" + hex.EncodeToString(transactionDigest[:8]),
		Operation:       operation,
		Phase:           LifecycleIntent,
		ProviderEffect:  effect,
		Identity:        identity,
		Recovery: LifecycleRecoveryAuthority{
			Config:           config,
			ComputeAgent:     LifecycleFileAttestation{Path: config.ComputeAgentPath, SHA256: computeAgentDigest},
			SupervisorConfig: LifecycleFileAttestation{Path: config.SupervisorConfigPath, SHA256: supervisorDigest},
			Podman:           LifecycleFileAttestation{Path: config.PodmanPath, SHA256: podmanDigest},
			Systemctl:        LifecycleFileAttestation{Path: config.SystemctlPath, SHA256: systemctlDigest},
			Loginctl:         LifecycleFileAttestation{Path: config.LoginctlPath, SHA256: loginctlDigest},
		},
		Uninstall: uninstall,
		Audit:     LifecycleAuditQueue{NextSequence: 1},
		StartedAt: now.UTC(), UpdatedAt: now.UTC(),
	}, nil
}

func startLifecycleTransaction(home string, paths LifecyclePaths, journal *LifecycleJournal) error {
	if journal == nil {
		return errors.New("lifecycle journal is required")
	}
	event := LifecycleAuditEvent{
		EventID: "event-" + fmt.Sprint(journal.Audit.NextSequence), Timestamp: journal.UpdatedAt,
		TransactionID: journal.TransactionID, WorkerID: journal.Identity.WorkerID,
		Operation: journal.Operation, Phase: journal.Phase, Kind: AuditPhase,
		ProviderEffect: journal.ProviderEffect,
	}
	if journal.Uninstall != nil {
		purge := journal.Uninstall.Purge
		event.Purge = &purge
	}
	if err := journal.Audit.EnqueueSafety(event); err != nil {
		return fmt.Errorf("enqueue lifecycle intent audit: %w", err)
	}
	if err := writeLifecycleJournal(home, paths, *journal); err != nil {
		return err
	}
	if err := drainLifecycleAudit(home, paths, journal); err != nil {
		return fmt.Errorf("drain lifecycle intent audit: %w", err)
	}
	return nil
}

func (installer Installer) recoverLifecycleTransaction(ctx context.Context, home string, paths LifecyclePaths, refresher Refresher) error {
	journal, found, err := readLifecycleJournal(home, paths)
	if err != nil {
		return fmt.Errorf("read retained provider lifecycle transaction: %w", err)
	}
	if !found {
		if _, legacyOuterFound, legacyErr := readInstallTransactionJournal(paths); legacyErr != nil {
			return fmt.Errorf("read legacy retained provider outer transaction: %w", legacyErr)
		} else if legacyOuterFound {
			return nil
		}
		return installer.adoptLegacyProviderTransaction(ctx, home, paths, refresher, nil, "")
	}
	if err := writeLifecycleDiagnostic(home, paths, &journal, AuditRecovery, "resume_"+string(journal.Phase), installer.now()); err != nil {
		return fmt.Errorf("record lifecycle recovery disposition: %w", err)
	}
	if err := journal.Recovery.Reattest(); err != nil {
		return err
	}
	if err := validateLifecycleProviderMatrix(paths, journal); err != nil {
		return err
	}
	if err := installer.validateLifecycleAgentUnit(ctx, home, journal); err != nil {
		return err
	}
	switch journal.Phase {
	case LifecycleIntent:
		return finishLifecycleTransaction(home, paths, &journal)
	case LifecycleFencing:
		return installer.recoverLifecycleFencing(ctx, home, paths, &journal)
	case LifecycleFenced:
		return installer.recoverLifecycleFenced(ctx, home, paths, &journal, refresher)
	case LifecycleReady, LifecycleReleasing:
		return installer.recoverLifecycleRelease(ctx, home, paths, &journal, refresher)
	case LifecycleCommitted:
		return finalizeLifecycleTransaction(home, paths, &journal, refresher)
	case LifecycleAdopting:
		return installer.recoverLifecycleAdopting(ctx, home, paths, &journal, refresher)
	default:
		return errors.New("lifecycle recovery phase is invalid")
	}
}

func (installer Installer) adoptLegacyProviderTransaction(ctx context.Context, home string, paths LifecyclePaths, refresher Refresher, trusted *Config, confirmation string) error {
	inner, found, err := readTransactionJournal(paths.Journal)
	if err != nil {
		return fmt.Errorf("read legacy provider transaction: %w", err)
	}
	if !found {
		return nil
	}
	if inner.OuterTransactionID != "" || inner.ProfileID != "" {
		return errors.New("bound provider transaction has no matching outer lifecycle journal")
	}
	if confirmation != "" && confirmation != inner.ID {
		return errors.New("legacy provider transaction confirmation does not match")
	}
	var config Config
	if trusted == nil {
		config, err = ReadConfigFile(paths.ConfigFile, home)
		if err != nil {
			return fmt.Errorf("automatic legacy recovery requires installed config; run retained recover with trusted config and exact transaction confirmation: %w", err)
		}
	} else {
		config = *trusted
		if err := config.Validate(home); err != nil {
			return fmt.Errorf("validate trusted legacy recovery config: %w", err)
		}
	}
	if LifecyclePathsFor(config).Root != paths.Root {
		return errors.New("legacy recovery config does not identify this provider root")
	}
	update := inner.Candidate.Update
	if update.WorkerID != config.WorkerID || update.PluginID != config.PluginID || update.ComponentID != config.ComponentID {
		return errors.New("legacy provider transaction identity does not match recovery config")
	}
	journal, err := newLifecycleJournal(config, LifecycleRefreshRecovery, ProviderChanged, nil, installer.now())
	if err != nil {
		return err
	}
	signature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return err
	}
	journal.Recovery.AgentUnitBefore = signature
	legacyDigest, err := hashRegularFile(paths.Journal, false)
	if err != nil {
		return fmt.Errorf("hash legacy provider transaction: %w", err)
	}
	journal.ProviderTransaction = &LifecycleProviderTransaction{
		TransactionID: inner.ID, ProfileID: config.ProfileID, Digest: update.SHA256, LegacyJournalSHA256: legacyDigest,
	}
	if err := startLifecycleTransaction(home, paths, &journal); err != nil {
		return err
	}
	if err := writeLifecycleTransition(home, paths, &journal, LifecycleAdopting, "", installer.now()); err != nil {
		return err
	}
	return installer.recoverLifecycleAdopting(ctx, home, paths, &journal, refresher)
}

func (installer Installer) recoverLifecycleAdopting(ctx context.Context, home string, paths LifecyclePaths, journal *LifecycleJournal, refresher Refresher) error {
	if journal.Operation != LifecycleRefreshRecovery || journal.ProviderTransaction == nil || journal.ProviderTransaction.LegacyJournalSHA256 == "" {
		return errors.New("adopting lifecycle is not bound to a legacy provider transaction")
	}
	if err := validateLifecycleProviderMatrix(paths, *journal); err != nil {
		return err
	}
	id, reason, err := lifecycleMaintenanceIdentity(*journal)
	if err != nil {
		return err
	}
	maintenance, err := installer.beginMaintenance(ctx, journal.Recovery.Config, id, reason)
	if err != nil {
		return fmt.Errorf("establish legacy recovery maintenance fence: %w", err)
	}
	if err := installer.waitLocalStateAfter(ctx, journal.Recovery.Config, "unavailable", maintenance.StartedAt); err != nil {
		return fmt.Errorf("drain legacy recovery maintenance fence: %w", err)
	}
	if err := writeLifecycleTransition(home, paths, journal, LifecycleFenced, "", installer.now()); err != nil {
		return err
	}
	return installer.recoverLifecycleFenced(ctx, home, paths, journal, refresher)
}

func (installer Installer) validateLifecycleAgentUnit(ctx context.Context, home string, journal LifecycleJournal) error {
	current, err := installer.inspectAgentUnitSignature(ctx, home, journal.Recovery.Config)
	if err != nil {
		return err
	}
	before := journal.Recovery.AgentUnitBefore
	if journal.Operation == LifecycleRefresh || journal.Operation == LifecycleRefreshRecovery {
		if !equalLifecycleSystemdSignature(current, before) {
			return errors.New("effective retained agent unit does not match the recorded pre-signature")
		}
		return nil
	}
	switch journal.Phase {
	case LifecycleFenced:
		if equalLifecycleSystemdSignature(current, before) || journal.AgentUnitIntended != nil && equalLifecycleSystemdSignature(current, *journal.AgentUnitIntended) {
			return nil
		}
		return errors.New("effective retained agent unit matches neither pre nor intended signature")
	case LifecycleReady, LifecycleReleasing, LifecycleCommitted:
		expected := before
		if journal.Outcome == LifecycleCommit {
			if journal.AgentUnitIntended == nil {
				return errors.New("committed lifecycle has no intended agent unit signature")
			}
			expected = *journal.AgentUnitIntended
		}
		if !equalLifecycleSystemdSignature(current, expected) {
			return errors.New("effective retained agent unit does not match the terminal signature")
		}
		return nil
	default:
		if !equalLifecycleSystemdSignature(current, before) {
			return errors.New("effective retained agent unit does not match the recorded pre-signature")
		}
		return nil
	}
}

func equalLifecycleSystemdSignature(left, right LifecycleSystemdSignature) bool {
	if left.Fragment != right.Fragment || left.ExecStart != right.ExecStart || len(left.DropIns) != len(right.DropIns) || len(left.EnvironmentFiles) != len(right.EnvironmentFiles) {
		return false
	}
	for index := range left.DropIns {
		if left.DropIns[index] != right.DropIns[index] {
			return false
		}
	}
	for index := range left.EnvironmentFiles {
		if left.EnvironmentFiles[index] != right.EnvironmentFiles[index] {
			return false
		}
	}
	return true
}

func deriveLifecycleAgentUnitSignature(before LifecycleSystemdSignature, paths LifecyclePaths, wiring []LifecycleManagedFileIntent, agentEnvironment *LifecycleFileAttestation) (LifecycleSystemdSignature, error) {
	var dropInIntent *LifecycleManagedFileIntent
	for index := range wiring {
		if wiring[index].Path == paths.AgentDropIn {
			dropInIntent = &wiring[index]
			break
		}
	}
	if dropInIntent == nil {
		return LifecycleSystemdSignature{}, errors.New("lifecycle wiring has no retained agent drop-in intent")
	}
	if dropInIntent.Present != (agentEnvironment != nil) {
		return LifecycleSystemdSignature{}, errors.New("lifecycle agent drop-in and environment intent disagree")
	}
	if agentEnvironment != nil {
		if err := agentEnvironment.Validate(); err != nil || agentEnvironment.Path != paths.AgentEnv {
			return LifecycleSystemdSignature{}, errors.New("lifecycle agent environment intent is invalid")
		}
	}
	intended := before
	intended.DropIns = replaceLifecycleAttestation(before.DropIns, paths.AgentDropIn, nil)
	intended.EnvironmentFiles = replaceLifecycleAttestation(before.EnvironmentFiles, paths.AgentEnv, nil)
	if dropInIntent.Present {
		dropIn := LifecycleFileAttestation{Path: paths.AgentDropIn, SHA256: dropInIntent.SHA256}
		intended.DropIns = append(intended.DropIns, dropIn)
		intended.EnvironmentFiles = append(intended.EnvironmentFiles, *agentEnvironment)
	}
	sort.Slice(intended.DropIns, func(left, right int) bool { return intended.DropIns[left].Path < intended.DropIns[right].Path })
	sort.Slice(intended.EnvironmentFiles, func(left, right int) bool {
		return intended.EnvironmentFiles[left].Path < intended.EnvironmentFiles[right].Path
	})
	if err := intended.Validate(lifecycleHome(paths)); err != nil {
		return LifecycleSystemdSignature{}, err
	}
	return intended, nil
}

func replaceLifecycleAttestation(attestations []LifecycleFileAttestation, path string, replacement *LifecycleFileAttestation) []LifecycleFileAttestation {
	result := make([]LifecycleFileAttestation, 0, len(attestations)+1)
	for _, attestation := range attestations {
		if attestation.Path != path {
			result = append(result, attestation)
		}
	}
	if replacement != nil {
		result = append(result, *replacement)
	}
	return result
}

func (installer Installer) recoverLifecycleFencing(ctx context.Context, home string, paths LifecyclePaths, journal *LifecycleJournal) error {
	id, reason, err := lifecycleMaintenanceIdentity(*journal)
	if err != nil {
		return err
	}
	config := journal.Recovery.Config
	maintenance, err := installer.beginMaintenance(ctx, config, id, reason)
	if err != nil {
		return fmt.Errorf("establish lifecycle maintenance fence: %w", err)
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", maintenance.StartedAt); err != nil {
		return fmt.Errorf("drain lifecycle maintenance fence: %w", err)
	}
	journal.ProviderTransaction = nil
	if err := writeLifecycleTransition(home, paths, journal, LifecycleReady, LifecycleRollback, installer.now()); err != nil {
		return err
	}
	return installer.recoverLifecycleRelease(ctx, home, paths, journal, Refresher{Runner: installer.Runner, Now: installer.Now, Sleep: installer.Sleep})
}

func (installer Installer) recoverLifecycleFenced(ctx context.Context, home string, paths LifecyclePaths, journal *LifecycleJournal, refresher Refresher) error {
	id, reason, err := lifecycleMaintenanceIdentity(*journal)
	if err != nil {
		return err
	}
	config := journal.Recovery.Config
	maintenance, err := installer.beginMaintenance(ctx, config, id, reason)
	if err != nil {
		return fmt.Errorf("re-establish fenced lifecycle maintenance: %w", err)
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", maintenance.StartedAt); err != nil {
		return fmt.Errorf("re-drain fenced lifecycle maintenance: %w", err)
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, *journal); err != nil {
		return err
	}
	if err := validateLifecycleProviderMatrix(paths, *journal); err != nil {
		return err
	}
	if err := validateLifecycleWiringVector(*journal, paths, lifecycleWiringMixed); err != nil {
		return err
	}
	if err := installer.systemctl(ctx, config.SystemctlPath, "stop", config.AgentUnit); err != nil {
		return fmt.Errorf("stop fenced lifecycle agent: %w", err)
	}
	inner, found, err := readTransactionJournalForConfig(paths.Journal, config)
	if err != nil {
		return fmt.Errorf("read fenced provider transaction: %w", err)
	}
	if found {
		activeChanged := false
		if active, activeFound, activeErr := readActiveStateForConfig(paths.ActiveState, config); activeErr != nil {
			return fmt.Errorf("read fenced provider active state: %w", activeErr)
		} else if activeFound {
			activeChanged = active.Current.ImageID == inner.Candidate.ImageID && active.Current.ImageRef == inner.Candidate.ImageRef
		}
		rollbackJournal := inner
		if rollbackJournal.Phase == JournalCommitted {
			rollbackJournal.Phase = JournalActivated
			activeChanged = true
		}
		if err := refresher.rollback(ctx, config, paths, rollbackJournal, activeChanged); err != nil {
			return fmt.Errorf("rollback fenced provider transaction: %w", err)
		}
	}
	if _, remains, err := readTransactionJournal(paths.Journal); err != nil {
		return fmt.Errorf("verify fenced provider rollback: %w", err)
	} else if remains {
		return errors.New("fenced provider transaction remains after rollback")
	}
	if journal.Operation == LifecycleRefresh && journal.ProviderEffect == ProviderUnchanged {
		if journal.Unchanged == nil {
			return errors.New("fenced unchanged refresh has no provider provenance")
		}
		if err := installer.systemctl(ctx, config.SystemctlPath, "restart", providerServiceUnit); err != nil {
			return fmt.Errorf("restart provider during fenced TLS recovery: %w", err)
		}
		if err := refresher.probeStableActive(ctx, config, paths, journal.Unchanged.Active); err != nil {
			return fmt.Errorf("probe provider during fenced TLS recovery: %w", err)
		}
		journal.Unchanged.StableProbeAt = installer.now()
		journal.UpdatedAt = installer.now()
		if err := writeLifecycleJournal(home, paths, *journal); err != nil {
			return fmt.Errorf("record recovered provider TLS readiness: %w", err)
		}
	}
	agentRestartedAfter := installer.now()
	if len(journal.Snapshots) > 0 || len(journal.PreviousUnits) > 0 || journal.Activation != (systemdActivation{}) {
		if err := installer.rollbackInstallBeforeStart(ctx, config, journal.Snapshots, journal.PreviousUnits, true, false, id, reason, journal.Activation, func(recoveryContext context.Context) error {
			return installer.reattestLifecycleAuthority(recoveryContext, home, *journal)
		}); err != nil {
			return fmt.Errorf("rollback fenced lifecycle wiring: %w", err)
		}
		if err := validateLifecycleWiringVector(*journal, paths, lifecycleWiringPre); err != nil {
			return fmt.Errorf("verify rolled back lifecycle wiring: %w", err)
		}
	} else {
		if err := installer.reattestLifecycleAuthority(ctx, home, *journal); err != nil {
			return err
		}
		if err := installer.systemctl(ctx, config.SystemctlPath, "start", config.AgentUnit); err != nil {
			return fmt.Errorf("restart fenced lifecycle agent: %w", err)
		}
	}
	if err := installer.waitLocalStateAfter(ctx, config, "unavailable", agentRestartedAfter); err != nil {
		return fmt.Errorf("observe restarted fenced lifecycle agent: %w", err)
	}
	journal.ProviderTransaction = nil
	if err := writeLifecycleTransition(home, paths, journal, LifecycleReady, LifecycleRollback, installer.now()); err != nil {
		return err
	}
	return installer.recoverLifecycleRelease(ctx, home, paths, journal, refresher)
}

func validateLifecycleProviderMatrix(paths LifecyclePaths, outer LifecycleJournal) error {
	inner, found, err := readTransactionJournalForConfig(paths.Journal, outer.Recovery.Config)
	if err != nil {
		return fmt.Errorf("read lifecycle provider transaction: %w", err)
	}
	legacyRecovery := outer.Operation == LifecycleRefreshRecovery && (outer.Phase == LifecycleAdopting || outer.Phase == LifecycleFenced)
	if legacyRecovery {
		if !found || outer.ProviderTransaction == nil || outer.ProviderTransaction.LegacyJournalSHA256 == "" {
			return errors.New("legacy recovery lifecycle requires an exact provider transaction binding")
		}
		binding := outer.ProviderTransaction
		if inner.ID != binding.TransactionID || inner.OuterTransactionID != "" || inner.ProfileID != "" || inner.Candidate.Update.SHA256 != binding.Digest {
			return errors.New("legacy provider transaction binding mismatch")
		}
		update := inner.Candidate.Update
		if update.WorkerID != outer.Identity.WorkerID || update.PluginID != outer.Identity.PluginID || update.ComponentID != outer.Identity.ComponentID {
			return errors.New("legacy provider transaction identity mismatch")
		}
		digest, err := hashRegularFile(paths.Journal, false)
		if err != nil || digest != binding.LegacyJournalSHA256 {
			return errors.New("legacy provider transaction journal hash mismatch")
		}
		return nil
	}
	requiresAbsent := outer.ProviderEffect != ProviderChanged || outer.Outcome == LifecycleRollback ||
		outer.Phase == LifecycleIntent || outer.Phase == LifecycleFencing || outer.Operation == LifecycleUninstall
	if requiresAbsent {
		if found {
			return errors.New("lifecycle phase requires an absent provider transaction")
		}
		return nil
	}
	if !found {
		if outer.Phase == LifecycleFenced || outer.Phase == LifecycleCommitted {
			return nil
		}
		return errors.New("lifecycle changed commit requires a provider transaction")
	}
	if outer.ProviderTransaction == nil {
		if outer.Phase == LifecycleFenced && inner.DeferredCommit &&
			inner.OuterTransactionID == outer.TransactionID && inner.ProfileID == outer.Identity.ProfileID {
			update := inner.Candidate.Update
			if update.WorkerID != outer.Identity.WorkerID || update.PluginID != outer.Identity.PluginID || update.ComponentID != outer.Identity.ComponentID {
				return errors.New("reciprocally bound provider transaction identity mismatch")
			}
			return nil
		}
		return errors.New("lifecycle changed provider transaction binding is absent")
	}
	binding := outer.ProviderTransaction
	if binding.LegacyJournalSHA256 != "" {
		return errors.New("non-legacy lifecycle contains a legacy provider journal binding")
	}
	if inner.ID != binding.TransactionID || inner.OuterTransactionID != outer.TransactionID {
		return errors.New("lifecycle provider outer transaction binding mismatch")
	}
	if inner.ProfileID != binding.ProfileID || inner.ProfileID != outer.Identity.ProfileID {
		return errors.New("lifecycle provider profile binding mismatch")
	}
	if inner.Candidate.Update.SHA256 != binding.Digest {
		return errors.New("lifecycle provider digest binding mismatch")
	}
	update := inner.Candidate.Update
	if update.WorkerID != outer.Identity.WorkerID || update.PluginID != outer.Identity.PluginID || update.ComponentID != outer.Identity.ComponentID {
		return errors.New("lifecycle provider candidate identity mismatch")
	}
	if !inner.DeferredCommit {
		return errors.New("lifecycle provider transaction is not deferred")
	}
	if (outer.Phase == LifecycleReady || outer.Phase == LifecycleReleasing) && outer.Outcome == LifecycleCommit && inner.Phase != JournalCommitted {
		return errors.New("lifecycle ready changed provider transaction is not committed")
	}
	return nil
}

func (installer Installer) recoverLifecycleRelease(ctx context.Context, home string, paths LifecyclePaths, journal *LifecycleJournal, refresher Refresher) error {
	id, reason, err := lifecycleMaintenanceIdentity(*journal)
	if err != nil {
		return err
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, *journal); err != nil {
		return err
	}
	if len(journal.WiringIntent) > 0 {
		expectation := lifecycleWiringPre
		if journal.Outcome == LifecycleCommit {
			expectation = lifecycleWiringIntended
		}
		if err := validateLifecycleWiringVector(*journal, paths, expectation); err != nil {
			return err
		}
	}
	if journal.Phase == LifecycleReady {
		if err := writeLifecycleTransition(home, paths, journal, LifecycleReleasing, journal.Outcome, installer.now()); err != nil {
			return err
		}
		_ = drainLifecycleAudit(home, paths, journal)
	}
	state, err := installer.maintenanceStatus(ctx, journal.Recovery.Config)
	if err != nil {
		return fmt.Errorf("read lifecycle maintenance status: %w", err)
	}
	switch classifyMaintenanceState(state, journal.Identity.ProfileID, id, reason) {
	case maintenanceExactActive:
		if err := installer.waitLocalDrainedAfter(ctx, journal.Recovery.Config, state.Maintenance.StartedAt); err != nil {
			return fmt.Errorf("wait for lifecycle maintenance drain: %w", err)
		}
		if err := installer.releaseLifecycleMaintenance(ctx, home, *journal); err != nil {
			return fmt.Errorf("release lifecycle maintenance: %w", err)
		}
	case maintenanceInactive:
	case maintenanceConflicting:
		return errors.New("lifecycle maintenance status has a conflicting active transaction")
	default:
		return errors.New("lifecycle maintenance status is invalid")
	}
	if err := writeLifecycleTransition(home, paths, journal, LifecycleCommitted, journal.Outcome, installer.now()); err != nil {
		return err
	}
	return finalizeLifecycleTransaction(home, paths, journal, refresher)
}

func (installer Installer) reattestLifecycleAuthority(ctx context.Context, home string, journal LifecycleJournal) error {
	if err := journal.Recovery.Reattest(); err != nil {
		return err
	}
	return installer.validateLifecycleAgentUnit(ctx, home, journal)
}

func finalizeLifecycleTransaction(home string, paths LifecyclePaths, journal *LifecycleJournal, refresher Refresher) error {
	if journal.Phase != LifecycleCommitted {
		return errors.New("lifecycle transaction is not committed")
	}
	if journal.Outcome == LifecycleCommit && journal.ProviderEffect == ProviderChanged {
		if _, found, err := readTransactionJournalForConfig(paths.Journal, journal.Recovery.Config); err != nil {
			return fmt.Errorf("read committed provider transaction: %w", err)
		} else if found {
			if err := refresher.finalizeDeferredRefresh(journal.Recovery.Config); err != nil {
				return fmt.Errorf("finalize committed provider rollback target: %w", err)
			}
		}
	}
	if journal.Outcome == LifecycleCommit && journal.Operation == LifecycleUninstall && journal.Uninstall != nil && journal.Uninstall.Purge {
		if info, err := os.Lstat(paths.Root); err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("purged lifecycle root is not a real directory")
			}
			if err := validateOwner(info); err != nil {
				return fmt.Errorf("validate purged lifecycle root owner: %w", err)
			}
			if err := os.RemoveAll(paths.Root); err != nil {
				return fmt.Errorf("purge retained provider state: %w", err)
			}
			if err := syncDirectory(filepath.Dir(paths.Root)); err != nil {
				return fmt.Errorf("sync retained provider purge: %w", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect retained provider purge root: %w", err)
		}
	}
	return finishLifecycleTransaction(home, paths, journal)
}

func finishLifecycleTransaction(home string, paths LifecyclePaths, journal *LifecycleJournal) error {
	if err := drainLifecycleAudit(home, paths, journal); err != nil {
		return fmt.Errorf("drain lifecycle audit before cleanup: %w", err)
	}
	transactionRoot := paths.LifecycleTransactionRoot(journal.TransactionID)
	if info, err := os.Lstat(transactionRoot); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("lifecycle transaction root is not a real directory")
		}
		if err := validateOwner(info); err != nil {
			return fmt.Errorf("validate lifecycle transaction root owner: %w", err)
		}
		if err := os.RemoveAll(transactionRoot); err != nil {
			return fmt.Errorf("remove lifecycle transaction root: %w", err)
		}
		if err := syncDirectory(paths.LifecycleTransactions); err != nil {
			return fmt.Errorf("sync lifecycle transactions after cleanup: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect lifecycle transaction root: %w", err)
	}
	if err := removeDurableFile(paths.LifecycleJournal); err != nil {
		return fmt.Errorf("remove lifecycle journal: %w", err)
	}
	return nil
}
