package retainedprovider

import (
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const (
	ActiveStateProtocolVersion        = "retained-provider.active.v1"
	TransactionJournalProtocolVersion = "retained-provider.transaction.v1"
	StatusProtocolVersion             = "retained-provider.status.v1"
)

var (
	digestPattern   = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	imageRefPattern = regexp.MustCompile(`^localhost/[a-z0-9]+(?:[._/-][a-z0-9]+)*:sha256-[0-9a-f]{64}$`)
)

type VerifiedUpdate struct {
	WorkerID    string `json:"worker_id"`
	DirectiveID string `json:"directive_id"`
	CampaignID  string `json:"campaign_id"`
	Component   string `json:"component"`
	PluginID    string `json:"plugin_id"`
	ComponentID string `json:"component_id"`
	Version     string `json:"version"`
	Format      string `json:"format"`
	Path        string `json:"path"`
	SHA256      string `json:"sha256"`
}

func (update VerifiedUpdate) Validate() error {
	for field, value := range map[string]string{
		"worker_id": update.WorkerID, "directive_id": update.DirectiveID,
		"campaign_id": update.CampaignID, "component_id": update.ComponentID,
		"version": update.Version,
	} {
		if !safeIdentifierPattern.MatchString(value) {
			return fmt.Errorf("%s contains an unsafe identifier", field)
		}
	}
	if update.Component != "provider" {
		return fmt.Errorf("component must be provider")
	}
	if update.PluginID != GitHubPluginID {
		return fmt.Errorf("plugin_id must be %q", GitHubPluginID)
	}
	if update.Format != "binary" {
		return fmt.Errorf("format must be binary")
	}
	if !filepath.IsAbs(update.Path) || containsControl(update.Path) {
		return fmt.Errorf("path must be absolute and safe")
	}
	if !digestPattern.MatchString(update.SHA256) {
		return fmt.Errorf("sha256 must be a lowercase SHA-256 digest")
	}
	return nil
}

func (update VerifiedUpdate) validateConfigIdentity(config Config) error {
	if update.WorkerID != config.WorkerID || update.PluginID != config.PluginID || update.ComponentID != config.ComponentID {
		return fmt.Errorf("provider update identity does not match retained config")
	}
	return nil
}

type ImageSelection struct {
	Update      VerifiedUpdate `json:"update"`
	ImageID     string         `json:"image_id"`
	ImageRef    string         `json:"image_ref"`
	ActivatedAt time.Time      `json:"activated_at"`
}

func (selection ImageSelection) Validate() error {
	if err := selection.Update.Validate(); err != nil {
		return err
	}
	if !digestPattern.MatchString(selection.ImageID) {
		return fmt.Errorf("image_id must be an immutable SHA-256 digest")
	}
	digest := strings.TrimPrefix(selection.Update.SHA256, "sha256:")
	if !imageRefPattern.MatchString(selection.ImageRef) || !strings.HasSuffix(selection.ImageRef, ":sha256-"+digest) {
		return fmt.Errorf("image_ref must be a safe localhost reference derived from the update digest")
	}
	if selection.ActivatedAt.IsZero() {
		return fmt.Errorf("activated_at must be set")
	}
	return nil
}

type ActiveState struct {
	ProtocolVersion string          `json:"protocol_version"`
	Current         ImageSelection  `json:"current"`
	Previous        *ImageSelection `json:"previous,omitempty"`
	UpdatedAt       time.Time       `json:"updated_at"`
}

func (state ActiveState) Validate() error {
	if state.ProtocolVersion != ActiveStateProtocolVersion {
		return fmt.Errorf("protocol_version must be %q", ActiveStateProtocolVersion)
	}
	if err := state.Current.Validate(); err != nil {
		return fmt.Errorf("current: %w", err)
	}
	if state.Previous != nil {
		if err := state.Previous.Validate(); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
		if state.Previous.ImageID == state.Current.ImageID || state.Previous.ImageRef == state.Current.ImageRef {
			return fmt.Errorf("previous image must differ from current image")
		}
	}
	if state.UpdatedAt.IsZero() {
		return fmt.Errorf("updated_at must be set")
	}
	return nil
}

func (state ActiveState) ValidateForConfig(config Config) error {
	if err := state.Validate(); err != nil {
		return err
	}
	if err := state.Current.Update.validateConfigIdentity(config); err != nil {
		return fmt.Errorf("current: %w", err)
	}
	if state.Previous != nil {
		if err := state.Previous.Update.validateConfigIdentity(config); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
	}
	return nil
}

type JournalPhase string

const (
	JournalStaging           JournalPhase = "staging"
	JournalPrepared          JournalPhase = "prepared"
	JournalStatePromoting    JournalPhase = "state_promoting"
	JournalStateDetached     JournalPhase = "state_detached"
	JournalStatePromoted     JournalPhase = "state_promoted"
	JournalActivated         JournalPhase = "activated"
	JournalCommitted         JournalPhase = "committed"
	JournalRollbackRestoring JournalPhase = "rollback_restoring"
	JournalRollbackRestored  JournalPhase = "rollback_restored"
	JournalRollbackCleaned   JournalPhase = "rollback_cleaned"
)

type TransactionJournal struct {
	ProtocolVersion    string         `json:"protocol_version"`
	ID                 string         `json:"id"`
	Phase              JournalPhase   `json:"phase"`
	RollbackFrom       JournalPhase   `json:"rollback_from,omitempty"`
	DeferredCommit     bool           `json:"deferred_commit,omitempty"`
	RuntimeRepair      bool           `json:"runtime_repair,omitempty"`
	OuterTransactionID string         `json:"outer_transaction_id,omitempty"`
	ProfileID          string         `json:"profile_id,omitempty"`
	Previous           *ActiveState   `json:"previous,omitempty"`
	Candidate          ImageSelection `json:"candidate"`
	StartedAt          time.Time      `json:"started_at"`
	UpdatedAt          time.Time      `json:"updated_at"`
}

func (journal TransactionJournal) Validate() error {
	if journal.ProtocolVersion != TransactionJournalProtocolVersion {
		return fmt.Errorf("protocol_version must be %q", TransactionJournalProtocolVersion)
	}
	if !safeIdentifierPattern.MatchString(journal.ID) {
		return fmt.Errorf("id contains an unsafe identifier")
	}
	bound := journal.OuterTransactionID != "" || journal.ProfileID != ""
	if bound && (!journal.DeferredCommit || !safeIdentifierPattern.MatchString(journal.OuterTransactionID) || !safeIdentifierPattern.MatchString(journal.ProfileID)) {
		return fmt.Errorf("outer transaction binding is invalid")
	}
	rollbackPhase := isRollbackPhase(journal.Phase)
	switch journal.Phase {
	case JournalStaging, JournalPrepared, JournalStatePromoting, JournalStateDetached, JournalStatePromoted, JournalActivated, JournalCommitted,
		JournalRollbackRestoring, JournalRollbackRestored, JournalRollbackCleaned:
	default:
		return fmt.Errorf("phase is invalid")
	}
	if rollbackPhase {
		if !isRollbackOrigin(journal.RollbackFrom) {
			return fmt.Errorf("rollback_from must identify a non-terminal forward phase")
		}
	} else if journal.RollbackFrom != "" {
		return fmt.Errorf("forward phase must not contain rollback_from")
	}
	effectivePhase := journal.Phase
	if rollbackPhase {
		effectivePhase = journal.RollbackFrom
	}
	if journal.Previous != nil {
		if err := journal.Previous.Validate(); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
	}
	if effectivePhase == JournalStaging {
		if err := journal.Candidate.Update.Validate(); err != nil {
			return fmt.Errorf("candidate update: %w", err)
		}
		if journal.Candidate.ImageID != "" || journal.Candidate.ImageRef != "" || !journal.Candidate.ActivatedAt.IsZero() {
			return fmt.Errorf("staging candidate must not contain image activation state")
		}
	} else if err := journal.Candidate.Validate(); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if journal.Previous != nil {
		if journal.RuntimeRepair {
			if journal.Candidate.Update.SHA256 != journal.Previous.Current.Update.SHA256 {
				return fmt.Errorf("runtime repair candidate must match the active update")
			}
		} else if journal.Candidate.Update.SHA256 == journal.Previous.Current.Update.SHA256 {
			return fmt.Errorf("candidate update must differ from the active update")
		}
		if !journal.RuntimeRepair && effectivePhase != JournalStaging && (journal.Candidate.ImageID == journal.Previous.Current.ImageID || journal.Candidate.ImageRef == journal.Previous.Current.ImageRef) {
			return fmt.Errorf("candidate image must differ from the active image")
		}
	} else if journal.RuntimeRepair {
		return fmt.Errorf("runtime repair requires previous active state")
	}
	if journal.StartedAt.IsZero() || journal.UpdatedAt.IsZero() || journal.UpdatedAt.Before(journal.StartedAt) {
		return fmt.Errorf("journal timestamps are invalid")
	}
	return nil
}

func isRollbackPhase(phase JournalPhase) bool {
	switch phase {
	case JournalRollbackRestoring, JournalRollbackRestored, JournalRollbackCleaned:
		return true
	default:
		return false
	}
}

func isRollbackOrigin(phase JournalPhase) bool {
	switch phase {
	case JournalStaging, JournalPrepared, JournalStatePromoting, JournalStateDetached, JournalStatePromoted, JournalActivated:
		return true
	default:
		return false
	}
}

func (journal TransactionJournal) ValidateForConfig(config Config) error {
	if err := journal.Validate(); err != nil {
		return err
	}
	if err := journal.Candidate.Update.validateConfigIdentity(config); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if journal.Previous != nil {
		if err := journal.Previous.ValidateForConfig(config); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
	}
	if journal.ProfileID != "" && journal.ProfileID != config.ProfileID {
		return fmt.Errorf("profile identity does not match retained config")
	}
	return nil
}

func RecoverActiveState(journal TransactionJournal) (ActiveState, error) {
	if err := journal.Validate(); err != nil {
		return ActiveState{}, err
	}
	if journal.Phase != JournalCommitted {
		if journal.Previous == nil {
			return ActiveState{}, fmt.Errorf("cannot recover %s transaction without previous state", journal.Phase)
		}
		return *journal.Previous, nil
	}
	recovered := ActiveState{
		ProtocolVersion: ActiveStateProtocolVersion,
		Current:         journal.Candidate,
		UpdatedAt:       journal.UpdatedAt,
	}
	if journal.RuntimeRepair && journal.Previous != nil && journal.Previous.Previous != nil {
		previous := *journal.Previous.Previous
		recovered.Previous = &previous
	} else if !journal.RuntimeRepair && journal.Previous != nil {
		previous := journal.Previous.Current
		recovered.Previous = &previous
	}
	if err := recovered.Validate(); err != nil {
		return ActiveState{}, fmt.Errorf("recover committed transaction: %w", err)
	}
	return recovered, nil
}

// Status deliberately contains only redacted, local lifecycle observations.
type Status struct {
	ProtocolVersion string    `json:"protocol_version"`
	Installed       bool      `json:"installed"`
	ServiceActive   bool      `json:"service_active"`
	CurrentVersion  string    `json:"current_version,omitempty"`
	CurrentSHA256   string    `json:"current_sha256,omitempty"`
	ObservedAt      time.Time `json:"observed_at,omitempty"`
}

func WriteStatus(writer io.Writer, status Status) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(status)
}
