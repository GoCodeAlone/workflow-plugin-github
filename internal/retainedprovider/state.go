package retainedprovider

import (
	"fmt"
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
	imageRefPattern = regexp.MustCompile(`^localhost/[a-z0-9]+(?:[._/-][a-z0-9]+)*:sha256-[0-9a-f]{12,64}$`)
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
	if !imageRefPattern.MatchString(selection.ImageRef) || !strings.HasSuffix(selection.ImageRef, ":sha256-"+digest[:12]) {
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

type JournalPhase string

const (
	JournalPrepared  JournalPhase = "prepared"
	JournalActivated JournalPhase = "activated"
	JournalCommitted JournalPhase = "committed"
)

type TransactionJournal struct {
	ProtocolVersion string         `json:"protocol_version"`
	ID              string         `json:"id"`
	Phase           JournalPhase   `json:"phase"`
	Previous        *ActiveState   `json:"previous,omitempty"`
	Candidate       ImageSelection `json:"candidate"`
	StartedAt       time.Time      `json:"started_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

func (journal TransactionJournal) Validate() error {
	if journal.ProtocolVersion != TransactionJournalProtocolVersion {
		return fmt.Errorf("protocol_version must be %q", TransactionJournalProtocolVersion)
	}
	if !safeIdentifierPattern.MatchString(journal.ID) {
		return fmt.Errorf("id contains an unsafe identifier")
	}
	switch journal.Phase {
	case JournalPrepared, JournalActivated, JournalCommitted:
	default:
		return fmt.Errorf("phase is invalid")
	}
	if journal.Previous != nil {
		if err := journal.Previous.Validate(); err != nil {
			return fmt.Errorf("previous: %w", err)
		}
	}
	if err := journal.Candidate.Validate(); err != nil {
		return fmt.Errorf("candidate: %w", err)
	}
	if journal.Previous != nil && (journal.Candidate.ImageID == journal.Previous.Current.ImageID || journal.Candidate.ImageRef == journal.Previous.Current.ImageRef) {
		return fmt.Errorf("candidate image must differ from the active image")
	}
	if journal.StartedAt.IsZero() || journal.UpdatedAt.IsZero() || journal.UpdatedAt.Before(journal.StartedAt) {
		return fmt.Errorf("journal timestamps are invalid")
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
	if journal.Previous != nil {
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
