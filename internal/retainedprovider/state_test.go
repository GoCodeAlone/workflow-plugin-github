package retainedprovider

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfigDecodeAndValidation(t *testing.T) {
	home := t.TempDir()
	valid := validTestConfig(home)
	data, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	got, err := DecodeConfig(bytes.NewReader(data), home)
	if err != nil {
		t.Fatalf("decode config: %v", err)
	}
	if got.WorkerID != valid.WorkerID || got.ComponentID != valid.ComponentID || got.RefreshIntervalSeconds != 300 {
		t.Fatalf("decoded config = %+v", got)
	}

	unknown := append(append([]byte(nil), bytes.TrimSuffix(data, []byte("}"))...), []byte(`,"unexpected":true}`)...)
	if _, err := DecodeConfig(bytes.NewReader(unknown), home); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown config field err = %v", err)
	}
	if _, err := DecodeConfig(strings.NewReader(string(data)+"\n{}"), home); err == nil || !strings.Contains(err.Error(), "multiple JSON") {
		t.Fatalf("multiple config values err = %v", err)
	}
	oversized := append(bytes.Repeat([]byte(" "), maxConfigBytes+1), data...)
	if _, err := DecodeConfig(bytes.NewReader(oversized), home); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized config err = %v", err)
	}
}

func TestConfigRejectsUnsafeIdentityAndPaths(t *testing.T) {
	home := t.TempDir()
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "wrong protocol", mutate: func(c *Config) { c.ProtocolVersion = "retained.v0" }, want: "protocol_version"},
		{name: "unsafe worker", mutate: func(c *Config) { c.WorkerID = "../worker" }, want: "worker_id"},
		{name: "unsafe profile", mutate: func(c *Config) { c.ProfileID = "profile\nnext" }, want: "profile_id"},
		{name: "wrong plugin", mutate: func(c *Config) { c.PluginID = "other" }, want: "plugin_id"},
		{name: "unsafe component", mutate: func(c *Config) { c.ComponentID = "component;rm" }, want: "component_id"},
		{name: "unsafe unit", mutate: func(c *Config) { c.AgentUnit = "agent.service\nEnvironment=TOKEN" }, want: "agent_unit"},
		{name: "relative install root", mutate: func(c *Config) { c.InstallRoot = "relative" }, want: "install_root"},
		{name: "outside home", mutate: func(c *Config) { c.SystemdDir = filepath.Join(filepath.Dir(home), "outside") }, want: "systemd_dir"},
		{name: "plaintext provider URL", mutate: func(c *Config) { c.ProviderURL = "http://provider:18090" }, want: "provider_url"},
		{name: "wrong network", mutate: func(c *Config) { c.ContainerNetwork = "host" }, want: "container_network"},
		{name: "short ref", mutate: func(c *Config) { c.Ref = "main" }, want: "ref"},
		{name: "fast timer", mutate: func(c *Config) { c.RefreshIntervalSeconds = 10 }, want: "refresh_interval_seconds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validTestConfig(home)
			tc.mutate(&cfg)
			if err := cfg.Validate(home); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate err = %v want %q", err, tc.want)
			}
		})
	}
}

func TestActiveStateAndVerifiedUpdateValidation(t *testing.T) {
	now := time.Now().UTC()
	selection := validTestSelection(now)
	state := ActiveState{
		ProtocolVersion: ActiveStateProtocolVersion,
		Current:         selection,
		UpdatedAt:       now,
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("valid active state: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*ActiveState)
		want   string
	}{
		{name: "worker", mutate: func(s *ActiveState) { s.Current.Update.WorkerID = "" }, want: "worker_id"},
		{name: "plugin", mutate: func(s *ActiveState) { s.Current.Update.PluginID = "other" }, want: "plugin_id"},
		{name: "component", mutate: func(s *ActiveState) { s.Current.Update.Component = "plugin" }, want: "component"},
		{name: "digest", mutate: func(s *ActiveState) { s.Current.Update.SHA256 = "sha256:bad" }, want: "sha256"},
		{name: "image id", mutate: func(s *ActiveState) { s.Current.ImageID = "latest" }, want: "image_id"},
		{name: "image ref digest", mutate: func(s *ActiveState) { s.Current.ImageRef = "localhost/provider:sha256-cccccccccccc" }, want: "image_ref"},
		{name: "duplicate previous", mutate: func(s *ActiveState) { previous := s.Current; s.Previous = &previous }, want: "previous"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := state
			tc.mutate(&candidate)
			if err := candidate.Validate(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate err = %v want %q", err, tc.want)
			}
		})
	}
}

func TestActiveStateRetainsDistinctCurrentAndPriorImages(t *testing.T) {
	now := time.Now().UTC()
	current := validTestSelection(now)
	previous := validTestSelection(now.Add(-time.Hour))
	previous.ImageID = "sha256:" + strings.Repeat("c", 64)
	previous.ImageRef = "localhost/workflow-plugin-github-runner-provider:sha256-dddddddddddd"
	previous.Update.DirectiveID = "directive-prior"
	previous.Update.SHA256 = "sha256:" + strings.Repeat("d", 64)
	state := ActiveState{
		ProtocolVersion: ActiveStateProtocolVersion,
		Current:         current,
		Previous:        &previous,
		UpdatedAt:       now,
	}
	if err := state.Validate(); err != nil {
		t.Fatalf("valid current/prior state: %v", err)
	}
	if state.Current.ImageID == state.Previous.ImageID || state.Current.ImageRef == state.Previous.ImageRef {
		t.Fatalf("current and prior images were not retained distinctly: %+v", state)
	}
}

func TestRecoverySelectionForEveryJournalPhase(t *testing.T) {
	now := time.Now().UTC()
	previous := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: validTestSelection(now.Add(-time.Hour)), UpdatedAt: now.Add(-time.Hour)}
	candidate := validTestSelection(now)
	candidate.ImageID = "sha256:" + strings.Repeat("c", 64)
	candidate.ImageRef = "localhost/workflow-plugin-github-runner-provider:sha256-dddddddddddd"
	candidate.Update.SHA256 = "sha256:" + strings.Repeat("d", 64)
	candidate.Update.DirectiveID = "directive-new"

	for _, tc := range []struct {
		phase JournalPhase
		want  ImageSelection
	}{
		{phase: JournalPrepared, want: previous.Current},
		{phase: JournalActivated, want: previous.Current},
		{phase: JournalCommitted, want: candidate},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "txn-1",
				Phase:           tc.phase,
				Previous:        &previous,
				Candidate:       candidate,
				StartedAt:       now,
				UpdatedAt:       now,
			}
			if err := journal.Validate(); err != nil {
				t.Fatalf("valid journal: %v", err)
			}
			got, err := RecoverActiveState(journal)
			if err != nil {
				t.Fatalf("recover: %v", err)
			}
			if got.Current.ImageID != tc.want.ImageID || got.Current.Update.DirectiveID != tc.want.Update.DirectiveID {
				t.Fatalf("recovered state = %+v want selection %+v", got, tc.want)
			}
		})
	}
}

func TestJournalRejectsCandidateMatchingActiveImage(t *testing.T) {
	now := time.Now().UTC()
	previous := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: validTestSelection(now.Add(-time.Hour)), UpdatedAt: now.Add(-time.Hour)}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "txn-duplicate",
		Phase:           JournalPrepared,
		Previous:        &previous,
		Candidate:       previous.Current,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := journal.Validate(); err == nil || !strings.Contains(err.Error(), "candidate") {
		t.Fatalf("duplicate candidate err = %v", err)
	}
}

func TestStatusContainsNoCredentialFields(t *testing.T) {
	status := Status{
		ProtocolVersion: StatusProtocolVersion,
		Installed:       true,
		ServiceActive:   true,
		CurrentVersion:  "v1.0.32",
		CurrentSHA256:   "sha256:" + strings.Repeat("a", 64),
		ObservedAt:      time.Now().UTC(),
	}
	data, err := json.Marshal(status)
	if err != nil {
		t.Fatalf("marshal status: %v", err)
	}
	text := string(data)
	for _, forbidden := range []string{"github_token", "provider_token", "credential", "secret", "ca_cert"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("status contains credential-shaped field %q: %s", forbidden, text)
		}
	}
}

func validTestConfig(home string) Config {
	root := filepath.Join(home, ".workflow-compute", "github-runner-provider")
	return Config{
		ProtocolVersion:        ConfigProtocolVersion,
		WorkerID:               "github-runner-linux-stg",
		ProfileID:              "github-runner-linux-stg",
		PluginID:               GitHubPluginID,
		ComponentID:            "github-runner-provider-sidecar",
		ComputeAgentPath:       filepath.Join(home, ".workflow-compute", "agent-core-bin", "github-runner-linux-stg", "compute-agent"),
		SupervisorConfigPath:   filepath.Join(home, ".workflow-compute", "github-runner-linux-stg", "supervisor.pb"),
		LocalStatusPath:        filepath.Join(home, ".workflow-compute", "github-runner-linux-stg", "agent-status.json"),
		InstallRoot:            root,
		SystemdDir:             filepath.Join(home, ".config", "systemd", "user"),
		AgentUnit:              "workflow-compute-github-runner-linux-stg.service",
		PodmanPath:             "/usr/bin/podman",
		ProviderURL:            "https://workflow-plugin-github-runner-provider:18090",
		StableContainer:        "workflow-plugin-github-runner-provider",
		CandidateContainer:     "workflow-plugin-github-runner-provider-candidate",
		ContainerNetwork:       "bridge",
		Organization:           "GoCodeAlone",
		Repository:             "GoCodeAlone/workflow-compute",
		Workflow:               "dogfood-provider-target.yml",
		Ref:                    strings.Repeat("a", 40),
		RunnerName:             "wfc-stg-ghp-linux-probe",
		RunnerGroup:            "ephemeral",
		Labels:                 []string{"self-hosted", "linux", "wfc-ghp-stg"},
		RefreshIntervalSeconds: 300,
	}
}

func validTestSelection(now time.Time) ImageSelection {
	return ImageSelection{
		Update: VerifiedUpdate{
			WorkerID:    "github-runner-linux-stg-supervisor",
			DirectiveID: "directive-1",
			CampaignID:  "campaign-1",
			Component:   "provider",
			PluginID:    GitHubPluginID,
			ComponentID: "github-runner-provider-sidecar",
			Version:     "v1.0.32",
			Format:      "binary",
			Path:        "/home/runner/.workflow-compute/updates/.candidate-provider",
			SHA256:      "sha256:" + strings.Repeat("a", 64),
		},
		ImageID:     "sha256:" + strings.Repeat("b", 64),
		ImageRef:    "localhost/workflow-plugin-github-runner-provider:sha256-aaaaaaaaaaaa",
		ActivatedAt: now,
	}
}
