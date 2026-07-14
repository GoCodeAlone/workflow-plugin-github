package retainedprovider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestProviderTransactionRequiresExactOuterBinding(t *testing.T) {
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := TransactionJournal{
		ProtocolVersion:    TransactionJournalProtocolVersion,
		ID:                 "refresh-transaction-123",
		Phase:              JournalCommitted,
		DeferredCommit:     true,
		OuterTransactionID: "install-transaction-123",
		ProfileID:          "github-runner-profile-stg",
		Candidate:          validTestSelection(now),
		StartedAt:          now,
		UpdatedAt:          now,
	}
	if err := journal.Validate(); err != nil {
		t.Fatalf("valid bound provider transaction: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*TransactionJournal)
	}{
		{name: "missing outer id", mutate: func(candidate *TransactionJournal) { candidate.OuterTransactionID = "" }},
		{name: "missing profile", mutate: func(candidate *TransactionJournal) { candidate.ProfileID = "" }},
		{name: "unsafe outer id", mutate: func(candidate *TransactionJournal) { candidate.OuterTransactionID = "../other" }},
		{name: "binding without deferred commit", mutate: func(candidate *TransactionJournal) { candidate.DeferredCommit = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := journal
			tc.mutate(&candidate)
			if err := candidate.Validate(); err == nil || !strings.Contains(err.Error(), "outer transaction binding") {
				t.Fatalf("Validate = %v", err)
			}
		})
	}
}

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

func TestConfigDecodesExplicitHostToolPaths(t *testing.T) {
	home := t.TempDir()
	fields := map[string]json.RawMessage{}
	encoded, err := json.Marshal(validTestConfig(home))
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("decode config fields: %v", err)
	}
	fields["systemctl_path"], _ = json.Marshal("/usr/bin/systemctl")
	fields["loginctl_path"], _ = json.Marshal("/usr/bin/loginctl")
	encoded, err = json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal explicit tool config: %v", err)
	}
	config, err := DecodeConfig(bytes.NewReader(encoded), home)
	if err != nil {
		t.Fatalf("decode explicit host tools: %v", err)
	}
	if config.SystemctlPath != "/usr/bin/systemctl" || config.LoginctlPath != "/usr/bin/loginctl" {
		t.Fatalf("decoded host tools = systemctl:%q loginctl:%q", config.SystemctlPath, config.LoginctlPath)
	}
}

func TestLifecycleAuditPathDoesNotDependOnAmbientStateHome(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "interactive-state"))
	interactive := LifecyclePathsFor(config)
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, "systemd-state"))
	systemd := LifecyclePathsFor(config)

	want := filepath.Join(home, ".local", "state", "wfctl", "plugins", GitHubPluginID, "retained-provider-audit.jsonl")
	if interactive.LifecycleAudit != want || systemd.LifecycleAudit != want {
		t.Fatalf("audit path changed across environments: interactive=%q systemd=%q want=%q", interactive.LifecycleAudit, systemd.LifecycleAudit, want)
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
		{name: "shared workflow compute root", mutate: func(c *Config) { c.InstallRoot = filepath.Join(home, ".workflow-compute") }, want: "dedicated provider root"},
		{name: "systemd directory as install root", mutate: func(c *Config) { c.InstallRoot = c.SystemdDir }, want: "dedicated provider root"},
		{name: "arbitrary provider root", mutate: func(c *Config) { c.InstallRoot = filepath.Join(home, "provider") }, want: "dedicated provider root"},
		{name: "outside home", mutate: func(c *Config) { c.SystemdDir = filepath.Join(filepath.Dir(home), "outside") }, want: "systemd_dir"},
		{name: "authority aliases managed file", mutate: func(c *Config) { c.SupervisorConfigPath = LifecyclePathsFor(*c).AgentDropIn }, want: "managed provider path"},
		{name: "authority paths alias", mutate: func(c *Config) { c.LocalStatusPath = c.SupervisorConfigPath }, want: "distinct"},
		{name: "agent inside install root", mutate: func(c *Config) { c.ComputeAgentPath = filepath.Join(c.InstallRoot, "compute-agent") }, want: "outside install_root"},
		{name: "systemd inside install root", mutate: func(c *Config) { c.SystemdDir = filepath.Join(c.InstallRoot, "systemd") }, want: "outside install_root"},
		{name: "podman wrapper", mutate: func(c *Config) { c.PodmanPath = filepath.Join(home, "podman-wrapper") }, want: "podman_path"},
		{name: "systemctl wrapper", mutate: func(c *Config) { c.SystemctlPath = filepath.Join(home, "systemctl-wrapper") }, want: "systemctl_path"},
		{name: "loginctl wrapper", mutate: func(c *Config) { c.LoginctlPath = filepath.Join(home, "loginctl-wrapper") }, want: "loginctl_path"},
		{name: "plaintext provider URL", mutate: func(c *Config) { c.ProviderURL = "http://provider:18090" }, want: "provider_url"},
		{name: "wrong provider host", mutate: func(c *Config) { c.ProviderURL = "https://host.containers.internal:18090" }, want: "provider_url"},
		{name: "wrong provider port", mutate: func(c *Config) { c.ProviderURL = "https://" + c.StableContainer + ":18091" }, want: "provider_url"},
		{name: "default bridge network", mutate: func(c *Config) { c.ContainerNetwork = "bridge" }, want: "container_network"},
		{name: "non-yaml workflow", mutate: func(c *Config) { c.Workflow = "dogfood-provider-target" }, want: "workflow"},
		{name: "long runner name", mutate: func(c *Config) { c.RunnerName = strings.Repeat("r", 101) }, want: "runner_name"},
		{name: "long label", mutate: func(c *Config) { c.Labels = []string{strings.Repeat("l", 101)} }, want: "labels"},
		{name: "too many labels", mutate: func(c *Config) {
			c.Labels = make([]string, 65)
			for index := range c.Labels {
				c.Labels[index] = fmt.Sprintf("label-%d", index)
			}
		}, want: "labels"},
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

func TestConfigRejectsManagedContainerNameCollisions(t *testing.T) {
	home := t.TempDir()
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "candidate aliases stable probe",
			mutate: func(config *Config) {
				config.CandidateContainer = config.StableContainer + "-probe"
			},
		},
		{
			name: "stable aliases candidate probe",
			mutate: func(config *Config) {
				config.StableContainer = config.CandidateContainer + "-probe"
				config.ProviderURL = "https://" + config.StableContainer + ":18090"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := validTestConfig(home)
			tc.mutate(&config)
			if err := config.Validate(home); err == nil || !strings.Contains(err.Error(), "container names") {
				t.Fatalf("Validate err = %v want managed container name collision", err)
			}
		})
	}
}

func TestConfigRejectsAuthorityOverlapWithLifecycleState(t *testing.T) {
	home := t.TempDir()
	base := validTestConfig(home)
	paths := LifecyclePathsFor(base)
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{name: "exact lifecycle journal", mutate: func(c *Config) { c.ProviderMarkerPath = paths.LifecycleJournal }},
		{name: "lifecycle journal parent", mutate: func(c *Config) { c.SupervisorConfigPath = filepath.Dir(paths.LifecycleJournal) }},
		{name: "inside lifecycle transactions", mutate: func(c *Config) { c.LocalStatusPath = filepath.Join(paths.LifecycleTransactions, "foreign-status.json") }},
		{name: "exact audit", mutate: func(c *Config) { c.ProviderMarkerPath = paths.LifecycleAudit }},
		{name: "authority paths nested", mutate: func(c *Config) { c.LocalStatusPath = filepath.Join(c.SupervisorConfigPath, "status.json") }},
		{name: "systemd contains lifecycle root", mutate: func(c *Config) { c.SystemdDir = filepath.Dir(paths.LifecycleJournal) }},
		{name: "agent contains install root", mutate: func(c *Config) { c.ComputeAgentPath = filepath.Dir(c.InstallRoot) }},
		{name: "podman inside install root", mutate: func(c *Config) { c.PodmanPath = filepath.Join(c.InstallRoot, "bin", "podman") }, want: "outside install_root"},
		{name: "systemctl contains compute agent", mutate: func(c *Config) { c.ComputeAgentPath = filepath.Join(c.SystemctlPath, "compute-agent") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := base
			tc.mutate(&config)
			want := tc.want
			if want == "" {
				want = "overlap"
			}
			if err := config.Validate(home); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Validate err = %v want %q", err, want)
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
	previous.ImageRef = "localhost/workflow-plugin-github-runner-provider:sha256-" + strings.Repeat("d", 64)
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
	candidate.ImageRef = "localhost/workflow-plugin-github-runner-provider:sha256-" + strings.Repeat("d", 64)
	candidate.Update.SHA256 = "sha256:" + strings.Repeat("d", 64)
	candidate.Update.DirectiveID = "directive-new"

	for _, tc := range []struct {
		phase JournalPhase
		want  ImageSelection
	}{
		{phase: JournalStaging, want: previous.Current},
		{phase: JournalPrepared, want: previous.Current},
		{phase: JournalStatePromoting, want: previous.Current},
		{phase: JournalPhase("state_detached"), want: previous.Current},
		{phase: JournalStatePromoted, want: previous.Current},
		{phase: JournalActivated, want: previous.Current},
		{phase: JournalCommitted, want: candidate},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			journalCandidate := candidate
			if tc.phase == JournalStaging {
				journalCandidate = ImageSelection{Update: candidate.Update}
			}
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "txn-1",
				Phase:           tc.phase,
				Previous:        &previous,
				Candidate:       journalCandidate,
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

func TestRollbackJournalRequiresValidForwardOrigin(t *testing.T) {
	now := time.Now().UTC()
	base := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "txn-rollback",
		Phase:           JournalActivated,
		Candidate:       validTestSelection(now),
		StartedAt:       now,
		UpdatedAt:       now,
	}
	encoded, err := json.Marshal(base)
	if err != nil {
		t.Fatalf("marshal base journal: %v", err)
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatalf("decode base journal: %v", err)
	}

	for _, tc := range []struct {
		name         string
		phase        string
		rollbackFrom any
		wantValid    bool
	}{
		{name: "restoring", phase: "rollback_restoring", rollbackFrom: "activated", wantValid: true},
		{name: "restored", phase: "rollback_restored", rollbackFrom: "state_promoted", wantValid: true},
		{name: "cleaned", phase: "rollback_cleaned", rollbackFrom: "prepared", wantValid: true},
		{name: "missing origin", phase: "rollback_restoring", rollbackFrom: nil},
		{name: "terminal origin", phase: "rollback_restoring", rollbackFrom: "committed"},
		{name: "rollback origin", phase: "rollback_restoring", rollbackFrom: "rollback_restored"},
		{name: "origin on forward phase", phase: "prepared", rollbackFrom: "staging"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := make(map[string]any, len(object)+1)
			for key, value := range object {
				candidate[key] = value
			}
			candidate["phase"] = tc.phase
			if tc.rollbackFrom == nil {
				delete(candidate, "rollback_from")
			} else {
				candidate["rollback_from"] = tc.rollbackFrom
			}
			data, err := json.Marshal(candidate)
			if err != nil {
				t.Fatalf("marshal candidate journal: %v", err)
			}
			path := filepath.Join(t.TempDir(), "journal.json")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatalf("write candidate journal: %v", err)
			}
			var journal TransactionJournal
			err = ReadStrictJSONFile(path, &journal)
			if err == nil {
				err = journal.Validate()
			}
			if tc.wantValid && err != nil {
				t.Fatalf("valid rollback journal: %v", err)
			}
			if !tc.wantValid && err == nil {
				t.Fatal("invalid rollback journal was accepted")
			}
		})
	}
}

func TestStagingJournalContainsOnlyDistinctVerifiedUpdateProvenance(t *testing.T) {
	now := time.Now().UTC()
	previous := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: validTestSelection(now.Add(-time.Hour)), UpdatedAt: now.Add(-time.Hour)}
	update := validTestSelection(now).Update
	update.SHA256 = "sha256:" + strings.Repeat("d", 64)
	update.DirectiveID = "directive-staging"
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "txn-staging",
		Phase:           JournalStaging,
		Previous:        &previous,
		Candidate:       ImageSelection{Update: update},
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := journal.Validate(); err != nil {
		t.Fatalf("valid staging journal: %v", err)
	}
	withImage := journal
	withImage.Candidate.ImageID = "sha256:" + strings.Repeat("c", 64)
	if err := withImage.Validate(); err == nil || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("staging journal accepted image activation: %v", err)
	}
	matching := journal
	matching.Candidate.Update.SHA256 = previous.Current.Update.SHA256
	if err := matching.Validate(); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("staging journal accepted active digest: %v", err)
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

func TestRuntimeRepairJournalAllowsSameDigestAndPreservesPriorSelection(t *testing.T) {
	now := time.Now().UTC()
	current := validTestSelection(now.Add(-time.Hour))
	prior := validTestSelection(now.Add(-2 * time.Hour))
	prior.Update.SHA256 = "sha256:" + strings.Repeat("c", 64)
	prior.ImageID = "sha256:" + strings.Repeat("d", 64)
	prior.ImageRef = providerImageRef(prior.Update.SHA256)
	previous := ActiveState{
		ProtocolVersion: ActiveStateProtocolVersion,
		Current:         current,
		Previous:        &prior,
		UpdatedAt:       current.ActivatedAt,
	}
	candidate := current
	candidate.Update.DirectiveID = "directive-runtime-repair"
	candidate.ActivatedAt = now
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "txn-runtime-repair",
		Phase:           JournalCommitted,
		RuntimeRepair:   true,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := journal.Validate(); err != nil {
		t.Fatalf("valid runtime repair journal: %v", err)
	}
	recovered, err := RecoverActiveState(journal)
	if err != nil {
		t.Fatalf("recover runtime repair: %v", err)
	}
	if recovered.Current.Update.DirectiveID != candidate.Update.DirectiveID || recovered.Previous == nil || recovered.Previous.Update.SHA256 != prior.Update.SHA256 {
		t.Fatalf("recovered runtime repair state = %+v", recovered)
	}

	regular := journal
	regular.RuntimeRepair = false
	if err := regular.Validate(); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Fatalf("regular same-digest journal err = %v", err)
	}
	mismatched := journal
	mismatched.Candidate.Update.SHA256 = "sha256:" + strings.Repeat("e", 64)
	mismatched.Candidate.ImageRef = providerImageRef(mismatched.Candidate.Update.SHA256)
	if err := mismatched.Validate(); err == nil || !strings.Contains(err.Error(), "repair") {
		t.Fatalf("mismatched runtime repair journal err = %v", err)
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
		ProfileID:              "github-runner-profile-stg",
		PluginID:               GitHubPluginID,
		ComponentID:            "github-runner-provider-sidecar",
		ComputeAgentPath:       filepath.Join(home, ".workflow-compute", "agent-core-bin", "github-runner-linux-stg", "compute-agent"),
		SupervisorConfigPath:   filepath.Join(home, ".workflow-compute", "github-runner-linux-stg", "supervisor.pb"),
		LocalStatusPath:        filepath.Join(home, ".workflow-compute", "github-runner-linux-stg", "agent-status.json"),
		ProviderMarkerPath:     filepath.Join(home, ".workflow-compute", "updates", "updates", "current", "provider-workflow-plugin-github--component-Z2l0aHViLXJ1bm5lci1wcm92aWRlci1zaWRlY2Fy.json"),
		InstallRoot:            root,
		SystemdDir:             filepath.Join(home, ".config", "systemd", "user"),
		AgentUnit:              "workflow-compute-github-runner-linux-stg.service",
		PodmanPath:             filepath.Join(home, ".local", "libexec", "podman"),
		SystemctlPath:          filepath.Join(home, ".local", "libexec", "systemctl"),
		LoginctlPath:           filepath.Join(home, ".local", "libexec", "loginctl"),
		ProviderURL:            "https://workflow-plugin-github-runner-provider:18090",
		StableContainer:        "workflow-plugin-github-runner-provider",
		CandidateContainer:     "workflow-plugin-github-runner-provider-candidate",
		ContainerNetwork:       "wfcompute-github-provider",
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
		ImageRef:    "localhost/workflow-plugin-github-runner-provider:sha256-" + strings.Repeat("a", 64),
		ActivatedAt: now,
	}
}
