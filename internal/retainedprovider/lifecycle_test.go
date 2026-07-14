package retainedprovider

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRecoverReadyLifecycleReleasesForwardWithoutRefencing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		status      func(Config) []byte
		wantErr     string
		wantEnd     bool
		wantCleanup bool
	}{
		{
			name: "exact active",
			status: func(config Config) []byte {
				return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason)
			},
			wantEnd: true, wantCleanup: true,
		},
		{
			name:        "already inactive",
			status:      func(Config) []byte { return []byte(`{"active":false,"durable":true}`) },
			wantCleanup: true,
		},
		{
			name: "conflicting active",
			status: func(config Config) []byte {
				return maintenanceStateJSON(true, "other-transaction", config.ProfileID, refreshMaintenanceReason)
			},
			wantErr: "conflicting",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
			config := validTestConfig(home)
			writeLifecycleRecoveryFiles(t, config)
			paths := LifecyclePathsFor(config)
			now := time.Unix(1_700_800_000, 0).UTC()
			journal := lifecycleRecoveryJournalForTest(t, config, now)
			journal.Operation = LifecycleRefresh
			journal.ProviderEffect = ProviderUnchanged
			setLifecycleUnchangedForTest(&journal, config, now)
			if err := writeLifecycleJournal(home, paths, journal); err != nil {
				t.Fatalf("write intent journal: %v", err)
			}
			journal.Phase = LifecycleReady
			journal.Outcome = LifecycleRollback
			if err := writeLifecycleJournal(home, paths, journal); err != nil {
				t.Fatalf("write ready journal: %v", err)
			}

			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				switch installCommandEvent(command, config) {
				case "agent-signature":
					return agentUnitSystemdOutputForTest(t, config), nil
				case "maintenance-status":
					return tc.status(config), nil
				case "local-status":
					return localStatusJSON(config.WorkerID, "unavailable"), nil
				case "maintenance-end":
					return maintenanceStateJSON(false, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
				default:
					return nil, nil
				}
			}}
			installer := Installer{Runner: runner, Now: func() time.Time { return now.Add(time.Minute) }, Sleep: func(context.Context, time.Duration) error { return nil }}
			err := installer.recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner, Now: installer.Now, Sleep: installer.Sleep})
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("recovery error = %v want %q", err, tc.wantErr)
				}
				if _, found, readErr := readLifecycleJournal(home, paths); readErr != nil || !found {
					t.Fatalf("conflicting recovery journal found=%v err=%v", found, readErr)
				}
			} else if err != nil {
				t.Fatalf("recover lifecycle: %v", err)
			}

			transcript := commandTranscript(runner.commands)
			if strings.Contains(transcript, "supervisor-maintenance begin") || strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
				t.Fatalf("terminal recovery re-fenced or stopped the agent:\n%s", transcript)
			}
			if got := strings.Contains(transcript, "supervisor-maintenance end"); got != tc.wantEnd {
				t.Fatalf("maintenance end present=%v want=%v:\n%s", got, tc.wantEnd, transcript)
			}
			if tc.wantCleanup {
				if _, found, readErr := readLifecycleJournal(home, paths); readErr != nil || found {
					t.Fatalf("terminal journal found=%v err=%v", found, readErr)
				}
				if _, statErr := os.Stat(paths.LifecycleTransactionRoot(journal.TransactionID)); !os.IsNotExist(statErr) {
					t.Fatalf("transaction root remains: %v", statErr)
				}
			}
		})
	}
}

func TestRecoverLifecycleReattestsJournalAuthorityBeforeCommands(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := lifecycleRecoveryJournalForTest(t, config, now)
	journal.Operation = LifecycleRefresh
	journal.ProviderEffect = ProviderUnchanged
	setLifecycleUnchangedForTest(&journal, config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write lifecycle journal: %v", err)
	}
	if err := os.WriteFile(config.ComputeAgentPath, []byte("replaced compute-agent"), 0o700); err != nil {
		t.Fatalf("replace compute-agent: %v", err)
	}
	runner := &recordingCommandRunner{}
	err := (Installer{Runner: runner}).recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("recovery error = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("recovery issued commands before re-attestation: %+v", runner.commands)
	}
}

func TestRecoverFencedLifecycleReattestsAfterDrainBeforeStop(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := lifecycleRecoveryJournalForTest(t, config, now)
	journal.Operation = LifecycleRefresh
	journal.ProviderEffect = ProviderUnchanged
	setLifecycleUnchangedForTest(&journal, config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write intent journal: %v", err)
	}
	journal.Phase = LifecycleFenced
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write fenced journal: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		switch installCommandEvent(command, config) {
		case "agent-signature":
			return agentUnitSystemdOutputForTest(t, config), nil
		case "maintenance-begin":
			return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "local-status":
			if err := os.WriteFile(config.ComputeAgentPath, []byte("replacement during drain"), 0o700); err != nil {
				t.Fatalf("replace compute-agent during drain: %v", err)
			}
			return localStatusJSON(config.WorkerID, "unavailable"), nil
		default:
			return nil, nil
		}
	}}
	err := (Installer{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}).recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("recovery error = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) || strings.Contains(transcript, "systemctl --user start "+config.AgentUnit) {
		t.Fatalf("changed authority crossed stop boundary:\n%s", transcript)
	}
	if _, found, readErr := readLifecycleJournal(home, paths); readErr != nil || !found {
		t.Fatalf("failed recovery lost journal found=%v err=%v", found, readErr)
	}
}

func TestRecoverLifecycleRejectsChangedEffectiveAgentUnitBeforeMutation(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := lifecycleRecoveryJournalForTest(t, config, now)
	journal.Operation = LifecycleRefresh
	journal.ProviderEffect = ProviderUnchanged
	setLifecycleUnchangedForTest(&journal, config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write intent journal: %v", err)
	}
	journal.Phase = LifecycleReady
	journal.Outcome = LifecycleRollback
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write ready journal: %v", err)
	}
	if err := os.WriteFile(agentUnitFragmentPathForTest(config), []byte("[Service]\nExecStart=/foreign/agent\n"), 0o600); err != nil {
		t.Fatalf("replace agent fragment: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "agent-signature" {
			return agentUnitSystemdOutputForTest(t, config), nil
		}
		return nil, nil
	}}
	err := (Installer{Runner: runner}).recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner})
	if err == nil || !strings.Contains(err.Error(), "pre-signature") {
		t.Fatalf("recovery error = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "systemctl --user show "+config.AgentUnit) || strings.Contains(transcript, "supervisor-maintenance") || strings.Contains(transcript, "systemctl --user stop") || strings.Contains(transcript, "systemctl --user start") {
		t.Fatalf("changed unit recovery crossed mutation boundary:\n%s", transcript)
	}
}

func TestRecoverLifecycleRejectsMismatchedProviderTransactionBeforeCommands(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*LifecycleJournal, *TransactionJournal)
		want   string
	}{
		{name: "outer id", mutate: func(_ *LifecycleJournal, inner *TransactionJournal) { inner.OuterTransactionID = "other-transaction" }, want: "outer transaction"},
		{name: "profile", mutate: func(_ *LifecycleJournal, inner *TransactionJournal) { inner.ProfileID = "other-profile" }, want: "profile"},
		{name: "digest", mutate: func(outer *LifecycleJournal, _ *TransactionJournal) {
			outer.ProviderTransaction.Digest = "sha256:" + strings.Repeat("c", 64)
		}, want: "digest"},
		{name: "non deferred", mutate: func(_ *LifecycleJournal, inner *TransactionJournal) { inner.DeferredCommit = false }, want: "outer transaction binding"},
		{name: "not committed", mutate: func(_ *LifecycleJournal, inner *TransactionJournal) { inner.Phase = JournalActivated }, want: "committed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
			config := validTestConfig(home)
			writeLifecycleRecoveryFiles(t, config)
			paths := LifecyclePathsFor(config)
			now := time.Unix(1_700_800_000, 0).UTC()
			outer := lifecycleRecoveryJournalForTest(t, config, now)
			outer.Operation = LifecycleRefresh
			outer.ProviderEffect = ProviderChanged
			selection := validTestSelection(now)
			selection.Update.WorkerID = config.WorkerID
			inner := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "provider-transaction-123", Phase: JournalCommitted, DeferredCommit: true,
				OuterTransactionID: outer.TransactionID, ProfileID: config.ProfileID,
				Candidate: selection, StartedAt: now, UpdatedAt: now,
			}
			outer.ProviderTransaction = &LifecycleProviderTransaction{
				TransactionID: inner.ID, ProfileID: config.ProfileID, Digest: selection.Update.SHA256,
			}
			if err := writeLifecycleJournal(home, paths, outer); err != nil {
				t.Fatalf("write intent lifecycle journal: %v", err)
			}
			outer.Phase = LifecycleReady
			outer.Outcome = LifecycleCommit
			tc.mutate(&outer, &inner)
			if err := writeLifecycleJournal(home, paths, outer); err != nil {
				t.Fatalf("write ready lifecycle journal: %v", err)
			}
			if err := AtomicWriteJSON(paths.Journal, inner); err != nil {
				t.Fatalf("write provider transaction: %v", err)
			}
			runner := &recordingCommandRunner{}
			err := (Installer{Runner: runner}).recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("recovery error = %v want %q", err, tc.want)
			}
			if len(runner.commands) != 0 {
				t.Fatalf("recovery issued commands before provider binding validation: %+v", runner.commands)
			}
		})
	}
}

func TestRecoverFencedLifecycleRollsBackEveryBoundProviderPhase(t *testing.T) {
	for _, phase := range []JournalPhase{JournalPrepared, JournalStatePromoting, JournalStatePromoted, JournalActivated, JournalCommitted} {
		t.Run(string(phase), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
			config := validTestConfig(home)
			writeLifecycleRecoveryFiles(t, config)
			paths := LifecyclePathsFor(config)
			writeRefreshEnvironmentFiles(t, paths)
			if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
				t.Fatalf("create provider state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
				t.Fatalf("write previous provider state: %v", err)
			}
			now := time.Unix(1_700_800_000, 0).UTC()
			previous := previousActiveStateForTest(t, home)
			payload := writeTestProviderPayload(t, home, "outer-candidate-"+string(phase))
			digest := fileDigestForTest(t, payload)
			candidate := selectionForDigest(payload, digest, "v1.0.32", "outer-directive-"+string(phase), "sha256:"+strings.Repeat("e", 64), now)
			if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
				t.Fatalf("prepare candidate state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "generation"), []byte("candidate"), 0o600); err != nil {
				t.Fatalf("write candidate provider state: %v", err)
			}
			switch phase {
			case JournalStatePromoting:
				if err := os.Rename(paths.ProviderState, paths.PreviousState(digest)); err != nil {
					t.Fatalf("simulate state promoting: %v", err)
				}
			case JournalStatePromoted, JournalActivated, JournalCommitted:
				if err := promoteCandidateProviderState(paths, digest); err != nil {
					t.Fatalf("simulate promoted state: %v", err)
				}
			}
			active := previous
			if phase == JournalActivated || phase == JournalCommitted {
				active = ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: candidate, Previous: &previous.Current, UpdatedAt: now}
			}
			if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
				t.Fatalf("write interrupted active state: %v", err)
			}
			outer := lifecycleRecoveryJournalForTest(t, config, now)
			outer.Operation = LifecycleRefresh
			outer.ProviderEffect = ProviderChanged
			inner := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "provider-transaction-" + string(phase), Phase: phase, DeferredCommit: true,
				OuterTransactionID: outer.TransactionID, ProfileID: config.ProfileID,
				Previous: &previous, Candidate: candidate, StartedAt: now, UpdatedAt: now,
			}
			outer.ProviderTransaction = &LifecycleProviderTransaction{TransactionID: inner.ID, ProfileID: config.ProfileID, Digest: digest}
			if err := writeLifecycleJournal(home, paths, outer); err != nil {
				t.Fatalf("write outer intent: %v", err)
			}
			outer.Phase = LifecycleFenced
			if err := writeLifecycleJournal(home, paths, outer); err != nil {
				t.Fatalf("write outer fenced: %v", err)
			}
			if err := AtomicWriteJSON(paths.Journal, inner); err != nil {
				t.Fatalf("write bound inner: %v", err)
			}

			maintenanceActive := false
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				switch installCommandEvent(command, config) {
				case "agent-signature":
					return agentUnitSystemdOutputForTest(t, config), nil
				case "maintenance-begin":
					maintenanceActive = true
					return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
				case "maintenance-status":
					if maintenanceActive {
						return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
					}
					return []byte(`{"active":false,"durable":true}`), nil
				case "maintenance-end":
					maintenanceActive = false
					return maintenanceStateJSON(false, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
				case "local-status":
					return localStatusJSON(config.WorkerID, "unavailable"), nil
				default:
					return nil, nil
				}
			}}
			installer := Installer{Runner: runner, Now: func() time.Time { return now.Add(time.Minute) }, Sleep: func(context.Context, time.Duration) error { return nil }}
			if err := installer.recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner, Now: installer.Now, Sleep: installer.Sleep}); err != nil {
				t.Fatalf("recover outer %s: %v", phase, err)
			}
			if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "generation")); err != nil || string(data) != "previous" {
				t.Fatalf("restored provider state = %q err=%v", data, err)
			}
			recovered, found, err := readActiveState(paths.ActiveState)
			if err != nil || !found || recovered.Current.ImageID != previous.Current.ImageID {
				t.Fatalf("restored active = %+v found=%v err=%v", recovered, found, err)
			}
			if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
				t.Fatalf("outer remains found=%v err=%v", found, err)
			}
			if _, found, err := readTransactionJournal(paths.Journal); err != nil || found {
				t.Fatalf("inner remains found=%v err=%v", found, err)
			}
		})
	}
}

func TestRecoverFencingAndFencedLifecycleRollsBackBeforeRelease(t *testing.T) {
	for _, tc := range []struct {
		phase    LifecyclePhase
		wantStop bool
	}{
		{phase: LifecycleFencing, wantStop: false},
		{phase: LifecycleFenced, wantStop: true},
	} {
		t.Run(string(tc.phase), func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
			config := validTestConfig(home)
			writeLifecycleRecoveryFiles(t, config)
			paths := LifecyclePathsFor(config)
			now := time.Unix(1_700_800_000, 0).UTC()
			journal := lifecycleRecoveryJournalForTest(t, config, now)
			journal.Operation = LifecycleRefresh
			journal.ProviderEffect = ProviderUnchanged
			setLifecycleUnchangedForTest(&journal, config, now)
			if err := writeLifecycleJournal(home, paths, journal); err != nil {
				t.Fatalf("write intent journal: %v", err)
			}
			journal.Phase = tc.phase
			if err := writeLifecycleJournal(home, paths, journal); err != nil {
				t.Fatalf("write %s journal: %v", tc.phase, err)
			}

			maintenanceActive := false
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				switch installCommandEvent(command, config) {
				case "agent-signature":
					return agentUnitSystemdOutputForTest(t, config), nil
				case "maintenance-begin":
					maintenanceActive = true
					return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
				case "maintenance-status":
					if maintenanceActive {
						return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
					}
					return []byte(`{"active":false,"durable":true}`), nil
				case "maintenance-end":
					maintenanceActive = false
					return maintenanceStateJSON(false, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
				case "local-status":
					return localStatusJSON(config.WorkerID, "unavailable"), nil
				default:
					return nil, nil
				}
			}}
			installer := Installer{Runner: runner, Now: func() time.Time { return now.Add(time.Minute) }, Sleep: func(context.Context, time.Duration) error { return nil }}
			if err := installer.recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner, Now: installer.Now, Sleep: installer.Sleep}); err != nil {
				t.Fatalf("recover %s lifecycle: %v", tc.phase, err)
			}
			transcript := commandTranscript(runner.commands)
			for _, required := range []string{"supervisor-maintenance begin", "supervisor-maintenance status", "supervisor-maintenance end"} {
				if !strings.Contains(transcript, required) {
					t.Fatalf("%s recovery missing %q:\n%s", tc.phase, required, transcript)
				}
			}
			stopped := strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit)
			if stopped != tc.wantStop {
				t.Fatalf("%s recovery stopped agent=%v want=%v:\n%s", tc.phase, stopped, tc.wantStop, transcript)
			}
			if tc.wantStop && !strings.Contains(transcript, "systemctl --user start "+config.AgentUnit) {
				t.Fatalf("fenced recovery did not restart agent:\n%s", transcript)
			}
			if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
				t.Fatalf("recovered journal found=%v err=%v", found, err)
			}
		})
	}
}

func TestRecoverLifecycleAdoptsLegacyInnerBeforeMaintenance(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("create provider root: %v", err)
	}
	if err := AtomicWriteJSON(paths.ConfigFile, config); err != nil {
		t.Fatalf("write installed config: %v", err)
	}
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	now := time.Unix(1_700_800_000, 0).UTC()
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "legacy-candidate")
	digest := fileDigestForTest(t, payload)
	candidate := selectionForDigest(payload, digest, "v1.0.32", "legacy-directive", "sha256:"+strings.Repeat("e", 64), now)
	if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
		t.Fatalf("prepare legacy candidate state: %v", err)
	}
	inner := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "legacy-provider-transaction", Phase: JournalPrepared,
		Previous: &previous, Candidate: candidate, StartedAt: now, UpdatedAt: now,
	}
	if err := AtomicWriteJSON(paths.Journal, inner); err != nil {
		t.Fatalf("write legacy inner journal: %v", err)
	}

	maintenanceActive := false
	sawAdopting := false
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		switch installCommandEvent(command, config) {
		case "agent-signature":
			return agentUnitSystemdOutputForTest(t, config), nil
		case "maintenance-begin":
			outer, found, err := readLifecycleJournal(home, paths)
			wantPhase := LifecycleAdopting
			if sawAdopting {
				wantPhase = LifecycleFenced
			}
			if err != nil || !found || outer.Operation != LifecycleRefreshRecovery || outer.Phase != wantPhase || outer.ProviderTransaction == nil || outer.ProviderTransaction.LegacyJournalSHA256 == "" {
				t.Fatalf("legacy adoption outer = %+v found=%v err=%v", outer, found, err)
			}
			if outer.Phase == LifecycleAdopting {
				sawAdopting = true
			}
			maintenanceActive = true
			return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "maintenance-status":
			if maintenanceActive {
				return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
			}
			return []byte(`{"active":false,"durable":true}`), nil
		case "maintenance-end":
			maintenanceActive = false
			return maintenanceStateJSON(false, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "local-status":
			return localStatusJSON(config.WorkerID, "unavailable"), nil
		default:
			return nil, nil
		}
	}}
	installer := Installer{Runner: runner, Now: func() time.Time { return now.Add(time.Minute) }, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := installer.recoverLifecycleTransaction(t.Context(), home, paths, Refresher{Runner: runner, Now: installer.Now, Sleep: installer.Sleep}); err != nil {
		t.Fatalf("recover adopted legacy transaction: %v", err)
	}
	if !sawAdopting {
		t.Fatal("legacy transaction was not durably adopting before maintenance")
	}
	if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
		t.Fatalf("outer lifecycle remains found=%v err=%v", found, err)
	}
	if _, found, err := readTransactionJournal(paths.Journal); err != nil || found {
		t.Fatalf("legacy inner remains found=%v err=%v", found, err)
	}
	transcript := commandTranscript(runner.commands)
	assertOrderedText(t, transcript, []string{"systemctl --user show " + config.AgentUnit, "supervisor-maintenance begin", "systemctl --user stop " + config.AgentUnit, "systemctl --user start " + config.AgentUnit, "supervisor-maintenance end"})
}

func TestExplicitLegacyRecoveryRequiresExactConfirmationBeforeCommands(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("create provider root: %v", err)
	}
	now := time.Unix(1_700_800_000, 0).UTC()
	selection := validTestSelection(now)
	selection.Update.WorkerID = config.WorkerID
	selection.Update.PluginID = config.PluginID
	selection.Update.ComponentID = config.ComponentID
	inner := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "legacy-provider-transaction", Phase: JournalPrepared,
		Candidate: selection, StartedAt: now, UpdatedAt: now,
	}
	if err := AtomicWriteJSON(paths.Journal, inner); err != nil {
		t.Fatalf("write legacy inner journal: %v", err)
	}
	runner := &recordingCommandRunner{}
	installer := Installer{Runner: runner}
	_, err := installer.Recover(t.Context(), home, config, "different-transaction")
	if err == nil || !strings.Contains(err.Error(), "confirmation") {
		t.Fatalf("explicit recovery error = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("mismatched confirmation issued commands: %+v", runner.commands)
	}
}

func writeLifecycleRecoveryFiles(t *testing.T, config Config) {
	t.Helper()
	for _, file := range []struct {
		path string
		mode os.FileMode
		data string
	}{
		{path: config.ComputeAgentPath, mode: 0o700, data: "compute-agent fixture"},
		{path: config.SupervisorConfigPath, mode: 0o600, data: "supervisor config fixture"},
		{path: agentUnitFragmentPathForTest(config), mode: 0o600, data: "[Service]\nExecStart=" + config.ComputeAgentPath + " run\n"},
	} {
		if err := os.MkdirAll(filepath.Dir(file.path), 0o700); err != nil {
			t.Fatalf("create recovery file directory: %v", err)
		}
		if _, err := os.Lstat(file.path); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			t.Fatalf("inspect recovery file: %v", err)
		}
		if err := os.WriteFile(file.path, []byte(file.data), file.mode); err != nil {
			t.Fatalf("write recovery file: %v", err)
		}
	}
}

func lifecycleRecoveryJournalForTest(t *testing.T, config Config, now time.Time) LifecycleJournal {
	t.Helper()
	journal := validLifecycleJournalForTest(config, now)
	computeAgentDigest, err := hashRegularFile(config.ComputeAgentPath, true)
	if err != nil {
		t.Fatalf("hash compute-agent: %v", err)
	}
	supervisorDigest, err := hashRegularFile(config.SupervisorConfigPath, false)
	if err != nil {
		t.Fatalf("hash supervisor config: %v", err)
	}
	journal.Recovery.ComputeAgent.SHA256 = computeAgentDigest
	journal.Recovery.SupervisorConfig.SHA256 = supervisorDigest
	journal.Recovery.AgentUnitBefore = agentUnitSignatureForTest(t, config)
	return journal
}

func setLifecycleUnchangedForTest(journal *LifecycleJournal, config Config, now time.Time) {
	selection := validTestSelection(now)
	selection.Update.WorkerID = config.WorkerID
	selection.Update.PluginID = config.PluginID
	selection.Update.ComponentID = config.ComponentID
	journal.Unchanged = &LifecycleUnchangedProvenance{Active: selection, Candidate: selection.Update}
}

func TestLifecyclePathsSurviveProviderRootPurge(t *testing.T) {
	home := t.TempDir()
	paths := LifecyclePathsFor(validTestConfig(home))
	transactionID := "install-transaction-123"

	for name, path := range map[string]string{
		"journal":          paths.LifecycleJournal,
		"transaction root": paths.LifecycleTransactionRoot(transactionID),
		"audit":            paths.LifecycleAudit,
	} {
		relative, err := filepath.Rel(paths.Root, path)
		if err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			t.Fatalf("%s %q is inside purgeable root %q", name, path, paths.Root)
		}
		if err := ValidateUserPath(home, path, false); err != nil {
			t.Fatalf("%s path: %v", name, err)
		}
	}
}

func TestLifecycleAuditDrainRecoversCompleteAndTornAppend(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, paths LifecyclePaths, journal *LifecycleJournal, payload []byte)
		wantErr string
	}{
		{name: "new append"},
		{name: "complete append before pending clear", prepare: func(t *testing.T, paths LifecyclePaths, journal *LifecycleJournal, payload []byte) {
			t.Helper()
			writeAuditFixture(t, paths.LifecycleAudit, payload)
			offset := int64(0)
			journal.Audit.Safety[0].Offset = &offset
			journal.Audit.Safety[0].Digest = digestBytes(payload)
		}},
		{name: "torn append", prepare: func(t *testing.T, paths LifecyclePaths, journal *LifecycleJournal, payload []byte) {
			t.Helper()
			writeAuditFixture(t, paths.LifecycleAudit, payload[:len(payload)/2])
			offset := int64(0)
			journal.Audit.Safety[0].Offset = &offset
			journal.Audit.Safety[0].Digest = digestBytes(payload)
		}},
		{name: "unrelated tail", prepare: func(t *testing.T, paths LifecyclePaths, journal *LifecycleJournal, payload []byte) {
			t.Helper()
			writeAuditFixture(t, paths.LifecycleAudit, []byte("unrelated\n"))
			offset := int64(0)
			journal.Audit.Safety[0].Offset = &offset
			journal.Audit.Safety[0].Digest = digestBytes(payload)
		}, wantErr: "unrelated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			now := time.Unix(1_700_800_000, 0).UTC()
			journal := validLifecycleJournalForTest(config, now)
			event := LifecycleAuditEvent{
				EventID: "event-1", Timestamp: now, TransactionID: journal.TransactionID,
				WorkerID: config.WorkerID, Operation: LifecycleInstall,
				Phase: LifecycleIntent, Kind: AuditPhase, ProviderEffect: ProviderChanged,
			}
			if err := journal.Audit.EnqueueSafety(event); err != nil {
				t.Fatalf("enqueue safety event: %v", err)
			}
			payload, err := lifecycleAuditPayload(journal.Audit.Safety[0])
			if err != nil {
				t.Fatalf("audit payload: %v", err)
			}
			if tc.prepare != nil {
				tc.prepare(t, paths, &journal, payload)
			}
			if err := writeLifecycleJournal(home, paths, journal); err != nil {
				t.Fatalf("write lifecycle journal: %v", err)
			}
			err = drainLifecycleAudit(home, paths, &journal)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("drain error = %v want %q", err, tc.wantErr)
				}
				if len(journal.Audit.Safety) != 1 {
					t.Fatalf("failed drain cleared queue: %+v", journal.Audit)
				}
				return
			}
			if err != nil {
				t.Fatalf("drain audit: %v", err)
			}
			if len(journal.Audit.Safety) != 0 {
				t.Fatalf("drained queue = %+v", journal.Audit)
			}
			data, err := os.ReadFile(paths.LifecycleAudit)
			if err != nil || !bytes.Equal(data, payload) {
				t.Fatalf("audit data = %q err=%v want=%q", data, err, payload)
			}
		})
	}
}

func writeAuditFixture(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir audit dir: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write audit fixture: %v", err)
	}
}

func TestLifecycleJournalValidatesOperationEffectAndIdentity(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)

	if err := journal.Validate(home, LifecyclePathsFor(config)); err != nil {
		t.Fatalf("valid lifecycle journal: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*LifecycleJournal)
		want   string
	}{
		{name: "uninstall changed effect", mutate: func(j *LifecycleJournal) {
			j.Operation = LifecycleUninstall
			j.ProviderEffect = ProviderChanged
			j.Uninstall = &LifecycleUninstallPayload{Purge: true}
		}, want: "not_applicable"},
		{name: "install not-applicable effect", mutate: func(j *LifecycleJournal) {
			j.ProviderEffect = ProviderNotApplicable
		}, want: "not_applicable"},
		{name: "ready without outcome", mutate: func(j *LifecycleJournal) {
			j.Phase = LifecycleReady
			j.Outcome = ""
		}, want: "outcome"},
		{name: "changed ready without inner binding", mutate: func(j *LifecycleJournal) {
			j.Operation = LifecycleRefresh
			j.Phase = LifecycleReady
			j.Outcome = LifecycleCommit
			j.ProviderTransaction = nil
		}, want: "provider transaction"},
		{name: "changed pre-provider without inner is valid", mutate: func(j *LifecycleJournal) {
			j.Operation = LifecycleRefresh
			j.Phase = LifecycleFenced
			j.ProviderTransaction = nil
		}, want: ""},
		{name: "retry identity mismatch", mutate: func(j *LifecycleJournal) {
			j.Recovery.Config.WorkerID = "different-worker"
		}, want: "identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := journal
			candidate.Uninstall = nil
			candidate.ProviderTransaction = nil
			tc.mutate(&candidate)
			err := candidate.Validate(home, LifecyclePathsFor(config))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v want %q", err, tc.want)
			}
		})
	}
}

func TestLifecycleJournalRequiresBoundUnchangedProvenance(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	now := time.Unix(1_700_800_000, 0).UTC()
	active := validTestSelection(now)
	active.Update.WorkerID = config.WorkerID
	active.Update.PluginID = config.PluginID
	active.Update.ComponentID = config.ComponentID
	candidate := active.Update
	candidate.DirectiveID = "directive-same-digest"

	valid := validLifecycleJournalForTest(config, now)
	valid.Operation = LifecycleRefresh
	valid.ProviderEffect = ProviderUnchanged
	valid.Unchanged = &LifecycleUnchangedProvenance{
		Active: active, Candidate: candidate, StableProbeAt: now.Add(time.Minute),
	}
	valid.Phase = LifecycleReady
	valid.Outcome = LifecycleCommit
	if err := valid.Validate(home, LifecyclePathsFor(config)); err != nil {
		t.Fatalf("valid unchanged lifecycle: %v", err)
	}

	for _, tc := range []struct {
		name   string
		mutate func(*LifecycleJournal)
		want   string
	}{
		{name: "missing provenance", mutate: func(j *LifecycleJournal) { j.Unchanged = nil }, want: "unchanged provenance"},
		{name: "candidate digest mismatch", mutate: func(j *LifecycleJournal) {
			j.Unchanged.Candidate.SHA256 = "sha256:" + strings.Repeat("c", 64)
		}, want: "digest"},
		{name: "candidate worker mismatch", mutate: func(j *LifecycleJournal) {
			j.Unchanged.Candidate.WorkerID = "other-worker"
		}, want: "identity"},
		{name: "missing successful probe", mutate: func(j *LifecycleJournal) {
			j.Unchanged.StableProbeAt = time.Time{}
		}, want: "probe"},
		{name: "changed effect with provenance", mutate: func(j *LifecycleJournal) {
			j.ProviderEffect = ProviderChanged
			j.ProviderTransaction = &LifecycleProviderTransaction{
				TransactionID: "provider-transaction-123", ProfileID: config.ProfileID, Digest: candidate.SHA256,
			}
		}, want: "unchanged provenance"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			journal := valid
			provenance := *valid.Unchanged
			journal.Unchanged = &provenance
			tc.mutate(&journal)
			err := journal.Validate(home, LifecyclePathsFor(config))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate = %v want %q", err, tc.want)
			}
		})
	}
}

func TestLifecycleJournalReservesSafetyEventsForTerminalRelease(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)
	journal.Operation = LifecycleRefresh
	journal.ProviderEffect = ProviderChanged
	journal.Phase = LifecycleReady
	journal.Outcome = LifecycleRollback
	for index := 0; index < maxLifecycleSafetyEvents-1; index++ {
		journal.Audit.Safety = append(journal.Audit.Safety, LifecycleAuditEvent{
			EventID: "event-" + strconv.Itoa(index+1), Sequence: uint64(index + 1), Timestamp: now,
			TransactionID: journal.TransactionID, WorkerID: config.WorkerID,
			Operation: journal.Operation, Phase: journal.Phase, Kind: AuditPhase,
			Outcome: journal.Outcome, ProviderEffect: journal.ProviderEffect,
		})
	}
	journal.Audit.NextSequence = maxLifecycleSafetyEvents
	if err := journal.Validate(home, LifecyclePathsFor(config)); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("Validate overcommitted safety queue = %v", err)
	}
}

func TestLifecycleAuditEventStrictUnion(t *testing.T) {
	now := time.Unix(1_700_800_000, 0).UTC()
	event := LifecycleAuditEvent{
		EventID: "event-1", Sequence: 1, Timestamp: now,
		TransactionID: "install-transaction-123", WorkerID: "worker-1",
		Operation: LifecycleInstall, Phase: LifecycleReady, Kind: AuditPhase,
		Outcome: LifecycleCommit, ProviderEffect: ProviderChanged,
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("valid phase event: %v", err)
	}

	invalid := event
	invalid.ErrorClass = "transport"
	if err := invalid.Validate(); err == nil || !strings.Contains(err.Error(), "error_class") {
		t.Fatalf("phase event with error class = %v", err)
	}

	errorEvent := event
	errorEvent.Kind = AuditError
	errorEvent.Outcome = ""
	errorEvent.ProviderEffect = ""
	errorEvent.ErrorClass = "provider_probe"
	errorEvent.Count = 1
	errorEvent.FirstSeen = now
	errorEvent.LastSeen = now
	if err := errorEvent.Validate(); err != nil {
		t.Fatalf("valid error event: %v", err)
	}
}

func TestLifecycleAuditDiagnosticsCoalesceAndOverflow(t *testing.T) {
	now := time.Unix(1_700_800_000, 0).UTC()
	queue := LifecycleAuditQueue{NextSequence: 1}
	base := LifecycleAuditEvent{
		EventID: "event-1", Timestamp: now, TransactionID: "install-transaction-123",
		WorkerID: "worker-1", Operation: LifecycleInstall, Phase: LifecycleFenced,
		Kind: AuditError, ErrorClass: "provider_probe",
	}
	if err := queue.EnqueueDiagnostic(base); err != nil {
		t.Fatalf("enqueue diagnostic: %v", err)
	}
	repeated := base
	repeated.EventID = "event-2"
	repeated.Timestamp = now.Add(time.Minute)
	if err := queue.EnqueueDiagnostic(repeated); err != nil {
		t.Fatalf("coalesce diagnostic: %v", err)
	}
	if len(queue.Diagnostics) != 1 || queue.Diagnostics[0].Count != 2 || !queue.Diagnostics[0].LastSeen.Equal(repeated.Timestamp) {
		t.Fatalf("coalesced diagnostics = %+v", queue.Diagnostics)
	}

	assigned := queue.Diagnostics[0]
	offset := int64(42)
	assigned.Offset = &offset
	assigned.Digest = "sha256:" + strings.Repeat("a", 64)
	queue.Diagnostics[0] = assigned
	third := base
	third.EventID = "event-3"
	third.Timestamp = now.Add(2 * time.Minute)
	if err := queue.EnqueueDiagnostic(third); err != nil {
		t.Fatalf("enqueue diagnostic after append assignment: %v", err)
	}
	if len(queue.Diagnostics) != 2 || queue.Diagnostics[0].Count != 2 || queue.Diagnostics[1].Count != 1 {
		t.Fatalf("assigned diagnostic head was mutated: %+v", queue.Diagnostics)
	}

	for index := len(queue.Diagnostics); index < maxLifecycleDiagnosticEvents-1; index++ {
		event := base
		event.EventID = "event-" + strconv.Itoa(index+2)
		event.ErrorClass = "class-" + strconv.Itoa(index)
		event.Timestamp = now.Add(time.Duration(index) * time.Minute)
		if err := queue.EnqueueDiagnostic(event); err != nil {
			t.Fatalf("fill diagnostic queue at %d: %v", index, err)
		}
	}
	overflow := base
	overflow.EventID = "overflow-source-1"
	overflow.ErrorClass = "beyond-capacity"
	overflow.Timestamp = now.Add(40 * time.Minute)
	if err := queue.EnqueueDiagnostic(overflow); err != nil {
		t.Fatalf("enqueue overflow diagnostic: %v", err)
	}
	if got := queue.Diagnostics[len(queue.Diagnostics)-1]; got.Kind != AuditOverflow || got.ErrorClass != "other" || got.Count != 1 {
		t.Fatalf("overflow diagnostic = %+v", got)
	}
	overflow.EventID = "overflow-source-2"
	overflow.Timestamp = now.Add(41 * time.Minute)
	if err := queue.EnqueueDiagnostic(overflow); err != nil {
		t.Fatalf("coalesce overflow diagnostic: %v", err)
	}
	if got := queue.Diagnostics[len(queue.Diagnostics)-1]; got.Count != 2 || !got.LastSeen.Equal(overflow.Timestamp) {
		t.Fatalf("coalesced overflow diagnostic = %+v", got)
	}
}

func TestLifecycleJournalOwnsSensitiveSnapshotsOutsideProviderRoot(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)
	journal.Phase = LifecycleFencing
	transactionRoot := paths.LifecycleTransactionRoot(journal.TransactionID)
	snapshotContents := []byte("secret rollback bytes")
	journal.Snapshots = []managedFileSnapshot{{
		Path: paths.ProviderEnv, Backup: filepath.Join(transactionRoot, "snapshots", "0"),
		Mode: 0o600, Existed: true, SHA256: digestBytes(snapshotContents),
	}}
	journal.PreviousUnits = map[string]systemdUnitState{}
	if err := os.MkdirAll(filepath.Dir(journal.Snapshots[0].Backup), 0o700); err != nil {
		t.Fatalf("create snapshot directory: %v", err)
	}
	if err := os.WriteFile(journal.Snapshots[0].Backup, snapshotContents, 0o600); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}
	if err := journal.Validate(home, paths); err != nil {
		t.Fatalf("valid lifecycle snapshot: %v", err)
	}

	insideProvider := journal
	insideProvider.Snapshots = append([]managedFileSnapshot(nil), journal.Snapshots...)
	insideProvider.Snapshots[0].Backup = filepath.Join(paths.Root, "snapshot")
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("create provider root: %v", err)
	}
	if err := os.WriteFile(insideProvider.Snapshots[0].Backup, []byte("wrongly rooted rollback bytes"), 0o600); err != nil {
		t.Fatalf("write provider-root snapshot: %v", err)
	}
	if err := insideProvider.Validate(home, paths); err == nil || !strings.Contains(err.Error(), "transaction root") {
		t.Fatalf("provider-root snapshot validation = %v", err)
	}

	refresh := journal
	refresh.Operation = LifecycleRefresh
	if err := refresh.Validate(home, paths); err == nil || !strings.Contains(err.Error(), "snapshots") {
		t.Fatalf("refresh snapshot validation = %v", err)
	}
}

func TestSnapshotManagedFilesUsesLifecycleTransactionRoot(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	transactionRoot := paths.LifecycleTransactionRoot("install-transaction-123")
	if err := os.MkdirAll(transactionRoot, 0o700); err != nil {
		t.Fatalf("create lifecycle transaction root: %v", err)
	}
	if err := atomicWriteFile(paths.ProviderEnv, []byte("GITHUB_TOKEN=secret\n"), 0o600); err != nil {
		t.Fatalf("write managed secret: %v", err)
	}
	snapshots, err := snapshotManagedFilesAt(paths, transactionRoot)
	if err != nil {
		t.Fatalf("snapshot managed files: %v", err)
	}
	snapshotRoot := filepath.Join(transactionRoot, "snapshots")
	if len(snapshots) != len(managedInstallPaths(paths)) {
		t.Fatalf("snapshot count = %d want %d", len(snapshots), len(managedInstallPaths(paths)))
	}
	for _, snapshot := range snapshots {
		if filepath.Dir(snapshot.Backup) != snapshotRoot {
			t.Fatalf("snapshot backup outside transaction root: %+v", snapshot)
		}
	}
	if err := os.RemoveAll(paths.Root); err != nil {
		t.Fatalf("purge provider root: %v", err)
	}
	var secretSnapshot managedFileSnapshot
	for _, snapshot := range snapshots {
		if snapshot.Path == paths.ProviderEnv {
			secretSnapshot = snapshot
			break
		}
	}
	data, err := os.ReadFile(secretSnapshot.Backup)
	if err != nil || string(data) != "GITHUB_TOKEN=secret\n" {
		t.Fatalf("snapshot after purge = %q err=%v", data, err)
	}
}

func TestLifecycleSnapshotsAreJournaledIncrementallyAndDigestBound(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write lifecycle journal: %v", err)
	}
	journal.Phase = LifecycleFencing
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write fencing lifecycle journal: %v", err)
	}
	if err := atomicWriteFile(paths.ConfigFile, []byte("first managed file"), 0o600); err != nil {
		t.Fatalf("write first managed file: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(paths.Launcher), 0o700); err != nil {
		t.Fatalf("create launcher directory: %v", err)
	}
	if err := os.Symlink(paths.ConfigFile, paths.Launcher); err != nil {
		t.Fatalf("create invalid second managed file: %v", err)
	}

	if err := snapshotManagedFilesForLifecycle(home, paths, &journal, now.Add(time.Second)); err == nil || !strings.Contains(err.Error(), "regular file") {
		t.Fatalf("incremental snapshot error = %v", err)
	}
	persisted, found, err := readLifecycleJournal(home, paths)
	if err != nil || !found {
		t.Fatalf("read incremental lifecycle journal found=%v err=%v", found, err)
	}
	if len(persisted.Snapshots) != 1 || persisted.Snapshots[0].Path != paths.ConfigFile || persisted.Snapshots[0].SHA256 == "" {
		t.Fatalf("incremental snapshots = %+v", persisted.Snapshots)
	}
	if err := os.WriteFile(persisted.Snapshots[0].Backup, []byte("tampered rollback bytes"), 0o600); err != nil {
		t.Fatalf("tamper snapshot: %v", err)
	}
	if _, _, err := readLifecycleJournal(home, paths); err == nil || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("tampered snapshot read error = %v", err)
	}
}

func TestFencedLifecycleRequiresCompleteManagedWiringIntent(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write lifecycle journal: %v", err)
	}
	journal.Phase = LifecycleFencing
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write fencing lifecycle journal: %v", err)
	}
	if err := snapshotManagedFilesForLifecycle(home, paths, &journal, now.Add(time.Second)); err != nil {
		t.Fatalf("snapshot lifecycle files: %v", err)
	}
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		t.Fatalf("render units: %v", err)
	}
	journal.WiringIntent = managedWiringIntent(paths, units, true)
	journal.Phase = LifecycleFenced
	journal.UpdatedAt = now.Add(2 * time.Second)
	if err := journal.Validate(home, paths); err != nil {
		t.Fatalf("valid fenced install journal: %v", err)
	}

	missingSnapshot := journal
	missingSnapshot.Snapshots = missingSnapshot.Snapshots[:len(missingSnapshot.Snapshots)-1]
	if err := missingSnapshot.Validate(home, paths); err == nil || !strings.Contains(err.Error(), "complete snapshots") {
		t.Fatalf("missing snapshot validation = %v", err)
	}
	missingIntent := journal
	missingIntent.WiringIntent = missingIntent.WiringIntent[:len(missingIntent.WiringIntent)-1]
	if err := missingIntent.Validate(home, paths); err == nil || !strings.Contains(err.Error(), "complete wiring intent") {
		t.Fatalf("missing intent validation = %v", err)
	}
	badDigest := journal
	badDigest.WiringIntent = append([]LifecycleManagedFileIntent(nil), journal.WiringIntent...)
	badDigest.WiringIntent[0].SHA256 = "sha256:" + strings.Repeat("f", 64)
	if err := badDigest.Validate(home, paths); err == nil || !strings.Contains(err.Error(), "wiring intent digest") {
		t.Fatalf("incorrect intent digest validation = %v", err)
	}
}

func TestLifecycleManagedWiringAcceptsOnlyPreOrIntendedCrashVectors(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_800_000, 0).UTC()
	journal := validLifecycleJournalForTest(config, now)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write intent journal: %v", err)
	}
	journal.Phase = LifecycleFencing
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write fencing journal: %v", err)
	}
	if err := snapshotManagedFilesForLifecycle(home, paths, &journal, now.Add(time.Second)); err != nil {
		t.Fatalf("snapshot lifecycle files: %v", err)
	}
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		t.Fatalf("render units: %v", err)
	}
	journal.WiringIntent = managedWiringIntent(paths, units, true)
	journal.Phase = LifecycleFenced
	journal.UpdatedAt = now.Add(2 * time.Second)
	if err := writeLifecycleJournal(home, paths, journal); err != nil {
		t.Fatalf("write fenced journal: %v", err)
	}

	for _, intent := range journal.WiringIntent[:2] {
		if err := atomicWriteFile(intent.Path, intent.Contents, intent.Mode); err != nil {
			t.Fatalf("write partial intended wiring: %v", err)
		}
	}
	if err := validateLifecycleWiringVector(journal, paths, lifecycleWiringMixed); err != nil {
		t.Fatalf("mixed pre/intended vector: %v", err)
	}
	if err := validateLifecycleWiringVector(journal, paths, lifecycleWiringIntended); err == nil || !strings.Contains(err.Error(), "intended") {
		t.Fatalf("partial ready vector validation = %v", err)
	}

	third := journal.WiringIntent[2]
	if err := atomicWriteFile(third.Path, []byte("foreign wiring bytes"), 0o600); err != nil {
		t.Fatalf("write foreign wiring: %v", err)
	}
	if err := validateLifecycleWiringVector(journal, paths, lifecycleWiringMixed); err == nil || !strings.Contains(err.Error(), "neither pre-state nor intended") {
		t.Fatalf("foreign fenced vector validation = %v", err)
	}

	for _, intent := range journal.WiringIntent {
		if err := atomicWriteFile(intent.Path, intent.Contents, intent.Mode); err != nil {
			t.Fatalf("write intended wiring: %v", err)
		}
	}
	if err := validateLifecycleWiringVector(journal, paths, lifecycleWiringIntended); err != nil {
		t.Fatalf("complete intended vector: %v", err)
	}
}

func TestLifecycleSystemdSignatureAttestsEveryEffectiveInput(t *testing.T) {
	home := t.TempDir()
	fragmentPath := filepath.Join(home, ".config", "systemd", "user", "workflow-compute-agent.service")
	dropInPath := filepath.Join(home, ".config", "systemd", "user", "workflow-compute-agent.service.d", "20-provider.conf")
	environmentPath := filepath.Join(home, ".workflow-compute", "agent.env")
	for path, contents := range map[string]string{
		fragmentPath:    "[Service]\nExecStart=/opt/workflow/compute-agent\n",
		dropInPath:      "[Service]\nEnvironmentFile=" + environmentPath + "\n",
		environmentPath: "WORKER_ID=worker-1\n",
	} {
		if err := atomicWriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write effective systemd input: %v", err)
		}
	}
	signature := LifecycleSystemdSignature{
		Fragment:         lifecycleAttestationForTest(t, fragmentPath),
		DropIns:          []LifecycleFileAttestation{lifecycleAttestationForTest(t, dropInPath)},
		ExecStart:        "{ path=/opt/workflow/compute-agent ; argv[]=/opt/workflow/compute-agent run ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }",
		EnvironmentFiles: []LifecycleFileAttestation{lifecycleAttestationForTest(t, environmentPath)},
	}
	if err := signature.Validate(home); err != nil {
		t.Fatalf("valid effective signature: %v", err)
	}
	if err := signature.Reattest(); err != nil {
		t.Fatalf("re-attest effective signature: %v", err)
	}

	if err := os.WriteFile(dropInPath, []byte("[Service]\nEnvironment=FOREIGN=1\n"), 0o600); err != nil {
		t.Fatalf("replace drop-in: %v", err)
	}
	if err := signature.Reattest(); err == nil || !strings.Contains(err.Error(), "drop-in") {
		t.Fatalf("re-attest replaced drop-in = %v", err)
	}

	duplicate := signature
	duplicate.DropIns = append(duplicate.DropIns, duplicate.DropIns[0])
	if err := duplicate.Validate(home); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate drop-in validation = %v", err)
	}
}

func TestDeriveIntendedAgentUnitSignatureClosesDaemonReloadCrashWindow(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	before := agentUnitSignatureForTest(t, config)
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		t.Fatalf("render systemd units: %v", err)
	}
	wiring := managedWiringIntent(paths, units, true)
	agentEnvironment := []byte("COMPUTE_GITHUB_RUNNER_PROVIDER_URL=https://provider\n")
	intended, err := deriveLifecycleAgentUnitSignature(before, paths, wiring, &LifecycleFileAttestation{
		Path: paths.AgentEnv, SHA256: digestBytes(agentEnvironment),
	})
	if err != nil {
		t.Fatalf("derive install signature: %v", err)
	}
	if len(intended.DropIns) != 1 || intended.DropIns[0].Path != paths.AgentDropIn || intended.DropIns[0].SHA256 != digestBytes([]byte(units.AgentDropIn)) {
		t.Fatalf("derived drop-ins = %+v", intended.DropIns)
	}
	if len(intended.EnvironmentFiles) != 1 || intended.EnvironmentFiles[0].Path != paths.AgentEnv || intended.EnvironmentFiles[0].SHA256 != digestBytes(agentEnvironment) {
		t.Fatalf("derived environment files = %+v", intended.EnvironmentFiles)
	}
	if err := atomicWriteFile(paths.AgentDropIn, []byte(units.AgentDropIn), 0o600); err != nil {
		t.Fatalf("write intended drop-in: %v", err)
	}
	if err := atomicWriteFile(paths.AgentEnv, agentEnvironment, 0o600); err != nil {
		t.Fatalf("write intended environment: %v", err)
	}
	if actual := agentUnitSignatureForTest(t, config); !equalLifecycleSystemdSignature(actual, intended) {
		t.Fatalf("actual signature = %+v want %+v", actual, intended)
	}

	uninstalled, err := deriveLifecycleAgentUnitSignature(intended, paths, managedWiringIntent(paths, SystemdUnits{}, false), nil)
	if err != nil {
		t.Fatalf("derive uninstall signature: %v", err)
	}
	if len(uninstalled.DropIns) != 0 || len(uninstalled.EnvironmentFiles) != 0 || uninstalled.Fragment != before.Fragment || uninstalled.ExecStart != before.ExecStart {
		t.Fatalf("derived uninstall signature = %+v", uninstalled)
	}
}

func TestDeriveIntendedAgentUnitSignatureCanonicalizesEnvironmentAttestations(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		t.Fatalf("render systemd units: %v", err)
	}
	foreignEnvironment := filepath.Join(home, "zz-foreign.env")
	if err := atomicWriteFile(foreignEnvironment, []byte("FOREIGN=1\n"), 0o600); err != nil {
		t.Fatalf("write foreign environment: %v", err)
	}
	before := agentUnitSignatureForTest(t, config)
	before.DropIns = append(before.DropIns, LifecycleFileAttestation{
		Path:   filepath.Join(config.SystemdDir, config.AgentUnit+".d", "70-foreign.conf"),
		SHA256: "sha256:" + strings.Repeat("d", 64),
	})
	before.EnvironmentFiles = append(before.EnvironmentFiles, lifecycleAttestationForTest(t, foreignEnvironment))
	agentEnvironment := []byte("COMPUTE_GITHUB_RUNNER_PROVIDER_URL=https://provider\n")
	intended, err := deriveLifecycleAgentUnitSignature(before, paths, managedWiringIntent(paths, units, true), &LifecycleFileAttestation{
		Path: paths.AgentEnv, SHA256: digestBytes(agentEnvironment),
	})
	if err != nil {
		t.Fatalf("derive install signature: %v", err)
	}
	want := []string{paths.AgentEnv, foreignEnvironment}
	got := make([]string, 0, len(intended.EnvironmentFiles))
	for _, attestation := range intended.EnvironmentFiles {
		got = append(got, attestation.Path)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("canonical environment attestations = %q want %q", got, want)
	}
}

func lifecycleAttestationForTest(t *testing.T, path string) LifecycleFileAttestation {
	t.Helper()
	digest, err := hashRegularFile(path, false)
	if err != nil {
		t.Fatalf("hash lifecycle attestation: %v", err)
	}
	return LifecycleFileAttestation{Path: path, SHA256: digest}
}

func validLifecycleJournalForTest(config Config, now time.Time) LifecycleJournal {
	return LifecycleJournal{
		ProtocolVersion: LifecycleJournalProtocolVersion,
		TransactionID:   "install-transaction-123",
		Operation:       LifecycleInstall,
		Phase:           LifecycleIntent,
		ProviderEffect:  ProviderChanged,
		Identity: LifecycleIdentity{
			WorkerID: config.WorkerID, ProfileID: config.ProfileID,
			PluginID: config.PluginID, ComponentID: config.ComponentID,
		},
		Recovery: LifecycleRecoveryAuthority{
			Config:           config,
			ComputeAgent:     LifecycleFileAttestation{Path: config.ComputeAgentPath, SHA256: "sha256:" + strings.Repeat("a", 64)},
			SupervisorConfig: LifecycleFileAttestation{Path: config.SupervisorConfigPath, SHA256: "sha256:" + strings.Repeat("b", 64)},
			AgentUnitBefore: LifecycleSystemdSignature{
				Fragment:  LifecycleFileAttestation{Path: agentUnitFragmentPathForTest(config), SHA256: "sha256:" + strings.Repeat("c", 64)},
				ExecStart: staticExecStartForTest(config),
			},
		},
		Audit:     LifecycleAuditQueue{NextSequence: 1},
		StartedAt: now,
		UpdatedAt: now,
	}
}

func agentUnitFragmentPathForTest(config Config) string {
	return filepath.Join(lifecycleHome(LifecyclePathsFor(config)), ".config", "systemd", "user", config.AgentUnit)
}

func agentUnitSignatureForTest(t *testing.T, config Config) LifecycleSystemdSignature {
	t.Helper()
	paths := LifecyclePathsFor(config)
	signature := LifecycleSystemdSignature{
		Fragment:  lifecycleAttestationForTest(t, agentUnitFragmentPathForTest(config)),
		ExecStart: staticExecStartForTest(config),
	}
	if _, err := os.Lstat(paths.AgentDropIn); err == nil {
		signature.DropIns = append(signature.DropIns, lifecycleAttestationForTest(t, paths.AgentDropIn))
	} else if !os.IsNotExist(err) {
		t.Fatalf("inspect agent drop-in: %v", err)
	}
	if _, err := os.Lstat(paths.AgentEnv); err == nil {
		signature.EnvironmentFiles = append(signature.EnvironmentFiles, lifecycleAttestationForTest(t, paths.AgentEnv))
	} else if !os.IsNotExist(err) {
		t.Fatalf("inspect agent environment: %v", err)
	}
	return signature
}

func agentUnitSystemdOutputForTest(t *testing.T, config Config) []byte {
	t.Helper()
	output, err := agentUnitSystemdOutput(config)
	if err != nil {
		t.Fatalf("build agent unit systemd output: %v", err)
	}
	return output
}

func agentUnitSystemdOutput(config Config) ([]byte, error) {
	fragmentDigest, err := hashRegularFile(agentUnitFragmentPathForTest(config), false)
	if err != nil {
		return nil, err
	}
	paths := LifecyclePathsFor(config)
	signature := LifecycleSystemdSignature{
		Fragment: LifecycleFileAttestation{Path: agentUnitFragmentPathForTest(config), SHA256: fragmentDigest},
	}
	if _, err := os.Lstat(paths.AgentDropIn); err == nil {
		digest, hashErr := hashRegularFile(paths.AgentDropIn, false)
		if hashErr != nil {
			return nil, hashErr
		}
		signature.DropIns = append(signature.DropIns, LifecycleFileAttestation{Path: paths.AgentDropIn, SHA256: digest})
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	dropIns := make([]string, 0, len(signature.DropIns))
	for _, attestation := range signature.DropIns {
		dropIns = append(dropIns, attestation.Path)
	}
	return []byte(strings.Join([]string{
		"LoadState=loaded",
		"FragmentPath=" + signature.Fragment.Path,
		"DropInPaths=" + strings.Join(dropIns, " "),
	}, "\n") + "\n"), nil
}

func staticExecStartForTest(config Config) string {
	return "[" + strconv.Quote(config.ComputeAgentPath+" run") + "]"
}
