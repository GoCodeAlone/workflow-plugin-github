package retainedprovider

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"
)

type recordingCommandRunner struct {
	commands []Command
	run      func(context.Context, Command) ([]byte, error)
	exec     func(Command) error
}

func (runner *recordingCommandRunner) Run(ctx context.Context, command Command) ([]byte, error) {
	runner.commands = append(runner.commands, command)
	if runner.run == nil {
		return nil, nil
	}
	return runner.run(ctx, command)
}

func (runner *recordingCommandRunner) Exec(command Command) error {
	runner.commands = append(runner.commands, command)
	if runner.exec == nil {
		return nil
	}
	return runner.exec(command)
}

func TestVerifyCurrentUpdateUsesExactComputeAgentCommandAndStrictProjection(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)
	runner := &recordingCommandRunner{}
	runner.run = func(_ context.Context, _ Command) ([]byte, error) {
		return testVerifiedUpdateJSON(config, payload, digest), nil
	}

	update, err := VerifyCurrentUpdate(t.Context(), config, runner)
	if err != nil {
		t.Fatalf("verify current update: %v", err)
	}
	wantArgs := []string{
		"supervisor-update", "verify",
		"-config", config.SupervisorConfigPath,
		"-format", "auto",
		"-component", "provider",
		"-plugin", GitHubPluginID,
		"-component-id", config.ComponentID,
	}
	if len(runner.commands) != 1 || runner.commands[0].Path != config.ComputeAgentPath || !reflect.DeepEqual(runner.commands[0].Args, wantArgs) {
		t.Fatalf("verify commands = %+v want path=%q args=%q", runner.commands, config.ComputeAgentPath, wantArgs)
	}
	if update.WorkerID != config.WorkerID || update.ComponentID != config.ComponentID || update.Path != payload || update.SHA256 != digest {
		t.Fatalf("verified update = %+v", update)
	}

	runner.commands = nil
	runner.run = func(_ context.Context, _ Command) ([]byte, error) {
		data := testVerifiedUpdateJSON(config, payload, digest)
		return append(data[:len(data)-2], []byte(`,"unexpected":true}`+"\n")...), nil
	}
	if _, err := VerifyCurrentUpdate(t.Context(), config, runner); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown verify field err = %v", err)
	}
}

func TestVerifyCurrentUpdateRejectsIdentityAndDigestMismatch(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)

	for _, tc := range []struct {
		name   string
		mutate func(*Config, *string, *string)
		want   string
	}{
		{name: "worker", mutate: func(c *Config, _, _ *string) { c.WorkerID = "other-worker" }, want: "worker_id"},
		{name: "component", mutate: func(c *Config, _, _ *string) { c.ComponentID = "other-component" }, want: "component_id"},
		{name: "digest", mutate: func(_ *Config, _ *string, d *string) { *d = "sha256:" + strings.Repeat("f", 64) }, want: "digest"},
		{name: "path", mutate: func(_ *Config, p, _ *string) { *p = filepath.Join(home, "missing-provider") }, want: "path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectedConfig := config
			outputPath, outputDigest := payload, digest
			tc.mutate(&expectedConfig, &outputPath, &outputDigest)
			runner := &recordingCommandRunner{run: func(_ context.Context, _ Command) ([]byte, error) {
				return testVerifiedUpdateJSON(expectedConfig, outputPath, outputDigest), nil
			}}
			if _, err := VerifyCurrentUpdate(t.Context(), config, runner); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyCurrentUpdate err = %v want %q", err, tc.want)
			}
		})
	}
}

func TestInitialRefreshRequiresInstallerDigestMatch(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	if err := os.MkdirAll(config.InstallRoot, 0o700); err != nil {
		t.Fatalf("mkdir install root: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)
	other := writeTestProviderPayload(t, home, "different-installer")
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return other, nil },
		Now:            func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "installer digest") {
		t.Fatalf("initial refresh err = %v", err)
	}
	if len(runner.commands) != 1 || runner.commands[0].Path != config.ComputeAgentPath {
		t.Fatalf("refresh mutated runtime before self-digest check: %+v", runner.commands)
	}
}

func TestInitialRefreshRevalidatesInstallerAfterVerifiedUpdateChanges(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	if err := os.MkdirAll(config.InstallRoot, 0o700); err != nil {
		t.Fatalf("mkdir install root: %v", err)
	}
	initial := writeTestProviderPayload(t, home, "verified-provider-initial")
	initialDigest := fileDigestForTest(t, initial)
	replacement := writeTestProviderPayload(t, home, "verified-provider-replacement")
	replacementDigest := fileDigestForTest(t, replacement)
	runner := refreshTestRunner(config, initial, initialDigest)
	baseRun := runner.run
	verifyCalls := 0
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.ComputeAgentPath && containsAdjacentArgs(command.Args, "supervisor-update", "verify") {
			verifyCalls++
			if verifyCalls == 2 {
				return testVerifiedUpdateJSON(config, replacement, replacementDigest), nil
			}
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return initial, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "installer digest") {
		t.Fatalf("changed initial verified update err = %v", err)
	}
	if verifyCalls != 2 || len(runner.commands) != 2 {
		t.Fatalf("changed initial verified update mutated runtime: verify_calls=%d commands=%+v", verifyCalls, runner.commands)
	}
}

func TestRefreshBoundsEverySubprocessContext(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-bounded-refresh-commands")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	var unbounded []Command
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if _, bounded := ctx.Deadline(); !bounded {
			unbounded = append(unbounded, command)
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if len(unbounded) != 0 {
		t.Fatalf("refresh issued unbounded subprocesses: %s", commandTranscript(unbounded))
	}
}

func TestRefreshFencesAgentDuringProviderMutation(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-fenced-refresh")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	statuses := []string{"unavailable", "unavailable", "idle"}
	var events []string
	maintenanceID := ""
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		switch installCommandEvent(command, config) {
		case "maintenance-begin":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleFencing {
				t.Fatalf("maintenance begin lifecycle journal = %+v found=%v err=%v", journal, found, err)
			}
			events = append(events, "maintenance-begin")
			maintenanceID = journal.TransactionID
			return maintenanceStateJSON(true, maintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "maintenance-status":
			events = append(events, "maintenance-status")
			return maintenanceStateJSON(true, maintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "maintenance-end":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleReleasing || journal.Outcome != LifecycleCommit || journal.ProviderTransaction == nil {
				t.Fatalf("maintenance end lifecycle journal = %+v found=%v err=%v", journal, found, err)
			}
			inner, innerFound, err := readTransactionJournal(paths.Journal)
			if err != nil || !innerFound || inner.Phase != JournalCommitted || inner.OuterTransactionID != journal.TransactionID || inner.ProfileID != config.ProfileID {
				t.Fatalf("maintenance end provider journal = %+v found=%v err=%v", inner, innerFound, err)
			}
			events = append(events, "maintenance-end")
			return maintenanceStateJSON(false, maintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "local-status":
			if len(statuses) == 0 {
				t.Fatal("unexpected extra local status read")
			}
			state := statuses[0]
			statuses = statuses[1:]
			events = append(events, "local-"+state)
			return localStatusJSON(config.WorkerID, state), nil
		case "agent-stop":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleFenced {
				t.Fatalf("agent stop lifecycle journal = %+v found=%v err=%v", journal, found, err)
			}
			events = append(events, "agent-stop")
		case "agent-start":
			events = append(events, "agent-start")
		}
		if isCandidateStart(command, config) {
			events = append(events, "candidate-start")
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	assertOrderedEvents(t, events, []string{
		"maintenance-begin", "local-unavailable", "agent-stop", "candidate-start",
		"agent-start", "local-unavailable", "maintenance-end", "local-idle",
	})
	if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
		t.Fatalf("completed refresh lifecycle journal found=%v err=%v", found, err)
	}
	if _, found, err := readTransactionJournal(paths.Journal); err != nil || found {
		t.Fatalf("completed refresh provider journal found=%v err=%v", found, err)
	}
}

func TestSameDigestRefreshHealthCheckDoesNotFenceAgent(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-same-digest")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-same", testProviderImageID, now)
	active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	originalRun := runner.run
	sawJournaledProbe := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if isProbeFor(command, config.StableContainer) {
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Operation != LifecycleRefresh || journal.Phase != LifecycleIntent || journal.ProviderEffect != ProviderUnchanged || journal.Unchanged == nil || journal.Unchanged.Active.Update.SHA256 != digest || journal.Unchanged.Candidate.SHA256 != digest || !journal.Unchanged.StableProbeAt.IsZero() {
				t.Fatalf("same-digest probe lifecycle = %+v found=%v err=%v", journal, found, err)
			}
			sawJournaledProbe = true
		}
		return originalRun(ctx, command)
	}
	refresher := Refresher{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return payload, nil },
		Sleep:          func(context.Context, time.Duration) error { return nil },
	}
	if _, err := refresher.Refresh(t.Context(), config); err != nil {
		t.Fatalf("same-digest refresh: %v", err)
	}
	transcript := commandTranscript(runner.commands)
	for _, forbidden := range []string{"supervisor-maintenance", "stop " + config.AgentUnit, "start " + config.AgentUnit} {
		if strings.Contains(transcript, forbidden) {
			t.Fatalf("same-digest health check fenced agent with %q:\n%s", forbidden, transcript)
		}
	}
	if !sawJournaledProbe {
		t.Fatal("same-digest stable probe did not carry lifecycle provenance")
	}
	if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
		t.Fatalf("completed same-digest journal found=%v err=%v", found, err)
	}
}

func TestSameDigestRefreshRepairsMissingActiveImageUnderFence(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-runtime-repair")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-repair", testProviderImageID, now)
	if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}); err != nil {
		t.Fatalf("write active state: %v", err)
	}

	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	imageBuilt := false
	sawRepairJournal := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && !imageBuilt {
			return nil, nil
		}
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			journal, found, err := readTransactionJournalForConfig(paths.Journal, config)
			if err != nil || !found || !journal.RuntimeRepair || journal.Previous == nil || journal.Candidate.Update.SHA256 != journal.Previous.Current.Update.SHA256 {
				t.Fatalf("runtime repair journal = %+v found=%v err=%v", journal, found, err)
			}
			sawRepairJournal = true
			imageBuilt = true
		}
		return baseRun(ctx, command)
	}

	status, err := (Refresher{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Sleep: func(context.Context, time.Duration) error { return nil },
	}).Refresh(t.Context(), config)
	if err != nil {
		t.Fatalf("repair missing active image: %v\n%s", err, commandTranscript(runner.commands))
	}
	if !sawRepairJournal || status.CurrentSHA256 != digest {
		t.Fatalf("runtime repair status=%+v journaled=%v", status, sawRepairJournal)
	}
	transcript := commandTranscript(runner.commands)
	for _, required := range []string{
		"supervisor-maintenance begin", "systemctl --user stop " + config.AgentUnit,
		"podman build", config.CandidateContainer + "-probe", config.StableContainer + "-probe",
		"supervisor-maintenance end",
	} {
		if !strings.Contains(transcript, required) {
			t.Fatalf("runtime repair transcript missing %q:\n%s", required, transcript)
		}
	}
	repaired, found, err := readActiveStateForConfig(paths.ActiveState, config)
	if err != nil || !found || repaired.Current.Update.SHA256 != digest || repaired.Current.ImageID != testProviderImageID || repaired.Previous != nil {
		t.Fatalf("repaired active state = %+v found=%v err=%v", repaired, found, err)
	}
}

func TestSameDigestImageLossRaceRestartsThroughFenceBeforeRepair(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-raced-runtime-repair")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-raced-repair", testProviderImageID, now)
	if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}); err != nil {
		t.Fatalf("write active state: %v", err)
	}

	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	imageChecks := 0
	imageBuilt := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && !imageBuilt {
			imageChecks++
			if imageChecks == 1 {
				return ownedProviderImageInventory(config, digest, testProviderImageID), nil
			}
			return nil, nil
		}
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			imageBuilt = true
		}
		return baseRun(ctx, command)
	}
	if _, err := (Refresher{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Sleep: func(context.Context, time.Duration) error { return nil },
	}).Refresh(t.Context(), config); err != nil {
		t.Fatalf("repair raced image loss: %v\n%s", err, commandTranscript(runner.commands))
	}
	transcript := commandTranscript(runner.commands)
	maintenance := strings.Index(transcript, "supervisor-maintenance begin")
	build := strings.Index(transcript, "podman build")
	if maintenance < 0 || build < 0 || maintenance > build {
		t.Fatalf("raced runtime repair was not fenced before build:\n%s", transcript)
	}
}

func TestSameDigestOwnershipDriftRequiresRuntimeRepair(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	payload := writeTestProviderPayload(t, home, "verified-provider-ownership-repair")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-ownership-repair", testProviderImageID, now)
	if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" {
			return []byte(testProviderImageID + "\t" + managedProviderValue + "\tother-worker\t" + providerImageRole + "\t" + digest + "\n"), nil
		}
		return baseRun(ctx, command)
	}
	repair, err := (Refresher{Runner: runner}).requiresMutation(t.Context(), config, paths)
	if err != nil || !repair {
		t.Fatalf("ownership drift repair=%v err=%v", repair, err)
	}
}

func TestFailedSameDigestRepairRemovesImageButRetainsVerifiedPackage(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-failed-repair")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-failed-repair", testProviderImageID, now)
	original := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}
	if err := AtomicWriteJSON(paths.ActiveState, original); err != nil {
		t.Fatalf("write active state: %v", err)
	}

	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	imageBuilt := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && !imageBuilt {
			return nil, nil
		}
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			imageBuilt = true
		}
		if isProbeFor(command, config.CandidateContainer) {
			return nil, errors.New("candidate repair probe failed")
		}
		return baseRun(ctx, command)
	}
	_, err := (Refresher{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Sleep: func(context.Context, time.Duration) error { return nil },
	}).Refresh(t.Context(), config)
	if err == nil || !strings.Contains(err.Error(), "candidate repair probe failed") {
		t.Fatalf("failed runtime repair err = %v", err)
	}
	repairErr := err
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "image rm --ignore "+testProviderImageID) {
		t.Fatalf("failed runtime repair retained rebuilt image:\n%s", transcript)
	}
	if strings.Contains(transcript, "probe -url "+config.ProviderURL) {
		t.Fatalf("failed runtime repair probed the known-missing prior image:\n%s", transcript)
	}
	if _, err := os.Stat(paths.PackageBinary(digest)); err != nil {
		t.Fatalf("failed runtime repair removed verified package: %v", err)
	}
	restored, found, err := readActiveStateForConfig(paths.ActiveState, config)
	if err != nil || !found || restored.Current != original.Current {
		t.Fatalf("failed runtime repair active state = %+v found=%v err=%v", restored, found, err)
	}
	if _, found, err := readTransactionJournal(paths.Journal); err != nil || found {
		t.Fatalf("failed runtime repair provider journal found=%v err=%v", found, err)
	}
	if _, found, err := readLifecycleJournal(home, paths); err != nil || found {
		t.Fatalf("failed runtime repair lifecycle journal found=%v err=%v repair_err=%v", found, err, repairErr)
	}
}

func TestNormalizePodmanImageIDCanonicalizesOnlyImmutableSHA256(t *testing.T) {
	hexDigest := strings.Repeat("a", 64)
	for _, input := range []string{hexDigest, "sha256:" + hexDigest, "\n" + hexDigest + "\n"} {
		got, err := normalizePodmanImageID(input)
		if err != nil || got != "sha256:"+hexDigest {
			t.Fatalf("normalize %q = %q err=%v", input, got, err)
		}
	}
	for _, input := range []string{"", strings.Repeat("a", 63), strings.Repeat("A", 64), "sha512:" + hexDigest, hexDigest + " extra"} {
		if got, err := normalizePodmanImageID(input); err == nil {
			t.Fatalf("normalize invalid %q = %q", input, got)
		}
	}
}

func TestRefreshRenewsExpiringTLSUnderFenceWithoutPackageChange(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-tls-renewal")
	digest := fileDigestForTest(t, payload)
	issuedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	material, err := GenerateInstallMaterial(config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}, bytes.NewReader(bytes.Repeat([]byte{0x47}, 4096)), issuedAt)
	if err != nil {
		t.Fatalf("generate install material: %v", err)
	}
	if err := WriteInstallMaterial(paths, material); err != nil {
		t.Fatalf("write install material: %v", err)
	}
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create provider state: %v", err)
	}
	selection := selectionForDigest(payload, digest, "v1.0.32", "directive-1", testProviderImageID, issuedAt)
	if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: issuedAt}); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	renewedAt := issuedAt.Add(350 * 24 * time.Hour)
	status, err := (Refresher{
		Runner: runner, Random: bytes.NewReader(bytes.Repeat([]byte{0x48}, 4096)),
		Now: func() time.Time { return renewedAt }, Sleep: func(context.Context, time.Duration) error { return nil },
	}).Refresh(t.Context(), config)
	if err != nil {
		t.Fatalf("refresh expiring TLS: %v\n%s", err, commandTranscript(runner.commands))
	}
	if status.CurrentSHA256 != digest {
		t.Fatalf("TLS-only refresh status = %+v", status)
	}
	transcript := commandTranscript(runner.commands)
	for _, want := range []string{
		"supervisor-maintenance begin", "systemctl --user stop " + config.AgentUnit,
		"systemctl --user restart " + providerServiceUnit, config.StableContainer + "-probe",
		"systemctl --user start " + config.AgentUnit, "supervisor-maintenance end",
	} {
		if !strings.Contains(transcript, want) {
			t.Fatalf("TLS renewal transcript missing %q:\n%s", want, transcript)
		}
	}
	if strings.Contains(transcript, "podman build") {
		t.Fatalf("TLS-only refresh rebuilt provider image:\n%s", transcript)
	}
	renewedPEM, err := os.ReadFile(paths.ServerCert)
	if err != nil || bytes.Equal(renewedPEM, material.ServerCert) {
		t.Fatalf("server certificate was not renewed: err=%v", err)
	}
}

func TestRefreshRejectsProviderNetworkWithoutDNSBeforeBuild(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-network")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && containsAdjacentArgs(command.Args, "network", "inspect") {
			return []byte("bridge false false\n"), nil
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "DNS") {
		t.Fatalf("refresh network err = %v", err)
	}
	for _, command := range runner.commands {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			t.Fatalf("refresh built image before network validation: %+v", runner.commands)
		}
	}
}

func TestRefreshBuildsAndPreflightsIsolatedCandidateThenStable(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_000_000, 0).UTC()
	writeRefreshEnvironmentFiles(t, paths, now)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "ownership.json"), []byte(`{"owner":"stg"}`), 0o600); err != nil {
		t.Fatalf("write provider state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "--user", "stop") && containsArg(command.Args, providerServiceUnit) {
			if err := os.WriteFile(filepath.Join(paths.ProviderState, "ownership.json"), []byte(`{"owner":"quiesced"}`), 0o600); err != nil {
				t.Fatalf("write quiesced provider state: %v", err)
			}
		}
		if isCandidateStart(command, config) {
			if data, err := os.ReadFile(filepath.Join(paths.CandidateState(digest), "ownership.json")); err != nil || string(data) != `{"owner":"quiesced"}` {
				t.Fatalf("candidate cloned live state before quiesce: data=%q err=%v", data, err)
			}
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "ownership.json"), []byte(`{"owner":"migrated"}`), 0o600); err != nil {
				t.Fatalf("mutate candidate state: %v", err)
			}
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return payload, nil },
		Now:            func() time.Time { return now },
	}
	status, err := refresher.Refresh(t.Context(), config)
	if err != nil {
		t.Fatalf("refresh: %v\ncommands=%+v", err, runner.commands)
	}
	if !status.Installed || !status.ServiceActive || status.CurrentSHA256 != digest || status.ObservedAt != now {
		t.Fatalf("refresh status = %+v", status)
	}
	var active ActiveState
	if err := ReadStrictJSONFile(paths.ActiveState, &active); err != nil {
		t.Fatalf("read active state: %v", err)
	}
	if active.Current.Update.SHA256 != digest || active.Current.ImageID != testProviderImageID {
		t.Fatalf("active state = %+v", active)
	}
	if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "ownership.json")); err != nil || string(data) != `{"owner":"migrated"}` {
		t.Fatalf("candidate state was not promoted: data=%q err=%v", data, err)
	}
	var buildCommand *Command
	for index := range runner.commands {
		if runner.commands[index].Path == config.PodmanPath && firstArg(runner.commands[index].Args) == "build" {
			buildCommand = &runner.commands[index]
			break
		}
	}
	if buildCommand == nil {
		t.Fatal("refresh did not build provider image")
	}
	for _, label := range []string{
		"io.workflow.compute.managed=github-runner-provider",
		"io.workflow.compute.worker=" + config.WorkerID,
		"io.workflow.compute.role=provider-image",
		"io.workflow.compute.digest=" + digest,
	} {
		if !containsAdjacentArgs(buildCommand.Args, "--label", label) {
			t.Fatalf("provider image build missing ownership label %q: %+v", label, buildCommand.Args)
		}
	}
	if _, err := os.Stat(paths.CandidateState(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("promoted candidate state remains at staging path: %v", err)
	}
	assertRefreshCommandIsolation(t, runner.commands, config, paths)

	before := len(runner.commands)
	status, err = refresher.Refresh(t.Context(), config)
	if err != nil || status.CurrentSHA256 != digest {
		t.Fatalf("idempotent refresh status=%+v err=%v", status, err)
	}
	idempotentCommands := runner.commands[before:]
	idempotentTranscript := commandTranscript(idempotentCommands)
	for _, required := range []string{
		"systemctl --user show " + providerServiceUnit + " --property ActiveState --value",
		"probe -url " + config.ProviderURL,
	} {
		if !strings.Contains(idempotentTranscript, required) {
			t.Fatalf("digest-idempotent refresh skipped health check %q:\n%s", required, idempotentTranscript)
		}
	}
	for _, forbidden := range []string{"podman build", "systemctl --user restart", config.CandidateContainer + " "} {
		if strings.Contains(idempotentTranscript, forbidden) {
			t.Fatalf("digest-idempotent refresh mutated runtime with %q:\n%s", forbidden, idempotentTranscript)
		}
	}
}

func TestDigestIdempotentRefreshFailsWhenStableServiceIsInactive(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-idempotent-health")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	active := ActiveState{
		ProtocolVersion: ActiveStateProtocolVersion,
		Current:         selectionForDigest(payload, digest, "v1.0.32", "directive-current", testProviderImageID, time.Unix(1_700_000_000, 0).UTC()),
		UpdatedAt:       time.Unix(1_700_000_000, 0).UTC(),
	}
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "--property", "ActiveState") {
			return []byte("inactive\n"), nil
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "not active") {
		t.Fatalf("idempotent inactive refresh err = %v", err)
	}
	if strings.Contains(commandTranscript(runner.commands), "podman build") {
		t.Fatalf("inactive idempotent refresh rebuilt unchanged image:\n%s", commandTranscript(runner.commands))
	}
}

func TestDeferredRefreshRetainsRollbackStateUntilInstallerFinalizes(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-deferred-refresh")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.refreshUnderLifecycleLock(t.Context(), config, false, true); err != nil {
		t.Fatalf("deferred refresh: %v", err)
	}
	journal, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found || journal.Phase != JournalCommitted || !journal.DeferredCommit {
		t.Fatalf("deferred journal = %+v found=%v err=%v", journal, found, err)
	}
	if _, err := os.Stat(paths.PreviousState(digest)); err != nil {
		t.Fatalf("deferred refresh removed rollback state: %v", err)
	}
	staleDigest := "sha256:" + strings.Repeat("d", 64)
	if err := os.MkdirAll(paths.PackageDir(staleDigest), 0o700); err != nil {
		t.Fatalf("create stale provider package: %v", err)
	}
	if err := os.WriteFile(paths.PackageBinary(staleDigest), []byte("stale"), 0o700); err != nil {
		t.Fatalf("write stale provider package: %v", err)
	}
	if err := refresher.finalizeDeferredRefresh(config); err != nil {
		t.Fatalf("finalize deferred refresh: %v", err)
	}
	if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized journal remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.CandidatesRoot, digestHex(digest))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized rollback state remains: %v", err)
	}
	if _, err := os.Stat(paths.PackageDir(staleDigest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized superseded package remains: %v", err)
	}
	if !strings.Contains(commandTranscript(runner.commands), "image rm --ignore "+testProviderImageID) {
		t.Fatalf("finalize did not remove superseded image: %s", commandTranscript(runner.commands))
	}
}

func TestDeferredRefreshBindsOuterLifecycleTransaction(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-bound-refresh")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.refreshUnderLifecycleTransaction(t.Context(), config, false, true, "install-transaction-123", config.ProfileID, "", ""); err != nil {
		t.Fatalf("bound deferred refresh: %v", err)
	}
	journal, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found {
		t.Fatalf("read bound provider journal found=%v err=%v", found, err)
	}
	if journal.OuterTransactionID != "install-transaction-123" || journal.ProfileID != config.ProfileID || !journal.DeferredCommit {
		t.Fatalf("provider transaction binding = %+v", journal)
	}
}

func TestRefreshRejectsGitHubCredentialInProbeEnvironment(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	if err := os.WriteFile(paths.ProbeEnv, []byte("GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret\nGITHUB_TOKEN=github-secret\n"), 0o600); err != nil {
		t.Fatalf("write invalid probe env: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "probe environment") {
		t.Fatalf("probe credential isolation err = %v", err)
	}
	if strings.Contains(commandTranscript(runner.commands), "github-secret") {
		t.Fatalf("command transcript leaked GitHub credential: %+v", runner.commands)
	}
}

func TestRefreshRejectsUnrelatedProviderEnvironmentVariable(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-provider-env")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	if err := os.WriteFile(paths.ProviderEnv, []byte("GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret\nGITHUB_RUNNER_PROVIDER_GITHUB_TOKEN=github-secret\nAWS_SECRET_ACCESS_KEY=unrelated-secret\n"), 0o600); err != nil {
		t.Fatalf("write invalid provider env: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "provider environment") {
		t.Fatalf("provider credential isolation err = %v", err)
	}
	if strings.Contains(commandTranscript(runner.commands), "unrelated-secret") {
		t.Fatalf("command transcript leaked unrelated credential: %+v", runner.commands)
	}
}

func TestRefreshRejectsIncompleteProviderEnvironment(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-incomplete-env")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	if err := os.WriteFile(paths.ProviderEnv, []byte("GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret\nGITHUB_RUNNER_PROVIDER_GITHUB_TOKEN=github-secret\n"), 0o600); err != nil {
		t.Fatalf("write incomplete provider env: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "provider environment") {
		t.Fatalf("incomplete provider environment err = %v", err)
	}
}

func TestProviderEnvironmentCannotBroadenConfiguredGitHubAuthority(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(string) string
	}{
		{name: "repository", mutate: func(value string) string {
			return strings.Replace(value, "GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute", "GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute,GoCodeAlone/other", 1)
		}},
		{name: "organization", mutate: func(value string) string {
			return strings.Replace(value, "GITHUB_RUNNER_PROVIDER_ORGANIZATIONS=GoCodeAlone", "GITHUB_RUNNER_PROVIDER_ORGANIZATIONS=GoCodeAlone,OtherOrg", 1)
		}},
		{name: "runner group", mutate: func(value string) string {
			return strings.Replace(value, "GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS=ephemeral", "GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS=ephemeral,Default", 1)
		}},
		{name: "api base url", mutate: func(value string) string {
			return value + "GITHUB_API_BASE_URL=https://github.example.invalid/api/v3\n"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			writeRefreshEnvironmentFiles(t, paths)
			environment, err := os.ReadFile(paths.ProviderEnv)
			if err != nil {
				t.Fatalf("read provider environment: %v", err)
			}
			if err := os.WriteFile(paths.ProviderEnv, []byte(tc.mutate(string(environment))), 0o600); err != nil {
				t.Fatalf("write broadened provider environment: %v", err)
			}
			if err := validateProviderEnvironment(config, paths.ProviderEnv); err == nil {
				t.Fatal("broadened provider environment was accepted")
			}
		})
	}
}

func TestRefreshFailurePreservesPreviousActiveImageAndCleansCandidate(t *testing.T) {
	for _, phase := range []string{"build", "stale-candidate", "stable-stop", "candidate", "candidate-probe", "stable-restart", "stable-probe", "canceled"} {
		t.Run(phase, func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			now := time.Unix(1_700_000_100, 0).UTC()
			writeRefreshEnvironmentFiles(t, paths, now)
			if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
				t.Fatalf("mkdir provider state: %v", err)
			}
			previous := previousActiveStateForTest(t, home)
			if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
				t.Fatalf("write previous active state: %v", err)
			}
			payload := writeTestProviderPayload(t, home, "verified-provider-v2")
			digest := fileDigestForTest(t, payload)
			runner := refreshTestRunner(config, payload, digest)
			baseRun := runner.run
			failedRestart := false
			failedStaleCleanup := false
			var rollbackProbeBudget time.Duration
			refreshContext := t.Context()
			cancelRefresh := func() {}
			if phase == "canceled" {
				refreshContext, cancelRefresh = context.WithCancel(t.Context())
			}
			runner.run = func(ctx context.Context, command Command) ([]byte, error) {
				if phase == "canceled" && isCandidateStart(command, config) {
					cancelRefresh()
					return nil, ctx.Err()
				}
				if phase == "build" && command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
					return nil, errors.New("build failed")
				}
				if phase == "candidate" && isCandidateStart(command, config) {
					return nil, errors.New("candidate failed")
				}
				if phase == "stale-candidate" && firstArg(command.Args) == "ps" && !failedStaleCleanup {
					failedStaleCleanup = true
					return nil, errors.New("stale candidate cleanup failed")
				}
				if phase == "candidate-probe" && isProbeFor(command, config.CandidateContainer) {
					return nil, errors.New("candidate probe failed")
				}
				if phase == "candidate-probe" && isProbeFor(command, config.StableContainer) {
					deadline, ok := ctx.Deadline()
					if !ok {
						t.Fatal("rollback stable probe has no deadline")
					}
					rollbackProbeBudget = time.Until(deadline)
				}
				if phase == "stable-stop" && filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "stop", providerServiceUnit) && !failedRestart {
					failedRestart = true
					return nil, errors.New("stop failed")
				}
				if phase == "stable-restart" && filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "restart", providerServiceUnit) && !failedRestart {
					failedRestart = true
					return nil, errors.New("restart failed")
				}
				if phase == "stable-probe" && isProbeFor(command, config.StableContainer) && containsArg(command.Args, testProviderImageID) {
					return nil, errors.New("stable probe failed")
				}
				return baseRun(ctx, command)
			}
			refresher := Refresher{
				Runner: runner, Now: func() time.Time { return now },
				Sleep: func(context.Context, time.Duration) error { return nil },
			}
			if _, err := refresher.Refresh(refreshContext, config); err == nil {
				t.Fatalf("%s refresh unexpectedly succeeded", phase)
			}
			var active ActiveState
			if err := ReadStrictJSONFile(paths.ActiveState, &active); err != nil {
				t.Fatalf("read active state after %s failure: %v", phase, err)
			}
			if active.Current.ImageID != previous.Current.ImageID || active.Current.Update.SHA256 != previous.Current.Update.SHA256 {
				t.Fatalf("%s failure replaced active state: got=%+v want=%+v", phase, active, previous)
			}
			if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s rollback journal remains: %v", phase, err)
			}
			transcript := commandTranscript(runner.commands)
			if !strings.Contains(transcript, "image rm --ignore "+testProviderImageID) {
				t.Fatalf("%s failure did not remove candidate image:\n%s", phase, transcript)
			}
			if strings.Contains(transcript, "image rm --ignore "+previous.Current.ImageRef) {
				t.Fatalf("%s failure removed retained image:\n%s", phase, transcript)
			}
			if _, err := os.Stat(paths.PackageDir(digest)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s failure retained candidate package: %v", phase, err)
			}
			if phase != "build" && !strings.Contains(transcript, "rm --force --ignore "+testCandidateContainerID) {
				t.Fatalf("%s failure did not clean candidate:\n%s", phase, transcript)
			}
			if phase == "candidate-probe" && rollbackProbeBudget < providerProbeTimeout-time.Second {
				t.Fatalf("rollback stable probe deadline = %s want approximately %s", rollbackProbeBudget, providerProbeTimeout)
			}
		})
	}
}

func TestRecoverInterruptedStagingRemovesCandidateArtifactsWithoutStoppingActive(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_000_100, 0).UTC()
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-staging-crash")
	digest := fileDigestForTest(t, payload)
	selection := selectionForDigest(payload, digest, "v1.0.32", "directive-staging-crash", "sha256:"+strings.Repeat("d", 64), now)
	if err := mkdirAllDurable(paths.PackageDir(digest), 0o700); err != nil {
		t.Fatalf("create staged package: %v", err)
	}
	if err := os.WriteFile(paths.PackageBinary(digest), []byte("candidate"), 0o700); err != nil {
		t.Fatalf("write staged package: %v", err)
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-staging-crash",
		Phase:           JournalStaging,
		Previous:        &previous,
		Candidate:       ImageSelection{Update: selection.Update},
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write staging journal: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" {
			return ownedProviderImageInventory(config, digest, testProviderImageID), nil
		}
		return nil, nil
	}}
	if err := (Refresher{Runner: runner}).recoverInterrupted(t.Context(), config, paths); err != nil {
		t.Fatalf("recover staged update: %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "image rm --ignore "+testProviderImageID) {
		t.Fatalf("staging recovery did not remove image:\n%s", transcript)
	}
	if strings.Contains(transcript, "systemctl --user stop "+providerServiceUnit) {
		t.Fatalf("staging recovery stopped active provider:\n%s", transcript)
	}
	if _, err := os.Stat(paths.PackageDir(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging recovery retained package: %v", err)
	}
	if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging recovery retained journal: %v", err)
	}
	active, found, err := readActiveStateForConfig(paths.ActiveState, config)
	if err != nil || !found || active.Current.ImageID != previous.Current.ImageID {
		t.Fatalf("staging recovery changed active state: found=%v state=%+v err=%v", found, active, err)
	}
}

func TestCleanupCandidateArtifactsRejectsSymlinkedPackagesRootWithoutTouchingTarget(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("symlink behavior varies on Windows")
	}
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	digest := "sha256:" + strings.Repeat("e", 64)
	outside := filepath.Join(t.TempDir(), "outside")
	sentinel := filepath.Join(outside, digestHex(digest), "sentinel")
	if err := os.MkdirAll(filepath.Dir(sentinel), 0o700); err != nil {
		t.Fatalf("create outside package: %v", err)
	}
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write outside sentinel: %v", err)
	}
	if err := mkdirAllDurable(filepath.Dir(paths.PackagesRoot), 0o700); err != nil {
		t.Fatalf("create install root: %v", err)
	}
	if err := os.Symlink(outside, paths.PackagesRoot); err != nil {
		t.Fatalf("symlink packages root: %v", err)
	}
	err := (Refresher{Runner: &recordingCommandRunner{}}).cleanupCandidateArtifacts(t.Context(), config, paths, ImageSelection{Update: VerifiedUpdate{SHA256: digest}}, false)
	if err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("symlinked package cleanup err = %v", err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
		t.Fatalf("outside package changed: data=%q err=%v", data, err)
	}
}

func TestFailedRefreshRetainsStagingJournalUntilArtifactCleanupSucceeds(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_000_100, 0).UTC()
	writeRefreshEnvironmentFiles(t, paths, now)
	if err := mkdirAllDurable(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-cleanup-retry")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if firstArg(command.Args) == "build" {
			return nil, errors.New("build failed")
		}
		if containsAdjacentArgs(command.Args, "image", "rm") {
			return nil, errors.New("image cleanup failed")
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, Now: func() time.Time { return now }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "image cleanup failed") {
		t.Fatalf("refresh cleanup failure err = %v", err)
	}
	journal, found, err := readTransactionJournalForConfig(paths.Journal, config)
	if err != nil || !found || journal.Phase != JournalRollbackRestored || journal.RollbackFrom != JournalStaging {
		t.Fatalf("retained rollback journal found=%v phase=%s from=%s err=%v", found, journal.Phase, journal.RollbackFrom, err)
	}
	if _, err := os.Stat(paths.PackageDir(digest)); err != nil {
		t.Fatalf("retry package missing before recovery: %v", err)
	}
	recoveryRunner := &recordingCommandRunner{}
	if err := (Refresher{Runner: recoveryRunner}).recoverInterrupted(t.Context(), config, paths); err != nil {
		t.Fatalf("retry staged cleanup: %v", err)
	}
	if _, err := os.Stat(paths.PackageDir(digest)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry retained candidate package: %v", err)
	}
	if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retry retained staging journal: %v", err)
	}
}

func TestCleanupCandidateArtifactsPreservesRetainedRollbackDigest(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_000_100, 0).UTC()
	active := previousActiveStateForTest(t, home)
	rollback := active.Current
	rollback.Update.DirectiveID = "directive-retained-rollback"
	rollback.Update.SHA256 = "sha256:" + strings.Repeat("e", 64)
	rollback.ImageID = "sha256:" + strings.Repeat("f", 64)
	rollback.ImageRef = providerImageRef(rollback.Update.SHA256)
	rollback.ActivatedAt = now.Add(-time.Hour)
	active.Previous = &rollback
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active rollback set: %v", err)
	}
	if err := mkdirAllDurable(paths.PackageDir(rollback.Update.SHA256), 0o700); err != nil {
		t.Fatalf("create retained rollback package: %v", err)
	}
	runner := &recordingCommandRunner{}
	if err := (Refresher{Runner: runner}).cleanupCandidateArtifacts(t.Context(), config, paths, rollback, false); err != nil {
		t.Fatalf("preserve retained rollback artifact: %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("retained rollback cleanup issued commands: %+v", runner.commands)
	}
	if _, err := os.Stat(paths.PackageDir(rollback.Update.SHA256)); err != nil {
		t.Fatalf("retained rollback package removed: %v", err)
	}
}

func TestGarbageCollectSupersededProviderPackagesAndImagesPreservesRollbackSet(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	digests := []string{
		"sha256:" + strings.Repeat("a", 64),
		"sha256:" + strings.Repeat("b", 64),
		"sha256:" + strings.Repeat("c", 64),
	}
	for _, digest := range digests {
		if err := os.MkdirAll(paths.PackageDir(digest), 0o700); err != nil {
			t.Fatalf("create provider package: %v", err)
		}
		if err := os.WriteFile(paths.PackageBinary(digest), []byte(digest), 0o700); err != nil {
			t.Fatalf("write provider package: %v", err)
		}
	}
	current := validTestSelection(time.Unix(1_700_000_000, 0).UTC())
	current.Update.SHA256 = digests[0]
	current.ImageRef = providerImageRef(digests[0])
	previous := current
	previous.Update.SHA256 = digests[1]
	previous.ImageRef = providerImageRef(digests[1])
	active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: current, Previous: &previous, UpdatedAt: current.ActivatedAt}

	failRemoval := true
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" {
			return ownedProviderImageInventory(config, digests[2], testProviderImageID), nil
		}
		if command.Path == config.PodmanPath && containsAdjacentArgs(command.Args, "image", "rm") {
			if _, err := os.Stat(paths.PackageDir(digests[2])); err != nil {
				t.Fatalf("stale package removed before image: %v", err)
			}
			if failRemoval {
				return nil, errors.New("image removal interrupted")
			}
		}
		return nil, nil
	}}
	refresher := Refresher{Runner: runner}
	if err := refresher.garbageCollectSupersededProviders(t.Context(), config, paths, active); err == nil || !strings.Contains(err.Error(), "image removal") {
		t.Fatalf("interrupted provider GC err = %v", err)
	}
	if _, err := os.Stat(paths.PackageDir(digests[2])); err != nil {
		t.Fatalf("interrupted provider GC removed package: %v", err)
	}
	failRemoval = false
	if err := refresher.garbageCollectSupersededProviders(t.Context(), config, paths, active); err != nil {
		t.Fatalf("retry provider GC: %v", err)
	}
	for _, digest := range digests[:2] {
		if _, err := os.Stat(paths.PackageDir(digest)); err != nil {
			t.Fatalf("retained rollback package %s missing: %v", digest, err)
		}
	}
	if _, err := os.Stat(paths.PackageDir(digests[2])); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale provider package remains: %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "image rm --ignore "+testProviderImageID) || strings.Contains(transcript, providerImageRef(digests[0])) || strings.Contains(transcript, providerImageRef(digests[1])) {
		t.Fatalf("provider GC touched wrong images:\n%s", transcript)
	}
}

func TestStableProbeFailureRestoresPreviousProviderState(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	stateFile := filepath.Join(paths.ProviderState, "state.json")
	if err := os.WriteFile(stateFile, []byte(`{"generation":"previous"}`), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-state-rollback")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	providerStops := 0
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "--user", "stop") && containsArg(command.Args, providerServiceUnit) {
			providerStops++
		}
		if isCandidateStart(command, config) {
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "state.json"), []byte(`{"generation":"candidate"}`), 0o600); err != nil {
				t.Fatalf("mutate candidate state: %v", err)
			}
		}
		if isProbeFor(command, config.StableContainer) && containsArg(command.Args, testProviderImageID) {
			return nil, errors.New("stable probe failed")
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "stable probe failed") {
		t.Fatalf("stable probe failure err = %v", err)
	}
	if data, err := os.ReadFile(stateFile); err != nil || string(data) != `{"generation":"previous"}` {
		t.Fatalf("rollback state = %q err=%v", data, err)
	}
	if _, err := os.Stat(filepath.Join(paths.CandidatesRoot, digestHex(digest))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("state transaction remains after rollback: %v", err)
	}
	if providerStops != 2 {
		t.Fatalf("provider stop count = %d want promotion and rollback stops", providerStops)
	}
}

func TestCommitJournalWriteFailureRollsBackLastDurablePhase(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	stateFile := filepath.Join(paths.ProviderState, "state.json")
	if err := os.WriteFile(stateFile, []byte(`{"generation":"previous"}`), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-commit-journal-failure")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if isCandidateStart(command, config) {
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "state.json"), []byte(`{"generation":"candidate"}`), 0o600); err != nil {
				t.Fatalf("mutate candidate state: %v", err)
			}
		}
		return baseRun(ctx, command)
	}
	blockedCommit := false
	refresher := Refresher{
		Runner: runner,
		Sleep:  func(context.Context, time.Duration) error { return nil },
		writeJournalPhaseFn: func(path string, journal *TransactionJournal, phase JournalPhase, updatedAt time.Time) error {
			if phase == JournalCommitted && !blockedCommit {
				blockedCommit = true
				return errors.New("commit journal write failed")
			}
			return writeJournalPhase(path, journal, phase, updatedAt)
		},
	}
	if _, err := refresher.Refresh(t.Context(), config); err == nil {
		t.Fatal("refresh with failed commit-journal write succeeded")
	}
	if data, err := os.ReadFile(stateFile); err != nil || string(data) != `{"generation":"previous"}` {
		t.Fatalf("commit-write rollback state = %q err=%v", data, err)
	}
}

func TestRefreshRecoversEveryInterruptedJournalPhaseIdempotently(t *testing.T) {
	for _, phase := range []JournalPhase{JournalPrepared, JournalStatePromoting, JournalStateDetached, JournalStatePromoted, JournalActivated, JournalCommitted} {
		t.Run(string(phase), func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			writeRefreshEnvironmentFiles(t, paths)
			if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
				t.Fatalf("mkdir provider state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
				t.Fatalf("write previous provider state: %v", err)
			}
			previous := previousActiveStateForTest(t, home)
			candidatePayload := writeTestProviderPayload(t, home, "candidate-recovery")
			candidateDigest := fileDigestForTest(t, candidatePayload)
			candidate := selectionForDigest(candidatePayload, candidateDigest, "v1.0.32", "directive-candidate", "sha256:"+strings.Repeat("e", 64), time.Unix(1_700_000_100, 0).UTC())
			if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(candidateDigest)); err != nil {
				t.Fatalf("prepare candidate state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(paths.CandidateState(candidateDigest), "generation"), []byte("candidate"), 0o600); err != nil {
				t.Fatalf("write candidate provider state: %v", err)
			}
			switch phase {
			case JournalStatePromoting:
				if err := os.Rename(paths.ProviderState, paths.PreviousState(candidateDigest)); err != nil {
					t.Fatalf("simulate partial provider state promotion: %v", err)
				}
			case JournalStateDetached:
				if err := detachProviderState(paths, candidateDigest); err != nil {
					t.Fatalf("simulate detached provider state: %v", err)
				}
			case JournalStatePromoted, JournalActivated, JournalCommitted:
				if err := promoteCandidateProviderState(paths, candidateDigest); err != nil {
					t.Fatalf("simulate provider state promotion: %v", err)
				}
			}
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "refresh-recovery",
				Phase:           phase,
				Previous:        &previous,
				Candidate:       candidate,
				StartedAt:       time.Unix(1_700_000_100, 0).UTC(),
				UpdatedAt:       time.Unix(1_700_000_101, 0).UTC(),
			}
			if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
				t.Fatalf("write journal: %v", err)
			}
			if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: candidate, Previous: &previous.Current, UpdatedAt: journal.UpdatedAt}); err != nil {
				t.Fatalf("write interrupted active state: %v", err)
			}
			runner := &recordingCommandRunner{}
			refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
			if err := refresher.recoverInterrupted(t.Context(), config, paths); err != nil {
				t.Fatalf("recover %s: %v", phase, err)
			}
			if err := refresher.recoverInterrupted(t.Context(), config, paths); err != nil {
				t.Fatalf("idempotent recover %s: %v", phase, err)
			}
			var active ActiveState
			if err := ReadStrictJSONFile(paths.ActiveState, &active); err != nil {
				t.Fatalf("read recovered active: %v", err)
			}
			wantImage := previous.Current.ImageID
			if phase == JournalCommitted {
				wantImage = candidate.ImageID
			}
			if active.Current.ImageID != wantImage {
				t.Fatalf("%s recovered image = %s want %s", phase, active.Current.ImageID, wantImage)
			}
			wantGeneration := "previous"
			if phase == JournalCommitted {
				wantGeneration = "candidate"
			}
			if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "generation")); err != nil || string(data) != wantGeneration {
				t.Fatalf("%s recovered provider state = %q err=%v want %q", phase, data, err, wantGeneration)
			}
			if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s journal remains after recovery: %v", phase, err)
			}
		})
	}
}

func TestProviderStatePromotionPersistsEachCrossDirectoryRename(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := mkdirAllDurable(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous generation: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "candidate-durable-promotion")
	digest := fileDigestForTest(t, payload)
	candidate := paths.CandidateState(digest)
	if err := prepareCandidateState(paths.ProviderState, candidate); err != nil {
		t.Fatalf("prepare candidate: %v", err)
	}
	if err := os.WriteFile(filepath.Join(candidate, "generation"), []byte("candidate"), 0o600); err != nil {
		t.Fatalf("write candidate generation: %v", err)
	}

	var synced []string
	recordSync := func(path string) error {
		synced = append(synced, path)
		return nil
	}
	if err := detachProviderStateWithSync(paths, digest, recordSync); err != nil {
		t.Fatalf("detach provider state: %v", err)
	}
	transactionRoot := filepath.Dir(candidate)
	if want := []string{paths.Root, transactionRoot}; !reflect.DeepEqual(synced, want) {
		t.Fatalf("detach sync order = %v want %v", synced, want)
	}
	if _, err := os.Stat(paths.ProviderState); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("provider state remains after detach: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(paths.PreviousState(digest), "generation")); err != nil || string(data) != "previous" {
		t.Fatalf("detached generation = %q err=%v", data, err)
	}

	synced = nil
	if err := activateCandidateProviderStateWithSync(paths, digest, recordSync); err != nil {
		t.Fatalf("activate candidate state: %v", err)
	}
	if want := []string{transactionRoot, paths.Root}; !reflect.DeepEqual(synced, want) {
		t.Fatalf("activation sync order = %v want %v", synced, want)
	}
	if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "generation")); err != nil || string(data) != "candidate" {
		t.Fatalf("active generation = %q err=%v", data, err)
	}
}

func TestInterruptedRefreshStopsCandidateBeforeDeletingStagedState(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	payload := writeTestProviderPayload(t, home, "candidate-recovery-order")
	digest := fileDigestForTest(t, payload)
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-candidate-order", "sha256:"+strings.Repeat("e", 64), time.Unix(1_700_000_100, 0).UTC())
	if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
		t.Fatalf("prepare candidate state: %v", err)
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-recovery-order",
		Phase:           JournalPrepared,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       time.Unix(1_700_000_100, 0).UTC(),
		UpdatedAt:       time.Unix(1_700_000_101, 0).UTC(),
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write journal: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if firstArg(command.Args) == "rm" && containsArg(command.Args, testCandidateContainerID) {
			if _, err := os.Stat(paths.CandidateState(digest)); err != nil {
				return nil, errors.New("candidate state deleted before container stop")
			}
		}
		return nil, nil
	}}
	if err := (Refresher{Runner: runner}).recoverInterrupted(t.Context(), config, paths); err != nil {
		t.Fatalf("recover interrupted candidate: %v", err)
	}
}

func TestInterruptedRecoveryStopsStableBeforeProviderStateRestore(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	payload := writeTestProviderPayload(t, home, "candidate-recovery-stop-order")
	digest := fileDigestForTest(t, payload)
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-candidate-stop-order", "sha256:"+strings.Repeat("e", 64), time.Unix(1_700_000_100, 0).UTC())
	if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
		t.Fatalf("prepare candidate state: %v", err)
	}
	if err := promoteCandidateProviderState(paths, digest); err != nil {
		t.Fatalf("promote candidate state: %v", err)
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-recovery-stop-order",
		Phase:           JournalStatePromoted,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       time.Unix(1_700_000_100, 0).UTC(),
		UpdatedAt:       time.Unix(1_700_000_101, 0).UTC(),
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write journal: %v", err)
	}
	if err := os.Chmod(paths.Root, 0o500); err != nil {
		t.Fatalf("restrict provider root: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(paths.Root, 0o700) })
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "--user", "stop") && containsArg(command.Args, providerServiceUnit) {
			if err := os.Chmod(paths.Root, 0o700); err != nil {
				t.Fatalf("unlock provider root after stop: %v", err)
			}
		}
		return nil, nil
	}}
	if err := (Refresher{Runner: runner}).recoverInterrupted(t.Context(), config, paths); err != nil {
		t.Fatalf("recover interrupted provider state: %v", err)
	}
}

func TestRecoverInterruptedResumesAfterPreviousStateWasAlreadyRestored(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := mkdirAllDurable(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create restored provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
		t.Fatalf("write restored provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write restored active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "candidate-already-restored")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_100, 0).UTC()
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-already-restored", "sha256:"+strings.Repeat("e", 64), now)
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-already-restored",
		Phase:           JournalRollbackRestoring,
		RollbackFrom:    JournalStatePromoted,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       now,
		UpdatedAt:       now.Add(time.Second),
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write restoring journal: %v", err)
	}
	refresher := Refresher{Runner: &recordingCommandRunner{}, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := refresher.recoverInterrupted(t.Context(), config, paths); err != nil {
		t.Fatalf("resume restored rollback: %v", err)
	}
	if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed rollback journal remains: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "generation")); err != nil || string(data) != "previous" {
		t.Fatalf("recovered provider state = %q err=%v", data, err)
	}
}

func TestRollbackPersistsRestoredPhaseBeforeArtifactCleanup(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := mkdirAllDurable(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("create provider state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "generation"), []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	payload := writeTestProviderPayload(t, home, "candidate-cleanup-replay")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_000_100, 0).UTC()
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-cleanup-replay", "sha256:"+strings.Repeat("e", 64), now)
	if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
		t.Fatalf("prepare candidate: %v", err)
	}
	if err := promoteCandidateProviderState(paths, digest); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	if err := AtomicWriteJSON(paths.ActiveState, ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: candidate, Previous: &previous.Current, UpdatedAt: now}); err != nil {
		t.Fatalf("write candidate active state: %v", err)
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-cleanup-replay",
		Phase:           JournalStatePromoted,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       now,
		UpdatedAt:       now.Add(time.Second),
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write promoted journal: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" {
			return ownedProviderImageInventory(config, digest, candidate.ImageID), nil
		}
		if command.Path == config.PodmanPath && containsAdjacentArgs(command.Args, "image", "rm") {
			return nil, errors.New("candidate image cleanup failed")
		}
		return nil, nil
	}}
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := refresher.recoverInterrupted(t.Context(), config, paths); err == nil || !strings.Contains(err.Error(), "candidate image cleanup failed") {
		t.Fatalf("cleanup failure err = %v", err)
	}
	recovered, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found {
		t.Fatalf("read recoverable rollback journal: found=%v err=%v", found, err)
	}
	if recovered.Phase != JournalRollbackRestored || recovered.RollbackFrom != JournalStatePromoted {
		t.Fatalf("rollback journal = phase %s from %s", recovered.Phase, recovered.RollbackFrom)
	}
}

func TestRecoverInterruptedFinishesEveryPersistedRollbackPhase(t *testing.T) {
	for _, phase := range []JournalPhase{JournalRollbackRestored, JournalRollbackCleaned} {
		t.Run(string(phase), func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			previous := previousActiveStateForTest(t, home)
			if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
				t.Fatalf("write restored active state: %v", err)
			}
			payload := writeTestProviderPayload(t, home, "candidate-"+string(phase))
			digest := fileDigestForTest(t, payload)
			now := time.Unix(1_700_000_100, 0).UTC()
			candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-"+string(phase), "sha256:"+strings.Repeat("e", 64), now)
			if phase == JournalRollbackRestored {
				if err := stageVerifiedProvider(candidate.Update, paths); err != nil {
					t.Fatalf("stage candidate package: %v", err)
				}
			}
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "refresh-" + string(phase),
				Phase:           phase,
				RollbackFrom:    JournalStatePromoted,
				Previous:        &previous,
				Candidate:       candidate,
				StartedAt:       now,
				UpdatedAt:       now.Add(time.Second),
			}
			if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
				t.Fatalf("write rollback journal: %v", err)
			}
			runner := &recordingCommandRunner{}
			if err := (Refresher{Runner: runner}).recoverInterrupted(t.Context(), config, paths); err != nil {
				t.Fatalf("recover %s: %v", phase, err)
			}
			if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("completed rollback journal remains: %v", err)
			}
			_, packageErr := os.Stat(paths.PackageDir(digest))
			if !errors.Is(packageErr, os.ErrNotExist) {
				t.Fatalf("rollback retained candidate package: %v", packageErr)
			}
			if phase == JournalRollbackCleaned && len(runner.commands) != 0 {
				t.Fatalf("cleaned rollback issued commands: %+v", runner.commands)
			}
		})
	}
}

func TestServeActiveValidatesImmutableImageThenExecsRestrictedPodman(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	writeLifecycleRecoveryFiles(t, config)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	active := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	execSentinel := errors.New("exec invoked")
	runner := &recordingCommandRunner{
		run: func(_ context.Context, command Command) ([]byte, error) {
			if command.Path == config.PodmanPath && firstArg(command.Args) == "images" {
				if !containsAdjacentArgs(command.Args, "--filter", "id="+active.Current.ImageID) || containsArg(command.Args, "reference="+active.Current.ImageRef) {
					return nil, errors.New("active image was not looked up by immutable id")
				}
				return []byte(strings.Join([]string{
					active.Current.ImageID, managedProviderValue, config.WorkerID, providerImageRole, active.Current.Update.SHA256,
				}, "\t") + "\n"), nil
			}
			if command.Path == config.PodmanPath && firstArg(command.Args) == "ps" {
				return []byte(testStableContainerID + "\t" + config.StableContainer + "\tgithub-runner-provider\t" + config.WorkerID + "\tstable\n"), nil
			}
			return nil, nil
		},
		exec: func(Command) error { return execSentinel },
	}
	refresher := Refresher{Runner: runner}
	if err := refresher.ServeActive(t.Context(), config); !errors.Is(err, execSentinel) {
		t.Fatalf("serve active err = %v", err)
	}
	if len(runner.commands) != 4 {
		t.Fatalf("serve active commands = %+v", runner.commands)
	}
	if firstArg(runner.commands[1].Args) != "ps" || !containsAdjacentArgs(runner.commands[2].Args, "--ignore", testStableContainerID) {
		t.Fatalf("serve active did not remove the owned stale stable container by immutable ID: %+v", runner.commands)
	}
	execCommand := runner.commands[3]
	if execCommand.Path != config.PodmanPath || firstArg(execCommand.Args) != "run" || !containsAdjacentArgs(execCommand.Args, "--name", config.StableContainer) || !containsAdjacentArgs(execCommand.Args, "--env-file", paths.ProviderEnv) {
		t.Fatalf("serve active exec command = %+v", execCommand)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, active.Current.ImageRef) {
		t.Fatalf("serve active depended on mutable image ref:\n%s", transcript)
	}
	for _, required := range []string{"--network wfcompute-github-provider", "--read-only", "--cap-drop all", "no-new-privileges", active.Current.ImageID} {
		if !strings.Contains(transcript, required) {
			t.Fatalf("serve active transcript missing %q:\n%s", required, transcript)
		}
	}
	for _, label := range []string{
		"io.workflow.compute.managed=github-runner-provider",
		"io.workflow.compute.worker=" + config.WorkerID,
		"io.workflow.compute.role=stable",
	} {
		if !containsAdjacentArgs(execCommand.Args, "--label", label) {
			t.Fatalf("serve active command missing ownership label %q: %+v", label, execCommand)
		}
	}
	for _, mount := range []string{
		paths.ProviderState + ":" + providerStateMount + ":rw,Z",
		paths.TLSRoot + ":" + providerTLSMount + ":ro,z",
	} {
		if !containsAdjacentArgs(execCommand.Args, "--volume", mount) {
			t.Fatalf("serve active command missing SELinux-safe mount %q: %+v", mount, execCommand)
		}
	}
	if strings.Contains(transcript, "provider-secret") || strings.Contains(transcript, "github-secret") || strings.Contains(transcript, "sock") {
		t.Fatalf("serve active leaked secret or socket mount:\n%s", transcript)
	}
}

func TestServeActiveRefusesImageIdentityMismatch(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	writeLifecycleRecoveryFiles(t, config)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	active := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) {
		return ownedProviderImageInventory(config, active.Current.Update.SHA256, "sha256:"+strings.Repeat("f", 64)), nil
	}}
	if err := (Refresher{Runner: runner}).ServeActive(t.Context(), config); err == nil || !strings.Contains(err.Error(), "image id") {
		t.Fatalf("serve active mismatch err = %v", err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("serve active executed mismatched image: %+v", runner.commands)
	}
}

func TestServeActiveRejectsCrossWorkerActiveStateBeforePodman(t *testing.T) {
	for _, target := range []string{"current", "previous"} {
		t.Run(target, func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			writeRefreshEnvironmentFiles(t, paths)
			if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
				t.Fatalf("mkdir provider state: %v", err)
			}
			active := previousActiveStateForTest(t, home)
			if target == "current" {
				active.Current.Update.WorkerID = "other-retained-worker"
			} else {
				previous := active.Current
				previous.Update.SHA256 = "sha256:" + strings.Repeat("e", 64)
				previous.ImageID = "sha256:" + strings.Repeat("f", 64)
				previous.ImageRef = providerImageRef(previous.Update.SHA256)
				previous.Update.WorkerID = "other-retained-worker"
				active.Previous = &previous
			}
			if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
				t.Fatalf("write cross-worker active state: %v", err)
			}
			runner := &recordingCommandRunner{}

			err := (Refresher{Runner: runner}).ServeActive(t.Context(), config)
			if err == nil || !strings.Contains(err.Error(), "identity") {
				t.Fatalf("cross-worker active state err = %v", err)
			}
			if len(runner.commands) != 0 {
				t.Fatalf("cross-worker active state reached Podman: %+v", runner.commands)
			}
		})
	}
}

func TestRecoverInterruptedRejectsCrossWorkerJournalBeforeCommands(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	selection := validTestSelection(time.Unix(1_700_000_000, 0).UTC())
	selection.Update.WorkerID = "other-retained-worker"
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-cross-worker", Phase: JournalPrepared,
		Candidate: selection, StartedAt: selection.ActivatedAt, UpdatedAt: selection.ActivatedAt,
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write cross-worker refresh journal: %v", err)
	}
	runner := &recordingCommandRunner{}

	err := (Refresher{Runner: runner}).recoverInterrupted(t.Context(), config, paths)
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("cross-worker refresh journal err = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("cross-worker refresh journal issued commands: %+v", runner.commands)
	}
}

func TestRefreshRetriesDetachedProviderProbe(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-retry")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	attempts := 0
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if isProbeFor(command, config.CandidateContainer) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("provider not ready")
			}
		}
		return baseRun(ctx, command)
	}
	var sleeps []time.Duration
	refresher := Refresher{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return payload, nil },
		Sleep: func(_ context.Context, duration time.Duration) error {
			sleeps = append(sleeps, duration)
			return nil
		},
	}
	if _, err := refresher.Refresh(t.Context(), config); err != nil {
		t.Fatalf("refresh with readiness retry: %v", err)
	}
	if attempts != 3 || !reflect.DeepEqual(sleeps, []time.Duration{250 * time.Millisecond, 500 * time.Millisecond}) {
		t.Fatalf("probe attempts=%d sleeps=%v", attempts, sleeps)
	}
}

func TestProviderCommandsCarryRoleSpecificCleanupOwnershipLabels(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	selection := validTestSelection(time.Unix(1_700_000_000, 0).UTC())
	commands := map[string]Command{
		"candidate": candidateProviderCommand(config, paths, paths.CandidateState(selection.Update.SHA256), selection),
		"probe":     providerProbeCommand(config, paths, config.CandidateContainer, selection),
	}
	for role, command := range commands {
		for _, label := range []string{
			"io.workflow.compute.managed=github-runner-provider",
			"io.workflow.compute.worker=" + config.WorkerID,
			"io.workflow.compute.role=" + role,
		} {
			if !containsAdjacentArgs(command.Args, "--label", label) {
				t.Fatalf("%s command missing ownership label %q: %v", role, label, command.Args)
			}
		}
	}
}

func TestRefreshRemovesOwnedStaleProbeBeforeRetry(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-probe-cleanup")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	probeAttempts := 0
	staleProbe := false
	probeRemoved := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if firstArg(command.Args) == "ps" && containsAdjacentArgs(command.Args, "--filter", "name=^"+regexp.QuoteMeta(config.CandidateContainer+"-probe")+"$") {
			if staleProbe {
				return []byte(testProbeContainerID + "\t" + config.CandidateContainer + "-probe\tgithub-runner-provider\t" + config.WorkerID + "\tprobe\n"), nil
			}
			return nil, nil
		}
		if firstArg(command.Args) == "rm" && containsArg(command.Args, testProbeContainerID) {
			staleProbe = false
			probeRemoved = true
			return nil, nil
		}
		if isProbeFor(command, config.CandidateContainer) {
			probeAttempts++
			if probeAttempts == 1 {
				staleProbe = true
				return nil, errors.New("probe interrupted after container creation")
			}
			if staleProbe {
				return nil, errors.New("probe name already in use")
			}
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return payload, nil },
		Sleep:          func(context.Context, time.Duration) error { return nil },
	}
	if _, err := refresher.Refresh(t.Context(), config); err != nil {
		t.Fatalf("refresh with stale probe recovery: %v\n%s", err, commandTranscript(runner.commands))
	}
	if probeAttempts != 2 || !probeRemoved {
		t.Fatalf("probe attempts=%d removed=%v\n%s", probeAttempts, probeRemoved, commandTranscript(runner.commands))
	}
}

func TestManagedProbeCleansOwnedOrphanAfterCallerCancellation(t *testing.T) {
	config := validTestConfig(t.TempDir())
	paths := LifecyclePathsFor(config)
	selection := validTestSelection(time.Unix(1_700_000_000, 0).UTC())
	ctx, cancel := context.WithCancel(t.Context())
	staleProbe := false
	probeRemoved := false
	var removeBudget time.Duration
	runner := &recordingCommandRunner{run: func(commandContext context.Context, command Command) ([]byte, error) {
		if err := commandContext.Err(); err != nil {
			return nil, err
		}
		if firstArg(command.Args) == "ps" {
			if staleProbe {
				time.Sleep(time.Second)
				return []byte(testProbeContainerID + "\t" + config.CandidateContainer + "-probe\tgithub-runner-provider\t" + config.WorkerID + "\tprobe\n"), nil
			}
			return nil, nil
		}
		if isProbeFor(command, config.CandidateContainer) {
			staleProbe = true
			cancel()
			return nil, context.Canceled
		}
		if firstArg(command.Args) == "rm" && containsArg(command.Args, testProbeContainerID) {
			deadline, ok := commandContext.Deadline()
			if !ok {
				t.Fatal("detached probe removal has no deadline")
			}
			removeBudget = time.Until(deadline)
			staleProbe = false
			probeRemoved = true
		}
		return nil, nil
	}}
	refresher := Refresher{Runner: runner, Sleep: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
	err := refresher.runManagedProbe(ctx, config, config.CandidateContainer, providerProbeCommand(config, paths, config.CandidateContainer, selection))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled probe error = %v", err)
	}
	if staleProbe || !probeRemoved {
		t.Fatalf("canceled probe orphan remained: stale=%v removed=%v\n%s", staleProbe, probeRemoved, commandTranscript(runner.commands))
	}
	if removeBudget < controlCommandTimeout-500*time.Millisecond {
		t.Fatalf("probe removal budget = %s want a fresh command budget after ownership inspection", removeBudget)
	}
}

func TestRollbackImageCleanupRequiresOwnershipAndImmutableID(t *testing.T) {
	const unownedImageID = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	for _, tc := range []struct {
		name        string
		inventoryID string
		managed     string
		wantErr     string
		wantRemove  bool
	}{
		{name: "absent"},
		{name: "unowned collision", inventoryID: testProviderImageID, managed: "other", wantErr: "ownership"},
		{name: "journal id mismatch", inventoryID: unownedImageID, managed: "github-runner-provider", wantErr: "image id"},
		{name: "owned", inventoryID: testProviderImageID, managed: "github-runner-provider", wantRemove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			payload := writeTestProviderPayload(t, home, "candidate-image-cleanup")
			digest := fileDigestForTest(t, payload)
			candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-image-cleanup", testProviderImageID, time.Unix(1_700_000_100, 0).UTC())
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion,
				ID:              "refresh-image-cleanup",
				Phase:           JournalRollbackRestored,
				RollbackFrom:    JournalPrepared,
				Candidate:       candidate,
				StartedAt:       candidate.ActivatedAt,
				UpdatedAt:       candidate.ActivatedAt,
			}
			if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
				t.Fatalf("write rollback journal: %v", err)
			}
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && tc.inventoryID != "" {
					return []byte(strings.Join([]string{
						tc.inventoryID, tc.managed, config.WorkerID, providerImageRole, digest,
					}, "\t") + "\n"), nil
				}
				return nil, nil
			}}
			err := (Refresher{Runner: runner}).rollback(t.Context(), config, paths, journal, false)
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("rollback cleanup error = %v want %q", err, tc.wantErr)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("rollback cleanup: %v", err)
			}
			transcript := commandTranscript(runner.commands)
			removed := strings.Contains(transcript, "image rm --ignore "+candidate.ImageID)
			if removed != tc.wantRemove {
				t.Fatalf("image cleanup transcript=%q wantRemove=%v", transcript, tc.wantRemove)
			}
			if strings.Contains(transcript, "image rm --ignore "+candidate.ImageRef) {
				t.Fatalf("candidate image removed by mutable ref: %s", transcript)
			}
		})
	}
}

func TestProviderImageCleanupUsesImmutableIDOrOwnershipLabelsWithoutTag(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	digest := "sha256:" + strings.Repeat("d", 64)
	for _, tc := range []struct {
		name            string
		expectedImageID string
		wantFilters     []string
	}{
		{name: "durable image", expectedImageID: testProviderImageID, wantFilters: []string{"id=" + testProviderImageID}},
		{name: "staging image", wantFilters: []string{
			"label=" + managedObjectLabel + "=" + managedProviderValue,
			"label=" + managedWorkerLabel + "=" + config.WorkerID,
			"label=" + managedRoleLabel + "=" + providerImageRole,
			"label=" + managedDigestLabel + "=" + digest,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				if firstArg(command.Args) != "images" {
					return nil, nil
				}
				for _, filter := range tc.wantFilters {
					if !containsAdjacentArgs(command.Args, "--filter", filter) {
						return nil, fmt.Errorf("missing image filter %s", filter)
					}
				}
				if strings.Contains(commandTranscript([]Command{command}), "reference=") {
					return nil, errors.New("mutable reference image filter")
				}
				return []byte(strings.Join([]string{
					testProviderImageID, managedProviderValue, config.WorkerID, providerImageRole, digest,
				}, "\t") + "\n"), nil
			}}
			if err := (Refresher{Runner: runner}).removeOwnedProviderImage(t.Context(), config, digest, tc.expectedImageID); err != nil {
				t.Fatalf("remove owned provider image: %v", err)
			}
			if transcript := commandTranscript(runner.commands); !strings.Contains(transcript, "image rm --ignore "+testProviderImageID) {
				t.Fatalf("immutable image was not removed:\n%s", transcript)
			}
		})
	}
}

func TestRemoveCandidateContainerRequiresExactOwnershipAndUsesImmutableID(t *testing.T) {
	containerID := strings.Repeat("a", 64)
	for _, tc := range []struct {
		name       string
		inventory  string
		wantErr    string
		wantRemove bool
	}{
		{name: "absent"},
		{name: "unowned collision", inventory: containerID + "\tworkflow-plugin-github-runner-provider-candidate\tother\tgithub-runner-linux-stg\tcandidate\n", wantErr: "ownership"},
		{name: "owned", inventory: containerID + "\tworkflow-plugin-github-runner-provider-candidate\tgithub-runner-provider\tgithub-runner-linux-stg\tcandidate\n", wantRemove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := validTestConfig(t.TempDir())
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				if firstArg(command.Args) == "ps" {
					return []byte(tc.inventory), nil
				}
				return nil, nil
			}}
			err := (Refresher{Runner: runner}).removeManagedContainer(t.Context(), config, config.CandidateContainer, candidateContainerRole)
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("remove error = %v want %q", err, tc.wantErr)
			}
			if tc.wantErr == "" && err != nil {
				t.Fatalf("remove candidate: %v", err)
			}
			transcript := commandTranscript(runner.commands)
			removed := strings.Contains(transcript, "rm --force --ignore "+containerID)
			if removed != tc.wantRemove {
				t.Fatalf("remove transcript = %q wantRemove=%v", transcript, tc.wantRemove)
			}
			if strings.Contains(transcript, "rm --force --ignore "+config.CandidateContainer) {
				t.Fatalf("candidate removed by mutable name: %s", transcript)
			}
		})
	}
}

func TestRemoveCandidateContainerQuotesConfiguredNameFilter(t *testing.T) {
	config := validTestConfig(t.TempDir())
	config.CandidateContainer = "candidate.v1"
	runner := &recordingCommandRunner{}
	if err := (Refresher{Runner: runner}).removeManagedContainer(t.Context(), config, config.CandidateContainer, candidateContainerRole); err != nil {
		t.Fatalf("inspect absent candidate: %v", err)
	}
	if len(runner.commands) != 1 || !containsAdjacentArgs(runner.commands[0].Args, "--filter", `name=^candidate\.v1$`) {
		t.Fatalf("candidate inventory filter is not exact: %+v", runner.commands)
	}
}

func TestRefreshLockRejectsConcurrentMutationBeforeVerification(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-lock")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	buildStarted := make(chan struct{})
	releaseBuild := make(chan struct{})
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			close(buildStarted)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-releaseBuild:
			}
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	done := make(chan error, 1)
	go func() {
		_, err := refresher.Refresh(t.Context(), config)
		done <- err
	}()
	<-buildStarted
	before := len(runner.commands)
	if _, err := refresher.Refresh(t.Context(), config); !errors.Is(err, ErrInstallLocked) {
		t.Fatalf("concurrent refresh err = %v", err)
	}
	if len(runner.commands) != before {
		t.Fatalf("concurrent refresh reached command runner: before=%d after=%d", before, len(runner.commands))
	}
	close(releaseBuild)
	if err := <-done; err != nil {
		t.Fatalf("first refresh: %v", err)
	}
}

func TestRefreshRejectsSymlinkedCandidateRootWithoutTouchingTarget(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("symlink behavior varies on Windows")
	}
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-symlink")
	digest := fileDigestForTest(t, payload)
	outside := filepath.Join(t.TempDir(), "outside")
	sentinel := filepath.Join(outside, digestHex(digest), "state", "sentinel")
	if err := os.MkdirAll(filepath.Dir(sentinel), 0o700); err != nil {
		t.Fatalf("mkdir outside target: %v", err)
	}
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write outside sentinel: %v", err)
	}
	if err := os.Symlink(outside, paths.CandidatesRoot); err != nil {
		t.Fatalf("symlink candidates root: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	if _, err := (Refresher{Runner: runner}).Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlinked candidates err = %v", err)
	}
	if data, err := os.ReadFile(sentinel); err != nil || string(data) != "keep" {
		t.Fatalf("outside sentinel changed: data=%q err=%v", data, err)
	}
}

func TestRollbackDoesNotRestartWhenDurableRestoreFails(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-restore-failure")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	restarts := 0
	poisoned := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "restart") {
			restarts++
		}
		if isProbeFor(command, config.StableContainer) && containsArg(command.Args, testProviderImageID) {
			if !poisoned {
				poisoned = true
				if err := os.Remove(paths.ActiveState); err != nil {
					t.Fatalf("remove active state: %v", err)
				}
				if err := os.Symlink(filepath.Join(home, "outside-active"), paths.ActiveState); err != nil {
					t.Fatalf("poison active state: %v", err)
				}
			}
			return nil, errors.New("stable probe failed")
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("restore failure err = %v", err)
	}
	if restarts != 1 {
		t.Fatalf("provider restarted %d times after durable restore failure", restarts)
	}
}

func TestRollbackDoesNotRestartWhenProviderStateRestoreFails(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-state-restore-failure")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	restarts := 0
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "restart") {
			restarts++
		}
		if isProbeFor(command, config.StableContainer) && containsArg(command.Args, testProviderImageID) {
			previousState := paths.PreviousState(digest)
			if err := os.RemoveAll(previousState); err != nil {
				t.Fatalf("remove previous provider state: %v", err)
			}
			if err := os.Symlink(filepath.Join(home, "outside-provider-state"), previousState); err != nil {
				t.Fatalf("poison previous provider state: %v", err)
			}
			return nil, errors.New("stable probe failed")
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "real directory") {
		t.Fatalf("provider state restore failure err = %v", err)
	}
	if restarts != 1 {
		t.Fatalf("provider restarted %d times after provider state restore failure", restarts)
	}
}

func TestInterruptedPromotedStateFailsClosedWhenRollbackStateIsMissing(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	stateFile := filepath.Join(paths.ProviderState, "generation")
	if err := os.WriteFile(stateFile, []byte("candidate"), 0o600); err != nil {
		t.Fatalf("write promoted provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-missing-rollback")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_400_000, 0).UTC()
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-missing-rollback", "sha256:"+strings.Repeat("d", 64), now)
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-missing-rollback",
		Phase:           JournalStatePromoted,
		Previous:        &previous,
		Candidate:       candidate,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write interrupted journal: %v", err)
	}
	runner := refreshTestRunner(config, payload, digest)
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	err := refresher.recoverInterrupted(t.Context(), config, paths)
	if err == nil || !strings.Contains(err.Error(), "missing previous provider state") {
		t.Fatalf("missing rollback recovery err = %v", err)
	}
	if data, readErr := os.ReadFile(stateFile); readErr != nil || string(data) != "candidate" {
		t.Fatalf("ambiguous provider state changed: data=%q err=%v", data, readErr)
	}
	if _, statErr := os.Stat(paths.Journal); statErr != nil {
		t.Fatalf("ambiguous recovery removed journal: %v", statErr)
	}
}

func TestCommittedCleanupFailureLeavesRecoverableJournal(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-cleanup")
	digest := fileDigestForTest(t, payload)
	runner := refreshTestRunner(config, payload, digest)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if isProbeFor(command, config.StableContainer) {
			if err := os.Chmod(paths.CandidatesRoot, 0o500); err != nil {
				t.Fatalf("restrict candidate cleanup root: %v", err)
			}
		}
		return baseRun(ctx, command)
	}
	t.Cleanup(func() { _ = os.Chmod(paths.CandidatesRoot, 0o700) })
	refresher := Refresher{Runner: runner, ExecutablePath: func() (string, error) { return payload, nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil || !strings.Contains(err.Error(), "rollback target") {
		t.Fatalf("cleanup failure err = %v", err)
	}
	var journal TransactionJournal
	if err := ReadStrictJSONFile(paths.Journal, &journal); err != nil {
		t.Fatalf("committed journal was not retained: %v", err)
	}
	if journal.Phase != JournalCommitted {
		t.Fatalf("journal phase = %s", journal.Phase)
	}
}

func TestOSCommandRunnerDoesNotEchoArgumentsOrOutputOnFailure(t *testing.T) {
	secret := "credential-that-must-not-leak"
	runner := OSCommandRunner{MaxOutputBytes: 1024}
	_, err := runner.Run(t.Context(), Command{
		Path: os.Args[0],
		Args: []string{"-test.not-a-real-flag=" + secret},
	})
	if err == nil {
		t.Fatal("failing command succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("command error leaked argument: %v", err)
	}
}

func TestOSCommandRunnerDoesNotInheritUnrelatedHostSecrets(t *testing.T) {
	const secret = "aws-host-secret-that-must-not-leak"
	t.Setenv("AWS_SECRET_ACCESS_KEY", secret)
	t.Setenv("LC_ALL", "workflow-provider-environment-helper")
	runner := OSCommandRunner{MaxOutputBytes: 1 << 20}
	output, err := runner.Run(t.Context(), Command{
		Path: os.Args[0],
		Args: []string{"-test.run=^TestOSCommandRunnerEnvironmentHelper$"},
	})
	if err != nil {
		t.Fatalf("run env: %v", err)
	}
	if strings.Contains(string(output), secret) || strings.Contains(string(output), "AWS_SECRET_ACCESS_KEY") {
		t.Fatalf("subprocess inherited unrelated host secret: %s", output)
	}
}

func TestOSCommandRunnerEnvironmentHelper(t *testing.T) {
	if os.Getenv("LC_ALL") != "workflow-provider-environment-helper" {
		return
	}
	for _, entry := range os.Environ() {
		if _, err := fmt.Fprintln(os.Stdout, entry); err != nil {
			t.Fatalf("write environment fixture: %v", err)
		}
	}
}

const (
	testProviderImageID      = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testCandidateContainerID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	testStableContainerID    = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	testProbeContainerID     = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

func refreshTestRunner(config Config, payload, digest string) *recordingCommandRunner {
	for _, file := range []struct {
		path string
		mode os.FileMode
		data string
	}{
		{path: config.ComputeAgentPath, mode: 0o700, data: "compute-agent fixture"},
		{path: config.SupervisorConfigPath, mode: 0o600, data: "supervisor config fixture"},
		{path: config.PodmanPath, mode: 0o500, data: "podman fixture"},
		{path: config.SystemctlPath, mode: 0o500, data: "systemctl fixture"},
		{path: config.LoginctlPath, mode: 0o500, data: "loginctl fixture"},
		{path: agentUnitFragmentPathForTest(config), mode: 0o600, data: "[Service]\nExecStart=" + config.ComputeAgentPath + " run\n"},
	} {
		if err := os.MkdirAll(filepath.Dir(file.path), 0o700); err != nil {
			panic(err)
		}
		if err := os.WriteFile(file.path, []byte(file.data), file.mode); err != nil {
			panic(err)
		}
	}
	maintenanceActive := false
	maintenanceID := ""
	maintenanceReason := ""
	return &recordingCommandRunner{run: func(ctx context.Context, command Command) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch installCommandEvent(command, config) {
		case "agent-signature":
			return agentUnitSystemdOutput(config)
		case "maintenance-begin":
			maintenanceActive = true
			maintenanceID = adjacentArgValue(command.Args, "-id")
			maintenanceReason = adjacentArgValue(command.Args, "-reason")
			return maintenanceStateJSON(true, maintenanceID, config.ProfileID, maintenanceReason), nil
		case "maintenance-end":
			maintenanceActive = false
			return maintenanceStateJSON(false, maintenanceID, config.ProfileID, maintenanceReason), nil
		case "maintenance-status":
			return maintenanceStateJSON(maintenanceActive, maintenanceID, config.ProfileID, maintenanceReason), nil
		case "local-status":
			state := "idle"
			if maintenanceActive {
				state = "unavailable"
			}
			return localStatusJSON(config.WorkerID, state), nil
		}
		switch {
		case command.Path == config.ComputeAgentPath:
			return testVerifiedUpdateJSON(config, payload, digest), nil
		case command.Path == config.PodmanPath && firstArg(command.Args) == "ps":
			var id, name, role string
			switch adjacentArgValue(command.Args, "--filter") {
			case "name=^" + regexp.QuoteMeta(config.CandidateContainer) + "$":
				id, name, role = testCandidateContainerID, config.CandidateContainer, candidateContainerRole
			case "name=^" + regexp.QuoteMeta(config.StableContainer) + "$":
				id, name, role = testStableContainerID, config.StableContainer, stableContainerRole
			case "name=^" + regexp.QuoteMeta(config.CandidateContainer+"-probe") + "$":
				id, name, role = testProbeContainerID, config.CandidateContainer+"-probe", probeContainerRole
			case "name=^" + regexp.QuoteMeta(config.StableContainer+"-probe") + "$":
				id, name, role = testProbeContainerID, config.StableContainer+"-probe", probeContainerRole
			default:
				return nil, nil
			}
			return []byte(id + "\t" + name + "\t" + managedProviderValue + "\t" + config.WorkerID + "\t" + role + "\n"), nil
		case command.Path == config.PodmanPath && firstArg(command.Args) == "images":
			imageDigest := digest
			for index := 0; index+1 < len(command.Args); index++ {
				if command.Args[index] == "--filter" {
					const prefix = "label=" + managedDigestLabel + "="
					if strings.HasPrefix(command.Args[index+1], prefix) {
						imageDigest = strings.TrimPrefix(command.Args[index+1], prefix)
					}
				}
			}
			return ownedProviderImageInventory(config, imageDigest, testProviderImageID), nil
		case command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "image" && command.Args[1] == "inspect":
			return []byte(testProviderImageID + "\n"), nil
		case command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "network" && command.Args[1] == "inspect":
			return []byte("bridge true false\n"), nil
		case filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "--property", "ActiveState"):
			return []byte("active\n"), nil
		default:
			return nil, nil
		}
	}}
}

func ownedProviderImageInventory(config Config, digest, imageID string) []byte {
	return []byte(strings.Join([]string{
		imageID, managedProviderValue, config.WorkerID, providerImageRole, digest,
	}, "\t") + "\n")
}

func assertRefreshCommandIsolation(t *testing.T, commands []Command, config Config, paths LifecyclePaths) {
	t.Helper()
	transcript := commandTranscript(commands)
	for _, required := range []string{
		"build", "FROM scratch", config.CandidateContainer, config.StableContainer,
		"network inspect --format {{.Driver}} {{.DNSEnabled}} {{.Internal}} wfcompute-github-provider",
		"--network wfcompute-github-provider", "--read-only", "--cap-drop all", "no-new-privileges",
		"--env-file " + paths.ProviderEnv, "--env-file " + paths.ProbeEnv,
		"probe -url https://" + config.CandidateContainer + ":18090", "probe -url " + config.ProviderURL,
		"systemctl --user restart",
		":" + providerStateMount + ":rw,Z",
		paths.TLSRoot + ":" + providerTLSMount + ":ro,z",
		paths.CAFile + ":" + providerCAPath + ":ro,z",
	} {
		if !strings.Contains(transcript, required) {
			t.Fatalf("command transcript missing %q:\n%s", required, transcript)
		}
	}
	if strings.Contains(transcript, "provider-secret") || strings.Contains(transcript, "github-secret") || strings.Contains(transcript, "/var/run/docker.sock") || strings.Contains(transcript, "/run/podman/podman.sock") {
		t.Fatalf("command transcript leaked a secret or mounted a runtime socket:\n%s", transcript)
	}
	if strings.Contains(transcript, "--publish") {
		t.Fatalf("command transcript exposed a host port:\n%s", transcript)
	}
	probeCommands := 0
	for _, command := range commands {
		if command.Path != config.PodmanPath || !containsArg(command.Args, "probe") {
			continue
		}
		probeCommands++
		if !containsAdjacentArgs(command.Args, "--env-file", paths.ProbeEnv) || containsAdjacentArgs(command.Args, "--env-file", paths.ProviderEnv) {
			t.Fatalf("probe command has wrong environment: %+v", command)
		}
	}
	if probeCommands != 2 {
		t.Fatalf("probe command count = %d, commands=%+v", probeCommands, commands)
	}
}

func writeRefreshEnvironmentFiles(t *testing.T, paths LifecyclePaths, observedAt ...time.Time) {
	t.Helper()
	certificateTime := time.Now().UTC()
	if len(observedAt) > 1 {
		t.Fatalf("write refresh environment files accepts at most one observation time")
	}
	if len(observedAt) == 1 {
		certificateTime = observedAt[0].UTC()
	}
	if err := os.MkdirAll(filepath.Dir(paths.ProviderEnv), 0o700); err != nil {
		t.Fatalf("mkdir env dir: %v", err)
	}
	providerEnvironment := strings.Join([]string{
		"GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret",
		"GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN=github-secret",
		"GITHUB_RUNNER_PROVIDER_STATE_DIR=" + providerStateMount,
		"GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute",
		"GITHUB_RUNNER_PROVIDER_ORGANIZATIONS=GoCodeAlone",
		"GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS=ephemeral",
		"GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE=" + providerTLSCertPath,
		"GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE=" + providerTLSKeyPath,
		"",
	}, "\n")
	if err := os.WriteFile(paths.ProviderEnv, []byte(providerEnvironment), 0o600); err != nil {
		t.Fatalf("write provider env: %v", err)
	}
	if err := os.WriteFile(paths.ProbeEnv, []byte("GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret\n"), 0o600); err != nil {
		t.Fatalf("write probe env: %v", err)
	}
	if err := os.MkdirAll(paths.TLSRoot, 0o700); err != nil {
		t.Fatalf("mkdir tls root: %v", err)
	}
	config := validTestConfig(lifecycleHome(paths))
	material, err := GenerateInstallMaterial(config, Credentials{
		GitHubToken: "github-secret", ProviderToken: "provider-secret",
	}, nil, certificateTime.Add(-time.Hour))
	if err != nil {
		t.Fatalf("generate refresh TLS material: %v", err)
	}
	for path, data := range map[string][]byte{
		paths.CAFile: material.CACert, paths.CAKey: material.CAKey,
		paths.ServerCert: material.ServerCert, paths.ServerKey: material.ServerKey,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write refresh TLS material: %v", err)
		}
	}
}

func writeTestProviderPayload(t *testing.T, home, contents string) string {
	t.Helper()
	path := filepath.Join(home, contents)
	if err := os.WriteFile(path, []byte(contents), 0o700); err != nil {
		t.Fatalf("write provider payload: %v", err)
	}
	return path
}

func fileDigestForTest(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read payload: %v", err)
	}
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func testVerifiedUpdateJSON(config Config, payload, digest string) []byte {
	return []byte(`{
  "worker_id": "` + config.WorkerID + `",
  "directive_id": "directive-1",
  "campaign_id": "campaign-1",
  "directive_issued_at": "2026-07-13T00:00:00Z",
  "directive_expires_at": "2026-07-14T00:00:00Z",
  "directive_signature": {},
  "component": "provider",
  "plugin_id": "` + GitHubPluginID + `",
  "component_id": "` + config.ComponentID + `",
  "version": "v1.0.32",
  "format": "binary",
  "artifact_url": "/v1/artifacts/provider",
  "artifact_size_bytes": 20,
  "artifact_signature": {},
  "directive": {},
  "artifact": {},
  "path": "` + payload + `",
  "sha256": "` + digest + `",
  "applied_at": "2026-07-13T00:01:00Z"
}
`)
}

func commandTranscript(commands []Command) string {
	var builder strings.Builder
	for _, command := range commands {
		builder.WriteString(filepath.Base(command.Path))
		builder.WriteByte(' ')
		builder.WriteString(strings.Join(command.Args, " "))
		builder.WriteByte('\n')
		if command.Stdin != nil {
			builder.Write(command.Stdin)
			builder.WriteByte('\n')
		}
	}
	return builder.String()
}

func previousActiveStateForTest(t *testing.T, home string) ActiveState {
	t.Helper()
	payload := writeTestProviderPayload(t, home, "verified-provider-v1")
	digest := fileDigestForTest(t, payload)
	selection := selectionForDigest(payload, digest, "v1.0.31", "directive-previous", "sha256:"+strings.Repeat("c", 64), time.Unix(1_700_000_000, 0).UTC())
	return ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: selection.ActivatedAt}
}

func selectionForDigest(path, digest, version, directiveID, imageID string, activatedAt time.Time) ImageSelection {
	return ImageSelection{
		Update: VerifiedUpdate{
			WorkerID: "github-runner-linux-stg", DirectiveID: directiveID, CampaignID: "campaign-1",
			Component: "provider", PluginID: GitHubPluginID, ComponentID: "github-runner-provider-sidecar",
			Version: version, Format: "binary", Path: path, SHA256: digest,
		},
		ImageID: imageID, ImageRef: providerImageRef(digest), ActivatedAt: activatedAt,
	}
}

func firstArg(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func isCandidateStart(command Command, config Config) bool {
	return command.Path == config.PodmanPath && firstArg(command.Args) == "run" && containsAdjacentArgs(command.Args, "--name", config.CandidateContainer) && !containsArg(command.Args, "probe")
}

func isProbeFor(command Command, target string) bool {
	return firstArg(command.Args) == "run" && containsAdjacentArgs(command.Args, "--name", target+"-probe") && containsArg(command.Args, "probe")
}

func containsArg(args []string, value string) bool {
	for _, arg := range args {
		if arg == value {
			return true
		}
	}
	return false
}

func containsAdjacentArgs(args []string, first, second string) bool {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == first && args[index+1] == second {
			return true
		}
	}
	return false
}
