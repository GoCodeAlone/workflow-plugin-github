package retainedprovider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		switch installCommandEvent(command, config) {
		case "maintenance-begin":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleFencing {
				t.Fatalf("maintenance begin lifecycle journal = %+v found=%v err=%v", journal, found, err)
			}
			events = append(events, "maintenance-begin")
			return maintenanceStateJSON(true, "workflow-plugin-github-retained-provider-refresh", config.ProfileID, "workflow-plugin-github-retained-provider-refresh"), nil
		case "maintenance-status":
			events = append(events, "maintenance-status")
			return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
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
			return maintenanceStateJSON(false, "workflow-plugin-github-retained-provider-refresh", config.ProfileID, "workflow-plugin-github-retained-provider-refresh"), nil
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
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
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
	writeRefreshEnvironmentFiles(t, paths)
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
	now := time.Unix(1_700_000_000, 0).UTC()
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
	if err := refresher.finalizeDeferredRefresh(config); err != nil {
		t.Fatalf("finalize deferred refresh: %v", err)
	}
	if _, err := os.Stat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized journal remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(paths.CandidatesRoot, digestHex(digest))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("finalized rollback state remains: %v", err)
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
	if _, err := refresher.refreshUnderLifecycleTransaction(t.Context(), config, false, true, "install-transaction-123", config.ProfileID, ""); err != nil {
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

func TestRefreshFailurePreservesPreviousActiveImageAndCleansCandidate(t *testing.T) {
	for _, phase := range []string{"build", "stale-candidate", "stable-stop", "candidate", "candidate-probe", "stable-restart", "stable-probe", "canceled"} {
		t.Run(phase, func(t *testing.T) {
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
			payload := writeTestProviderPayload(t, home, "verified-provider-v2")
			digest := fileDigestForTest(t, payload)
			runner := refreshTestRunner(config, payload, digest)
			baseRun := runner.run
			failedRestart := false
			failedStaleCleanup := false
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
				if phase == "stale-candidate" && firstArg(command.Args) == "rm" && containsArg(command.Args, config.CandidateContainer) && !failedStaleCleanup {
					failedStaleCleanup = true
					return nil, errors.New("stale candidate cleanup failed")
				}
				if phase == "candidate-probe" && isProbeFor(command, config.CandidateContainer) {
					return nil, errors.New("candidate probe failed")
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
				Runner: runner, Now: func() time.Time { return time.Unix(1_700_000_100, 0).UTC() },
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
			if strings.Contains(transcript, "image rm") {
				t.Fatalf("%s failure attempted to prune retained image:\n%s", phase, transcript)
			}
			if phase != "build" && !strings.Contains(transcript, "rm --force --ignore "+config.CandidateContainer) {
				t.Fatalf("%s failure did not clean candidate:\n%s", phase, transcript)
			}
		})
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
	blockedCommit := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if isCandidateStart(command, config) {
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "state.json"), []byte(`{"generation":"candidate"}`), 0o600); err != nil {
				t.Fatalf("mutate candidate state: %v", err)
			}
		}
		if isProbeFor(command, config.StableContainer) && !blockedCommit {
			blockedCommit = true
			if err := os.Remove(paths.Journal); err != nil {
				t.Fatalf("remove journal before commit: %v", err)
			}
			if err := os.Mkdir(paths.Journal, 0o700); err != nil {
				t.Fatalf("block journal commit: %v", err)
			}
		}
		return baseRun(ctx, command)
	}
	refresher := Refresher{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := refresher.Refresh(t.Context(), config); err == nil {
		t.Fatal("refresh with failed commit-journal write succeeded")
	}
	if data, err := os.ReadFile(stateFile); err != nil || string(data) != `{"generation":"previous"}` {
		t.Fatalf("commit-write rollback state = %q err=%v", data, err)
	}
}

func TestRefreshRecoversEveryInterruptedJournalPhaseIdempotently(t *testing.T) {
	for _, phase := range []JournalPhase{JournalPrepared, JournalStatePromoting, JournalStatePromoted, JournalActivated, JournalCommitted} {
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
		if firstArg(command.Args) == "rm" && containsArg(command.Args, config.CandidateContainer) {
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

func TestServeActiveValidatesImmutableImageThenExecsRestrictedPodman(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	writeRefreshEnvironmentFiles(t, paths)
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
			if command.Path == config.PodmanPath && len(command.Args) > 1 && command.Args[0] == "image" {
				return []byte(strings.TrimPrefix(active.Current.ImageID, "sha256:") + "\n"), nil
			}
			return nil, nil
		},
		exec: func(Command) error { return execSentinel },
	}
	refresher := Refresher{Runner: runner}
	if err := refresher.ServeActive(t.Context(), config); !errors.Is(err, execSentinel) {
		t.Fatalf("serve active err = %v", err)
	}
	if len(runner.commands) != 2 {
		t.Fatalf("serve active commands = %+v", runner.commands)
	}
	execCommand := runner.commands[1]
	if execCommand.Path != config.PodmanPath || firstArg(execCommand.Args) != "run" || !containsAdjacentArgs(execCommand.Args, "--name", config.StableContainer) || !containsAdjacentArgs(execCommand.Args, "--env-file", paths.ProviderEnv) {
		t.Fatalf("serve active exec command = %+v", execCommand)
	}
	transcript := commandTranscript(runner.commands)
	for _, required := range []string{"--network wfcompute-github-provider", "--read-only", "--cap-drop all", "no-new-privileges", active.Current.ImageID} {
		if !strings.Contains(transcript, required) {
			t.Fatalf("serve active transcript missing %q:\n%s", required, transcript)
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
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	active := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) {
		return []byte("sha256:" + strings.Repeat("f", 64) + "\n"), nil
	}}
	if err := (Refresher{Runner: runner}).ServeActive(t.Context(), config); err == nil || !strings.Contains(err.Error(), "image id") {
		t.Fatalf("serve active mismatch err = %v", err)
	}
	if len(runner.commands) != 1 {
		t.Fatalf("serve active executed mismatched image: %+v", runner.commands)
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
		Path: "/usr/bin/false",
		Args: []string{secret},
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
	runner := OSCommandRunner{MaxOutputBytes: 1 << 20}
	output, err := runner.Run(t.Context(), Command{Path: "/usr/bin/env"})
	if err != nil {
		t.Fatalf("run env: %v", err)
	}
	if strings.Contains(string(output), secret) || strings.Contains(string(output), "AWS_SECRET_ACCESS_KEY") {
		t.Fatalf("subprocess inherited unrelated host secret: %s", output)
	}
}

const testProviderImageID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func refreshTestRunner(config Config, payload, digest string) *recordingCommandRunner {
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
			panic(err)
		}
		if err := os.WriteFile(file.path, []byte(file.data), file.mode); err != nil {
			panic(err)
		}
	}
	maintenanceActive := false
	return &recordingCommandRunner{run: func(ctx context.Context, command Command) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		switch installCommandEvent(command, config) {
		case "agent-signature":
			return agentUnitSystemdOutput(config)
		case "maintenance-begin":
			maintenanceActive = true
			return maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
		case "maintenance-end":
			maintenanceActive = false
			return maintenanceStateJSON(false, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason), nil
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

func writeRefreshEnvironmentFiles(t *testing.T, paths LifecyclePaths) {
	t.Helper()
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
	if err := os.WriteFile(paths.CAFile, []byte("test-ca"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
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
