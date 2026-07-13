package retainedprovider

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	providerServiceUnit     = "workflow-plugin-github-runner-provider.service"
	providerStateMount      = "/var/lib/workflow-github-runner-provider"
	providerTLSMount        = "/tls"
	providerCAPath          = "/tls/ca.pem"
	providerTLSCertPath     = "/tls/server.crt"
	providerTLSKeyPath      = "/tls/server.key"
	providerListenAddr      = "0.0.0.0:18090"
	maxProviderPackageBytes = 512 << 20
)

var providerContainerfile = []byte("FROM scratch\nCOPY --chmod=0555 github-runner-provider /github-runner-provider\nENTRYPOINT [\"/github-runner-provider\"]\n")

type LifecyclePaths struct {
	Root           string
	ActiveState    string
	Journal        string
	InstallLock    string
	ProviderState  string
	PackagesRoot   string
	CandidatesRoot string
	ProviderEnv    string
	ProbeEnv       string
	TLSRoot        string
	CAFile         string
}

func LifecyclePathsFor(config Config) LifecyclePaths {
	root := config.InstallRoot
	return LifecyclePaths{
		Root:           root,
		ActiveState:    filepath.Join(root, "lifecycle", "active.json"),
		Journal:        filepath.Join(root, "lifecycle", "transaction.json"),
		InstallLock:    filepath.Join(root, "lifecycle", "install.lock"),
		ProviderState:  filepath.Join(root, "provider-state"),
		PackagesRoot:   filepath.Join(root, "packages"),
		CandidatesRoot: filepath.Join(root, "candidates"),
		ProviderEnv:    filepath.Join(root, "secrets", "provider.env"),
		ProbeEnv:       filepath.Join(root, "secrets", "probe.env"),
		TLSRoot:        filepath.Join(root, "tls"),
		CAFile:         filepath.Join(root, "tls", "ca.pem"),
	}
}

func (paths LifecyclePaths) CandidateState(digest string) string {
	return filepath.Join(paths.CandidatesRoot, digestHex(digest), "state")
}

func (paths LifecyclePaths) PackageDir(digest string) string {
	return filepath.Join(paths.PackagesRoot, digestHex(digest))
}

func (paths LifecyclePaths) PackageBinary(digest string) string {
	return filepath.Join(paths.PackageDir(digest), "github-runner-provider")
}

type Refresher struct {
	Runner         CommandRunner
	ExecutablePath func() (string, error)
	Now            func() time.Time
	Sleep          func(context.Context, time.Duration) error
}

func (refresher Refresher) Refresh(ctx context.Context, config Config) (status Status, returnErr error) {
	if refresher.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	paths := LifecyclePathsFor(config)
	if err := validateInstallRoot(paths.Root); err != nil {
		return Status{}, err
	}
	if err := ValidateUserPath(paths.Root, paths.InstallLock, false); err != nil {
		return Status{}, fmt.Errorf("install lock path: %w", err)
	}
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		return Status{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	if err := refresher.recoverInterrupted(ctx, config, paths); err != nil {
		return Status{}, err
	}
	update, err := VerifyCurrentUpdate(ctx, config, refresher.Runner)
	if err != nil {
		return Status{}, err
	}
	active, activeFound, err := readActiveState(paths.ActiveState)
	if err != nil {
		return Status{}, err
	}
	now := refresher.now()
	if activeFound && active.Current.Update.SHA256 == update.SHA256 {
		return statusForActive(active, true, now), nil
	}
	if !activeFound {
		executablePath := refresher.ExecutablePath
		if executablePath == nil {
			executablePath = os.Executable
		}
		currentExecutable, err := executablePath()
		if err != nil {
			return Status{}, fmt.Errorf("resolve installer executable: %w", err)
		}
		digest, err := hashRegularFile(currentExecutable, true)
		if err != nil {
			return Status{}, fmt.Errorf("hash installer executable: %w", err)
		}
		if digest != update.SHA256 {
			return Status{}, errors.New("installer digest does not match verified provider update")
		}
	}
	for name, path := range map[string]string{
		"provider environment": paths.ProviderEnv,
		"probe environment":    paths.ProbeEnv,
		"provider CA":          paths.CAFile,
		"provider state":       paths.ProviderState,
	} {
		if err := ValidateUserPath(paths.Root, path, true); err != nil {
			return Status{}, fmt.Errorf("%s path: %w", name, err)
		}
	}
	if err := validateProviderEnvironment(config, paths.ProviderEnv); err != nil {
		return Status{}, err
	}
	if err := validateProbeEnvironment(paths.ProbeEnv); err != nil {
		return Status{}, err
	}
	if err := validateSecretFile(paths.CAFile); err != nil {
		return Status{}, fmt.Errorf("provider CA file: %w", err)
	}
	if err := ValidateUserPath(paths.Root, paths.PackageDir(update.SHA256), false); err != nil {
		return Status{}, fmt.Errorf("provider package path: %w", err)
	}
	if err := stageVerifiedProvider(update, paths); err != nil {
		return Status{}, err
	}
	imageRef := providerImageRef(update.SHA256)
	if _, err := refresher.Runner.Run(ctx, Command{
		Path:  config.PodmanPath,
		Args:  []string{"build", "--file", "-", "--tag", imageRef, paths.PackageDir(update.SHA256)},
		Stdin: providerContainerfile,
	}); err != nil {
		return Status{}, fmt.Errorf("build provider candidate image: %w", err)
	}
	imageOutput, err := refresher.Runner.Run(ctx, Command{
		Path: config.PodmanPath,
		Args: []string{"image", "inspect", "--format", "{{.Id}}", imageRef},
	})
	if err != nil {
		return Status{}, fmt.Errorf("inspect provider candidate image: %w", err)
	}
	imageID := strings.TrimSpace(string(imageOutput))
	selection := ImageSelection{Update: update, ImageID: imageID, ImageRef: imageRef, ActivatedAt: now}
	if err := selection.Validate(); err != nil {
		return Status{}, fmt.Errorf("validate provider candidate image: %w", err)
	}
	candidateState := paths.CandidateState(update.SHA256)
	if err := ValidateUserPath(paths.Root, candidateState, false); err != nil {
		return Status{}, fmt.Errorf("provider candidate state path: %w", err)
	}
	if err := prepareCandidateState(paths.ProviderState, candidateState); err != nil {
		return Status{}, err
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-" + digestHex(update.SHA256)[:16],
		Phase:           JournalPrepared,
		Candidate:       selection,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if activeFound {
		previous := active
		journal.Previous = &previous
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		return Status{}, fmt.Errorf("write prepared refresh journal: %w", err)
	}
	activeChanged := false
	rollback := func(cause error) error {
		return errors.Join(cause, refresher.rollback(ctx, config, paths, journal, activeChanged))
	}
	if err := refresher.removeContainer(ctx, config, config.CandidateContainer); err != nil {
		return Status{}, rollback(fmt.Errorf("remove stale provider candidate: %w", err))
	}
	if _, err := refresher.Runner.Run(ctx, candidateProviderCommand(config, paths, candidateState, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("start provider candidate: %w", err))
	}
	if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.CandidateContainer, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("probe provider candidate: %w", err))
	}
	journal.Phase = JournalActivated
	journal.UpdatedAt = refresher.now()
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		return Status{}, rollback(fmt.Errorf("write activated refresh journal: %w", err))
	}
	newActive := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: journal.UpdatedAt}
	if activeFound {
		previous := active.Current
		newActive.Previous = &previous
	}
	if err := AtomicWriteJSON(paths.ActiveState, newActive); err != nil {
		return Status{}, rollback(fmt.Errorf("activate provider state: %w", err))
	}
	activeChanged = true
	if err := refresher.restartProvider(ctx); err != nil {
		return Status{}, rollback(fmt.Errorf("restart active provider: %w", err))
	}
	if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.StableContainer, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("probe active provider: %w", err))
	}
	journal.Phase = JournalCommitted
	journal.UpdatedAt = refresher.now()
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		return Status{}, rollback(fmt.Errorf("commit refresh journal: %w", err))
	}
	if err := refresher.removeContainer(ctx, config, config.CandidateContainer); err != nil {
		return Status{}, fmt.Errorf("remove provider candidate: %w", err)
	}
	if err := removeDurableFile(paths.Journal); err != nil {
		return Status{}, fmt.Errorf("remove committed refresh journal: %w", err)
	}
	return statusForActive(newActive, true, refresher.now()), nil
}

func (refresher Refresher) ServeActive(ctx context.Context, config Config) error {
	if refresher.Runner == nil {
		return errors.New("command runner is required")
	}
	paths := LifecyclePathsFor(config)
	if err := validateInstallRoot(paths.Root); err != nil {
		return err
	}
	active, found, err := readActiveState(paths.ActiveState)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("retained provider has no active image")
	}
	if err := validateProviderEnvironment(config, paths.ProviderEnv); err != nil {
		return err
	}
	if err := validateSecretFile(paths.CAFile); err != nil {
		return fmt.Errorf("provider CA file: %w", err)
	}
	if err := ValidateUserPath(config.InstallRoot, paths.ProviderState, true); err != nil {
		return fmt.Errorf("provider state path: %w", err)
	}
	for name, path := range map[string]string{"provider environment": paths.ProviderEnv, "provider CA": paths.CAFile} {
		if err := ValidateUserPath(paths.Root, path, true); err != nil {
			return fmt.Errorf("%s path: %w", name, err)
		}
	}
	output, err := refresher.Runner.Run(ctx, Command{
		Path: config.PodmanPath,
		Args: []string{"image", "inspect", "--format", "{{.Id}}", active.Current.ImageRef},
	})
	if err != nil {
		return fmt.Errorf("inspect active provider image: %w", err)
	}
	if imageID := strings.TrimSpace(string(output)); imageID != active.Current.ImageID {
		return errors.New("active provider image id does not match durable state")
	}
	return refresher.Runner.Exec(Command{Path: config.PodmanPath, Args: []string{
		"run", "--rm", "--name", config.StableContainer,
		"--network", config.ContainerNetwork,
		"--read-only", "--cap-drop", "all", "--security-opt", "no-new-privileges",
		"--env-file", paths.ProviderEnv,
		"--volume", paths.ProviderState + ":" + providerStateMount + ":rw",
		"--volume", paths.TLSRoot + ":" + providerTLSMount + ":ro",
		active.Current.ImageID, providerListenAddr,
	}})
}

func VerifyCurrentUpdate(ctx context.Context, config Config, runner CommandRunner) (VerifiedUpdate, error) {
	output, err := runner.Run(ctx, Command{
		Path: config.ComputeAgentPath,
		Args: []string{
			"supervisor-update", "verify",
			"-config", config.SupervisorConfigPath,
			"-format", "auto",
			"-component", "provider",
			"-plugin", GitHubPluginID,
			"-component-id", config.ComponentID,
		},
	})
	if err != nil {
		return VerifiedUpdate{}, fmt.Errorf("verify supervisor provider update: %w", err)
	}
	if len(output) > MaxStateFileBytes {
		return VerifiedUpdate{}, errors.New("verified update output exceeds 1 MiB")
	}
	var envelope verifiedUpdateCommandOutput
	if err := decodeStrictJSON(bytes.NewReader(output), &envelope); err != nil {
		return VerifiedUpdate{}, fmt.Errorf("decode verified update output: %w", err)
	}
	update := VerifiedUpdate{
		WorkerID: envelope.WorkerID, DirectiveID: envelope.DirectiveID,
		CampaignID: envelope.CampaignID, Component: envelope.Component,
		PluginID: envelope.PluginID, ComponentID: envelope.ComponentID,
		Version: envelope.Version, Format: envelope.Format,
		Path: envelope.Path, SHA256: envelope.SHA256,
	}
	if err := update.Validate(); err != nil {
		return VerifiedUpdate{}, fmt.Errorf("validate verified update: %w", err)
	}
	if update.WorkerID != config.WorkerID {
		return VerifiedUpdate{}, errors.New("verified update worker_id does not match retained worker")
	}
	if update.PluginID != config.PluginID || update.ComponentID != config.ComponentID {
		return VerifiedUpdate{}, errors.New("verified update plugin_id or component_id does not match retained provider")
	}
	digest, err := hashRegularFile(update.Path, true)
	if err != nil {
		return VerifiedUpdate{}, fmt.Errorf("verify update path: %w", err)
	}
	if digest != update.SHA256 {
		return VerifiedUpdate{}, errors.New("verified update path digest does not match command projection")
	}
	return update, nil
}

type verifiedUpdateCommandOutput struct {
	WorkerID           string          `json:"worker_id"`
	DirectiveID        string          `json:"directive_id"`
	CampaignID         string          `json:"campaign_id"`
	DirectiveIssuedAt  time.Time       `json:"directive_issued_at"`
	DirectiveExpiresAt time.Time       `json:"directive_expires_at"`
	DirectiveSignature json.RawMessage `json:"directive_signature"`
	Component          string          `json:"component"`
	PluginID           string          `json:"plugin_id"`
	ComponentID        string          `json:"component_id"`
	Version            string          `json:"version"`
	Format             string          `json:"format"`
	ArtifactURL        string          `json:"artifact_url"`
	ArtifactSizeBytes  int64           `json:"artifact_size_bytes"`
	ArtifactSignature  json.RawMessage `json:"artifact_signature"`
	Directive          json.RawMessage `json:"directive"`
	Artifact           json.RawMessage `json:"artifact"`
	Path               string          `json:"path"`
	SHA256             string          `json:"sha256"`
	AppliedAt          time.Time       `json:"applied_at"`
}

func candidateProviderCommand(config Config, paths LifecyclePaths, candidateState string, selection ImageSelection) Command {
	return Command{Path: config.PodmanPath, Args: []string{
		"run", "--detach", "--name", config.CandidateContainer,
		"--network", config.ContainerNetwork,
		"--read-only", "--cap-drop", "all", "--security-opt", "no-new-privileges",
		"--env-file", paths.ProviderEnv,
		"--volume", candidateState + ":" + providerStateMount + ":rw",
		"--volume", paths.TLSRoot + ":" + providerTLSMount + ":ro",
		selection.ImageID, providerListenAddr,
	}}
}

func providerProbeCommand(config Config, paths LifecyclePaths, target string, selection ImageSelection) Command {
	arguments := []string{
		"run", "--rm", "--name", target + "-probe",
		"--network", config.ContainerNetwork,
		"--read-only", "--cap-drop", "all", "--security-opt", "no-new-privileges",
		"--env-file", paths.ProbeEnv,
		"--volume", paths.CAFile + ":" + providerCAPath + ":ro",
		selection.ImageID,
		"probe", "-url", "https://" + target + ":18090", "-ca-file", providerCAPath,
		"-organization", config.Organization, "-repository", config.Repository,
		"-workflow", config.Workflow, "-ref", config.Ref,
		"-runner-name", config.RunnerName, "-runner-group", config.RunnerGroup,
	}
	for _, label := range config.Labels {
		arguments = append(arguments, "-label", label)
	}
	return Command{Path: config.PodmanPath, Args: arguments}
}

func (refresher Refresher) restartProvider(ctx context.Context) error {
	_, err := refresher.Runner.Run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "restart", providerServiceUnit}})
	return err
}

func (refresher Refresher) removeContainer(ctx context.Context, config Config, name string) error {
	_, err := refresher.Runner.Run(ctx, Command{Path: config.PodmanPath, Args: []string{"rm", "--force", "--ignore", name}})
	return err
}

func (refresher Refresher) runProbe(ctx context.Context, command Command) error {
	delays := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		if _, err := refresher.Runner.Run(ctx, command); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == len(delays) {
			break
		}
		if err := refresher.sleep(ctx, delays[attempt]); err != nil {
			return err
		}
	}
	return lastErr
}

func (refresher Refresher) sleep(ctx context.Context, duration time.Duration) error {
	if refresher.Sleep != nil {
		return refresher.Sleep(ctx, duration)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (refresher Refresher) rollback(ctx context.Context, config Config, paths LifecyclePaths, journal TransactionJournal, activeChanged bool) error {
	rollbackContext, cancelRollback := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelRollback()
	var rollbackErr error
	if activeChanged {
		if journal.Previous != nil {
			if err := AtomicWriteJSON(paths.ActiveState, *journal.Previous); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			} else if err := refresher.restartProvider(rollbackContext); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			} else if err := refresher.runProbe(rollbackContext, providerProbeCommand(config, paths, config.StableContainer, journal.Previous.Current)); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			}
		} else {
			rollbackErr = errors.Join(rollbackErr, removeDurableFile(paths.ActiveState))
			_, stopErr := refresher.Runner.Run(rollbackContext, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}})
			rollbackErr = errors.Join(rollbackErr, stopErr)
		}
	}
	rollbackErr = errors.Join(rollbackErr, refresher.removeContainer(rollbackContext, config, config.CandidateContainer))
	if rollbackErr == nil {
		rollbackErr = removeDurableFile(paths.Journal)
	}
	return rollbackErr
}

func (refresher Refresher) recoverInterrupted(ctx context.Context, config Config, paths LifecyclePaths) error {
	var journal TransactionJournal
	if err := ReadStrictJSONFile(paths.Journal, &journal); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read interrupted refresh journal: %w", err)
	}
	if err := journal.Validate(); err != nil {
		return fmt.Errorf("validate interrupted refresh journal: %w", err)
	}
	if journal.Previous == nil && journal.Phase != JournalCommitted {
		if err := removeDurableFile(paths.ActiveState); err != nil {
			return fmt.Errorf("remove interrupted initial active state: %w", err)
		}
		if journal.Phase == JournalActivated {
			if _, err := refresher.Runner.Run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}}); err != nil {
				return fmt.Errorf("stop interrupted initial provider: %w", err)
			}
		}
	} else {
		recovered, err := RecoverActiveState(journal)
		if err != nil {
			return err
		}
		if err := AtomicWriteJSON(paths.ActiveState, recovered); err != nil {
			return fmt.Errorf("write recovered active state: %w", err)
		}
		if journal.Phase == JournalActivated {
			if err := refresher.restartProvider(ctx); err != nil {
				return fmt.Errorf("restart recovered provider: %w", err)
			}
			if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.StableContainer, recovered.Current)); err != nil {
				return fmt.Errorf("probe recovered provider: %w", err)
			}
		}
	}
	if err := refresher.removeContainer(ctx, config, config.CandidateContainer); err != nil {
		return fmt.Errorf("remove interrupted provider candidate: %w", err)
	}
	if err := removeDurableFile(paths.Journal); err != nil {
		return fmt.Errorf("remove recovered refresh journal: %w", err)
	}
	return nil
}

func (refresher Refresher) now() time.Time {
	if refresher.Now == nil {
		return time.Now().UTC()
	}
	return refresher.Now().UTC()
}

func stageVerifiedProvider(update VerifiedUpdate, paths LifecyclePaths) error {
	destination := paths.PackageBinary(update.SHA256)
	if existingDigest, err := hashRegularFile(destination, true); err == nil && existingDigest == update.SHA256 {
		return nil
	}
	if err := os.MkdirAll(paths.PackageDir(update.SHA256), 0o700); err != nil {
		return fmt.Errorf("create provider package directory: %w", err)
	}
	temporary, err := os.CreateTemp(paths.PackageDir(update.SHA256), ".provider-*.tmp")
	if err != nil {
		return fmt.Errorf("create provider package temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}()
	source, err := os.Open(update.Path)
	if err != nil {
		return fmt.Errorf("open verified provider package: %w", err)
	}
	defer source.Close()
	copied, err := io.Copy(temporary, io.LimitReader(source, maxProviderPackageBytes+1))
	if err != nil {
		return fmt.Errorf("copy verified provider package: %w", err)
	}
	if copied > maxProviderPackageBytes {
		return fmt.Errorf("verified provider package exceeds %d bytes", maxProviderPackageBytes)
	}
	if err := temporary.Chmod(0o700); err != nil {
		return fmt.Errorf("mark provider package executable: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync provider package: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close provider package: %w", err)
	}
	if digest, err := hashRegularFile(temporaryPath, true); err != nil || digest != update.SHA256 {
		return errors.New("staged provider package digest mismatch")
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("activate provider package: %w", err)
	}
	return syncDirectory(filepath.Dir(destination))
}

func prepareCandidateState(source, destination string) error {
	if err := os.MkdirAll(source, 0o700); err != nil {
		return fmt.Errorf("create provider state: %w", err)
	}
	if err := os.RemoveAll(destination); err != nil {
		return fmt.Errorf("remove stale candidate state: %w", err)
	}
	if err := CloneRegularTree(source, destination, CloneLimits{MaxFiles: 10_000, MaxBytes: 1 << 30}); err != nil {
		return fmt.Errorf("clone provider candidate state: %w", err)
	}
	return nil
}

func readActiveState(path string) (ActiveState, bool, error) {
	var active ActiveState
	if err := ReadStrictJSONFile(path, &active); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ActiveState{}, false, nil
		}
		return ActiveState{}, false, err
	}
	if err := active.Validate(); err != nil {
		return ActiveState{}, false, fmt.Errorf("validate active state: %w", err)
	}
	return active, true, nil
}

func statusForActive(active ActiveState, serviceActive bool, now time.Time) Status {
	return Status{
		ProtocolVersion: StatusProtocolVersion,
		Installed:       true,
		ServiceActive:   serviceActive,
		CurrentVersion:  active.Current.Update.Version,
		CurrentSHA256:   active.Current.Update.SHA256,
		ObservedAt:      now,
	}
}

func validateProviderEnvironment(config Config, path string) error {
	values, err := readEnvironmentFile(path)
	if err != nil {
		return fmt.Errorf("provider environment: %w", err)
	}
	for _, required := range []string{"GITHUB_RUNNER_PROVIDER_TOKEN", "GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN"} {
		if values[required] == "" {
			return fmt.Errorf("provider environment is missing %s", required)
		}
	}
	for _, expected := range []struct {
		key   string
		value string
	}{
		{key: "GITHUB_RUNNER_PROVIDER_STATE_DIR", value: providerStateMount},
		{key: "GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE", value: providerTLSCertPath},
		{key: "GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE", value: providerTLSKeyPath},
	} {
		if values[expected.key] != expected.value {
			return errors.New("provider environment contains an invalid runtime path")
		}
	}
	for _, expected := range []struct {
		key   string
		value string
	}{
		{key: "GITHUB_RUNNER_PROVIDER_REPOSITORIES", value: config.Repository},
		{key: "GITHUB_RUNNER_PROVIDER_ORGANIZATIONS", value: config.Organization},
		{key: "GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS", value: config.RunnerGroup},
	} {
		if !commaSeparatedEnvironmentContains(values[expected.key], expected.value) {
			return errors.New("provider environment is missing a required GitHub allowlist value")
		}
	}
	for key := range values {
		if !allowedProviderEnvironmentKey(key) {
			return errors.New("provider environment contains an unsupported key")
		}
	}
	return nil
}

func commaSeparatedEnvironmentContains(value, expected string) bool {
	for item := range strings.SplitSeq(value, ",") {
		if strings.TrimSpace(item) == expected {
			return true
		}
	}
	return false
}

func allowedProviderEnvironmentKey(key string) bool {
	switch key {
	case "GITHUB_RUNNER_PROVIDER_TOKEN",
		"GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN",
		"GITHUB_RUNNER_PROVIDER_STATE_DIR",
		"GITHUB_RUNNER_PROVIDER_REPOSITORIES",
		"GITHUB_RUNNER_PROVIDER_ORGANIZATIONS",
		"GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS",
		"GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE",
		"GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE",
		"GITHUB_API_BASE_URL":
		return true
	default:
		return false
	}
}

func validateProbeEnvironment(path string) error {
	values, err := readEnvironmentFile(path)
	if err != nil {
		return fmt.Errorf("probe environment: %w", err)
	}
	if len(values) != 1 || values["GITHUB_RUNNER_PROVIDER_TOKEN"] == "" {
		return errors.New("probe environment must contain only GITHUB_RUNNER_PROVIDER_TOKEN")
	}
	return nil
}

func readEnvironmentFile(path string) (map[string]string, error) {
	if err := validateSecretFile(path); err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(io.LimitReader(file, MaxStateFileBytes+1))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if !found || !safeEnvironmentKey(key) || value == "" || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New("environment file contains an invalid entry")
		}
		if _, exists := values[key]; exists {
			return nil, errors.New("environment file contains a duplicate key")
		}
		values[key] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("read environment file")
	}
	return values, nil
}

func safeEnvironmentKey(value string) bool {
	if value == "" {
		return false
	}
	for index, r := range value {
		if r >= 'A' && r <= 'Z' || r == '_' || index > 0 && r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

func validateSecretFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > MaxStateFileBytes {
		return errors.New("secret file must be a non-empty regular file of at most 1 MiB")
	}
	if err := validateStateMode(info); err != nil {
		return err
	}
	return validateOwner(info)
}

func hashRegularFile(path string, requireExecutable bool) (string, error) {
	entry, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !entry.Mode().IsRegular() {
		return "", errors.New("path must be a regular file")
	}
	if requireExecutable && executableModeRequired() && entry.Mode().Perm()&0o111 == 0 {
		return "", errors.New("path must be executable")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(entry, opened) {
		return "", errors.New("path changed during open")
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hasher.Sum(nil)), nil
}

func executableModeRequired() bool {
	return os.PathSeparator == '/'
}

func providerImageRef(digest string) string {
	return "localhost/workflow-plugin-github-runner-provider:sha256-" + digestHex(digest)
}

func digestHex(digest string) string {
	return strings.TrimPrefix(digest, "sha256:")
}

func removeDurableFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func validateInstallRoot(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect retained provider install root: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("retained provider install root must be a real directory")
	}
	if err := validateOwner(info); err != nil {
		return fmt.Errorf("retained provider install root ownership: %w", err)
	}
	if executableModeRequired() && info.Mode().Perm()&0o077 != 0 {
		return errors.New("retained provider install root mode must not allow group or other access")
	}
	return nil
}
