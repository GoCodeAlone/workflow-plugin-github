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
	Root                  string
	ConfigFile            string
	Launcher              string
	ActiveState           string
	Journal               string
	InstallLock           string
	InstallJournal        string
	LifecycleJournal      string
	LifecycleTransactions string
	LifecycleAudit        string
	LifecycleAuditLock    string
	ProviderState         string
	PackagesRoot          string
	CandidatesRoot        string
	ProviderEnv           string
	ProbeEnv              string
	AgentEnv              string
	TLSRoot               string
	CAFile                string
	ServerCert            string
	ServerKey             string
	ContainersConf        string
	ProviderUnit          string
	RefreshUnit           string
	PathUnit              string
	TimerUnit             string
	AgentDropIn           string
}

func LifecyclePathsFor(config Config) LifecyclePaths {
	root := config.InstallRoot
	workspaceRoot := filepath.Dir(root)
	home := filepath.Dir(workspaceRoot)
	stateHome := os.Getenv("XDG_STATE_HOME")
	if stateHome == "" {
		stateHome = filepath.Join(home, ".local", "state")
	}
	audit := filepath.Join(stateHome, "wfctl", "plugins", GitHubPluginID, "retained-provider-audit.jsonl")
	return LifecyclePaths{
		Root:                  root,
		ConfigFile:            filepath.Join(root, "config.json"),
		Launcher:              filepath.Join(root, "bin", "github-runner-provider"),
		ActiveState:           filepath.Join(root, "lifecycle", "active.json"),
		Journal:               filepath.Join(root, "lifecycle", "transaction.json"),
		InstallLock:           filepath.Join(filepath.Dir(root), ".workflow-plugin-github-runner-provider.install.lock"),
		InstallJournal:        filepath.Join(filepath.Dir(root), ".workflow-plugin-github-runner-provider.install-transaction.json"),
		LifecycleJournal:      filepath.Join(workspaceRoot, ".workflow-plugin-github-runner-provider.lifecycle-transaction.json"),
		LifecycleTransactions: filepath.Join(workspaceRoot, ".workflow-plugin-github-runner-provider-transactions"),
		LifecycleAudit:        audit,
		LifecycleAuditLock:    audit + ".lock",
		ProviderState:         filepath.Join(root, "provider-state"),
		PackagesRoot:          filepath.Join(root, "packages"),
		CandidatesRoot:        filepath.Join(root, "candidates"),
		ProviderEnv:           filepath.Join(root, "secrets", "provider.env"),
		ProbeEnv:              filepath.Join(root, "secrets", "probe.env"),
		AgentEnv:              filepath.Join(root, "secrets", "agent.env"),
		TLSRoot:               filepath.Join(root, "tls"),
		CAFile:                filepath.Join(root, "tls", "ca.pem"),
		ServerCert:            filepath.Join(root, "tls", "server.crt"),
		ServerKey:             filepath.Join(root, "tls", "server.key"),
		ContainersConf:        filepath.Join(root, "runtime", "containers.conf"),
		ProviderUnit:          filepath.Join(config.SystemdDir, providerServiceUnit),
		RefreshUnit:           filepath.Join(config.SystemdDir, refreshServiceUnit),
		PathUnit:              filepath.Join(config.SystemdDir, refreshPathUnit),
		TimerUnit:             filepath.Join(config.SystemdDir, refreshTimerUnit),
		AgentDropIn:           filepath.Join(config.SystemdDir, config.AgentUnit+".d", "50-workflow-plugin-github-runner-provider.conf"),
	}
}

func (paths LifecyclePaths) LifecycleTransactionRoot(transactionID string) string {
	return filepath.Join(paths.LifecycleTransactions, transactionID)
}

func (paths LifecyclePaths) CandidateState(digest string) string {
	return filepath.Join(paths.CandidatesRoot, digestHex(digest), "state")
}

func (paths LifecyclePaths) PreviousState(digest string) string {
	return filepath.Join(paths.CandidatesRoot, digestHex(digest), "previous-state")
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
	if err := ValidateUserPath(filepath.Dir(paths.Root), paths.InstallLock, false); err != nil {
		return Status{}, fmt.Errorf("install lock path: %w", err)
	}
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		return Status{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	home := lifecycleHome(paths)
	installer := Installer{Runner: refresher.Runner, Now: refresher.Now, Sleep: refresher.Sleep}
	if err := installer.recoverLifecycleTransaction(ctx, home, paths, refresher); err != nil {
		return Status{}, err
	}
	if err := installer.recoverInstallTransaction(ctx, config, paths, refresher); err != nil {
		return Status{}, err
	}
	requiresMutation, err := refresher.requiresMutation(ctx, config, paths)
	if err != nil {
		return Status{}, err
	}
	if !requiresMutation {
		return refresher.refreshUnchangedUnderLifecycleLock(ctx, home, config, paths)
	}
	return refresher.refreshFencedUnderLifecycleLock(ctx, home, config, paths)
}

func (refresher Refresher) refreshUnchangedUnderLifecycleLock(ctx context.Context, home string, config Config, paths LifecyclePaths) (Status, error) {
	update, err := VerifyCurrentUpdate(ctx, config, refresher.Runner)
	if err != nil {
		return Status{}, err
	}
	active, found, err := readActiveState(paths.ActiveState)
	if err != nil {
		return Status{}, err
	}
	if !found || active.Current.Update.SHA256 != update.SHA256 {
		return refresher.refreshFencedUnderLifecycleLock(ctx, home, config, paths)
	}
	installer := Installer{Runner: refresher.Runner, Now: refresher.Now, Sleep: refresher.Sleep}
	transaction, err := newLifecycleJournal(config, LifecycleRefresh, ProviderUnchanged, nil, refresher.now())
	if err != nil {
		return Status{}, err
	}
	signature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return Status{}, err
	}
	transaction.Recovery.AgentUnitBefore = signature
	transaction.Unchanged = &LifecycleUnchangedProvenance{Active: active.Current, Candidate: update}
	if err := startLifecycleTransaction(home, paths, &transaction); err != nil {
		return Status{}, err
	}
	status, err := refresher.refreshUnderLifecycleTransaction(ctx, config, true, false, "", "", update.SHA256)
	if err != nil {
		return Status{}, errors.Join(err, finishLifecycleTransaction(home, paths, &transaction))
	}
	transaction.Unchanged.StableProbeAt = refresher.now()
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReady, LifecycleCommit, refresher.now()); err != nil {
		return Status{}, err
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReleasing, LifecycleCommit, refresher.now()); err != nil {
		return Status{}, err
	}
	_ = drainLifecycleAudit(home, paths, &transaction)
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleCommitted, LifecycleCommit, refresher.now()); err != nil {
		return Status{}, err
	}
	if err := finalizeLifecycleTransaction(home, paths, &transaction, refresher); err != nil {
		return Status{}, err
	}
	return status, nil
}

func (refresher Refresher) requiresMutation(ctx context.Context, config Config, paths LifecyclePaths) (bool, error) {
	update, err := VerifyCurrentUpdate(ctx, config, refresher.Runner)
	if err != nil {
		return false, err
	}
	active, found, err := readActiveState(paths.ActiveState)
	if err != nil {
		return false, err
	}
	if !found {
		if err := refresher.validateInitialInstaller(update); err != nil {
			return false, err
		}
	}
	return !found || active.Current.Update.SHA256 != update.SHA256, nil
}

func (refresher Refresher) validateInitialInstaller(update VerifiedUpdate) error {
	executablePath := refresher.ExecutablePath
	if executablePath == nil {
		executablePath = os.Executable
	}
	currentExecutable, err := executablePath()
	if err != nil {
		return fmt.Errorf("resolve installer executable: %w", err)
	}
	digest, err := hashRegularFile(currentExecutable, true)
	if err != nil {
		return fmt.Errorf("hash installer executable: %w", err)
	}
	if digest != update.SHA256 {
		return errors.New("installer digest does not match verified provider update")
	}
	return nil
}

func (refresher Refresher) refreshFencedUnderLifecycleLock(ctx context.Context, home string, config Config, paths LifecyclePaths) (Status, error) {
	installer := Installer{Runner: refresher.Runner, Now: refresher.Now, Sleep: refresher.Sleep}
	transaction, err := newLifecycleJournal(config, LifecycleRefresh, ProviderChanged, nil, refresher.now())
	if err != nil {
		return Status{}, err
	}
	beforeSignature, err := installer.inspectAgentUnitSignature(ctx, home, config)
	if err != nil {
		return Status{}, err
	}
	transaction.Recovery.AgentUnitBefore = beforeSignature
	if err := startLifecycleTransaction(home, paths, &transaction); err != nil {
		return Status{}, err
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFencing, "", refresher.now()); err != nil {
		return Status{}, err
	}
	fail := func(cause error) (Status, error) {
		rollbackContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return Status{}, errors.Join(cause, installer.recoverLifecycleTransaction(rollbackContext, home, paths, refresher))
	}
	if err := installer.beginMaintenance(ctx, config, refreshMaintenanceID, refreshMaintenanceReason); err != nil {
		return fail(err)
	}
	if err := installer.waitLocalState(ctx, config, "unavailable"); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent refresh fence: %w", err)
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return Status{}, err
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleFenced, "", refresher.now()); err != nil {
		return fail(err)
	}
	if err := installer.systemctl(ctx, "stop", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("stop retained agent for provider refresh: %w", err))
	}
	status, err := refresher.refreshUnderLifecycleTransaction(ctx, config, true, true, transaction.TransactionID, config.ProfileID, "")
	if err != nil {
		return fail(err)
	}
	inner, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found || inner.Phase != JournalCommitted {
		return fail(errors.Join(errors.New("provider refresh did not leave a deferred committed transaction"), err))
	}
	transaction.ProviderTransaction = &LifecycleProviderTransaction{
		TransactionID: inner.ID, ProfileID: config.ProfileID, Digest: inner.Candidate.Update.SHA256,
	}
	transaction.UpdatedAt = refresher.now()
	if err := writeLifecycleJournal(home, paths, transaction); err != nil {
		return fail(err)
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	if err := installer.systemctl(ctx, "start", config.AgentUnit); err != nil {
		return fail(fmt.Errorf("restart retained agent after provider refresh: %w", err))
	}
	if err := installer.waitLocalState(ctx, config, "unavailable"); err != nil {
		return fail(fmt.Errorf("verify retained agent remains refresh-fenced: %w", err))
	}
	if err := installer.reattestLifecycleAuthority(ctx, home, transaction); err != nil {
		return fail(err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReady, LifecycleCommit, refresher.now()); err != nil {
		return fail(err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleReleasing, LifecycleCommit, refresher.now()); err != nil {
		return fail(err)
	}
	_ = drainLifecycleAudit(home, paths, &transaction)
	if err := installer.releaseLifecycleMaintenance(ctx, home, transaction); err != nil {
		return Status{}, fmt.Errorf("release retained agent refresh fence: %w", err)
	}
	if err := writeLifecycleTransition(home, paths, &transaction, LifecycleCommitted, LifecycleCommit, refresher.now()); err != nil {
		return Status{}, err
	}
	if err := finalizeLifecycleTransaction(home, paths, &transaction, refresher); err != nil {
		return Status{}, err
	}
	if err := installer.waitLocalState(ctx, config, "idle"); err != nil {
		return Status{}, fmt.Errorf("wait for retained agent after provider refresh: %w", err)
	}
	return status, nil
}

func (refresher Refresher) refreshUnderLifecycleLock(ctx context.Context, config Config, verifyCurrent, deferCommit bool) (Status, error) {
	return refresher.refreshUnderLifecycleTransaction(ctx, config, verifyCurrent, deferCommit, "", "", "")
}

func (refresher Refresher) refreshUnderLifecycleTransaction(ctx context.Context, config Config, verifyCurrent, deferCommit bool, outerTransactionID, profileID, expectedDigest string) (Status, error) {
	if refresher.Runner == nil {
		return Status{}, errors.New("command runner is required")
	}
	paths := LifecyclePathsFor(config)
	if err := refresher.recoverInterrupted(ctx, config, paths); err != nil {
		return Status{}, err
	}
	update, err := VerifyCurrentUpdate(ctx, config, refresher.Runner)
	if err != nil {
		return Status{}, err
	}
	if expectedDigest != "" && update.SHA256 != expectedDigest {
		return Status{}, errors.New("verified provider update changed during unchanged refresh")
	}
	active, activeFound, err := readActiveState(paths.ActiveState)
	if err != nil {
		return Status{}, err
	}
	now := refresher.now()
	if !activeFound {
		if err := refresher.validateInitialInstaller(update); err != nil {
			return Status{}, err
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
	if err := refresher.validateProviderNetwork(ctx, config); err != nil {
		return Status{}, err
	}
	if activeFound && active.Current.Update.SHA256 == update.SHA256 {
		if verifyCurrent {
			if err := refresher.probeStableActive(ctx, config, paths, active.Current); err != nil {
				return Status{}, err
			}
		}
		return statusForActive(active, true, now), nil
	}
	if err := ValidateUserPath(paths.Root, paths.PackageDir(update.SHA256), false); err != nil {
		return Status{}, fmt.Errorf("provider package path: %w", err)
	}
	if err := stageVerifiedProvider(update, paths); err != nil {
		return Status{}, err
	}
	imageRef := providerImageRef(update.SHA256)
	if _, err := refresher.run(ctx, Command{
		Path:  config.PodmanPath,
		Args:  []string{"build", "--file", "-", "--tag", imageRef, paths.PackageDir(update.SHA256)},
		Stdin: providerContainerfile,
	}); err != nil {
		return Status{}, fmt.Errorf("build provider candidate image: %w", err)
	}
	imageOutput, err := refresher.run(ctx, Command{
		Path: config.PodmanPath,
		Args: []string{"image", "inspect", "--format", "{{.Id}}", imageRef},
	})
	if err != nil {
		return Status{}, fmt.Errorf("inspect provider candidate image: %w", err)
	}
	imageID, err := normalizePodmanImageID(string(imageOutput))
	if err != nil {
		return Status{}, fmt.Errorf("validate provider candidate image id: %w", err)
	}
	selection := ImageSelection{Update: update, ImageID: imageID, ImageRef: imageRef, ActivatedAt: now}
	if err := selection.Validate(); err != nil {
		return Status{}, fmt.Errorf("validate provider candidate image: %w", err)
	}
	candidateState := paths.CandidateState(update.SHA256)
	if err := ValidateUserPath(paths.Root, candidateState, false); err != nil {
		return Status{}, fmt.Errorf("provider candidate state path: %w", err)
	}
	journal := TransactionJournal{
		ProtocolVersion:    TransactionJournalProtocolVersion,
		ID:                 "refresh-" + digestHex(update.SHA256)[:16],
		Phase:              JournalPrepared,
		DeferredCommit:     deferCommit,
		OuterTransactionID: outerTransactionID,
		ProfileID:          profileID,
		Candidate:          selection,
		StartedAt:          now,
		UpdatedAt:          now,
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
	if _, err := refresher.run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}}); err != nil {
		return Status{}, rollback(fmt.Errorf("quiesce active provider before state clone: %w", err))
	}
	if err := prepareCandidateState(paths.ProviderState, candidateState); err != nil {
		return Status{}, rollback(err)
	}
	if _, err := refresher.run(ctx, candidateProviderCommand(config, paths, candidateState, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("start provider candidate: %w", err))
	}
	if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.CandidateContainer, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("probe provider candidate: %w", err))
	}
	if err := writeJournalPhase(paths.Journal, &journal, JournalStatePromoting, refresher.now()); err != nil {
		return Status{}, rollback(fmt.Errorf("write state-promoting refresh journal: %w", err))
	}
	if err := refresher.removeContainer(ctx, config, config.CandidateContainer); err != nil {
		return Status{}, rollback(fmt.Errorf("stop probed provider candidate: %w", err))
	}
	if err := promoteCandidateProviderState(paths, update.SHA256); err != nil {
		return Status{}, rollback(fmt.Errorf("promote provider candidate state: %w", err))
	}
	if err := writeJournalPhase(paths.Journal, &journal, JournalStatePromoted, refresher.now()); err != nil {
		return Status{}, rollback(fmt.Errorf("write state-promoted refresh journal: %w", err))
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
	if err := writeJournalPhase(paths.Journal, &journal, JournalActivated, refresher.now()); err != nil {
		return Status{}, rollback(fmt.Errorf("write activated refresh journal: %w", err))
	}
	if err := refresher.restartProvider(ctx); err != nil {
		return Status{}, rollback(fmt.Errorf("restart active provider: %w", err))
	}
	if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.StableContainer, selection)); err != nil {
		return Status{}, rollback(fmt.Errorf("probe active provider: %w", err))
	}
	if err := writeJournalPhase(paths.Journal, &journal, JournalCommitted, refresher.now()); err != nil {
		return Status{}, rollback(fmt.Errorf("commit refresh journal: %w", err))
	}
	if deferCommit {
		return statusForActive(newActive, true, refresher.now()), nil
	}
	if err := cleanupProviderStateTransaction(paths, update.SHA256); err != nil {
		return Status{}, fmt.Errorf("remove committed provider state rollback target: %w", err)
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
	output, err := refresher.run(ctx, Command{
		Path: config.PodmanPath,
		Args: []string{"image", "inspect", "--format", "{{.Id}}", active.Current.ImageRef},
	})
	if err != nil {
		return fmt.Errorf("inspect active provider image: %w", err)
	}
	imageID, err := normalizePodmanImageID(string(output))
	if err != nil {
		return fmt.Errorf("validate active provider image id: %w", err)
	}
	if imageID != active.Current.ImageID {
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
	output, err := runBoundedCommand(ctx, runner, Command{
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

func (refresher Refresher) validateProviderNetwork(ctx context.Context, config Config) error {
	output, err := refresher.run(ctx, Command{Path: config.PodmanPath, Args: []string{
		"network", "inspect", "--format", "{{.Driver}} {{.DNSEnabled}} {{.Internal}}", config.ContainerNetwork,
	}})
	if err != nil {
		return fmt.Errorf("inspect provider network: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 3 || fields[0] != "bridge" || fields[1] != "true" || fields[2] != "false" {
		return errors.New("provider network must be a non-internal bridge with DNS enabled")
	}
	return nil
}

func (refresher Refresher) restartProvider(ctx context.Context) error {
	_, err := refresher.run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "restart", providerServiceUnit}})
	return err
}

// RestartAndProbeActive revalidates the stable service even when the package
// digest did not change, as happens during credential rotation.
func (refresher Refresher) RestartAndProbeActive(ctx context.Context, config Config) error {
	if refresher.Runner == nil {
		return errors.New("command runner is required")
	}
	paths := LifecyclePathsFor(config)
	active, found, err := readActiveState(paths.ActiveState)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("retained provider has no active image")
	}
	if err := refresher.restartProvider(ctx); err != nil {
		return fmt.Errorf("restart active provider: %w", err)
	}
	return refresher.probeStableActive(ctx, config, paths, active.Current)
}

func (refresher Refresher) probeStableActive(ctx context.Context, config Config, paths LifecyclePaths, selection ImageSelection) error {
	output, err := refresher.run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{
		"--user", "show", providerServiceUnit, "--property", "ActiveState", "--value",
	}})
	if err != nil {
		return fmt.Errorf("inspect active provider service: %w", err)
	}
	if strings.TrimSpace(string(output)) != "active" {
		return errors.New("retained provider service is not active")
	}
	if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.StableContainer, selection)); err != nil {
		return fmt.Errorf("probe active provider: %w", err)
	}
	return nil
}

func (refresher Refresher) removeContainer(ctx context.Context, config Config, name string) error {
	_, err := refresher.run(ctx, Command{Path: config.PodmanPath, Args: []string{"rm", "--force", "--ignore", name}})
	return err
}

func (refresher Refresher) runProbe(ctx context.Context, command Command) error {
	delays := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second}
	var lastErr error
	for attempt := 0; attempt <= len(delays); attempt++ {
		if _, err := refresher.run(ctx, command); err == nil {
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
	rollbackErr = errors.Join(rollbackErr, refresher.removeContainer(rollbackContext, config, config.CandidateContainer))
	providerStateRestored := true
	if journal.Phase != JournalPrepared && journal.Phase != JournalCommitted {
		if _, err := refresher.run(rollbackContext, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}}); err != nil {
			providerStateRestored = false
			rollbackErr = errors.Join(rollbackErr, err)
		} else if err := restorePreviousProviderState(paths, journal.Candidate.Update.SHA256, journal.Phase); err != nil {
			providerStateRestored = false
			rollbackErr = errors.Join(rollbackErr, err)
		}
	}
	if activeChanged && providerStateRestored {
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
			_, stopErr := refresher.run(rollbackContext, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}})
			rollbackErr = errors.Join(rollbackErr, stopErr)
		}
	}
	if !activeChanged && providerStateRestored && journal.Phase != JournalPrepared && journal.Phase != JournalCommitted {
		if journal.Previous != nil {
			if err := refresher.restartProvider(rollbackContext); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			} else if err := refresher.runProbe(rollbackContext, providerProbeCommand(config, paths, config.StableContainer, journal.Previous.Current)); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			}
		} else {
			_, stopErr := refresher.run(rollbackContext, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}})
			rollbackErr = errors.Join(rollbackErr, stopErr)
		}
	}
	if journal.Phase == JournalPrepared {
		rollbackErr = errors.Join(rollbackErr, cleanupProviderStateTransaction(paths, journal.Candidate.Update.SHA256))
		if journal.Previous != nil {
			if err := refresher.restartProvider(rollbackContext); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			} else if err := refresher.runProbe(rollbackContext, providerProbeCommand(config, paths, config.StableContainer, journal.Previous.Current)); err != nil {
				rollbackErr = errors.Join(rollbackErr, err)
			}
		} else {
			_, stopErr := refresher.run(rollbackContext, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}})
			rollbackErr = errors.Join(rollbackErr, stopErr)
		}
	}
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
	if err := refresher.removeContainer(ctx, config, config.CandidateContainer); err != nil {
		return fmt.Errorf("remove interrupted provider candidate: %w", err)
	}
	if journal.Phase != JournalCommitted {
		if _, err := refresher.run(ctx, Command{Path: "/usr/bin/systemctl", Args: []string{"--user", "stop", providerServiceUnit}}); err != nil {
			return fmt.Errorf("stop interrupted provider before state recovery: %w", err)
		}
	}
	if journal.Phase == JournalCommitted {
		if journal.DeferredCommit {
			return errors.New("deferred installer refresh requires installer finalization or rollback")
		}
		recovered, err := RecoverActiveState(journal)
		if err != nil {
			return err
		}
		if err := AtomicWriteJSON(paths.ActiveState, recovered); err != nil {
			return fmt.Errorf("write recovered active state: %w", err)
		}
		if err := cleanupProviderStateTransaction(paths, journal.Candidate.Update.SHA256); err != nil {
			return fmt.Errorf("clean committed provider state transaction: %w", err)
		}
	} else if journal.Previous == nil {
		if journal.Phase != JournalPrepared {
			if err := restorePreviousProviderState(paths, journal.Candidate.Update.SHA256, journal.Phase); err != nil {
				return fmt.Errorf("restore interrupted initial provider state: %w", err)
			}
		} else if err := cleanupProviderStateTransaction(paths, journal.Candidate.Update.SHA256); err != nil {
			return fmt.Errorf("clean interrupted initial candidate state: %w", err)
		}
		if err := removeDurableFile(paths.ActiveState); err != nil {
			return fmt.Errorf("remove interrupted initial active state: %w", err)
		}
	} else {
		if journal.Phase != JournalPrepared {
			if err := restorePreviousProviderState(paths, journal.Candidate.Update.SHA256, journal.Phase); err != nil {
				return fmt.Errorf("restore interrupted provider state: %w", err)
			}
		} else if err := cleanupProviderStateTransaction(paths, journal.Candidate.Update.SHA256); err != nil {
			return fmt.Errorf("clean interrupted candidate state: %w", err)
		}
		recovered, err := RecoverActiveState(journal)
		if err != nil {
			return err
		}
		if err := AtomicWriteJSON(paths.ActiveState, recovered); err != nil {
			return fmt.Errorf("write recovered active state: %w", err)
		}
		if err := refresher.restartProvider(ctx); err != nil {
			return fmt.Errorf("restart recovered provider: %w", err)
		}
		if err := refresher.runProbe(ctx, providerProbeCommand(config, paths, config.StableContainer, recovered.Current)); err != nil {
			return fmt.Errorf("probe recovered provider: %w", err)
		}
	}
	if err := removeDurableFile(paths.Journal); err != nil {
		return fmt.Errorf("remove recovered refresh journal: %w", err)
	}
	return nil
}

func writeJournalPhase(path string, journal *TransactionJournal, phase JournalPhase, updatedAt time.Time) error {
	next := *journal
	next.Phase = phase
	next.UpdatedAt = updatedAt
	if err := AtomicWriteJSON(path, next); err != nil {
		return err
	}
	*journal = next
	return nil
}

func (refresher Refresher) finalizeDeferredRefresh(config Config) error {
	paths := LifecyclePathsFor(config)
	journal, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found {
		return err
	}
	if !journal.DeferredCommit || journal.Phase != JournalCommitted {
		return errors.New("retained provider journal is not a deferred committed refresh")
	}
	if err := cleanupProviderStateTransaction(paths, journal.Candidate.Update.SHA256); err != nil {
		return err
	}
	return removeDurableFile(paths.Journal)
}

func (refresher Refresher) finalizeInterruptedDeferredCommit(config Config) error {
	paths := LifecyclePathsFor(config)
	journal, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found || !journal.DeferredCommit || journal.Phase != JournalCommitted {
		return err
	}
	return refresher.finalizeDeferredRefresh(config)
}

func (refresher Refresher) rollbackDeferredRefresh(ctx context.Context, config Config) error {
	paths := LifecyclePathsFor(config)
	journal, found, err := readTransactionJournal(paths.Journal)
	if err != nil || !found {
		return err
	}
	if !journal.DeferredCommit || journal.Phase != JournalCommitted {
		return errors.New("retained provider journal is not a deferred committed refresh")
	}
	journal.Phase = JournalActivated
	return refresher.rollback(ctx, config, paths, journal, true)
}

func readTransactionJournal(path string) (TransactionJournal, bool, error) {
	var journal TransactionJournal
	if err := ReadStrictJSONFile(path, &journal); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return TransactionJournal{}, false, nil
		}
		return TransactionJournal{}, false, err
	}
	if err := journal.Validate(); err != nil {
		return TransactionJournal{}, false, err
	}
	return journal, true, nil
}

func promoteCandidateProviderState(paths LifecyclePaths, digest string) error {
	candidate := paths.CandidateState(digest)
	previous := paths.PreviousState(digest)
	if err := validateOwnedDirectory(paths.ProviderState); err != nil {
		return fmt.Errorf("validate active provider state: %w", err)
	}
	if err := validateOwnedDirectory(candidate); err != nil {
		return fmt.Errorf("validate candidate provider state: %w", err)
	}
	if _, err := os.Lstat(previous); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("provider state rollback target already exists")
		}
		return fmt.Errorf("inspect provider state rollback target: %w", err)
	}
	if err := os.Rename(paths.ProviderState, previous); err != nil {
		return fmt.Errorf("retain previous provider state: %w", err)
	}
	if err := os.Rename(candidate, paths.ProviderState); err != nil {
		restoreErr := os.Rename(previous, paths.ProviderState)
		return errors.Join(fmt.Errorf("activate candidate provider state: %w", err), restoreErr)
	}
	return errors.Join(syncDirectory(paths.Root), syncDirectory(filepath.Dir(candidate)))
}

func restorePreviousProviderState(paths LifecyclePaths, digest string, phase JournalPhase) error {
	previous := paths.PreviousState(digest)
	if _, err := os.Lstat(previous); errors.Is(err, os.ErrNotExist) {
		if phase != JournalStatePromoting {
			return fmt.Errorf("missing previous provider state during %s recovery", phase)
		}
		if err := validateOwnedDirectory(paths.ProviderState); err != nil {
			return fmt.Errorf("validate unpromoted provider state: %w", err)
		}
		if err := validateOwnedDirectory(paths.CandidateState(digest)); err != nil {
			return fmt.Errorf("validate unpromoted candidate state: %w", err)
		}
		return cleanupProviderStateTransaction(paths, digest)
	} else if err != nil {
		return fmt.Errorf("inspect previous provider state: %w", err)
	}
	if err := validateOwnedDirectory(previous); err != nil {
		return fmt.Errorf("validate previous provider state: %w", err)
	}
	if _, err := os.Lstat(paths.ProviderState); err == nil {
		if err := removeOwnedDirectory(paths.ProviderState); err != nil {
			return fmt.Errorf("remove uncommitted provider state: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect uncommitted provider state: %w", err)
	}
	if err := os.Rename(previous, paths.ProviderState); err != nil {
		return fmt.Errorf("restore previous provider state: %w", err)
	}
	if err := syncDirectory(paths.Root); err != nil {
		return err
	}
	return cleanupProviderStateTransaction(paths, digest)
}

func cleanupProviderStateTransaction(paths LifecyclePaths, digest string) error {
	transactionRoot := filepath.Join(paths.CandidatesRoot, digestHex(digest))
	info, err := os.Lstat(transactionRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("provider state transaction root must be a real directory")
	}
	if err := validateOwner(info); err != nil {
		return err
	}
	if err := os.RemoveAll(transactionRoot); err != nil {
		return err
	}
	return syncDirectory(paths.CandidatesRoot)
}

func validateOwnedDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("path must be a real directory")
	}
	return validateOwner(info)
}

func removeOwnedDirectory(path string) error {
	if err := validateOwnedDirectory(path); err != nil {
		return err
	}
	return os.RemoveAll(path)
}

func (refresher Refresher) now() time.Time {
	if refresher.Now == nil {
		return time.Now().UTC()
	}
	return refresher.Now().UTC()
}

func (refresher Refresher) run(ctx context.Context, command Command) ([]byte, error) {
	return runBoundedCommand(ctx, refresher.Runner, command)
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

func normalizePodmanImageID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if digestPattern.MatchString(value) {
		return value, nil
	}
	if len(value) == 64 {
		candidate := "sha256:" + value
		if digestPattern.MatchString(candidate) {
			return candidate, nil
		}
	}
	return "", errors.New("image id must be a lowercase SHA-256 digest")
}

func digestHex(digest string) string {
	return strings.TrimPrefix(digest, "sha256:")
}

func removeDurableFile(path string) error {
	if err := os.Remove(path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if _, parentErr := os.Lstat(filepath.Dir(path)); errors.Is(parentErr, os.ErrNotExist) {
			return nil
		} else if parentErr != nil {
			return parentErr
		}
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
