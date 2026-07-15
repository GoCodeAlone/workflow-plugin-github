package retainedprovider

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestRenderSystemdUnitsUsesStableAbsolutePathsAndNoShell(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	units, err := RenderSystemdUnits(config, paths)
	if err != nil {
		t.Fatalf("render units: %v", err)
	}

	for name, unit := range map[string]string{
		"provider": units.ProviderService,
		"refresh":  units.RefreshService,
		"path":     units.RefreshPath,
		"timer":    units.RefreshTimer,
		"drop-in":  units.AgentDropIn,
	} {
		if strings.Contains(unit, "ExecStart=/bin/sh") || strings.Contains(unit, "ExecStart=\"/bin/sh\"") || strings.Contains(unit, " /bin/sh ") || strings.Contains(unit, "provider-secret") || strings.Contains(unit, "github-secret") {
			t.Fatalf("%s unit contains shell or secret: %s", name, unit)
		}
	}
	for _, required := range []string{
		"ExecStart=" + systemdQuote(paths.Launcher) + " retained serve-active -config " + systemdQuote(paths.ConfigFile),
		"Restart=on-failure",
	} {
		if !strings.Contains(units.ProviderService, required) {
			t.Fatalf("provider unit missing %q:\n%s", required, units.ProviderService)
		}
	}
	wantRefreshStartTimeout := "TimeoutStartSec=" + strconv.FormatInt(ceilDurationSeconds(retainedRefreshServiceStartTimeout), 10) + "s"
	wantRefreshStopTimeout := "TimeoutStopSec=" + strconv.FormatInt(ceilDurationSeconds(retainedRefreshServiceStopTimeout), 10) + "s"
	if !strings.Contains(units.RefreshService, "ExecStart="+systemdQuote(paths.Launcher)+" retained refresh -config "+systemdQuote(paths.ConfigFile)) || !strings.Contains(units.RefreshService, "Type=oneshot") || !strings.Contains(units.RefreshService, wantRefreshStartTimeout) || !strings.Contains(units.RefreshService, wantRefreshStopTimeout) {
		t.Fatalf("refresh service = %s", units.RefreshService)
	}
	for directive, minimum := range map[string]time.Duration{
		"TimeoutStartSec": retainedRefreshServiceStartTimeout,
		"TimeoutStopSec":  retainedRefreshServiceStopTimeout,
	} {
		var rendered time.Duration
		for _, line := range strings.Split(units.RefreshService, "\n") {
			value, found := strings.CutPrefix(line, directive+"=")
			if !found {
				continue
			}
			seconds, err := strconv.ParseInt(strings.TrimSuffix(value, "s"), 10, 64)
			if err != nil || !strings.HasSuffix(value, "s") {
				t.Fatalf("parse %s=%q: %v", directive, value, err)
			}
			rendered = time.Duration(seconds) * time.Second
		}
		if rendered < minimum {
			t.Fatalf("%s = %s want at least %s", directive, rendered, minimum)
		}
	}
	if !strings.Contains(units.RefreshPath, "PathChanged="+systemdPathValue(config.ProviderMarkerPath)) || !strings.Contains(units.RefreshPath, "Unit="+refreshServiceUnit) {
		t.Fatalf("refresh path = %s", units.RefreshPath)
	}
	if !strings.Contains(units.RefreshTimer, "OnBootSec=30s") || !strings.Contains(units.RefreshTimer, "OnUnitInactiveSec=300s") || strings.Contains(units.RefreshTimer, "OnUnitActiveSec=") || !strings.Contains(units.RefreshTimer, "Persistent=true") {
		t.Fatalf("refresh timer = %s", units.RefreshTimer)
	}
	if !strings.Contains(units.AgentDropIn, "EnvironmentFile="+systemdPathValue(paths.AgentEnv)) || strings.Contains(units.AgentDropIn, `EnvironmentFile="`) || strings.Contains(units.AgentDropIn, paths.ProviderEnv) {
		t.Fatalf("agent drop-in = %s", units.AgentDropIn)
	}
	if strings.Contains(units.ProviderService, "--network") || strings.Contains(units.ProviderService, "podman") || strings.Contains(units.ProviderService, "EnvironmentFile=") {
		t.Fatalf("provider unit bypasses serve-active/env boundary: %s", units.ProviderService)
	}
	for name, unit := range map[string]string{"provider": units.ProviderService, "refresh": units.RefreshService} {
		if strings.Contains(unit, "NoNewPrivileges=true") || strings.Contains(unit, "PrivateTmp=true") {
			t.Fatalf("%s unit blocks rootless Podman namespace setup: %s", name, unit)
		}
	}
}

func ceilDurationSeconds(duration time.Duration) int64 {
	return int64((duration + time.Second - 1) / time.Second)
}

func TestRetainedTimeoutsCoverBoundedRefreshAndRollbackOperations(t *testing.T) {
	localStatusBudget := time.Duration(localStatusAttempts)*controlCommandTimeout + time.Duration(localStatusAttempts-1)*time.Second
	probeBudget := 5*providerProbeTimeout + 250*time.Millisecond + 500*time.Millisecond + time.Second + 2*time.Second
	probeOwnershipBudget := 4 * providerProbeAttemptCount * controlCommandTimeout
	controlAndFilesystemMargin := 20*controlCommandTimeout + 5*time.Minute
	minimumRefresh := 3*localStatusBudget + providerBuildTimeout + 2*(probeBudget+probeOwnershipBudget) + 2*containerStartTimeout + controlAndFilesystemMargin
	if retainedRefreshTimeout < minimumRefresh {
		t.Fatalf("refresh timeout = %s want at least %s", retainedRefreshTimeout, minimumRefresh)
	}
	minimumRollback := probeBudget + probeOwnershipBudget + 6*controlCommandTimeout + 5*time.Minute
	if retainedRollbackTimeout < minimumRollback {
		t.Fatalf("rollback timeout = %s want at least %s", retainedRollbackTimeout, minimumRollback)
	}
	minimumServiceStart := 2*lifecycleRecoveryTimeout + retainedRollbackTimeout + installRollbackTimeout + retainedRefreshTimeout
	if retainedRefreshServiceStartTimeout < minimumServiceStart {
		t.Fatalf("refresh service start timeout = %s want at least %s", retainedRefreshServiceStartTimeout, minimumServiceStart)
	}
	if retainedRefreshServiceStopTimeout < lifecycleRecoveryTimeout {
		t.Fatalf("refresh service stop timeout = %s want at least %s", retainedRefreshServiceStopTimeout, lifecycleRecoveryTimeout)
	}

	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_000_000, 0).UTC()
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "rollback-timeout",
		Phase:           JournalStaging,
		Candidate:       ImageSelection{Update: validTestSelection(now).Update},
		StartedAt:       now,
		UpdatedAt:       now,
	}
	runner := &recordingCommandRunner{}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write rollback journal: %v", err)
	}
	if err := (Refresher{Runner: runner}).rollback(t.Context(), config, paths, journal, false); err != nil {
		t.Fatalf("rollback timeout budget: %v", err)
	}
}

func TestSystemdQuoteEscapesSpecifierExpansion(t *testing.T) {
	if got, want := systemdQuote(`/home/user%name/"provider"`), `"/home/user%%name/\"provider\""`; got != want {
		t.Fatalf("systemdQuote = %q want %q", got, want)
	}
}

func TestRenderSystemdUnitsEscapesPathSettingWithoutGenericQuotes(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	config.ProviderMarkerPath = filepath.Join(home, `updates/current provider%marker`)
	units, err := RenderSystemdUnits(config, LifecyclePathsFor(config))
	if err != nil {
		t.Fatalf("render units: %v", err)
	}
	want := "PathChanged=" + strings.ReplaceAll(strings.ReplaceAll(config.ProviderMarkerPath, " ", `\x20`), "%", "%%")
	if !strings.Contains(units.RefreshPath, want) {
		t.Fatalf("refresh path missing %q:\n%s", want, units.RefreshPath)
	}
	if strings.Contains(units.RefreshPath, `PathChanged="`) {
		t.Fatalf("refresh path used ExecStart-style quoting:\n%s", units.RefreshPath)
	}
}

func TestGenerateInstallMaterialSeparatesProviderProbeAndAgentSecrets(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	credentials := Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}
	now := time.Unix(1_700_000_000, 0).UTC()
	material, err := GenerateInstallMaterial(config, credentials, bytes.NewReader(bytes.Repeat([]byte{0x42}, 4096)), now)
	if err != nil {
		t.Fatalf("generate install material: %v", err)
	}
	if !strings.Contains(string(material.ProviderEnv), "GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN=github-secret") || !strings.Contains(string(material.ProviderEnv), "GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret") {
		t.Fatalf("provider env missing credentials: %s", material.ProviderEnv)
	}
	if strings.Contains(string(material.ProbeEnv), "github-secret") || string(material.ProbeEnv) != "GITHUB_RUNNER_PROVIDER_TOKEN=provider-secret\n" {
		t.Fatalf("probe env scope = %s", material.ProbeEnv)
	}
	agentText := string(material.AgentEnv)
	for key, expected := range map[string]string{
		"WORKFLOW_COMPUTE_DYNAMIC_PROVIDER_GITHUB_ACTIONS_RUNNER_ENV_KEYS": "COMPUTE_GITHUB_RUNNER_PROVIDER_URL,COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN,COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64",
		"COMPUTE_GITHUB_RUNNER_PROVIDER_URL":                               config.ProviderURL,
		"COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN":                             "provider-secret",
		"CONTAINERS_CONF":                                                  paths.ContainersConf,
	} {
		if got := systemdEnvironmentValue(agentText, key); got != expected {
			t.Fatalf("agent env %s = %q want %q: %s", key, got, expected, agentText)
		}
	}
	if strings.Contains(agentText, "github-secret") || strings.Contains(agentText, "GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN") {
		t.Fatalf("agent env contains GitHub credential: %s", agentText)
	}
	encodedCA := systemdEnvironmentValue(agentText, "COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64")
	decodedCA, err := base64.StdEncoding.DecodeString(encodedCA)
	if err != nil || !bytes.Equal(decodedCA, material.CACert) {
		t.Fatalf("agent CA does not match generated CA: err=%v", err)
	}
	if bytes.Equal(material.ProviderEnv, material.AgentEnv) || bytes.Equal(material.ProviderEnv, material.ProbeEnv) {
		t.Fatal("Podman and systemd environment files were not rendered separately")
	}

	ca := parseCertificateForTest(t, material.CACert)
	server := parseCertificateForTest(t, material.ServerCert)
	if !ca.IsCA || server.NotBefore.After(now) || server.NotAfter.Before(now.Add(24*time.Hour)) {
		t.Fatalf("certificate validity CA=%+v server=%+v", ca, server)
	}
	for _, dns := range []string{"localhost", config.StableContainer, config.CandidateContainer} {
		if !containsString(server.DNSNames, dns) {
			t.Fatalf("server certificate missing DNS SAN %q: %v", dns, server.DNSNames)
		}
	}
	for _, ip := range []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")} {
		if !containsIP(server.IPAddresses, ip) {
			t.Fatalf("server certificate missing IP SAN %s: %v", ip, server.IPAddresses)
		}
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	if _, err := server.Verify(x509.VerifyOptions{Roots: pool, DNSName: config.StableContainer, CurrentTime: now.Add(time.Hour)}); err != nil {
		t.Fatalf("verify server certificate: %v", err)
	}

	if err := WriteInstallMaterial(paths, material); err != nil {
		t.Fatalf("write install material: %v", err)
	}
	for _, path := range []string{paths.ProviderEnv, paths.ProbeEnv, paths.AgentEnv, paths.ContainersConf, paths.CAFile, paths.CAKey, paths.ServerCert, paths.ServerKey} {
		info, err := os.Stat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
			t.Fatalf("generated file %s mode=%v err=%v", path, info, err)
		}
	}
	if data, err := os.ReadFile(paths.ContainersConf); err != nil || string(data) != "[network]\ndefault_network = \"wfcompute-github-provider\"\n" {
		t.Fatalf("containers.conf data=%q err=%v", data, err)
	}
	if strings.HasPrefix(paths.CAKey, paths.TLSRoot+string(os.PathSeparator)) {
		t.Fatalf("CA signing key is exposed through the provider TLS mount: %s", paths.CAKey)
	}
}

func TestCredentialsRespectEnvironmentReaderBoundary(t *testing.T) {
	const credentialLimit = 32 << 10
	if err := validateCredential(strings.Repeat("x", credentialLimit)); err != nil {
		t.Fatalf("credential at limit: %v", err)
	}
	if err := validateCredential(strings.Repeat("x", credentialLimit+1)); err == nil {
		t.Fatal("credential above environment reader limit was accepted")
	}
}

func TestRenewProviderServerCertificateKeepsCAAndServerKeyStable(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	issuedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	material, err := GenerateInstallMaterial(config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}, bytes.NewReader(bytes.Repeat([]byte{0x44}, 4096)), issuedAt)
	if err != nil {
		t.Fatalf("generate install material: %v", err)
	}
	if err := WriteInstallMaterial(paths, material); err != nil {
		t.Fatalf("write install material: %v", err)
	}
	renewedAt := issuedAt.Add(350 * 24 * time.Hour)
	renewed, err := renewProviderServerCertificate(config, paths, bytes.NewReader(bytes.Repeat([]byte{0x45}, 4096)), renewedAt)
	if err != nil {
		t.Fatalf("renew server certificate: %v", err)
	}
	if !renewed {
		t.Fatal("expiring server certificate was not renewed")
	}
	for path, want := range map[string][]byte{paths.CAFile: material.CACert, paths.CAKey: material.CAKey, paths.ServerKey: material.ServerKey} {
		got, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("stable TLS authority %s changed: err=%v", path, err)
		}
	}
	renewedPEM, err := os.ReadFile(paths.ServerCert)
	if err != nil {
		t.Fatalf("read renewed server certificate: %v", err)
	}
	if bytes.Equal(renewedPEM, material.ServerCert) {
		t.Fatal("server certificate bytes did not change")
	}
	renewedCertificate := parseCertificateForTest(t, renewedPEM)
	if !renewedCertificate.NotAfter.After(parseCertificateForTest(t, material.ServerCert).NotAfter) {
		t.Fatalf("renewed expiry = %s", renewedCertificate.NotAfter)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parseCertificateForTest(t, material.CACert))
	if _, err := renewedCertificate.Verify(x509.VerifyOptions{Roots: pool, DNSName: config.StableContainer, CurrentTime: renewedAt.Add(time.Hour)}); err != nil {
		t.Fatalf("verify renewed server certificate: %v", err)
	}
	renewed, err = renewProviderServerCertificate(config, paths, bytes.NewReader(bytes.Repeat([]byte{0x46}, 4096)), renewedAt)
	if err != nil || renewed {
		t.Fatalf("fresh certificate renewed=%v err=%v", renewed, err)
	}
}

func TestProviderServerCertificateRejectsFutureValidity(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Date(2026, 7, 14, 0, 0, 0, 0, time.UTC)
	material, err := GenerateInstallMaterial(config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}, nil, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("generate install material: %v", err)
	}
	if err := WriteInstallMaterial(paths, material); err != nil {
		t.Fatalf("write install material: %v", err)
	}

	caKeyBlock, _ := pem.Decode(material.CAKey)
	serverKeyBlock, _ := pem.Decode(material.ServerKey)
	if caKeyBlock == nil || serverKeyBlock == nil {
		t.Fatal("decode generated private keys")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse CA key: %v", err)
	}
	serverKey, err := x509.ParseECPrivateKey(serverKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse server key: %v", err)
	}
	template := *parseCertificateForTest(t, material.ServerCert)
	template.NotBefore = now.Add(time.Hour)
	template.NotAfter = now.AddDate(1, 0, 0)
	der, err := x509.CreateCertificate(bytes.NewReader(bytes.Repeat([]byte{0x47}, 4096)), &template, parseCertificateForTest(t, material.CACert), &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create future-dated server certificate: %v", err)
	}
	if err := atomicWriteFile(paths.ServerCert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write future-dated server certificate: %v", err)
	}

	if _, err := providerServerCertificateNeedsRenewal(config, paths, now); err == nil || !strings.Contains(err.Error(), "not currently valid") {
		t.Fatalf("future-dated server certificate err = %v", err)
	}
}

func TestRenderSystemdEnvironmentQuotesSpecialCharacters(t *testing.T) {
	contents, err := renderSystemdEnvironment([]environmentValue{{Name: "PROVIDER_TOKEN", Value: `provider token"\tail#value`}})
	if err != nil {
		t.Fatalf("render systemd environment: %v", err)
	}
	if got, want := string(contents), "PROVIDER_TOKEN=\"provider token\\\"\\\\tail#value\"\n"; got != want {
		t.Fatalf("systemd environment = %q want %q", got, want)
	}
}

func TestGenerateInstallMaterialRejectsCredentialInjection(t *testing.T) {
	config := validTestConfig(t.TempDir())
	for _, credentials := range []Credentials{
		{GitHubToken: "", ProviderToken: "provider-token"},
		{GitHubToken: "github-token", ProviderToken: ""},
		{GitHubToken: "github\nTOKEN=forged", ProviderToken: "provider-token"},
		{GitHubToken: "github-token", ProviderToken: "provider\rTOKEN=forged"},
	} {
		if _, err := GenerateInstallMaterial(config, credentials, bytes.NewReader(bytes.Repeat([]byte{0x24}, 4096)), time.Now().UTC()); err == nil {
			t.Fatalf("credential injection accepted: %+v", credentials)
		}
	}
}

func TestInspectAgentUnitSignatureParsesAndAttestsSystemdShow(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	fragment := filepath.Join(home, ".config", "systemd", "user", config.AgentUnit)
	dropIn := filepath.Join(home, ".config", "systemd", "user", config.AgentUnit+".d", "20-provider.conf")
	environment := filepath.Join(home, ".workflow-compute", "agent.env")
	for path, contents := range map[string]string{
		fragment:    "[Service]\nExecStart=" + config.ComputeAgentPath + " run\n",
		dropIn:      "[Service]\nEnvironmentFile=" + environment + "\n",
		environment: "WORKER_ID=" + config.WorkerID + "\n",
	} {
		if err := atomicWriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write systemd signature fixture: %v", err)
		}
	}
	output := strings.Join([]string{
		"LoadState=loaded",
		"FragmentPath=" + fragment,
		"DropInPaths=" + dropIn,
	}, "\n") + "\n"
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) != "systemctl" || !containsArg(command.Args, config.AgentUnit) || !containsAdjacentArgs(command.Args, "--property", "DropInPaths") {
			t.Fatalf("signature command = %+v", command)
		}
		if containsAdjacentArgs(command.Args, "--property", "EnvironmentFiles") {
			t.Fatalf("signature command requested unavailable EnvironmentFiles property: %+v", command)
		}
		if containsAdjacentArgs(command.Args, "--property", "ExecStart") {
			t.Fatalf("signature command requested runtime-varying ExecStart property: %+v", command)
		}
		return []byte(output), nil
	}}
	installer := Installer{Runner: runner}
	signature, err := installer.inspectAgentUnitSignature(t.Context(), home, config)
	if err != nil {
		t.Fatalf("inspect agent unit signature: %v", err)
	}
	if signature.Fragment.Path != fragment || len(signature.DropIns) != 1 || signature.DropIns[0].Path != dropIn || len(signature.EnvironmentFiles) != 1 || signature.EnvironmentFiles[0].Path != environment {
		t.Fatalf("signature = %+v", signature)
	}
	if want := `["` + config.ComputeAgentPath + ` run"]`; signature.ExecStart != want {
		t.Fatalf("static ExecStart = %q want %q", signature.ExecStart, want)
	}
	if err := signature.Reattest(); err != nil {
		t.Fatalf("re-attest inspected signature: %v", err)
	}

	runner.run = func(_ context.Context, _ Command) ([]byte, error) {
		return []byte(output + "FragmentPath=" + fragment + "\n"), nil
	}
	if _, err := installer.inspectAgentUnitSignature(t.Context(), home, config); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate systemd property error = %v", err)
	}
}

func TestEffectiveSystemdEnvironmentFilesHonorsResetAndQuotedPaths(t *testing.T) {
	paths, err := effectiveSystemdEnvironmentFiles([][]byte{
		[]byte("[Service]\nEnvironmentFile=/home/runner/old.env\n"),
		[]byte("[Service]\nEnvironmentFile=\nEnvironmentFile=\"/home/runner/env files/agent%%active.env\"\n"),
	})
	if err != nil {
		t.Fatalf("derive effective environment files: %v", err)
	}
	want := []string{"/home/runner/env files/agent%active.env"}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("environment files = %q want %q", paths, want)
	}
}

func TestEffectiveSystemdExecStartUsesStaticResetAwareCommands(t *testing.T) {
	value, err := effectiveSystemdExecStart([][]byte{
		[]byte("[Service]\nExecStart=/usr/bin/old-agent run\n"),
		[]byte("[Service]\nExecStart=\nExecStart=/usr/bin/current-agent run --profile retained\n"),
	})
	if err != nil {
		t.Fatalf("derive effective ExecStart: %v", err)
	}
	if want := `["/usr/bin/current-agent run --profile retained"]`; value != want {
		t.Fatalf("static ExecStart = %q want %q", value, want)
	}
}

func TestEffectiveSystemdEnvironmentFilesRejectsUnattestablePaths(t *testing.T) {
	for _, value := range []string{
		"-/home/runner/optional.env",
		"/home/runner/*.env",
		"/home/%h/agent.env",
		`"/home/runner/trailing\`,
	} {
		t.Run(value, func(t *testing.T) {
			_, err := effectiveSystemdEnvironmentFiles([][]byte{[]byte("[Service]\nEnvironmentFile=" + value + "\n")})
			if err == nil {
				t.Fatalf("unsupported EnvironmentFile path %q was accepted", value)
			}
		})
	}
}

func TestInstallTransactionOrdersMaintenanceAgentAndProviderActivation(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-install")
	digest := fileDigestForTest(t, payload)
	events := make([]string, 0, 24)
	statusQueue := []string{"unavailable", "unavailable", "idle"}
	runner := &recordingCommandRunner{}
	runner.run = func(_ context.Context, command Command) ([]byte, error) {
		if command.Path == config.LoginctlPath {
			return []byte("yes\n"), nil
		}
		if command.Path == config.PodmanPath && containsArg(command.Args, "info") {
			return []byte("true\n"), nil
		}
		if command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "image" && command.Args[1] == "inspect" {
			return []byte(testProviderImageID + "\n"), nil
		}
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "show") && (containsArg(command.Args, providerServiceUnit) || containsArg(command.Args, refreshPathUnit) || containsArg(command.Args, refreshTimerUnit)) {
			if containsAdjacentArgs(command.Args, "--property", "ActiveState") && containsArg(command.Args, "--value") {
				return []byte("active\n"), nil
			}
			return []byte("LoadState=not-found\nFragmentPath=\nActiveState=inactive\nUnitFileState=\n"), nil
		}
		if command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "network" && command.Args[1] == "inspect" {
			return []byte("bridge true false\n"), nil
		}
		event := installCommandEvent(command, config)
		events = append(events, event)
		switch event {
		case "agent-signature":
			return agentUnitSystemdOutputForTest(t, config), nil
		case "verify-update":
			return testVerifiedUpdateJSON(config, payload, digest), nil
		case "maintenance-begin":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleFencing {
				t.Fatalf("install maintenance begin lifecycle = %+v found=%v err=%v", journal, found, err)
			}
			if !containsAdjacentArgs(command.Args, "-id", journal.TransactionID) {
				t.Fatalf("install maintenance id is not the lifecycle transaction: %+v", command)
			}
			return maintenanceStateJSON(true, journal.TransactionID, config.ProfileID, installMaintenanceReason), nil
		case "maintenance-end":
			journal, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || journal.Phase != LifecycleReleasing || journal.Outcome != LifecycleCommit || journal.ProviderTransaction == nil {
				t.Fatalf("install maintenance end lifecycle = %+v found=%v err=%v", journal, found, err)
			}
			if !containsAdjacentArgs(command.Args, "-id", journal.TransactionID) {
				t.Fatalf("install maintenance release id is not the lifecycle transaction: %+v", command)
			}
			return maintenanceStateJSON(false, journal.TransactionID, config.ProfileID, installMaintenanceReason), nil
		case "local-status":
			if len(statusQueue) == 0 {
				t.Fatal("unexpected extra local status read")
			}
			state := statusQueue[0]
			statusQueue = statusQueue[1:]
			return localStatusJSON(config.WorkerID, state), nil
		default:
			if event == "agent-stop" {
				journal, found, err := readLifecycleJournal(home, paths)
				if err != nil || !found || journal.Phase != LifecycleFenced {
					t.Fatalf("install agent stop lifecycle = %+v found=%v err=%v", journal, found, err)
				}
			}
			if isCandidateStart(command, config) {
				events = append(events, "provider-refresh")
			}
			return nil, nil
		}
	}
	installer := Installer{
		Runner:         runner,
		ExecutablePath: func() (string, error) { return payload, nil },
		Random:         bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)),
		Now:            func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
		Sleep:          func(context.Context, time.Duration) error { return nil },
	}
	status, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"})
	if err != nil {
		t.Fatalf("install: %v\nevents=%v", err, events)
	}
	if status.ProtocolVersion != StatusProtocolVersion || !status.Installed || !status.ServiceActive || status.CurrentSHA256 != digest {
		t.Fatalf("install status = %+v", status)
	}
	assertOrderedEvents(t, events, []string{
		"systemd-preflight", "podman-preflight", "supervisor-config-validate", "verify-update",
		"maintenance-begin", "local-status", "agent-stop", "daemon-reload", "provider-enable",
		"provider-refresh", "refresh-watch-enable", "agent-start",
		"local-status", "maintenance-end", "local-status",
	})
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "github-secret") || strings.Contains(transcript, "provider-secret") || strings.Contains(transcript, "COMPUTE_API_TOKEN") || strings.Contains(transcript, "https://stg") {
		t.Fatalf("install command transcript leaked credential or STG access:\n%s", transcript)
	}
	for _, path := range []string{
		paths.ConfigFile, paths.Launcher, paths.ProviderEnv, paths.ProbeEnv, paths.AgentEnv,
		paths.CAFile, paths.ServerCert, paths.ServerKey,
		paths.ProviderUnit, paths.RefreshUnit, paths.PathUnit, paths.TimerUnit, paths.AgentDropIn,
	} {
		if info, err := os.Stat(path); err != nil || !info.Mode().IsRegular() {
			t.Fatalf("installed file %s info=%v err=%v", path, info, err)
		}
	}
	if data, err := os.ReadFile(paths.ProviderEnv); err != nil || !bytes.Contains(data, []byte("github-secret")) {
		t.Fatalf("provider credential file data=%q err=%v", data, err)
	}
	if data, err := os.ReadFile(paths.AgentEnv); err != nil || bytes.Contains(data, []byte("github-secret")) {
		t.Fatalf("agent credential file data=%q err=%v", data, err)
	}
}

func TestInstallHostPreflightRequiresNonRootLingeringAndRootlessPodman(t *testing.T) {
	config := validTestConfig(t.TempDir())
	writeLifecycleRecoveryFiles(t, config)
	for _, tc := range []struct {
		name         string
		uid          string
		linger       string
		rootless     string
		want         string
		wantLoginctl bool
	}{
		{name: "root user", uid: "0", want: "non-root"},
		{name: "linger disabled", uid: "1001", linger: "no\n", rootless: "true\n", want: "lingering", wantLoginctl: true},
		{name: "rootful podman", uid: "1001", linger: "yes\n", rootless: "false\n", want: "rootless", wantLoginctl: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				if filepath.Base(command.Path) == "systemctl" && command.Path != config.SystemctlPath {
					t.Fatalf("systemctl command path = %q want %q", command.Path, config.SystemctlPath)
				}
				if filepath.Base(command.Path) == "loginctl" && command.Path != config.LoginctlPath {
					t.Fatalf("loginctl command path = %q want %q", command.Path, config.LoginctlPath)
				}
				switch command.Path {
				case config.LoginctlPath:
					return []byte(tc.linger), nil
				case config.PodmanPath:
					if containsArg(command.Args, "info") {
						return []byte(tc.rootless), nil
					}
					return []byte("5.5.0\n"), nil
				default:
					return nil, nil
				}
			}}
			installer := Installer{Runner: runner, UserID: func() (string, error) { return tc.uid, nil }}
			err := installer.runSystemPreflight(t.Context(), config)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("preflight error = %v want %q", err, tc.want)
			}
			transcript := commandTranscript(runner.commands)
			if strings.Contains(transcript, "supervisor-update verify") {
				t.Fatalf("host preflight crossed package verification boundary:\n%s", transcript)
			}
			if tc.wantLoginctl != strings.Contains(transcript, "loginctl show-user "+tc.uid+" --property Linger --value") {
				t.Fatalf("loginctl invocation mismatch:\n%s", transcript)
			}
		})
	}
}

func TestHostPreflightRejectsUntrustedExecutableBeforeCommands(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	if err := os.Chmod(config.PodmanPath, 0o722); err != nil {
		t.Fatalf("make podman fixture group-writable: %v", err)
	}
	runner := &recordingCommandRunner{}
	installer := Installer{Runner: runner, UserID: func() (string, error) { return "1001", nil }}
	if err := installer.runSystemPreflight(t.Context(), config); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("untrusted executable preflight err = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("untrusted executable preflight issued commands: %+v", runner.commands)
	}
}

func TestWaitLocalStateRequiresObservationAfterFence(t *testing.T) {
	config := validTestConfig(t.TempDir())
	fenceStartedAt := time.Date(2026, 7, 14, 14, 0, 0, 0, time.UTC)
	statuses := [][]byte{
		localStatusAtJSON(config.WorkerID, "unavailable", fenceStartedAt.Add(-time.Second)),
		localStatusAtJSON(config.WorkerID, "unavailable", fenceStartedAt.Add(time.Second)),
	}
	reads := 0
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if !containsArg(command.Args, "local-status") {
			t.Fatalf("unexpected command: %+v", command)
		}
		result := statuses[reads]
		reads++
		return result, nil
	}}
	installer := Installer{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if err := installer.waitLocalStateAfter(t.Context(), config, "unavailable", fenceStartedAt); err != nil {
		t.Fatalf("wait for fresh local state: %v", err)
	}
	if reads != 2 {
		t.Fatalf("local status reads = %d want 2", reads)
	}
}

func TestInstallReattestsAuthorityAfterDrainBeforeStop(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-install-reattest")
	digest := fileDigestForTest(t, payload)
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	originalRun := runner.run
	replaced := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		output, err := originalRun(ctx, command)
		if err == nil && !replaced && installCommandEvent(command, config) == "local-status" {
			replaced = true
			if writeErr := os.WriteFile(config.ComputeAgentPath, []byte("replacement during install drain"), 0o700); writeErr != nil {
				t.Fatalf("replace compute-agent during install drain: %v", writeErr)
			}
		}
		return output, err
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	_, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("install error = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
		t.Fatalf("changed authority crossed install stop boundary:\n%s", transcript)
	}
}

func TestInstallFailureEmitsRedactedErrorAndRecoveryAudit(t *testing.T) {
	home := t.TempDir()
	t.Setenv("XDG_STATE_HOME", filepath.Join(home, ".state"))
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-audit-failure")
	digest := fileDigestForTest(t, payload)
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	baseRun := runner.run
	failed := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if !failed && installCommandEvent(command, config) == "provider-enable" {
			failed = true
			return nil, errors.New("provider-enable-secret-detail")
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x35}, 4096)),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil {
		t.Fatal("install failure was not returned")
	}
	audit, err := os.ReadFile(LifecyclePathsFor(config).LifecycleAudit)
	if err != nil {
		t.Fatalf("read lifecycle audit: %v", err)
	}
	for _, want := range []string{`"kind":"error"`, `"error_class":"operation_failed"`, `"kind":"recovery"`, `"disposition":"resume_fenced"`} {
		if !bytes.Contains(audit, []byte(want)) {
			t.Fatalf("lifecycle audit missing %s:\n%s", want, audit)
		}
	}
	if bytes.Contains(audit, []byte("provider-enable-secret-detail")) || bytes.Contains(audit, []byte("github-secret")) || bytes.Contains(audit, []byte("provider-secret")) {
		t.Fatalf("lifecycle audit leaked failure or credentials:\n%s", audit)
	}
}

func TestInstallReattestsAuthorityBeforeRestart(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-install-restart-reattest")
	digest := fileDigestForTest(t, payload)
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	originalRun := runner.run
	replaced := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		output, err := originalRun(ctx, command)
		if err == nil && !replaced && installCommandEvent(command, config) == "refresh-watch-enable" {
			replaced = true
			if writeErr := os.WriteFile(config.ComputeAgentPath, []byte("replacement before install restart"), 0o700); writeErr != nil {
				t.Fatalf("replace compute-agent before restart: %v", writeErr)
			}
		}
		return output, err
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	_, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("install error = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "systemctl --user start "+config.AgentUnit) {
		t.Fatalf("changed authority crossed install restart boundary:\n%s", transcript)
	}
}

func TestInstallReattestsAuthorityBeforeReadyAndMaintenanceEnd(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-install-release-reattest")
	digest := fileDigestForTest(t, payload)
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	originalRun := runner.run
	localStatusReads := 0
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		output, err := originalRun(ctx, command)
		if err == nil && installCommandEvent(command, config) == "local-status" {
			localStatusReads++
			if localStatusReads == 2 {
				if writeErr := os.WriteFile(config.ComputeAgentPath, []byte("replacement before install release"), 0o700); writeErr != nil {
					t.Fatalf("replace compute-agent before release: %v", writeErr)
				}
			}
		}
		return output, err
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x31}, 4096)),
		Sleep:  func(context.Context, time.Duration) error { return nil },
	}
	_, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"})
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("install error = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("changed authority crossed maintenance release boundary:\n%s", transcript)
	}
	journal, found, readErr := readLifecycleJournal(home, LifecyclePathsFor(config))
	if readErr != nil || !found || journal.Phase != LifecycleFenced {
		t.Fatalf("failed release journal = %+v found=%v err=%v", journal, found, readErr)
	}
}

func TestReleaseLifecycleMaintenanceReattestsAuthorityBeforeCommand(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	journal := lifecycleRecoveryJournalForTest(t, config, time.Now().UTC())
	journal.Operation = LifecycleInstall
	if err := os.WriteFile(config.ComputeAgentPath, []byte("replacement before maintenance release"), 0o700); err != nil {
		t.Fatalf("replace compute-agent before maintenance release: %v", err)
	}
	runner := &recordingCommandRunner{}
	installer := Installer{Runner: runner}

	err := installer.releaseLifecycleMaintenance(t.Context(), home, journal)
	if err == nil || !strings.Contains(err.Error(), "attestation") {
		t.Fatalf("release error = %v", err)
	}
	if transcript := commandTranscript(runner.commands); strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("changed authority crossed maintenance release command:\n%s", transcript)
	}
}

func TestInstallBoundsEverySubprocessContext(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-bounded-install-commands")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable", "unavailable", "idle"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	var unbounded []Command
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if _, bounded := ctx.Deadline(); !bounded {
			unbounded = append(unbounded, command)
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x32}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err != nil {
		t.Fatalf("install: %v", err)
	}
	if len(unbounded) != 0 {
		t.Fatalf("install issued unbounded subprocesses: %s", commandTranscript(unbounded))
	}
}

func TestInstallLockContentionDoesNotMutateMaintenanceOrAgent(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-install-lock")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		t.Fatalf("hold install lock: %v", err)
	}
	defer func() { _ = lock.Release() }()
	statuses := []string{"unavailable", "unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x32}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh:     func(context.Context, Config) (Status, error) { return Status{}, nil },
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); !errors.Is(err, ErrInstallLocked) {
		t.Fatalf("contended install err = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "supervisor-maintenance begin") || strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
		t.Fatalf("contended install mutated maintenance or agent:\n%s", transcript)
	}
}

func TestInstallHoldsLifecycleLockUntilMaintenanceFenceReleased(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-lifecycle-lock")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	statuses := []string{"unavailable", "unavailable", "idle"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	lockHeldAtFenceRelease := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "maintenance-end" {
			contender, err := AcquireInstallLock(paths.InstallLock)
			lockHeldAtFenceRelease = errors.Is(err, ErrInstallLocked)
			if contender != nil {
				_ = contender.Release()
			}
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x33}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err != nil {
		t.Fatalf("install: %v", err)
	}
	if !lockHeldAtFenceRelease {
		t.Fatal("install lock was released before maintenance fence")
	}
}

func TestInstallCredentialRotationPreservesProviderStateAndWorkerIdentity(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-rotation")
	digest := fileDigestForTest(t, payload)
	paths := LifecyclePathsFor(config)
	statuses := []string{"unavailable", "unavailable", "idle", "unavailable", "unavailable", "idle"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	newInstaller := func(randomByte byte) Installer {
		return Installer{
			Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
			Random: bytes.NewReader(bytes.Repeat([]byte{randomByte}, 4096)), Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
			Sleep: func(context.Context, time.Duration) error { return nil },
		}
	}
	if _, err := newInstaller(0x41).Install(t.Context(), home, config, Credentials{GitHubToken: "github-old", ProviderToken: "provider-old"}); err != nil {
		t.Fatalf("initial install: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.ProviderState, "retained.json"), []byte("retained-state"), 0o600); err != nil {
		t.Fatalf("write retained state: %v", err)
	}
	if _, err := newInstaller(0x51).Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err != nil {
		t.Fatalf("credential rotation: %v", err)
	}
	if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "retained.json")); err != nil || string(data) != "retained-state" {
		t.Fatalf("provider state changed: data=%q err=%v", data, err)
	}
	providerEnv, _ := os.ReadFile(paths.ProviderEnv)
	agentEnv, _ := os.ReadFile(paths.AgentEnv)
	if !bytes.Contains(providerEnv, []byte("github-new")) || bytes.Contains(providerEnv, []byte("github-old")) || !bytes.Contains(agentEnv, []byte("provider-new")) || bytes.Contains(agentEnv, []byte("github-new")) {
		t.Fatalf("rotated provider=%s agent=%s", providerEnv, agentEnv)
	}
	if config.ProfileID != "github-runner-profile-stg" || config.WorkerID != "github-runner-linux-stg" {
		t.Fatalf("worker identity changed: %+v", config)
	}
}

func TestReinstallMissingActiveImageUsesChangedProviderTransaction(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-reinstall-repair")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable", "unavailable", "idle", "unavailable", "unavailable", "idle"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	imagePresent := false
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && containsAdjacentArgs(command.Args, "--filter", "id="+testProviderImageID) {
			if !imagePresent {
				return nil, nil
			}
			return ownedProviderImageInventory(config, digest, testProviderImageID), nil
		}
		if command.Path == config.PodmanPath && firstArg(command.Args) == "build" {
			imagePresent = true
		}
		return baseRun(ctx, command)
	}
	newInstaller := func() Installer {
		return Installer{
			Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
			Random: bytes.NewReader(bytes.Repeat([]byte{0x61}, 4096)),
			Now:    func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
			Sleep:  func(context.Context, time.Duration) error { return nil },
		}
	}
	if _, err := newInstaller().Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err != nil {
		t.Fatalf("initial install: %v", err)
	}
	imagePresent = false
	commandCount := len(runner.commands)
	if _, err := newInstaller().Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err != nil {
		t.Fatalf("repairing reinstall: %v\n%s", err, commandTranscript(runner.commands[commandCount:]))
	}
	if transcript := commandTranscript(runner.commands[commandCount:]); !strings.Contains(transcript, "podman build") {
		t.Fatalf("repairing reinstall did not rebuild missing active image:\n%s", transcript)
	}
}

func TestReinstallRejectsChangedInstalledConfigBeforeCommands(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("create install root: %v", err)
	}
	if err := AtomicWriteJSON(paths.ConfigFile, config); err != nil {
		t.Fatalf("write installed config: %v", err)
	}
	changed := config
	changed.WorkerID = "different-retained-worker"
	changed.AgentUnit = "workflow-compute-different-retained-worker.service"
	changed.Labels = append([]string(nil), config.Labels...)

	runner := &recordingCommandRunner{}
	installer := Installer{Runner: runner}
	if _, err := installer.Install(t.Context(), home, changed, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "must exactly match") {
		t.Fatalf("identity-changing reinstall err = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("identity-changing reinstall issued commands: %+v", runner.commands)
	}
}

func TestRefreshAndUninstallRejectChangedInstalledConfigBeforeCommands(t *testing.T) {
	for _, operation := range []string{"refresh", "uninstall"} {
		t.Run(operation, func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			paths := LifecyclePathsFor(config)
			if err := os.MkdirAll(paths.Root, 0o700); err != nil {
				t.Fatalf("create install root: %v", err)
			}
			if err := AtomicWriteJSON(paths.ConfigFile, config); err != nil {
				t.Fatalf("write installed config: %v", err)
			}
			changed := config
			changed.WorkerID = "different-retained-worker"
			changed.AgentUnit = "workflow-compute-different-retained-worker.service"
			changed.Labels = append([]string(nil), config.Labels...)
			runner := &recordingCommandRunner{}
			var err error
			switch operation {
			case "refresh":
				_, err = (Refresher{Runner: runner}).Refresh(t.Context(), changed)
			case "uninstall":
				_, err = (Installer{Runner: runner}).Uninstall(t.Context(), home, changed, false)
			}
			if err == nil || !strings.Contains(err.Error(), "must exactly match") {
				t.Fatalf("identity-changing %s err = %v", operation, err)
			}
			if len(runner.commands) != 0 {
				t.Fatalf("identity-changing %s issued commands: %+v", operation, runner.commands)
			}
		})
	}
}

func TestInstallLeavesMaintenanceActiveWhenRollbackCannotRestartAgent(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-rollback")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		event := installCommandEvent(command, config)
		if event == "daemon-reload" || event == "agent-start" {
			return nil, errors.New("systemd unavailable")
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x61}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh:     func(context.Context, Config) (Status, error) { return Status{}, nil },
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil {
		t.Fatal("install with incomplete rollback succeeded")
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("incomplete rollback released maintenance:\n%s", transcript)
	}
}

func TestInstallRollbackDoesNotDisableInactiveRefreshWatchUnits(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-inactive-watch-rollback")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "disable") && (containsArg(command.Args, refreshPathUnit) || containsArg(command.Args, refreshTimerUnit)) {
			return nil, errors.New("inactive refresh watch cannot be disabled")
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x63}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh: func(context.Context, Config) (Status, error) {
			return Status{}, errors.New("provider activation failed")
		},
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil || !strings.Contains(err.Error(), "provider activation failed") {
		t.Fatalf("install activation failure = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "disable --now "+refreshPathUnit) || strings.Contains(transcript, "disable --now "+refreshTimerUnit) {
		t.Fatalf("rollback disabled inactive refresh watch units:\n%s", transcript)
	}
	if !strings.Contains(transcript, "systemctl --user disable --now "+providerServiceUnit) || !strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("rollback did not disable the activated provider and release maintenance:\n%s", transcript)
	}
}

func TestInstallRollbackDisablesPartiallyEnabledRefreshWatchUnits(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-partial-watch-rollback")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	enabled := map[string]bool{}
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "enable", "--now") {
			hasPath := containsArg(command.Args, refreshPathUnit)
			hasTimer := containsArg(command.Args, refreshTimerUnit)
			if hasPath && hasTimer {
				return nil, errors.New("refresh units must be activated independently")
			}
			if hasPath {
				enabled[refreshPathUnit] = true
			}
			if hasTimer {
				enabled[refreshTimerUnit] = true
				return nil, errors.New("timer start failed after enable")
			}
		}
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "show") && containsArg(command.Args, refreshTimerUnit) {
			return []byte("LoadState=loaded\nFragmentPath=/tmp/provider.service\nActiveState=active\nUnitFileState=enabled\n"), nil
		}
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "disable", "--now") {
			for unit := range enabled {
				if containsArg(command.Args, unit) {
					delete(enabled, unit)
				}
			}
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x64}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil || !strings.Contains(err.Error(), "timer start failed after enable") {
		t.Fatalf("partial watch enable failure = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	wantDisable := "systemctl --user disable --now " + refreshPathUnit + " " + refreshTimerUnit + " " + providerServiceUnit
	if !strings.Contains(transcript, wantDisable) || !strings.Contains(transcript, "supervisor-maintenance end") || len(enabled) != 0 {
		t.Fatalf("rollback did not disable partially activated units and release maintenance:\n%s", transcript)
	}
}

func TestInstallRollbackConservativelyRestoresWatchUnitAfterAmbiguousEnableFailure(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-unchanged-watch-rollback")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsAdjacentArgs(command.Args, "enable", "--now") && containsArg(command.Args, refreshPathUnit) {
			return nil, errors.New("path enable failed before mutation")
		}
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "show") && containsArg(command.Args, refreshPathUnit) {
			return []byte("LoadState=loaded\nFragmentPath=/tmp/provider.service\nActiveState=inactive\nUnitFileState=disabled\n"), nil
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x65}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil || !strings.Contains(err.Error(), "path enable failed before mutation") {
		t.Fatalf("unchanged watch enable failure = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	for _, command := range []string{
		"disable --now " + refreshPathUnit,
		"disable " + refreshPathUnit,
		"stop " + refreshPathUnit,
		"disable --now " + refreshPathUnit + " " + providerServiceUnit,
		"supervisor-maintenance end",
	} {
		if !strings.Contains(transcript, command) {
			t.Fatalf("rollback did not conservatively restore ambiguous watch activation %q:\n%s", command, transcript)
		}
	}
}

func TestInstallRollbackRestoresPriorActiveStateAndServiceWiring(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-post-activation-rollback")
	digest := fileDigestForTest(t, payload)
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	if err := atomicWriteFile(paths.ProviderEnv, []byte("OLD_PROVIDER_ENV=retained\n"), 0o600); err != nil {
		t.Fatalf("write previous provider env: %v", err)
	}
	for _, path := range managedWiringPaths(paths) {
		if err := atomicWriteFile(path, []byte("prior-unit\n"), 0o600); err != nil {
			t.Fatalf("write prior wiring %s: %v", path, err)
		}
	}
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x62}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh: func(context.Context, Config) (Status, error) {
			selection := selectionForDigest(payload, digest, "v1.0.32", "directive-new", "sha256:"+strings.Repeat("d", 64), time.Unix(1_700_100_000, 0).UTC())
			active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, Previous: &previous.Current, UpdatedAt: selection.ActivatedAt}
			return statusForActive(active, true, active.UpdatedAt), AtomicWriteJSON(paths.ActiveState, active)
		},
		ProbeActive: func(context.Context, Config) error { return errors.New("post-activation probe failed") },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil {
		t.Fatal("post-activation probe failure succeeded")
	}
	restored, found, err := readActiveState(paths.ActiveState)
	if err != nil || !found || restored.Current.ImageID != previous.Current.ImageID {
		t.Fatalf("restored active = %+v found=%v err=%v", restored, found, err)
	}
	providerEnv, err := os.ReadFile(paths.ProviderEnv)
	if err != nil || string(providerEnv) != "OLD_PROVIDER_ENV=retained\n" {
		t.Fatalf("restored provider env = %q err=%v", providerEnv, err)
	}
	transcript := commandTranscript(runner.commands)
	for _, command := range []string{
		"systemctl --user enable " + providerServiceUnit,
		"systemctl --user start " + providerServiceUnit,
		"systemctl --user enable " + refreshPathUnit,
		"systemctl --user start " + refreshPathUnit,
		"systemctl --user enable " + refreshTimerUnit,
		"systemctl --user start " + refreshTimerUnit,
	} {
		if !strings.Contains(transcript, command) {
			t.Fatalf("rollback did not restore service activation %q:\n%s", command, transcript)
		}
	}
}

func TestInstallRollbackPreservesPreviouslyDisabledProviderUnits(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-disabled-unit-rollback")
	digest := fileDigestForTest(t, payload)
	for _, path := range managedWiringPaths(paths) {
		if err := atomicWriteFile(path, []byte("prior-unit\n"), 0o600); err != nil {
			t.Fatalf("write prior wiring %s: %v", path, err)
		}
	}
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "show") && (containsArg(command.Args, providerServiceUnit) || containsArg(command.Args, refreshPathUnit) || containsArg(command.Args, refreshTimerUnit)) {
			return []byte("LoadState=loaded\nFragmentPath=/tmp/provider.service\nActiveState=inactive\nUnitFileState=disabled\n"), nil
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x67}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh: func(context.Context, Config) (Status, error) {
			return Status{}, errors.New("provider activation failed")
		},
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "provider activation failed") {
		t.Fatalf("disabled-unit rollback failure = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "restart "+providerServiceUnit) || strings.Contains(transcript, "enable --now "+refreshPathUnit) {
		t.Fatalf("rollback activated previously disabled units:\n%s", transcript)
	}
}

func TestRestoreUnitStatePreservesRuntimeEnablement(t *testing.T) {
	systemctlPath := validTestConfig(t.TempDir()).SystemctlPath
	runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) { return nil, nil }}
	installer := Installer{Runner: runner}
	if err := installer.restoreUnitState(t.Context(), systemctlPath, providerServiceUnit, systemdUnitState{LoadState: "loaded", FragmentPath: "/tmp/provider.service", UnitFileState: "enabled-runtime", ActiveState: "active"}); err != nil {
		t.Fatalf("restore runtime-enabled unit: %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "systemctl --user enable --runtime "+providerServiceUnit) {
		t.Fatalf("runtime enablement became persistent:\n%s", transcript)
	}
}

func TestCaptureManagedUnitStatesRejectsUnrestorableSemantics(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state systemdUnitState
	}{
		{name: "linked unit", state: systemdUnitState{LoadState: "loaded", FragmentPath: "/tmp/provider.service", UnitFileState: "linked", ActiveState: "inactive"}},
		{name: "failed unit", state: systemdUnitState{LoadState: "loaded", FragmentPath: "/tmp/provider.service", UnitFileState: "enabled", ActiveState: "failed"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			systemctlPath := validTestConfig(t.TempDir()).SystemctlPath
			runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) {
				return []byte("LoadState=" + tc.state.LoadState + "\nFragmentPath=" + tc.state.FragmentPath + "\nActiveState=" + tc.state.ActiveState + "\nUnitFileState=" + tc.state.UnitFileState + "\n"), nil
			}}
			installer := Installer{Runner: runner}
			if _, err := installer.captureManagedUnitStates(t.Context(), systemctlPath); err == nil || !strings.Contains(err.Error(), "unsupported prior") {
				t.Fatalf("capture state %+v err = %v", tc.state, err)
			}
		})
	}
}

func TestCaptureManagedUnitStatesIncludesUnitsLoadedOutsideManagedPaths(t *testing.T) {
	systemctlPath := validTestConfig(t.TempDir()).SystemctlPath
	fragment := "/usr/lib/systemd/user/vendor-provider.service"
	runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) {
		return []byte("LoadState=loaded\nFragmentPath=" + fragment + "\nActiveState=active\nUnitFileState=enabled\n"), nil
	}}
	states, err := (Installer{Runner: runner}).captureManagedUnitStates(t.Context(), systemctlPath)
	if err != nil {
		t.Fatalf("capture loaded units: %v", err)
	}
	if len(states) != 3 {
		t.Fatalf("captured states = %+v", states)
	}
	for _, unit := range []string{providerServiceUnit, refreshPathUnit, refreshTimerUnit} {
		state, found := states[unit]
		if !found || state.ActiveState != "active" || state.UnitFileState != "enabled" {
			t.Fatalf("state[%s] = %+v found=%v", unit, state, found)
		}
	}
}

func TestInstallRollbackRestoresProviderStateFromCommittedNestedRefresh(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	stateFile := filepath.Join(paths.ProviderState, "generation")
	if err := os.WriteFile(stateFile, []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	previous := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	payload := writeTestProviderPayload(t, home, "verified-provider-nested-refresh-rollback")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "agent-start" {
			return nil, errors.New("agent restart failed after provider refresh")
		}
		return baseRun(ctx, command)
	}
	now := time.Unix(1_700_200_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.32", "directive-nested", "sha256:"+strings.Repeat("d", 64), now)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x66}, 4096)), Now: func() time.Time { return now },
		Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh: func(context.Context, Config) (Status, error) {
			outer, found, err := readLifecycleJournal(home, paths)
			if err != nil || !found || outer.Phase != LifecycleFenced {
				return Status{}, fmt.Errorf("read outer lifecycle binding found=%v: %w", found, err)
			}
			if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
				return Status{}, err
			}
			if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "generation"), []byte("candidate"), 0o600); err != nil {
				return Status{}, err
			}
			if err := promoteCandidateProviderState(paths, digest); err != nil {
				return Status{}, err
			}
			active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, Previous: &previous.Current, UpdatedAt: now}
			if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
				return Status{}, err
			}
			journal := TransactionJournal{
				ProtocolVersion: TransactionJournalProtocolVersion, ID: "refresh-nested-install", Phase: JournalCommitted, DeferredCommit: true,
				OuterTransactionID: outer.TransactionID, ProfileID: config.ProfileID,
				Previous: &previous, Candidate: selection, StartedAt: now, UpdatedAt: now,
			}
			if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
				return Status{}, err
			}
			return statusForActive(active, true, now), nil
		},
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "agent restart failed") {
		t.Fatalf("install post-refresh failure = %v", err)
	}
	if data, err := os.ReadFile(stateFile); err != nil || string(data) != "previous" {
		t.Fatalf("nested refresh rollback state = %q err=%v", data, err)
	}
}

func TestInstallRecoversDeferredCommittedRefreshAfterProcessRestart(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-deferred-install-recovery")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_300_000, 0).UTC()
	selection := selectionForDigest(payload, digest, "v1.0.32", "directive-deferred-recovery", testProviderImageID, now)
	active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: selection, UpdatedAt: now}
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write committed active state: %v", err)
	}
	transactionRoot := filepath.Dir(paths.CandidateState(digest))
	if err := os.MkdirAll(transactionRoot, 0o700); err != nil {
		t.Fatalf("create deferred transaction root: %v", err)
	}
	journal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-deferred-install-recovery",
		Phase:           JournalCommitted,
		DeferredCommit:  true,
		Candidate:       selection,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := AtomicWriteJSON(paths.Journal, journal); err != nil {
		t.Fatalf("write deferred refresh journal: %v", err)
	}
	snapshots, err := snapshotManagedFiles(paths)
	if err != nil {
		t.Fatalf("snapshot committed outer install: %v", err)
	}
	outerJournal := newInstallTransactionJournal("install", snapshots, map[string]systemdUnitState{}, now)
	outerJournal.Phase = installTransactionCommitted
	if err := writeInstallTransactionJournal(paths, outerJournal); err != nil {
		t.Fatalf("write committed outer install journal: %v", err)
	}
	statuses := []string{"unavailable", "unavailable", "idle"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x68}, 4096)), Now: func() time.Time { return now },
		Sleep: func(context.Context, time.Duration) error { return nil },
		Refresh: func(context.Context, Config) (Status, error) {
			if _, err := os.Lstat(paths.Journal); !errors.Is(err, os.ErrNotExist) {
				return Status{}, fmt.Errorf("deferred journal was not recovered before retry: %v", err)
			}
			if _, err := os.Lstat(transactionRoot); !errors.Is(err, os.ErrNotExist) {
				return Status{}, fmt.Errorf("deferred rollback state was not finalized before retry: %v", err)
			}
			return statusForActive(active, true, now), nil
		},
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err != nil {
		t.Fatalf("recover deferred install: %v", err)
	}
	if transcript := commandTranscript(runner.commands); !strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("recovered install did not release maintenance:\n%s", transcript)
	}
}

func TestRecoverInstallRejectsCrossWorkerDeferredJournalBeforeMutation(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	now := time.Unix(1_700_300_000, 0).UTC()
	payload := writeTestProviderPayload(t, home, "cross-worker-deferred-recovery")
	digest := fileDigestForTest(t, payload)
	selection := selectionForDigest(payload, digest, "v1.0.32", "directive-cross-worker", "sha256:"+strings.Repeat("d", 64), now)
	selection.Update.WorkerID = "other-retained-worker"
	providerJournal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion,
		ID:              "refresh-cross-worker-deferred",
		Phase:           JournalCommitted,
		DeferredCommit:  true,
		Candidate:       selection,
		StartedAt:       now,
		UpdatedAt:       now,
	}
	if err := AtomicWriteJSON(paths.Journal, providerJournal); err != nil {
		t.Fatalf("write cross-worker provider journal: %v", err)
	}
	transactionRoot := filepath.Dir(paths.CandidateState(digest))
	if err := os.MkdirAll(transactionRoot, 0o700); err != nil {
		t.Fatalf("create provider transaction root: %v", err)
	}
	sentinel := filepath.Join(transactionRoot, "must-remain")
	if err := os.WriteFile(sentinel, []byte("retained"), 0o600); err != nil {
		t.Fatalf("write provider transaction sentinel: %v", err)
	}
	snapshots, err := snapshotManagedFiles(paths)
	if err != nil {
		t.Fatalf("snapshot outer install: %v", err)
	}
	outer := newInstallTransactionJournal("install", snapshots, map[string]systemdUnitState{}, now)
	outer.Phase = installTransactionCommitted
	if err := writeInstallTransactionJournal(paths, outer); err != nil {
		t.Fatalf("write outer install journal: %v", err)
	}
	runner := &recordingCommandRunner{}

	err = (Installer{Runner: runner}).recoverInstallTransaction(t.Context(), config, paths, Refresher{Runner: runner})
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("cross-worker deferred recovery err = %v", err)
	}
	if len(runner.commands) != 0 {
		t.Fatalf("cross-worker deferred recovery issued commands: %+v", runner.commands)
	}
	if data, readErr := os.ReadFile(sentinel); readErr != nil || string(data) != "retained" {
		t.Fatalf("cross-worker deferred recovery mutated transaction state: %q err=%v", data, readErr)
	}
}

func TestInstallCrashRecoveryRestoresDurableOuterBaselineBeforeRetry(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-outer-crash-recovery")
	digest := fileDigestForTest(t, payload)
	now := time.Unix(1_700_500_000, 0).UTC()
	previous := previousActiveStateForTest(t, home)
	if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
		t.Fatalf("mkdir provider state: %v", err)
	}
	stateFile := filepath.Join(paths.ProviderState, "generation")
	if err := os.WriteFile(stateFile, []byte("previous"), 0o600); err != nil {
		t.Fatalf("write previous provider state: %v", err)
	}
	if err := AtomicWriteJSON(paths.ActiveState, previous); err != nil {
		t.Fatalf("write previous active state: %v", err)
	}
	if err := prepareCandidateState(paths.ProviderState, paths.CandidateState(digest)); err != nil {
		t.Fatalf("prepare candidate state: %v", err)
	}
	if err := os.WriteFile(filepath.Join(paths.CandidateState(digest), "generation"), []byte("candidate"), 0o600); err != nil {
		t.Fatalf("write candidate provider state: %v", err)
	}
	if err := promoteCandidateProviderState(paths, digest); err != nil {
		t.Fatalf("promote candidate state: %v", err)
	}
	candidate := selectionForDigest(payload, digest, "v1.0.32", "directive-outer-crash", "sha256:"+strings.Repeat("d", 64), now)
	active := ActiveState{ProtocolVersion: ActiveStateProtocolVersion, Current: candidate, Previous: &previous.Current, UpdatedAt: now}
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write candidate active state: %v", err)
	}
	providerJournal := TransactionJournal{
		ProtocolVersion: TransactionJournalProtocolVersion, ID: "refresh-outer-crash", Phase: JournalCommitted, DeferredCommit: true,
		Previous: &previous, Candidate: candidate, StartedAt: now, UpdatedAt: now,
	}
	if err := AtomicWriteJSON(paths.Journal, providerJournal); err != nil {
		t.Fatalf("write deferred provider journal: %v", err)
	}
	if err := atomicWriteFile(paths.AgentEnv, []byte("PARTIAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write partial agent wiring: %v", err)
	}
	backupRoot := filepath.Join(paths.Root, ".install-backup-crashed")
	backup := filepath.Join(backupRoot, "0")
	if err := atomicWriteFile(backup, []byte("ORIGINAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write durable outer backup: %v", err)
	}
	outerJournalPath := filepath.Join(filepath.Dir(paths.Root), ".workflow-plugin-github-runner-provider.install-transaction.json")
	type snapshotFixture struct {
		Path    string      `json:"path"`
		Backup  string      `json:"backup"`
		Mode    os.FileMode `json:"mode"`
		Existed bool        `json:"existed"`
	}
	type activationFixture struct {
		ProviderService bool `json:"provider_service"`
		RefreshPath     bool `json:"refresh_path"`
		RefreshTimer    bool `json:"refresh_timer"`
	}
	outerJournal := struct {
		ProtocolVersion string                      `json:"protocol_version"`
		Operation       string                      `json:"operation"`
		Phase           string                      `json:"phase"`
		MaintenanceID   string                      `json:"maintenance_id"`
		AgentStopped    bool                        `json:"agent_stopped"`
		Snapshots       []snapshotFixture           `json:"snapshots"`
		PreviousUnits   map[string]systemdUnitState `json:"previous_units"`
		Activation      activationFixture           `json:"activation"`
		StartedAt       time.Time                   `json:"started_at"`
		UpdatedAt       time.Time                   `json:"updated_at"`
	}{
		ProtocolVersion: "retained-provider.install-transaction.v1", Operation: "install", Phase: "prepared",
		MaintenanceID: installMaintenanceID, AgentStopped: true,
		Snapshots:     []snapshotFixture{{Path: paths.AgentEnv, Backup: backup, Mode: 0o600, Existed: true}},
		PreviousUnits: map[string]systemdUnitState{}, Activation: activationFixture{}, StartedAt: now, UpdatedAt: now,
	}
	if err := AtomicWriteJSON(outerJournalPath, outerJournal); err != nil {
		t.Fatalf("write outer install journal: %v", err)
	}
	statuses := []string{"unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x6a}, 4096)), Now: func() time.Time { return now },
		Sleep:       func(context.Context, time.Duration) error { return nil },
		Refresh:     func(context.Context, Config) (Status, error) { return Status{}, errors.New("retry activation failed") },
		ProbeActive: func(context.Context, Config) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "retry activation failed") {
		t.Fatalf("retry failure = %v", err)
	}
	if data, err := os.ReadFile(paths.AgentEnv); err != nil || string(data) != "ORIGINAL_AGENT_ENV=1\n" {
		t.Fatalf("outer baseline wiring = %q err=%v", data, err)
	}
	if data, err := os.ReadFile(stateFile); err != nil || string(data) != "previous" {
		t.Fatalf("outer baseline provider state = %q err=%v", data, err)
	}
	restored, found, err := readActiveState(paths.ActiveState)
	if err != nil || !found || restored.Current.ImageID != previous.Current.ImageID {
		t.Fatalf("outer baseline active = %+v found=%v err=%v", restored, found, err)
	}
	if _, err := os.Stat(outerJournalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered outer journal remains: %v", err)
	}
}

func TestInstallRecoversOuterTransactionBeforeNewUpdatePreflight(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := atomicWriteFile(paths.AgentEnv, []byte("PARTIAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write partial agent env: %v", err)
	}
	backup := filepath.Join(paths.Root, ".install-backup-preflight-recovery", "0")
	if err := atomicWriteFile(backup, []byte("ORIGINAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write agent env backup: %v", err)
	}
	now := time.Unix(1_700_550_000, 0).UTC()
	journal := newInstallTransactionJournal("install", []managedFileSnapshot{{
		Path: paths.AgentEnv, Backup: backup, Mode: 0o600, Existed: true,
	}}, map[string]systemdUnitState{}, now)
	if err := writeInstallTransactionJournal(paths, journal); err != nil {
		t.Fatalf("write prepared outer transaction: %v", err)
	}
	runner := installSuccessRunner(t, config, "", "", new([]string))
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "verify-update" {
			return nil, errors.New("new update preflight unavailable")
		}
		return baseRun(ctx, command)
	}
	installer := Installer{Runner: runner, Now: func() time.Time { return now }}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "new update preflight unavailable") {
		t.Fatalf("install preflight err = %v", err)
	}
	if data, err := os.ReadFile(paths.AgentEnv); err != nil || string(data) != "ORIGINAL_AGENT_ENV=1\n" {
		t.Fatalf("preflight failure blocked local recovery = %q err=%v", data, err)
	}
	if _, err := os.Stat(paths.InstallJournal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered outer transaction remains after preflight failure: %v", err)
	}
}

func TestInstallRejectsMalformedOuterTransactionBeforeMaintenanceMutation(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-malformed-outer-transaction")
	digest := fileDigestForTest(t, payload)
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("mkdir install root: %v", err)
	}
	now := time.Unix(1_700_600_000, 0).UTC()
	malformed := installTransactionJournal{
		ProtocolVersion: installTransactionProtocol,
		Operation:       "install",
		Phase:           installTransactionPrepared,
		MaintenanceID:   installMaintenanceID,
		AgentStopped:    true,
		Snapshots: []managedFileSnapshot{{
			Path: filepath.Join(home, "unmanaged"), Backup: filepath.Join(paths.Root, ".install-backup-invalid", "0"),
		}},
		PreviousUnits: map[string]systemdUnitState{},
		StartedAt:     now,
		UpdatedAt:     now,
	}
	if err := AtomicWriteJSON(paths.InstallJournal, malformed); err != nil {
		t.Fatalf("write malformed outer transaction: %v", err)
	}
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x6b}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "unmanaged path") {
		t.Fatalf("malformed outer transaction err = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "supervisor-maintenance begin") || strings.Contains(transcript, "stop "+config.AgentUnit) {
		t.Fatalf("malformed outer transaction mutated maintenance or agent:\n%s", transcript)
	}
}

func TestInstallRecoversReadyOuterTransactionForwardBeforeRetry(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-ready-outer-transaction")
	digest := fileDigestForTest(t, payload)
	if err := atomicWriteFile(paths.AgentEnv, []byte("ORIGINAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write original agent env: %v", err)
	}
	snapshots, err := snapshotManagedFiles(paths)
	if err != nil {
		t.Fatalf("snapshot outer transaction: %v", err)
	}
	if err := atomicWriteFile(paths.AgentEnv, []byte("COMMITTED_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write committed agent env: %v", err)
	}
	now := time.Unix(1_700_700_000, 0).UTC()
	journal := newInstallTransactionJournal("install", snapshots, map[string]systemdUnitState{}, now)
	journal.Phase = installTransactionReady
	if err := writeInstallTransactionJournal(paths, journal); err != nil {
		t.Fatalf("write ready outer transaction: %v", err)
	}
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "maintenance-begin" {
			return nil, errors.New("stop after recovered transaction")
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x6c}, 4096)), Now: func() time.Time { return now },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"}); err == nil || !strings.Contains(err.Error(), "stop after recovered transaction") {
		t.Fatalf("retry after ready recovery err = %v", err)
	}
	if data, err := os.ReadFile(paths.AgentEnv); err != nil || string(data) != "COMMITTED_AGENT_ENV=1\n" {
		t.Fatalf("ready recovery rolled back committed wiring = %q err=%v", data, err)
	}
	if _, err := os.Stat(paths.InstallJournal); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready outer transaction remains: %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if !strings.Contains(transcript, "supervisor-maintenance end") || strings.Contains(transcript, "disable --now") || strings.Contains(transcript, "start "+config.AgentUnit) {
		t.Fatalf("ready outer transaction did not finish forward:\n%s", transcript)
	}
}

func TestInstallCleanupFailureStillReleasesMaintenance(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	payload := writeTestProviderPayload(t, home, "verified-provider-install-cleanup-failure")
	digest := fileDigestForTest(t, payload)
	statuses := []string{"unavailable", "unavailable"}
	runner := installSuccessRunner(t, config, payload, digest, &statuses)
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x69}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
		ProbeActive: func(context.Context, Config) error {
			return os.Chmod(paths.LifecycleTransactions, 0o500)
		},
	}
	t.Cleanup(func() { _ = os.Chmod(paths.LifecycleTransactions, 0o700) })
	_, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-new", ProviderToken: "provider-new"})
	if err == nil || !strings.Contains(err.Error(), "remove lifecycle transaction root") {
		t.Fatalf("install cleanup failure = %v", err)
	}
	if transcript := commandTranscript(runner.commands); !strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("install cleanup failure stranded maintenance:\n%s", transcript)
	}
}

func TestUninstallCleanupFailureStillReleasesMaintenance(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := AtomicWriteJSON(paths.ConfigFile, config); err != nil {
		t.Fatalf("write previous config: %v", err)
	}
	statuses := []string{"unavailable", "unavailable"}
	runner := installSuccessRunner(t, config, "", "", &statuses)
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		output, err := baseRun(ctx, command)
		if err == nil && installCommandEvent(command, config) == "maintenance-end" {
			err = os.Chmod(paths.LifecycleTransactions, 0o500)
		}
		return output, err
	}
	installer := Installer{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	t.Cleanup(func() { _ = os.Chmod(paths.LifecycleTransactions, 0o700) })
	_, err := installer.Uninstall(t.Context(), home, config, false)
	if err == nil || !strings.Contains(err.Error(), "remove lifecycle transaction root") {
		t.Fatalf("uninstall cleanup failure = %v", err)
	}
	if transcript := commandTranscript(runner.commands); !strings.Contains(transcript, "supervisor-maintenance end") {
		t.Fatalf("uninstall cleanup failure stranded maintenance:\n%s", transcript)
	}
}

func TestInstallRejectsMismatchedMaintenanceIdentityBeforeMutation(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-maintenance-mismatch")
	digest := fileDigestForTest(t, payload)
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "maintenance-begin" {
			return maintenanceStateJSON(true, "different-transaction", config.ProfileID, installMaintenanceReason), nil
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x71}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil || !strings.Contains(err.Error(), "mismatched") {
		t.Fatalf("mismatched maintenance err = %v", err)
	}
	if transcript := commandTranscript(runner.commands); strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
		t.Fatalf("agent mutated after mismatched maintenance response:\n%s", transcript)
	}
}

func TestMaintenanceStatusClassifiesExactInactiveAndConflictingFence(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	for _, tc := range []struct {
		name  string
		state []byte
		want  maintenanceDisposition
		err   string
	}{
		{
			name:  "exact active",
			state: maintenanceStateJSON(true, refreshMaintenanceID, config.ProfileID, refreshMaintenanceReason),
			want:  maintenanceExactActive,
		},
		{
			name:  "inactive",
			state: []byte(`{"active":false,"durable":true}`),
			want:  maintenanceInactive,
		},
		{
			name:  "conflicting active",
			state: maintenanceStateJSON(true, "other-transaction", config.ProfileID, refreshMaintenanceReason),
			want:  maintenanceConflicting,
		},
		{
			name:  "non-durable",
			state: []byte(`{"active":false,"durable":false}`),
			err:   "durable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
				if got := installCommandEvent(command, config); got != "maintenance-status" {
					t.Fatalf("command event = %q command=%+v", got, command)
				}
				if containsArg(command.Args, "-id") || containsArg(command.Args, "-reason") {
					t.Fatalf("status command carries transaction mutation arguments: %+v", command.Args)
				}
				return tc.state, nil
			}}
			installer := Installer{Runner: runner}
			state, err := installer.maintenanceStatus(t.Context(), config)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("maintenanceStatus error = %v want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("maintenanceStatus: %v", err)
			}
			if got := classifyMaintenanceState(state, config.ProfileID, refreshMaintenanceID, refreshMaintenanceReason); got != tc.want {
				t.Fatalf("classification = %q want %q", got, tc.want)
			}
		})
	}
}

func TestInstallBoundsTransientLocalStatusPolling(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	payload := writeTestProviderPayload(t, home, "verified-provider-bounded-status")
	digest := fileDigestForTest(t, payload)
	statusReads := 0
	runner := installSuccessRunner(t, config, payload, digest, new([]string))
	baseRun := runner.run
	runner.run = func(ctx context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "local-status" {
			statusReads++
			return localStatusJSON(config.WorkerID, "processing"), nil
		}
		return baseRun(ctx, command)
	}
	installer := Installer{
		Runner: runner, ExecutablePath: func() (string, error) { return payload, nil },
		Random: bytes.NewReader(bytes.Repeat([]byte{0x72}, 4096)), Sleep: func(context.Context, time.Duration) error { return nil },
	}
	if _, err := installer.Install(t.Context(), home, config, Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}); err == nil || !strings.Contains(err.Error(), "did not reach unavailable") {
		t.Fatalf("bounded status err = %v", err)
	}
	if statusReads != localStatusAttempts {
		t.Fatalf("local status reads = %d want %d", statusReads, localStatusAttempts)
	}
	if transcript := commandTranscript(runner.commands); strings.Contains(transcript, "supervisor-maintenance end") || strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
		t.Fatalf("pre-mutation timeout released the fence or stopped the agent:\n%s", transcript)
	}
	paths := LifecyclePathsFor(config)
	if journal, found, err := readLifecycleJournal(home, paths); err != nil || !found || journal.Phase != LifecycleFencing {
		t.Fatalf("bounded timeout lifecycle = %+v found=%v err=%v", journal, found, err)
	}
}

func TestInstallerStatusReportsOnlyLocalRedactedLifecycleState(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	writeLifecycleRecoveryFiles(t, config)
	paths := LifecyclePathsFor(config)
	active := previousActiveStateForTest(t, home)
	if err := AtomicWriteJSON(paths.ActiveState, active); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	if err := atomicWriteFile(paths.ProviderUnit, []byte("[Service]\n"), 0o600); err != nil {
		t.Fatalf("write provider service unit: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if filepath.Base(command.Path) == "systemctl" {
			return []byte("active\n"), nil
		}
		return nil, nil
	}}
	now := time.Unix(1_700_100_000, 0).UTC()
	status, err := (Installer{Runner: runner, Now: func() time.Time { return now }}).Status(t.Context(), home, config)
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	want := statusForActive(active, true, now)
	if status != want {
		t.Fatalf("status = %+v want %+v", status, want)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "github-secret") || strings.Contains(transcript, "provider-token") || strings.Contains(transcript, "COMPUTE_API_TOKEN") || strings.Contains(transcript, "https://stg") {
		t.Fatalf("status crossed a non-local boundary or leaked a secret:\n%s", transcript)
	}
}

func TestInstallerStatusReportsUninstalledWhenPreservedStateHasNoServiceUnit(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	if err := AtomicWriteJSON(paths.ActiveState, previousActiveStateForTest(t, home)); err != nil {
		t.Fatalf("write preserved active state: %v", err)
	}
	runner := &recordingCommandRunner{run: func(context.Context, Command) ([]byte, error) {
		return nil, errors.New("systemctl must not run for an uninstalled provider")
	}}
	now := time.Unix(1_700_200_000, 0).UTC()
	status, err := (Installer{Runner: runner, Now: func() time.Time { return now }}).Status(t.Context(), home, config)
	if err != nil {
		t.Fatalf("status after preserved-state uninstall: %v", err)
	}
	want := Status{ProtocolVersion: StatusProtocolVersion, ObservedAt: now}
	if status != want || len(runner.commands) != 0 {
		t.Fatalf("status = %+v commands=%+v want=%+v", status, runner.commands, want)
	}
}

func TestRestoreManagedFilesPreservesBackupAfterRestoreFailure(t *testing.T) {
	root := t.TempDir()
	backupRoot := filepath.Join(root, "backup")
	backup := filepath.Join(backupRoot, "0")
	destination := filepath.Join(root, "managed")
	if err := os.MkdirAll(backupRoot, 0o700); err != nil {
		t.Fatalf("mkdir backup: %v", err)
	}
	if err := os.WriteFile(backup, []byte("prior managed data"), 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatalf("mkdir invalid destination: %v", err)
	}
	snapshots := []managedFileSnapshot{{Path: destination, Backup: backup, Mode: 0o600, Existed: true}}
	if err := restoreManagedFiles(snapshots); err == nil {
		t.Fatal("restore into directory succeeded")
	}
	if data, err := os.ReadFile(backup); err != nil || string(data) != "prior managed data" {
		t.Fatalf("failed restore discarded backup: data=%q err=%v", data, err)
	}
}

func TestRollbackInstallPreservesSnapshotsUntilAgentAndMaintenanceRestore(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	backupRoot := filepath.Join(config.InstallRoot, ".install-backup-retry")
	backup := filepath.Join(backupRoot, "0")
	destination := LifecyclePathsFor(config).AgentEnv
	if err := atomicWriteFile(backup, []byte("ORIGINAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	if err := atomicWriteFile(destination, []byte("PARTIAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write partial destination: %v", err)
	}
	runner := &recordingCommandRunner{run: func(_ context.Context, command Command) ([]byte, error) {
		if installCommandEvent(command, config) == "agent-start" {
			return nil, errors.New("agent restart failed")
		}
		return nil, nil
	}}
	snapshots := []managedFileSnapshot{{Path: destination, Backup: backup, Mode: 0o600, Existed: true}}
	err := (Installer{Runner: runner}).rollbackInstall(t.Context(), config, snapshots, map[string]systemdUnitState{}, true, true, installMaintenanceID, installMaintenanceReason, systemdActivation{})
	if err == nil || !strings.Contains(err.Error(), "agent restart failed") {
		t.Fatalf("rollback err = %v", err)
	}
	if data, err := os.ReadFile(backup); err != nil || string(data) != "ORIGINAL_AGENT_ENV=1\n" {
		t.Fatalf("rollback discarded retry backup = %q err=%v", data, err)
	}
}

func TestRollbackInstallBeforeStartRetainsSnapshotsForLifecycleCommit(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	backupRoot := filepath.Join(config.InstallRoot, ".install-backup-lifecycle")
	backup := filepath.Join(backupRoot, "0")
	destination := LifecyclePathsFor(config).AgentEnv
	if err := atomicWriteFile(backup, []byte("ORIGINAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	if err := atomicWriteFile(destination, []byte("PARTIAL_AGENT_ENV=1\n"), 0o600); err != nil {
		t.Fatalf("write partial destination: %v", err)
	}
	snapshots := []managedFileSnapshot{{Path: destination, Backup: backup, Mode: 0o600, Existed: true}}
	if err := (Installer{Runner: &recordingCommandRunner{}}).rollbackInstallBeforeStart(
		t.Context(), config, snapshots, map[string]systemdUnitState{}, true, false, "", "", systemdActivation{}, func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Fatal("rollback callback has no deadline")
			}
			if remaining := time.Until(deadline); remaining < 2*controlCommandTimeout {
				t.Fatalf("rollback callback deadline = %s want at least %s", remaining, 2*controlCommandTimeout)
			}
			return nil
		},
	); err != nil {
		t.Fatalf("rollback before lifecycle commit: %v", err)
	}
	if data, err := os.ReadFile(backup); err != nil || string(data) != "ORIGINAL_AGENT_ENV=1\n" {
		t.Fatalf("lifecycle rollback discarded durable backup = %q err=%v", data, err)
	}
	if data, err := os.ReadFile(destination); err != nil || string(data) != "ORIGINAL_AGENT_ENV=1\n" {
		t.Fatalf("lifecycle rollback did not restore destination = %q err=%v", data, err)
	}
}

func TestRestoreManagedFilesPropagatesSnapshotCleanupFailure(t *testing.T) {
	root := t.TempDir()
	cleanupParent := filepath.Join(root, "cleanup-parent")
	backupRoot := filepath.Join(cleanupParent, "backup")
	backup := filepath.Join(backupRoot, "0")
	destination := filepath.Join(root, "managed")
	if err := os.MkdirAll(backupRoot, 0o700); err != nil {
		t.Fatalf("mkdir backup: %v", err)
	}
	if err := os.WriteFile(backup, []byte("prior managed data"), 0o600); err != nil {
		t.Fatalf("write backup: %v", err)
	}
	if err := os.WriteFile(destination, []byte("current managed data"), 0o600); err != nil {
		t.Fatalf("write destination: %v", err)
	}
	if err := os.Chmod(cleanupParent, 0o500); err != nil {
		t.Fatalf("restrict cleanup parent: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(cleanupParent, 0o700) })
	snapshots := []managedFileSnapshot{{Path: destination, Backup: backup, Mode: 0o600, Existed: true}}
	if err := restoreManagedFiles(snapshots); err == nil {
		t.Fatal("snapshot cleanup failure was discarded")
	}
}

func TestUninstallRemovesWiringAndPreservesStateUnlessPurged(t *testing.T) {
	for _, purge := range []bool{false, true} {
		t.Run("purge="+strings.ToLower(strconv.FormatBool(purge)), func(t *testing.T) {
			home := t.TempDir()
			config := validTestConfig(home)
			writeLifecycleRecoveryFiles(t, config)
			paths := LifecyclePathsFor(config)
			if err := os.MkdirAll(paths.ProviderState, 0o700); err != nil {
				t.Fatalf("mkdir provider state: %v", err)
			}
			if err := os.WriteFile(filepath.Join(paths.ProviderState, "retained.json"), []byte("state"), 0o600); err != nil {
				t.Fatalf("write state: %v", err)
			}
			for _, path := range []string{paths.ProviderUnit, paths.RefreshUnit, paths.PathUnit, paths.TimerUnit, paths.AgentDropIn} {
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatalf("mkdir unit dir: %v", err)
				}
				if err := os.WriteFile(path, []byte("unit"), 0o600); err != nil {
					t.Fatalf("write unit: %v", err)
				}
			}
			statuses := []string{"unavailable", "unavailable", "idle"}
			runner := installSuccessRunner(t, config, "", "", &statuses)
			baseRun := runner.run
			runner.run = func(ctx context.Context, command Command) ([]byte, error) {
				switch installCommandEvent(command, config) {
				case "maintenance-begin":
					journal, found, err := readLifecycleJournal(home, paths)
					if err != nil || !found || journal.Phase != LifecycleFencing || journal.Uninstall == nil || journal.Uninstall.Purge != purge {
						t.Fatalf("uninstall maintenance begin lifecycle = %+v found=%v err=%v", journal, found, err)
					}
				case "agent-stop":
					journal, found, err := readLifecycleJournal(home, paths)
					if err != nil || !found || journal.Phase != LifecycleFenced {
						t.Fatalf("uninstall agent stop lifecycle = %+v found=%v err=%v", journal, found, err)
					}
				case "maintenance-end":
					journal, found, err := readLifecycleJournal(home, paths)
					if err != nil || !found || journal.Phase != LifecycleReleasing || journal.Outcome != LifecycleCommit {
						t.Fatalf("uninstall maintenance end lifecycle = %+v found=%v err=%v", journal, found, err)
					}
				}
				return baseRun(ctx, command)
			}
			installer := Installer{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
			if _, err := installer.Uninstall(t.Context(), home, config, purge); err != nil {
				t.Fatalf("uninstall: %v", err)
			}
			for _, path := range []string{paths.ProviderUnit, paths.RefreshUnit, paths.PathUnit, paths.TimerUnit, paths.AgentDropIn} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("wiring remains %s: %v", path, err)
				}
			}
			_, rootErr := os.Stat(paths.Root)
			if purge && !errors.Is(rootErr, os.ErrNotExist) {
				t.Fatalf("purged root remains: %v", rootErr)
			}
			if !purge {
				if data, err := os.ReadFile(filepath.Join(paths.ProviderState, "retained.json")); err != nil || string(data) != "state" {
					t.Fatalf("non-purge state data=%q err=%v", data, err)
				}
			}
			transcript := commandTranscript(runner.commands)
			assertOrderedText(t, transcript, []string{"supervisor-maintenance begin", "local-status sanitize", "systemctl --user stop " + config.AgentUnit, "systemctl --user disable --now", "systemctl --user daemon-reload", "systemctl --user start " + config.AgentUnit, "supervisor-maintenance end", "local-status sanitize"})
		})
	}
}

func TestUninstallLockContentionDoesNotMutateMaintenanceOrAgent(t *testing.T) {
	home := t.TempDir()
	config := validTestConfig(home)
	paths := LifecyclePathsFor(config)
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		t.Fatalf("hold install lock: %v", err)
	}
	defer func() { _ = lock.Release() }()
	runner := installSuccessRunner(t, config, "", "", new([]string))
	installer := Installer{Runner: runner, Sleep: func(context.Context, time.Duration) error { return nil }}
	if _, err := installer.Uninstall(t.Context(), home, config, false); !errors.Is(err, ErrInstallLocked) {
		t.Fatalf("contended uninstall err = %v", err)
	}
	transcript := commandTranscript(runner.commands)
	if strings.Contains(transcript, "supervisor-maintenance begin") || strings.Contains(transcript, "systemctl --user stop "+config.AgentUnit) {
		t.Fatalf("contended uninstall mutated maintenance or agent:\n%s", transcript)
	}
}

func installSuccessRunner(t *testing.T, config Config, payload, digest string, statuses *[]string) *recordingCommandRunner {
	t.Helper()
	writeLifecycleRecoveryFiles(t, config)
	maintenanceActive := false
	activeMaintenanceID := ""
	activeMaintenanceReason := ""
	runner := &recordingCommandRunner{}
	runner.run = func(_ context.Context, command Command) ([]byte, error) {
		if command.Path == config.LoginctlPath {
			return []byte("yes\n"), nil
		}
		if installCommandEvent(command, config) == "agent-signature" {
			return agentUnitSystemdOutputForTest(t, config), nil
		}
		if command.Path == config.PodmanPath && containsArg(command.Args, "info") {
			return []byte("true\n"), nil
		}
		if command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "image" && command.Args[1] == "inspect" {
			return []byte(testProviderImageID + "\n"), nil
		}
		if command.Path == config.PodmanPath && firstArg(command.Args) == "images" && containsAdjacentArgs(command.Args, "--filter", "id="+testProviderImageID) {
			return ownedProviderImageInventory(config, digest, testProviderImageID), nil
		}
		if command.Path == config.PodmanPath && len(command.Args) >= 2 && command.Args[0] == "network" && command.Args[1] == "inspect" {
			return []byte("bridge true false\n"), nil
		}
		if filepath.Base(command.Path) == "systemctl" && containsArg(command.Args, "show") && (containsArg(command.Args, providerServiceUnit) || containsArg(command.Args, refreshPathUnit) || containsArg(command.Args, refreshTimerUnit)) {
			if containsAdjacentArgs(command.Args, "--property", "ActiveState") && containsArg(command.Args, "--value") {
				return []byte("active\n"), nil
			}
			unitPath := LifecyclePathsFor(config).ProviderUnit
			switch {
			case containsArg(command.Args, refreshPathUnit):
				unitPath = LifecyclePathsFor(config).PathUnit
			case containsArg(command.Args, refreshTimerUnit):
				unitPath = LifecyclePathsFor(config).TimerUnit
			}
			if _, err := os.Lstat(unitPath); errors.Is(err, os.ErrNotExist) {
				return []byte("LoadState=not-found\nFragmentPath=\nActiveState=inactive\nUnitFileState=\n"), nil
			} else if err != nil {
				return nil, err
			}
			return []byte("LoadState=loaded\nFragmentPath=" + unitPath + "\nActiveState=active\nUnitFileState=enabled\n"), nil
		}
		switch installCommandEvent(command, config) {
		case "verify-update":
			return testVerifiedUpdateJSON(config, payload, digest), nil
		case "maintenance-begin":
			maintenanceActive = true
			id := adjacentArgValue(command.Args, "-id")
			reason := adjacentArgValue(command.Args, "-reason")
			activeMaintenanceID, activeMaintenanceReason = id, reason
			return maintenanceStateJSON(true, id, config.ProfileID, reason), nil
		case "maintenance-status":
			if !maintenanceActive {
				return []byte(`{"active":false,"durable":true}`), nil
			}
			return maintenanceStateJSON(true, activeMaintenanceID, config.ProfileID, activeMaintenanceReason), nil
		case "maintenance-end":
			maintenanceActive = false
			id, reason := activeMaintenanceID, activeMaintenanceReason
			if id == "" {
				id = adjacentArgValue(command.Args, "-id")
				switch id {
				case installMaintenanceID:
					reason = installMaintenanceReason
				case uninstallMaintenanceID:
					reason = uninstallMaintenanceReason
				}
			}
			return maintenanceStateJSON(false, id, config.ProfileID, reason), nil
		case "local-status":
			if len(*statuses) == 0 {
				state := "idle"
				if maintenanceActive {
					state = "unavailable"
				}
				return localStatusJSON(config.WorkerID, state), nil
			}
			state := (*statuses)[0]
			*statuses = (*statuses)[1:]
			return localStatusJSON(config.WorkerID, state), nil
		default:
			return nil, nil
		}
	}
	return runner
}

func installCommandEvent(command Command, config Config) string {
	if command.Path == config.ComputeAgentPath && len(command.Args) > 0 {
		switch command.Args[0] {
		case "supervisor-update":
			return "verify-update"
		case "supervisor-config":
			return "supervisor-config-validate"
		case "supervisor-maintenance":
			if len(command.Args) > 1 && command.Args[1] == "begin" {
				return "maintenance-begin"
			}
			if len(command.Args) > 1 && command.Args[1] == "status" {
				return "maintenance-status"
			}
			return "maintenance-end"
		case "local-status":
			return "local-status"
		}
	}
	if command.Path == config.PodmanPath {
		return "podman-preflight"
	}
	if filepath.Base(command.Path) == "systemctl" {
		joined := strings.Join(command.Args, " ")
		switch {
		case containsArg(command.Args, "show") && containsArg(command.Args, config.AgentUnit) && containsAdjacentArgs(command.Args, "--property", "DropInPaths"):
			return "agent-signature"
		case strings.Contains(joined, "show-environment"):
			return "systemd-preflight"
		case strings.Contains(joined, "daemon-reload"):
			return "daemon-reload"
		case strings.Contains(joined, "enable "+providerServiceUnit):
			return "provider-enable"
		case strings.Contains(joined, "enable --now "+refreshPathUnit):
			return "refresh-watch-enable"
		case strings.Contains(joined, "stop "+config.AgentUnit):
			return "agent-stop"
		case strings.Contains(joined, "start "+config.AgentUnit):
			return "agent-start"
		}
	}
	return filepath.Base(command.Path) + " " + strings.Join(command.Args, " ")
}

func maintenanceStateJSON(active bool, id, profileID, reason string) []byte {
	return []byte(`{"active":` + strconv.FormatBool(active) + `,"durable":true,"maintenance":{"kind":"workflow-compute.supervisor-maintenance.v1","id":"` + id + `","profile_id":"` + profileID + `","reason":"` + reason + `","started_at":"2026-07-13T00:00:00Z"}}`)
}

func localStatusJSON(workerID, state string) []byte {
	return []byte(`{"protocol_version":"compute.local_status.v1","worker_id":"` + workerID + `","state":"` + state + `","updated_at":"2099-01-01T00:00:00Z"}`)
}

func localStatusAtJSON(workerID, state string, updatedAt time.Time) []byte {
	return []byte(`{"protocol_version":"compute.local_status.v1","worker_id":"` + workerID + `","state":"` + state + `","updated_at":` + strconv.Quote(updatedAt.UTC().Format(time.RFC3339Nano)) + `}`)
}

func adjacentArgValue(args []string, key string) string {
	for index := 0; index+1 < len(args); index++ {
		if args[index] == key {
			return args[index+1]
		}
	}
	return ""
}

func assertOrderedEvents(t *testing.T, events, expected []string) {
	t.Helper()
	position := -1
	for _, want := range expected {
		found := -1
		for index := position + 1; index < len(events); index++ {
			if events[index] == want {
				found = index
				break
			}
		}
		if found < 0 {
			t.Fatalf("event %q missing after %d: %v", want, position, events)
		}
		position = found
	}
}

func assertOrderedText(t *testing.T, text string, expected []string) {
	t.Helper()
	position := -1
	for _, want := range expected {
		found := strings.Index(text[position+1:], want)
		if found < 0 {
			t.Fatalf("text missing ordered %q after %d:\n%s", want, position, text)
		}
		position += found + 1
	}
}

func parseCertificateForTest(t *testing.T, data []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("decode certificate PEM: %s", data)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return certificate
}

func systemdEnvironmentValue(contents, key string) string {
	for line := range strings.SplitSeq(contents, "\n") {
		if value, found := strings.CutPrefix(line, key+"="); found {
			return strings.Trim(value, "\"")
		}
	}
	return ""
}

func containsString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func containsIP(values []net.IP, expected net.IP) bool {
	for _, value := range values {
		if value.Equal(expected) {
			return true
		}
	}
	return false
}
