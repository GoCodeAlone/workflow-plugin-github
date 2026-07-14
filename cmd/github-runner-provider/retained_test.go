package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal/retainedprovider"
)

func TestRetainedRefreshLoadsStrictConfigAndEmitsTypedStatus(t *testing.T) {
	home := t.TempDir()
	config := retainedCommandTestConfig(home)
	wantStatus := retainedprovider.Status{
		ProtocolVersion: retainedprovider.StatusProtocolVersion,
		Installed:       true,
		ServiceActive:   true,
		CurrentVersion:  "v1.0.32",
		CurrentSHA256:   "sha256:" + strings.Repeat("a", 64),
		ObservedAt:      time.Unix(1_700_000_000, 0).UTC(),
	}
	var received retainedprovider.Config
	dependencies := retainedProviderCommandDependencies{
		GOOS:    "linux",
		HomeDir: func() (string, error) { return home, nil },
		ReadConfig: func(path, gotHome string) (retainedprovider.Config, error) {
			if path != filepath.Join(home, "config.json") || gotHome != home {
				t.Fatalf("ReadConfig path=%q home=%q", path, gotHome)
			}
			return config, nil
		},
		Refresh: func(_ context.Context, got retainedprovider.Config) (retainedprovider.Status, error) {
			received = got
			return wantStatus, nil
		},
		ServeActive: func(context.Context, retainedprovider.Config) error { return nil },
	}
	var stdout bytes.Buffer
	err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{
		"refresh", "-config", filepath.Join(home, "config.json"),
	}, &stdout, dependencies)
	if err != nil {
		t.Fatalf("retained refresh: %v", err)
	}
	if received.WorkerID != config.WorkerID || received.ProfileID != config.ProfileID || received.ComponentID != config.ComponentID {
		t.Fatalf("refresh config = %+v", received)
	}
	var status retainedprovider.Status
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&status); err != nil {
		t.Fatalf("decode status: %v output=%s", err, stdout.String())
	}
	if status != wantStatus {
		t.Fatalf("status = %+v want %+v", status, wantStatus)
	}
}

func TestRetainedServeActiveDispatchesWithoutWritingOutput(t *testing.T) {
	home := t.TempDir()
	config := retainedCommandTestConfig(home)
	sentinel := errors.New("foreground exec returned")
	dependencies := retainedProviderCommandDependencies{
		GOOS:       "linux",
		HomeDir:    func() (string, error) { return home, nil },
		ReadConfig: func(string, string) (retainedprovider.Config, error) { return config, nil },
		Refresh: func(context.Context, retainedprovider.Config) (retainedprovider.Status, error) {
			return retainedprovider.Status{}, nil
		},
		ServeActive: func(context.Context, retainedprovider.Config) error { return sentinel },
	}
	var stdout bytes.Buffer
	err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{
		"serve-active", "-config", filepath.Join(home, "config.json"),
	}, &stdout, dependencies)
	if !errors.Is(err, sentinel) {
		t.Fatalf("serve-active err = %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("serve-active wrote output: %q", stdout.String())
	}
}

func TestRetainedInstallStatusAndUninstallDispatchTypedLifecycle(t *testing.T) {
	home := t.TempDir()
	config := retainedCommandTestConfig(home)
	wantStatus := retainedprovider.Status{ProtocolVersion: retainedprovider.StatusProtocolVersion, Installed: true, ServiceActive: true}
	var installedCredentials retainedprovider.Credentials
	var events []string
	dependencies := retainedProviderCommandDependencies{
		GOOS:    "linux",
		HomeDir: func() (string, error) { return home, nil },
		LookupEnv: func(key string) (string, bool) {
			values := map[string]string{
				"GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN": "github-secret",
				"GITHUB_RUNNER_PROVIDER_TOKEN":        "provider-secret",
			}
			value, found := values[key]
			return value, found
		},
		ReadConfig: func(path, gotHome string) (retainedprovider.Config, error) {
			if path != filepath.Join(home, "bootstrap-config.json") || gotHome != home {
				t.Fatalf("ReadConfig path=%q home=%q", path, gotHome)
			}
			return config, nil
		},
		Install: func(_ context.Context, gotHome string, got retainedprovider.Config, credentials retainedprovider.Credentials) (retainedprovider.Status, error) {
			if gotHome != home || got.WorkerID != config.WorkerID || got.ProfileID != config.ProfileID {
				t.Fatalf("install home=%q config=%+v", gotHome, got)
			}
			installedCredentials = credentials
			events = append(events, "install")
			return wantStatus, nil
		},
		Status: func(_ context.Context, gotHome string, got retainedprovider.Config) (retainedprovider.Status, error) {
			if gotHome != home || got.WorkerID != config.WorkerID || got.ProfileID != config.ProfileID {
				t.Fatalf("status home=%q config=%+v", gotHome, got)
			}
			events = append(events, "status")
			return wantStatus, nil
		},
		Uninstall: func(_ context.Context, gotHome string, got retainedprovider.Config, purge bool) (retainedprovider.Status, error) {
			if gotHome != home || got.WorkerID != config.WorkerID || got.ProfileID != config.ProfileID || !purge {
				t.Fatalf("uninstall home=%q config=%+v purge=%v", gotHome, got, purge)
			}
			events = append(events, "uninstall")
			return retainedprovider.Status{ProtocolVersion: retainedprovider.StatusProtocolVersion}, nil
		},
		Refresh: func(context.Context, retainedprovider.Config) (retainedprovider.Status, error) {
			return retainedprovider.Status{}, nil
		},
		ServeActive: func(context.Context, retainedprovider.Config) error { return nil },
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	configPath := filepath.Join(home, "bootstrap-config.json")
	for _, args := range [][]string{
		{"install", "-config", configPath},
		{"status", "-config", configPath},
		{"uninstall", "-config", configPath, "-purge"},
	} {
		var stdout bytes.Buffer
		if err := runRetainedProviderCommandWithDependencies(t.Context(), logger, args, &stdout, dependencies); err != nil {
			t.Fatalf("retained %v: %v", args, err)
		}
		var status retainedprovider.Status
		if err := json.Unmarshal(stdout.Bytes(), &status); err != nil || status.ProtocolVersion != retainedprovider.StatusProtocolVersion {
			t.Fatalf("decode %v status=%+v err=%v output=%s", args, status, err, stdout.String())
		}
	}
	if installedCredentials != (retainedprovider.Credentials{GitHubToken: "github-secret", ProviderToken: "provider-secret"}) {
		t.Fatalf("install credentials = %+v", installedCredentials)
	}
	if got, want := strings.Join(events, ","), "install,status,uninstall"; got != want {
		t.Fatalf("events = %q want %q", got, want)
	}
}

func TestRetainedRecoverRequiresAndDispatchesExactConfirmation(t *testing.T) {
	home := t.TempDir()
	config := retainedCommandTestConfig(home)
	wantStatus := retainedprovider.Status{ProtocolVersion: retainedprovider.StatusProtocolVersion, ObservedAt: time.Unix(1_700_000_000, 0).UTC()}
	called := false
	dependencies := retainedProviderCommandDependencies{
		GOOS:    "linux",
		HomeDir: func() (string, error) { return home, nil },
		ReadConfig: func(path, gotHome string) (retainedprovider.Config, error) {
			if path != filepath.Join(home, "trusted-config.json") || gotHome != home {
				t.Fatalf("ReadConfig path=%q home=%q", path, gotHome)
			}
			return config, nil
		},
		Recover: func(_ context.Context, gotHome string, got retainedprovider.Config, confirmation string) (retainedprovider.Status, error) {
			if gotHome != home || got.WorkerID != config.WorkerID || confirmation != "legacy-provider-transaction" {
				t.Fatalf("recover home=%q config=%+v confirmation=%q", gotHome, got, confirmation)
			}
			called = true
			return wantStatus, nil
		},
	}
	var stdout bytes.Buffer
	if err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{
		"recover", "-config", filepath.Join(home, "trusted-config.json"), "-confirm", "legacy-provider-transaction",
	}, &stdout, dependencies); err != nil {
		t.Fatalf("retained recover: %v", err)
	}
	if !called {
		t.Fatal("recover dependency was not called")
	}
	var status retainedprovider.Status
	if err := json.Unmarshal(stdout.Bytes(), &status); err != nil || status != wantStatus {
		t.Fatalf("recover status=%+v err=%v output=%s", status, err, stdout.String())
	}

	called = false
	if err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{
		"recover", "-config", filepath.Join(home, "trusted-config.json"),
	}, io.Discard, dependencies); err == nil || !strings.Contains(err.Error(), "-confirm") {
		t.Fatalf("missing confirmation error = %v", err)
	}
	if called {
		t.Fatal("recover called without exact confirmation")
	}
}

func TestRetainedInstallRejectsCredentialFlagsAndMissingEnvironment(t *testing.T) {
	home := t.TempDir()
	config := retainedCommandTestConfig(home)
	base := retainedProviderCommandDependencies{
		GOOS:       "linux",
		HomeDir:    func() (string, error) { return home, nil },
		LookupEnv:  func(string) (string, bool) { return "", false },
		ReadConfig: func(string, string) (retainedprovider.Config, error) { return config, nil },
		Install: func(context.Context, string, retainedprovider.Config, retainedprovider.Credentials) (retainedprovider.Status, error) {
			t.Fatal("install called without credentials")
			return retainedprovider.Status{}, nil
		},
		Refresh: func(context.Context, retainedprovider.Config) (retainedprovider.Status, error) {
			return retainedprovider.Status{}, nil
		},
		ServeActive: func(context.Context, retainedprovider.Config) error { return nil },
	}
	for _, tc := range []struct {
		args []string
		want string
	}{
		{args: []string{"install", "-config", filepath.Join(home, "config.json")}, want: "environment"},
		{args: []string{"install", "-config", filepath.Join(home, "config.json"), "-token", "secret"}, want: "flag"},
	} {
		err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), tc.args, io.Discard, base)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("args=%v err=%v want %q", tc.args, err, tc.want)
		}
	}
}

func TestRetainedCommandFailsClosedOnUnsupportedPlatformAndInvalidShape(t *testing.T) {
	base := retainedProviderCommandDependencies{
		GOOS:       "linux",
		HomeDir:    func() (string, error) { return t.TempDir(), nil },
		ReadConfig: func(string, string) (retainedprovider.Config, error) { return retainedprovider.Config{}, nil },
		Refresh: func(context.Context, retainedprovider.Config) (retainedprovider.Status, error) {
			return retainedprovider.Status{}, nil
		},
		ServeActive: func(context.Context, retainedprovider.Config) error { return nil },
	}
	for _, tc := range []struct {
		name string
		deps retainedProviderCommandDependencies
		args []string
		want string
	}{
		{name: "unsupported", deps: func() retainedProviderCommandDependencies { value := base; value.GOOS = "darwin"; return value }(), args: []string{"refresh"}, want: "unsupported"},
		{name: "missing subcommand", deps: base, want: "subcommand"},
		{name: "unknown", deps: base, args: []string{"install-now"}, want: "unknown"},
		{name: "missing config", deps: base, args: []string{"refresh"}, want: "-config"},
		{name: "positional", deps: base, args: []string{"refresh", "-config", "/tmp/config", "extra"}, want: "positional"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := runRetainedProviderCommandWithDependencies(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), tc.args, io.Discard, tc.deps)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v want %q", err, tc.want)
			}
		})
	}
}

func retainedCommandTestConfig(home string) retainedprovider.Config {
	root := filepath.Join(home, ".workflow-compute", "github-runner-provider")
	return retainedprovider.Config{
		ProtocolVersion: retainedprovider.ConfigProtocolVersion,
		WorkerID:        "github-runner-linux-stg", ProfileID: "github-runner-profile-stg",
		PluginID: retainedprovider.GitHubPluginID, ComponentID: "github-runner-provider-sidecar",
		ComputeAgentPath: filepath.Join(home, "compute-agent"), SupervisorConfigPath: filepath.Join(home, "supervisor.pb"),
		LocalStatusPath: filepath.Join(home, "status.json"), ProviderMarkerPath: filepath.Join(home, "updates", "current-provider.json"), InstallRoot: root,
		SystemdDir: filepath.Join(home, ".config", "systemd", "user"), AgentUnit: "workflow-compute-agent.service",
		PodmanPath: "/usr/bin/podman", ProviderURL: "https://workflow-plugin-github-runner-provider:18090",
		StableContainer: "workflow-plugin-github-runner-provider", CandidateContainer: "workflow-plugin-github-runner-provider-candidate", ContainerNetwork: "wfcompute-github-provider",
		Organization: "GoCodeAlone", Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood-provider-target.yml",
		Ref: strings.Repeat("a", 40), RunnerName: "wfc-stg-ghp-linux-probe", RunnerGroup: "ephemeral",
		Labels: []string{"self-hosted", "linux", "wfc-ghp-stg"}, RefreshIntervalSeconds: 300,
	}
}
