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
	if received.WorkerID != config.WorkerID || received.ComponentID != config.ComponentID {
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
		WorkerID:        "github-runner-linux-stg", ProfileID: "github-runner-linux-stg",
		PluginID: retainedprovider.GitHubPluginID, ComponentID: "github-runner-provider-sidecar",
		ComputeAgentPath: filepath.Join(home, "compute-agent"), SupervisorConfigPath: filepath.Join(home, "supervisor.pb"),
		LocalStatusPath: filepath.Join(home, "status.json"), InstallRoot: root,
		SystemdDir: filepath.Join(home, ".config", "systemd", "user"), AgentUnit: "workflow-compute-agent.service",
		PodmanPath: "/usr/bin/podman", ProviderURL: "https://workflow-plugin-github-runner-provider:18090",
		StableContainer: "workflow-plugin-github-runner-provider", CandidateContainer: "workflow-plugin-github-runner-provider-candidate", ContainerNetwork: "bridge",
		Organization: "GoCodeAlone", Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood-provider-target.yml",
		Ref: strings.Repeat("a", 40), RunnerName: "wfc-stg-ghp-linux-probe", RunnerGroup: "ephemeral",
		Labels: []string{"self-hosted", "linux", "wfc-ghp-stg"}, RefreshIntervalSeconds: 300,
	}
}
