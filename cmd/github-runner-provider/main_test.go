package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
)

type deadlineShutdowner struct {
	closed bool
}

func (s *deadlineShutdowner) Shutdown(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *deadlineShutdowner) Close() error {
	s.closed = true
	return nil
}

type contextCheckingStopper struct {
	called  bool
	expired bool
}

func (s *contextCheckingStopper) Stop(ctx context.Context) error {
	s.called = true
	s.expired = ctx.Err() != nil
	return ctx.Err()
}

func TestProviderHTTPServerHasBoundedConnectionTimeouts(t *testing.T) {
	server := newProviderHTTPServer("127.0.0.1:0", http.NewServeMux())
	if server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("provider HTTP timeouts are not fully bounded: %+v", server)
	}
}

func TestShutdownProviderGivesServiceIndependentCleanupDeadline(t *testing.T) {
	server := &deadlineShutdowner{}
	service := &contextCheckingStopper{}
	err := shutdownProvider(server, service, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	if !server.closed {
		t.Fatal("timed-out graceful shutdown did not force-close the HTTP server")
	}
	if !service.called {
		t.Fatal("service cleanup was not called after HTTP shutdown deadline")
	}
	if service.expired {
		t.Fatal("service cleanup inherited the expired HTTP shutdown context")
	}
}

func TestWaitForProviderServeDoneIsBounded(t *testing.T) {
	started := time.Now()
	err := waitForProviderServeDone(make(chan error), 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("wait error = %v, want bounded timeout", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("serve wait exceeded bound: %s", elapsed)
	}
}

func TestRunnerProviderConfigFromEnvironmentMatchesStrictContract(t *testing.T) {
	t.Setenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN", "github-token")
	t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_API_BASE_URL", "")
	t.Setenv("GITHUB_RUNNER_PROVIDER_REPOSITORIES", " GoCodeAlone/workflow-compute ")
	t.Setenv("GITHUB_RUNNER_PROVIDER_ORGANIZATIONS", " GoCodeAlone , OtherOrg ")
	t.Setenv("GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS", "ephemeral, Default")
	t.Setenv("GITHUB_RUNNER_PROVIDER_STATE_DIR", t.TempDir())

	config, err := runnerProviderConfigFromEnvironment()
	if err != nil {
		t.Fatalf("build provider config: %v", err)
	}
	if err := githubplugin.ValidateGitHubRunnerProviderConfigValue(config); err != nil {
		t.Fatalf("strict provider config rejected command environment: %v", err)
	}
	if _, ok := config["api_base_url"]; ok {
		t.Fatalf("unset API URL must be omitted: %+v", config)
	}
	if got := config["repositories"]; !reflect.DeepEqual(got, []string{"GoCodeAlone/workflow-compute"}) {
		t.Fatalf("repositories = %#v", got)
	}
	if got := config["organizations"]; !reflect.DeepEqual(got, []string{"GoCodeAlone", "OtherOrg"}) {
		t.Fatalf("organizations = %#v", got)
	}
	if got := config["runner_groups"]; !reflect.DeepEqual(got, []string{"ephemeral", "Default"}) {
		t.Fatalf("runner groups = %#v", got)
	}
	if got, _ := config["state_dir"].(string); got == "" {
		t.Fatalf("state_dir = %#v", config["state_dir"])
	}
}

func TestProviderRunStopsModuleAndFlushesJournalOnCancellation(t *testing.T) {
	stateDir := t.TempDir()
	t.Setenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN", "github-token")
	t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_RUNNER_PROVIDER_REPOSITORIES", "GoCodeAlone/workflow-compute")
	t.Setenv("GITHUB_RUNNER_PROVIDER_ORGANIZATIONS", "GoCodeAlone")
	t.Setenv("GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS", "ephemeral")
	t.Setenv("GITHUB_RUNNER_PROVIDER_STATE_DIR", stateDir)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), []string{"127.0.0.1:0"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("cancel provider: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provider ignored cancellation")
	}
	if _, err := os.Stat(filepath.Join(stateDir, "jit-ownership.json")); err != nil {
		t.Fatalf("provider shutdown did not flush ownership journal: %v", err)
	}
}
