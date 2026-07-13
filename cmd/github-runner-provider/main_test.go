package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
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

type recordingProviderHTTPServer struct {
	plainCalls int
	tlsCalls   int
	certFile   string
	keyFile    string
	address    string
}

type closeTrackingListener struct {
	net.Listener
	closed bool
}

func (l *closeTrackingListener) Close() error {
	l.closed = true
	return l.Listener.Close()
}

func (s *recordingProviderHTTPServer) Serve(listener net.Listener) error {
	s.plainCalls++
	s.address = listener.Addr().String()
	return http.ErrServerClosed
}

func (s *recordingProviderHTTPServer) ServeTLS(listener net.Listener, certFile, keyFile string) error {
	s.tlsCalls++
	s.certFile = certFile
	s.keyFile = keyFile
	s.address = listener.Addr().String()
	return http.ErrServerClosed
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
	if server.TLSConfig == nil || server.TLSConfig.MinVersion < tls.VersionTLS12 {
		t.Fatalf("provider TLS minimum is not pinned to TLS 1.2+: %+v", server.TLSConfig)
	}
}

func TestProviderTLSFilesRequireCertificateAndKeyTogether(t *testing.T) {
	for _, tc := range []struct {
		name     string
		certFile string
		keyFile  string
		wantErr  bool
	}{
		{name: "disabled"},
		{name: "enabled", certFile: "/tls/provider.crt", keyFile: "/tls/provider.key"},
		{name: "certificate only", certFile: "/tls/provider.crt", wantErr: true},
		{name: "key only", keyFile: "/tls/provider.key", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE", tc.certFile)
			t.Setenv("GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE", tc.keyFile)
			certFile, keyFile, err := providerTLSFilesFromEnvironment()
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "must be set together") {
					t.Fatalf("TLS file error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("TLS files: %v", err)
			}
			if certFile != tc.certFile || keyFile != tc.keyFile {
				t.Fatalf("TLS files = %q, %q", certFile, keyFile)
			}
		})
	}
}

func TestProviderTransportRequiresTLSOutsideLiteralLoopback(t *testing.T) {
	for _, tc := range []struct {
		name     string
		addr     string
		certFile string
		keyFile  string
		wantErr  bool
	}{
		{name: "IPv4 loopback plaintext", addr: "127.0.0.1:8090"},
		{name: "IPv6 loopback plaintext", addr: "[::1]:8090"},
		{name: "hostname plaintext", addr: "localhost:8090", wantErr: true},
		{name: "wildcard plaintext", addr: "0.0.0.0:8090", wantErr: true},
		{name: "empty host plaintext", addr: ":8090", wantErr: true},
		{name: "external plaintext", addr: "192.0.2.10:8090", wantErr: true},
		{name: "wildcard TLS", addr: "0.0.0.0:8090", certFile: "/tls/provider.crt", keyFile: "/tls/provider.key"},
		{name: "malformed", addr: "not-an-address", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateProviderTransport(tc.addr, tc.certFile, tc.keyFile)
			if tc.wantErr {
				if err == nil {
					t.Fatal("unsafe provider transport accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("safe provider transport rejected: %v", err)
			}
		})
	}
}

func TestServeProviderHTTPUsesConfiguredTransport(t *testing.T) {
	plain := &recordingProviderHTTPServer{}
	plainSocket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("plain listener: %v", err)
	}
	plainListener := &closeTrackingListener{Listener: plainSocket}
	t.Cleanup(func() { _ = plainListener.Close() })
	if err := serveProviderHTTP(plain, plainListener, "", ""); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("plain serve: %v", err)
	}
	if plain.plainCalls != 1 || plain.tlsCalls != 0 || plain.address != plainListener.Addr().String() || !plainListener.closed {
		t.Fatalf("plain calls = %+v", plain)
	}

	tlsServer := &recordingProviderHTTPServer{}
	tlsSocket, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("TLS listener: %v", err)
	}
	tlsListener := &closeTrackingListener{Listener: tlsSocket}
	t.Cleanup(func() { _ = tlsListener.Close() })
	if err := serveProviderHTTP(tlsServer, tlsListener, "/tls/provider.crt", "/tls/provider.key"); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("TLS serve: %v", err)
	}
	if tlsServer.plainCalls != 0 || tlsServer.tlsCalls != 1 || tlsServer.certFile != "/tls/provider.crt" || tlsServer.keyFile != "/tls/provider.key" || tlsServer.address != tlsListener.Addr().String() || !tlsListener.closed {
		t.Fatalf("TLS calls = %+v", tlsServer)
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
