package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

func TestProviderBinaryHasFallbackCertificateRoots(t *testing.T) {
	const helperEnvironment = "GITHUB_PROVIDER_TEST_FALLBACK_ROOTS"
	if os.Getenv(helperEnvironment) == "1" {
		pool, err := x509.SystemCertPool()
		if err != nil {
			panic(err)
		}
		if len(pool.Subjects()) == 0 {
			panic("provider binary has no fallback certificate roots")
		}
		return
	}

	command := exec.Command(os.Args[0], "-test.run=^TestProviderBinaryHasFallbackCertificateRoots$")
	command.Env = append(environmentWithout(os.Environ(), helperEnvironment, "GODEBUG", "SSL_CERT_FILE", "SSL_CERT_DIR"),
		helperEnvironment+"=1",
		"GODEBUG=x509usefallbackroots=1",
		"SSL_CERT_FILE=/nonexistent/provider-ca-bundle",
		"SSL_CERT_DIR=/nonexistent/provider-ca-directory",
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("provider fallback roots subprocess: %v\n%s", err, output)
	}
}

func environmentWithout(environment []string, keys ...string) []string {
	blocked := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		blocked[key] = struct{}{}
	}
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if _, exists := blocked[key]; !exists {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

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

func TestProviderCommandVersionDoesNotRequireServiceCredentials(t *testing.T) {
	t.Setenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", "")
	var stdout bytes.Buffer
	handled, err := dispatchProviderCommand(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{"version"}, &stdout)
	if err != nil {
		t.Fatalf("version: %v", err)
	}
	if !handled {
		t.Fatal("version was treated as a legacy listen address")
	}
	if got, want := strings.TrimSpace(stdout.String()), internal.Version; got != want || got == "" {
		t.Fatalf("version output = %q want %q", got, want)
	}
}

func TestProviderCommandRejectsUnknownSubcommand(t *testing.T) {
	var stdout bytes.Buffer
	handled, err := dispatchProviderCommand(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{"unknown-command"}, &stdout)
	if !handled || err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unknown command handled=%t err=%v", handled, err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unknown command wrote stdout: %q", stdout.String())
	}
}

func TestProviderCommandPreservesLegacyListenAddress(t *testing.T) {
	var stdout bytes.Buffer
	handled, err := dispatchProviderCommand(t.Context(), slog.New(slog.NewTextHandler(io.Discard, nil)), []string{"127.0.0.1:0"}, &stdout)
	if handled || err != nil {
		t.Fatalf("legacy address handled=%t err=%v", handled, err)
	}
}

func TestProviderProbeAuthenticatesAndEmitsRedactedSemanticEvidence(t *testing.T) {
	const providerToken = "provider-secret-token"
	const githubToken = "github-secret-token"
	var readyAuth string
	var preflightAuth string
	var request providerProbePreflightRequest
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/readyz":
			readyAuth = r.Header.Get("Authorization")
			_ = json.NewEncoder(w).Encode(providerProbeReadyResponse{Status: "ok"})
		case "/v1/actions/orgs/GoCodeAlone/runners/preflight":
			preflightAuth = r.Header.Get("Authorization")
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				t.Fatalf("decode preflight request: %v", err)
			}
			_ = json.NewEncoder(w).Encode(internal.GitHubRunnerProviderPreflight{
				Organization:         "GoCodeAlone",
				RunnerGroup:          "ephemeral",
				RunnerGroupID:        41,
				Ref:                  strings.Repeat("a", 40),
				ResolvedWorkflowPath: ".github/workflows/dogfood-provider-target.yml",
				ResolvedRefSHA:       strings.Repeat("a", 40),
				LabelsObserved:       5,
				RunnerCountChecked:   3,
				ActionsEnabled:       true,
				SelfHostedAllowed:    true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", providerToken)
	t.Setenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN", githubToken)
	var stdout bytes.Buffer
	err := runProviderProbe(t.Context(), []string{
		"-url", server.URL,
		"-ca-file", caFile,
		"-organization", "GoCodeAlone",
		"-repository", "GoCodeAlone/workflow-compute",
		"-workflow", "dogfood-provider-target.yml",
		"-ref", strings.Repeat("a", 40),
		"-runner-name", "wfc-stg-ghp-linux-probe",
		"-runner-group", "ephemeral",
		"-label", "self-hosted",
		"-label", "linux",
		"-label", "wfc-ghp-stg",
	}, &stdout)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if readyAuth != "Bearer "+providerToken || preflightAuth != "Bearer "+providerToken {
		t.Fatalf("probe auth ready=%q preflight=%q", readyAuth, preflightAuth)
	}
	if request.Repository != "GoCodeAlone/workflow-compute" || request.RunnerName != "wfc-stg-ghp-linux-probe" || len(request.Labels) != 3 {
		t.Fatalf("preflight request = %+v", request)
	}
	var result providerProbeResult
	decoder := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("decode probe output: %v output=%s", err, stdout.String())
	}
	if result.Status != "passed" || !result.Ready || result.Organization != "GoCodeAlone" || result.RunnerGroupID != 41 || result.ResolvedRefSHA != strings.Repeat("a", 40) || result.ObservedAt.IsZero() {
		t.Fatalf("probe result = %+v", result)
	}
	if strings.Contains(stdout.String(), providerToken) || strings.Contains(stdout.String(), githubToken) {
		t.Fatalf("probe output leaked credential: %s", stdout.String())
	}
}

func TestProviderProbeFailsClosedOnInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		args  []string
		token string
		want  string
	}{
		{name: "missing token", args: []string{"-url", "https://provider.test", "-ca-file", "/ca.pem"}, want: "GITHUB_RUNNER_PROVIDER_TOKEN"},
		{name: "plaintext URL", args: []string{"-url", "http://provider.test", "-ca-file", "/ca.pem"}, token: "provider-token", want: "HTTPS"},
		{name: "missing CA", args: []string{"-url", "https://provider.test"}, token: "provider-token", want: "ca-file"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", tc.token)
			var stdout bytes.Buffer
			err := runProviderProbe(t.Context(), tc.args, &stdout)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("probe err = %v want %q", err, tc.want)
			}
			if stdout.Len() != 0 {
				t.Fatalf("failed probe wrote stdout: %q", stdout.String())
			}
		})
	}
}

func TestProviderProbeRejectsUnknownResponseFieldsAndDoesNotEchoErrorBody(t *testing.T) {
	const providerToken = "provider-secret-token"
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{
			name: "unknown readiness field",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, `{"status":"ok","unexpected":true}`)
			},
			want: "invalid JSON",
		},
		{
			name: "secret upstream error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "upstream rejected "+providerToken, http.StatusBadGateway)
			},
			want: "HTTP status 502",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewTLSServer(tc.handler)
			defer server.Close()
			caFile := writeProviderProbeTestCA(t, server)
			t.Setenv("GITHUB_RUNNER_PROVIDER_TOKEN", providerToken)
			var stdout bytes.Buffer
			err := runProviderProbe(t.Context(), validProviderProbeTestArgs(server.URL, caFile), &stdout)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("probe err = %v want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), providerToken) || stdout.Len() != 0 {
				t.Fatalf("failed probe leaked output: err=%v stdout=%q", err, stdout.String())
			}
		})
	}
}

func writeProviderProbeTestCA(t *testing.T, server *httptest.Server) string {
	t.Helper()
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	if err := os.WriteFile(caFile, caPEM, 0o600); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	return caFile
}

func validProviderProbeTestArgs(providerURL, caFile string) []string {
	return []string{
		"-url", providerURL,
		"-ca-file", caFile,
		"-organization", "GoCodeAlone",
		"-repository", "GoCodeAlone/workflow-compute",
		"-workflow", "dogfood-provider-target.yml",
		"-ref", strings.Repeat("a", 40),
		"-runner-name", "wfc-stg-ghp-linux-probe",
		"-runner-group", "ephemeral",
		"-label", "self-hosted",
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
