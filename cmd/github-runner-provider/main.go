// Command github-runner-provider serves the GitHub-owned runner provider API
// used by workflow-compute without placing GitHub API credentials in compute.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

const providerShutdownTimeout = 10 * time.Second

type providerHTTPShutdowner interface {
	Shutdown(context.Context) error
	Close() error
}

type providerServiceStopper interface {
	Stop(context.Context) error
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), providerTerminationSignals()...)
	defer stop()
	if err := run(ctx, logger, os.Args[1:]); err != nil {
		logger.Error("github-runner-provider failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, args []string) error {
	addr := "127.0.0.1:8090"
	if len(args) > 0 {
		addr = args[0]
	}
	config, err := runnerProviderConfigFromEnvironment()
	if err != nil {
		return err
	}
	service, err := internal.NewGitHubRunnerProviderHTTPService("github-runner-provider", config)
	if err != nil {
		return err
	}
	server := newProviderHTTPServer(addr, service.Handler())
	logger.InfoContext(ctx, "starting github-runner-provider", "addr", addr)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.ListenAndServe() }()
	select {
	case serveErr := <-serveDone:
		return errors.Join(normalizeProviderServeError(serveErr), stopProvider(service, providerShutdownTimeout))
	case <-ctx.Done():
		shutdownErr := shutdownProvider(server, service, providerShutdownTimeout)
		serveErr := waitForProviderServeDone(serveDone, providerShutdownTimeout)
		return errors.Join(shutdownErr, serveErr)
	}
}

func waitForProviderServeDone(serveDone <-chan error, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case serveErr := <-serveDone:
		return normalizeProviderServeError(serveErr)
	case <-timer.C:
		return fmt.Errorf("timed out waiting for provider HTTP server to stop after %s", timeout)
	}
}

func newProviderHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      35 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func shutdownProvider(server providerHTTPShutdowner, service providerServiceStopper, timeout time.Duration) error {
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), timeout)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancelShutdown()
	var closeErr error
	if shutdownErr != nil {
		closeErr = server.Close()
	}
	return errors.Join(shutdownErr, closeErr, stopProvider(service, timeout))
}

func stopProvider(service providerServiceStopper, timeout time.Duration) error {
	stopCtx, cancelStop := context.WithTimeout(context.Background(), timeout)
	defer cancelStop()
	return service.Stop(stopCtx)
}

func normalizeProviderServeError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func runnerProviderConfigFromEnvironment() (map[string]any, error) {
	githubToken := strings.TrimSpace(os.Getenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN"))
	if githubToken == "" {
		githubToken = strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	}
	if githubToken == "" {
		return nil, fmt.Errorf("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN or GITHUB_TOKEN is required")
	}
	providerToken := strings.TrimSpace(os.Getenv("GITHUB_RUNNER_PROVIDER_TOKEN"))
	if providerToken == "" {
		return nil, fmt.Errorf("GITHUB_RUNNER_PROVIDER_TOKEN is required")
	}
	config := map[string]any{
		"token":          githubToken,
		"provider_token": providerToken,
	}
	for key, environmentName := range map[string]string{
		"repositories":  "GITHUB_RUNNER_PROVIDER_REPOSITORIES",
		"organizations": "GITHUB_RUNNER_PROVIDER_ORGANIZATIONS",
		"runner_groups": "GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS",
	} {
		if values := commaSeparatedValues(os.Getenv(environmentName)); len(values) > 0 {
			config[key] = values
		}
	}
	if apiBaseURL := strings.TrimSpace(os.Getenv("GITHUB_API_BASE_URL")); apiBaseURL != "" {
		config["api_base_url"] = apiBaseURL
	}
	stateDir := strings.TrimSpace(os.Getenv("GITHUB_RUNNER_PROVIDER_STATE_DIR"))
	if stateDir == "" {
		return nil, fmt.Errorf("GITHUB_RUNNER_PROVIDER_STATE_DIR is required")
	}
	config["state_dir"] = stateDir
	return config, nil
}

func commaSeparatedValues(value string) []string {
	var values []string
	for item := range strings.SplitSeq(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			values = append(values, item)
		}
	}
	return values
}
