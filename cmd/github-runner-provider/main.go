// Command github-runner-provider serves the GitHub-owned runner provider API
// used by workflow-compute without placing GitHub API credentials in compute.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(context.Background(), logger, os.Args[1:]); err != nil {
		logger.Error("github-runner-provider failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, args []string) error {
	addr := "127.0.0.1:8090"
	if len(args) > 0 {
		addr = args[0]
	}
	githubToken := os.Getenv("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN")
	if githubToken == "" {
		githubToken = os.Getenv("GITHUB_TOKEN")
	}
	if githubToken == "" {
		return fmt.Errorf("GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN or GITHUB_TOKEN is required")
	}
	providerToken := os.Getenv("GITHUB_RUNNER_PROVIDER_TOKEN")
	if providerToken == "" {
		return fmt.Errorf("GITHUB_RUNNER_PROVIDER_TOKEN is required")
	}
	handler, err := internal.NewGitHubRunnerProviderHTTPHandler("github-runner-provider", map[string]any{
		"token":          githubToken,
		"provider_token": providerToken,
		"api_base_url":   os.Getenv("GITHUB_API_BASE_URL"),
		"repositories":   strings.TrimSpace(os.Getenv("GITHUB_RUNNER_PROVIDER_REPOSITORIES")),
	})
	if err != nil {
		return err
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	logger.InfoContext(ctx, "starting github-runner-provider", "addr", addr)
	return server.ListenAndServe()
}
