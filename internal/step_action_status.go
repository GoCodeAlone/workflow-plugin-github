package internal

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// actionStatusStep implements sdk.StepInstance.
// It checks (and optionally polls) the status of a GitHub Actions workflow run.
//
// Config:
//
//	owner:         "GoCodeAlone"
//	repo:          "workflow"
//	run_id:        "{{.steps.trigger.run_id}}"
//	token:         "${GITHUB_TOKEN}"
//	wait:          true          # poll until complete (default: false)
//	poll_interval: "10s"
//	timeout:       "30m"
type actionStatusStep struct {
	name     string
	config   actionStatusConfig
	ghClient GitHubClient
}

// actionStatusConfig holds the parsed configuration for step.gh_action_status.
type actionStatusConfig struct {
	Owner        string        `yaml:"owner"`
	Repo         string        `yaml:"repo"`
	RunID        int64         `yaml:"run_id"`
	Token        string        `yaml:"token"`
	Wait         bool          `yaml:"wait"`
	PollInterval time.Duration `yaml:"poll_interval"`
	Timeout      time.Duration `yaml:"timeout"`
}

// newActionStatusStep parses config and returns an actionStatusStep.
func newActionStatusStep(name string, config map[string]any, client GitHubClient) (*actionStatusStep, error) {
	cfg, err := parseActionStatusConfig(config)
	if err != nil {
		return nil, fmt.Errorf("step.gh_action_status %q: %w", name, err)
	}
	if client == nil {
		client = newHTTPGitHubClient()
	}
	return &actionStatusStep{
		name:     name,
		config:   cfg,
		ghClient: client,
	}, nil
}

// parseActionStatusConfig converts a raw config map to actionStatusConfig.
func parseActionStatusConfig(raw map[string]any) (actionStatusConfig, error) {
	var cfg actionStatusConfig

	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return cfg, fmt.Errorf("config.owner is required")
	}

	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return cfg, fmt.Errorf("config.repo is required")
	}

	// run_id can be provided as int, int64, float64, or string.
	switch v := raw["run_id"].(type) {
	case int:
		cfg.RunID = int64(v)
	case int64:
		cfg.RunID = v
	case float64:
		cfg.RunID = int64(v)
	case string:
		if v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return cfg, fmt.Errorf("config.run_id is not a valid integer: %w", err)
			}
			cfg.RunID = n
		}
	}
	if cfg.RunID == 0 {
		return cfg, fmt.Errorf("config.run_id is required")
	}

	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)

	cfg.Wait, _ = raw["wait"].(bool)

	pollStr, _ := raw["poll_interval"].(string)
	if pollStr == "" {
		pollStr = "10s"
	}
	var err error
	cfg.PollInterval, err = time.ParseDuration(pollStr)
	if err != nil {
		return cfg, fmt.Errorf("config.poll_interval is invalid: %w", err)
	}

	timeoutStr, _ := raw["timeout"].(string)
	if timeoutStr == "" {
		timeoutStr = "30m"
	}
	cfg.Timeout, err = time.ParseDuration(timeoutStr)
	if err != nil {
		return cfg, fmt.Errorf("config.timeout is invalid: %w", err)
	}

	return cfg, nil
}

// Execute checks the status of the configured workflow run.
// When wait=true it polls until the run completes or the timeout elapses.
func (s *actionStatusStep) Execute(
	ctx context.Context,
	_ map[string]any,
	_ map[string]map[string]any,
	_ map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	token := s.config.Token
	if token == "" {
		return errorResult("GITHUB_TOKEN is not configured"), nil
	}

	if !s.config.Wait {
		return s.fetchStatus(ctx, token)
	}

	// Poll with timeout.
	deadline := time.Now().Add(s.config.Timeout)
	for {
		result, err := s.fetchStatus(ctx, token)
		if err != nil {
			return nil, err
		}

		status, _ := result.Output["status"].(string)
		if isTerminalStatus(status) {
			return result, nil
		}

		if time.Now().After(deadline) {
			return errorResult(fmt.Sprintf("timeout waiting for workflow run %d after %s", s.config.RunID, s.config.Timeout)), nil
		}

		select {
		case <-ctx.Done():
			return errorResult("context cancelled while waiting for workflow run"), nil
		case <-time.After(s.config.PollInterval):
		}
	}
}

// fetchStatus retrieves the current state of the workflow run from the GitHub API.
func (s *actionStatusStep) fetchStatus(ctx context.Context, token string) (*sdk.StepResult, error) {
	run, err := s.ghClient.GetWorkflowRun(ctx, s.config.Owner, s.config.Repo, s.config.RunID, token)
	if err != nil {
		return errorResult(fmt.Sprintf("failed to get workflow run: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"run_id":     run.ID,
			"status":     run.Status,
			"conclusion": run.Conclusion,
			"url":        run.HTMLURL,
		},
	}, nil
}

// isTerminalStatus reports whether a workflow run status is in a terminal state.
func isTerminalStatus(status string) bool {
	return status == "completed"
}
