package internal

import (
	"context"
	"fmt"
	"os"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// createCheckStep implements sdk.StepInstance.
// It creates a GitHub Check Run (status check) on a specific commit.
//
// Config:
//
//	owner:      "GoCodeAlone"
//	repo:       "workflow"
//	sha:        "{{.commit}}"
//	name:       "workflow-ci"
//	status:     "completed"     # queued, in_progress, completed
//	conclusion: "success"       # success, failure, neutral, cancelled, skipped
//	title:      "CI Pipeline"
//	summary:    "All tests passed"
//	token:      "${GITHUB_TOKEN}"
type createCheckStep struct {
	name     string
	config   createCheckConfig
	ghClient GitHubClient
}

// createCheckConfig holds the parsed configuration for step.gh_create_check.
type createCheckConfig struct {
	Owner      string `yaml:"owner"`
	Repo       string `yaml:"repo"`
	SHA        string `yaml:"sha"`
	Name       string `yaml:"name"`
	Status     string `yaml:"status"`
	Conclusion string `yaml:"conclusion"`
	Title      string `yaml:"title"`
	Summary    string `yaml:"summary"`
	Token      string `yaml:"token"`
}

// validStatuses lists the valid values for the status field.
var validStatuses = map[string]bool{
	"queued":      true,
	"in_progress": true,
	"completed":   true,
}

// validConclusions lists the valid values for the conclusion field.
var validConclusions = map[string]bool{
	"success":         true,
	"failure":         true,
	"neutral":         true,
	"cancelled":       true,
	"skipped":         true,
	"timed_out":       true,
	"action_required": true,
}

// newCreateCheckStep parses config and returns a createCheckStep.
func newCreateCheckStep(name string, config map[string]any, client GitHubClient) (*createCheckStep, error) {
	cfg, err := parseCreateCheckConfig(config)
	if err != nil {
		return nil, fmt.Errorf("step.gh_create_check %q: %w", name, err)
	}
	if client == nil {
		client = newHTTPGitHubClient()
	}
	return &createCheckStep{
		name:     name,
		config:   cfg,
		ghClient: client,
	}, nil
}

// parseCreateCheckConfig converts a raw config map to createCheckConfig.
func parseCreateCheckConfig(raw map[string]any) (createCheckConfig, error) {
	var cfg createCheckConfig

	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return cfg, fmt.Errorf("config.owner is required")
	}

	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return cfg, fmt.Errorf("config.repo is required")
	}

	cfg.SHA, _ = raw["sha"].(string)
	// sha may be a dynamic template reference (e.g. {{.commit}}) resolved at Execute time.
	if cfg.SHA == "" {
		return cfg, fmt.Errorf("config.sha is required")
	}

	cfg.Name, _ = raw["name"].(string)
	if cfg.Name == "" {
		return cfg, fmt.Errorf("config.name is required")
	}

	cfg.Status, _ = raw["status"].(string)
	if cfg.Status == "" {
		cfg.Status = "queued"
	}
	if !validStatuses[cfg.Status] {
		return cfg, fmt.Errorf("config.status %q is invalid; must be one of: queued, in_progress, completed", cfg.Status)
	}

	cfg.Conclusion, _ = raw["conclusion"].(string)
	if cfg.Status == "completed" && cfg.Conclusion == "" {
		return cfg, fmt.Errorf("config.conclusion is required when status=completed")
	}
	if cfg.Conclusion != "" && !validConclusions[cfg.Conclusion] {
		return cfg, fmt.Errorf("config.conclusion %q is invalid", cfg.Conclusion)
	}

	cfg.Title, _ = raw["title"].(string)
	cfg.Summary, _ = raw["summary"].(string)

	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)

	return cfg, nil
}

// Execute creates the GitHub Check Run.
// triggerData, stepOutputs, and current are used to resolve dynamic field
// references (e.g. {{.commit}}, {{.steps.prev.sha}}) in owner, repo, and sha.
func (s *createCheckStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	token := s.config.Token
	if token == "" {
		return errorResult("GITHUB_TOKEN is not configured"), nil
	}

	owner := resolveField(s.config.Owner, triggerData, stepOutputs, current)
	repo := resolveField(s.config.Repo, triggerData, stepOutputs, current)
	sha := resolveField(s.config.SHA, triggerData, stepOutputs, current)

	req := &CreateCheckRunRequest{
		Name:       s.config.Name,
		HeadSHA:    sha,
		Status:     s.config.Status,
		Conclusion: s.config.Conclusion,
	}

	if s.config.Title != "" || s.config.Summary != "" {
		req.Output = &CheckRunOutput{
			Title:   s.config.Title,
			Summary: s.config.Summary,
		}
	}

	check, err := s.ghClient.CreateCheckRun(ctx, owner, repo, req, token)
	if err != nil {
		return errorResult(fmt.Sprintf("failed to create check run: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"check_run_id": check.ID,
			"status":       check.Status,
			"url":          check.HTMLURL,
		},
	}, nil
}
