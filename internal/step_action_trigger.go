package internal

import (
	"context"
	"fmt"
	"os"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// actionTriggerStep implements sdk.StepInstance.
// It triggers a GitHub Actions workflow run via the workflow_dispatch API.
//
// Config:
//
//	owner:    "GoCodeAlone"
//	repo:     "workflow"
//	workflow: "ci.yml"          # workflow filename or ID
//	ref:      "main"            # branch/tag
//	inputs:                     # optional workflow_dispatch inputs (map[string]string)
//	  environment: "staging"
//	token: "${GITHUB_TOKEN}"
type actionTriggerStep struct {
	name     string
	config   actionTriggerConfig
	ghClient GitHubClient
}

// actionTriggerConfig holds the parsed configuration for step.gh_action_trigger.
type actionTriggerConfig struct {
	Owner    string            `yaml:"owner"`
	Repo     string            `yaml:"repo"`
	Workflow string            `yaml:"workflow"`
	Ref      string            `yaml:"ref"`
	Inputs   map[string]string `yaml:"inputs"`
	Token    string            `yaml:"token"`
}

// newActionTriggerStep parses config and returns an actionTriggerStep.
func newActionTriggerStep(name string, config map[string]any, client GitHubClient) (*actionTriggerStep, error) {
	cfg, err := parseActionTriggerConfig(config)
	if err != nil {
		return nil, fmt.Errorf("step.gh_action_trigger %q: %w", name, err)
	}
	if client == nil {
		client = newHTTPGitHubClient()
	}
	return &actionTriggerStep{
		name:     name,
		config:   cfg,
		ghClient: client,
	}, nil
}

// parseActionTriggerConfig converts a raw config map to actionTriggerConfig.
func parseActionTriggerConfig(raw map[string]any) (actionTriggerConfig, error) {
	var cfg actionTriggerConfig

	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return cfg, fmt.Errorf("config.owner is required")
	}

	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return cfg, fmt.Errorf("config.repo is required")
	}

	cfg.Workflow, _ = raw["workflow"].(string)
	if cfg.Workflow == "" {
		return cfg, fmt.Errorf("config.workflow is required")
	}

	cfg.Ref, _ = raw["ref"].(string)
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}

	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)

	if inputs, ok := raw["inputs"].(map[string]any); ok {
		cfg.Inputs = make(map[string]string, len(inputs))
		for k, v := range inputs {
			if s, ok := v.(string); ok {
				cfg.Inputs[k] = s
			}
		}
	}

	return cfg, nil
}

// Execute triggers the configured GitHub Actions workflow.
// It returns the trigger confirmation and stops on error.
func (s *actionTriggerStep) Execute(
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

	err := s.ghClient.TriggerWorkflow(
		ctx,
		s.config.Owner,
		s.config.Repo,
		s.config.Workflow,
		s.config.Ref,
		s.config.Inputs,
		token,
	)
	if err != nil {
		return errorResult(fmt.Sprintf("failed to trigger workflow: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"triggered": true,
			"owner":     s.config.Owner,
			"repo":      s.config.Repo,
			"workflow":  s.config.Workflow,
			"ref":       s.config.Ref,
		},
	}, nil
}

// errorResult returns a StepResult that stops the pipeline with an error message.
func errorResult(msg string) *sdk.StepResult {
	return &sdk.StepResult{
		StopPipeline: true,
		Output: map[string]any{
			"response_status":  500,
			"response_body":    fmt.Sprintf(`{"error":%q}`, msg),
			"response_headers": map[string]any{"Content-Type": "application/json"},
			"error":            msg,
		},
	}
}
