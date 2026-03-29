package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// repoDispatchStep implements sdk.StepInstance.
// It sends a repository_dispatch event to trigger external workflows.
//
// Config:
//
//	owner:      "GoCodeAlone"
//	repo:       "workflow"
//	event_type: "deploy"
//	payload:    {environment: "staging"}
//	token:      "${GITHUB_TOKEN}"
type repoDispatchStep struct {
	name   string
	config repoDispatchConfig
}

type repoDispatchConfig struct {
	Owner     string         `yaml:"owner"`
	Repo      string         `yaml:"repo"`
	EventType string         `yaml:"event_type"`
	Payload   map[string]any `yaml:"payload"`
	Token     string         `yaml:"token"`
}

func newRepoDispatchStep(name string, raw map[string]any) (*repoDispatchStep, error) {
	var cfg repoDispatchConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_repo_dispatch %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_repo_dispatch %q: config.repo is required", name)
	}
	cfg.EventType, _ = raw["event_type"].(string)
	if cfg.EventType == "" {
		return nil, fmt.Errorf("step.gh_repo_dispatch %q: config.event_type is required", name)
	}
	cfg.Payload, _ = raw["payload"].(map[string]any)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &repoDispatchStep{name: name, config: cfg}, nil
}

func (s *repoDispatchStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	token := s.config.Token
	if token == "" {
		return errorResult("GITHUB_TOKEN is not configured"), nil
	}
	owner := resolveField(s.config.Owner, triggerData, stepOutputs, current)
	repo := resolveField(s.config.Repo, triggerData, stepOutputs, current)
	eventType := resolveField(s.config.EventType, triggerData, stepOutputs, current)

	opts := github.DispatchRequestOptions{
		EventType: eventType,
	}
	if len(s.config.Payload) > 0 {
		data, err := json.Marshal(s.config.Payload)
		if err != nil {
			return errorResult(fmt.Sprintf("marshal payload: %v", err)), nil
		}
		raw := json.RawMessage(data)
		opts.ClientPayload = &raw
	}

	client := NewSDKClient(token)
	_, _, err := client.GH.Repositories.Dispatch(ctx, owner, repo, opts)
	if err != nil {
		return errorResult(fmt.Sprintf("repo dispatch: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"dispatched": true,
			"event_type": eventType,
			"owner":      owner,
			"repo":       repo,
		},
	}, nil
}
