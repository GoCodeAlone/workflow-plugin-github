package internal

import (
	"context"
	"fmt"
	"os"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// issueLabelStep implements sdk.StepInstance.
// It adds or removes labels on a GitHub issue or PR.
//
// Config:
//
//	owner:        "GoCodeAlone"
//	repo:         "workflow"
//	issue_number: 42
//	add:          ["bug", "priority-high"]
//	remove:       ["triage"]
//	token:        "${GITHUB_TOKEN}"
type issueLabelStep struct {
	name   string
	config issueLabelConfig
}

type issueLabelConfig struct {
	Owner       string   `yaml:"owner"`
	Repo        string   `yaml:"repo"`
	IssueNumber int      `yaml:"issue_number"`
	Add         []string `yaml:"add"`
	Remove      []string `yaml:"remove"`
	Token       string   `yaml:"token"`
}

func newIssueLabelStep(name string, raw map[string]any) (*issueLabelStep, error) {
	var cfg issueLabelConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_issue_label %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_issue_label %q: config.repo is required", name)
	}
	switch v := raw["issue_number"].(type) {
	case int:
		cfg.IssueNumber = v
	case int64:
		cfg.IssueNumber = int(v)
	case float64:
		cfg.IssueNumber = int(v)
	}
	if cfg.IssueNumber == 0 {
		return nil, fmt.Errorf("step.gh_issue_label %q: config.issue_number is required", name)
	}
	if add, ok := raw["add"].([]any); ok {
		for _, l := range add {
			if s, ok := l.(string); ok {
				cfg.Add = append(cfg.Add, s)
			}
		}
	}
	if remove, ok := raw["remove"].([]any); ok {
		for _, l := range remove {
			if s, ok := l.(string); ok {
				cfg.Remove = append(cfg.Remove, s)
			}
		}
	}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &issueLabelStep{name: name, config: cfg}, nil
}

func (s *issueLabelStep) Execute(
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

	client := NewSDKClient(token)

	var added, removed []string

	if len(s.config.Add) > 0 {
		labels, _, err := client.GH.Issues.AddLabelsToIssue(ctx, owner, repo, s.config.IssueNumber, s.config.Add)
		if err != nil {
			return errorResult(fmt.Sprintf("add labels: %v", err)), nil
		}
		for _, l := range labels {
			added = append(added, l.GetName())
		}
	}

	for _, label := range s.config.Remove {
		_, err := client.GH.Issues.RemoveLabelForIssue(ctx, owner, repo, s.config.IssueNumber, label)
		if err != nil {
			return errorResult(fmt.Sprintf("remove label %q: %v", label, err)), nil
		}
		removed = append(removed, label)
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"added":   added,
			"removed": removed,
		},
	}, nil
}
