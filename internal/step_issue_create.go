package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// issueCreateStep implements sdk.StepInstance.
// It creates a GitHub issue.
//
// Config:
//
//	owner:    "GoCodeAlone"
//	repo:     "workflow"
//	title:    "Bug report"
//	body:     "Description"
//	labels:   ["bug", "triage"]
//	assignees: ["username"]
//	token:    "${GITHUB_TOKEN}"
type issueCreateStep struct {
	name   string
	config issueCreateConfig
}

type issueCreateConfig struct {
	Owner     string   `yaml:"owner"`
	Repo      string   `yaml:"repo"`
	Title     string   `yaml:"title"`
	Body      string   `yaml:"body"`
	Labels    []string `yaml:"labels"`
	Assignees []string `yaml:"assignees"`
	Token     string   `yaml:"token"`
}

func newIssueCreateStep(name string, raw map[string]any) (*issueCreateStep, error) {
	var cfg issueCreateConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_issue_create %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_issue_create %q: config.repo is required", name)
	}
	cfg.Title, _ = raw["title"].(string)
	cfg.Body, _ = raw["body"].(string)
	if labels, ok := raw["labels"].([]any); ok {
		for _, l := range labels {
			if s, ok := l.(string); ok {
				cfg.Labels = append(cfg.Labels, s)
			}
		}
	}
	if assignees, ok := raw["assignees"].([]any); ok {
		for _, a := range assignees {
			if s, ok := a.(string); ok {
				cfg.Assignees = append(cfg.Assignees, s)
			}
		}
	}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &issueCreateStep{name: name, config: cfg}, nil
}

func (s *issueCreateStep) Execute(
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
	title := resolveField(s.config.Title, triggerData, stepOutputs, current)
	body := resolveField(s.config.Body, triggerData, stepOutputs, current)

	req := &github.IssueRequest{
		Title:     github.Ptr(title),
		Body:      github.Ptr(body),
		Labels:    &s.config.Labels,
		Assignees: &s.config.Assignees,
	}

	client := NewSDKClient(token)
	issue, _, err := client.GH.Issues.Create(ctx, owner, repo, req)
	if err != nil {
		return errorResult(fmt.Sprintf("create issue: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"number": issue.GetNumber(),
			"url":    issue.GetHTMLURL(),
			"id":     issue.GetID(),
			"state":  issue.GetState(),
		},
	}, nil
}
