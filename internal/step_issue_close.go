package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// issueCloseStep implements sdk.StepInstance.
// It closes a GitHub issue with an optional comment.
//
// Config:
//
//	owner:        "GoCodeAlone"
//	repo:         "workflow"
//	issue_number: 42
//	comment:      "Closing as fixed in v1.2.0"  # optional
//	token:        "${GITHUB_TOKEN}"
type issueCloseStep struct {
	name   string
	config issueCloseConfig
}

type issueCloseConfig struct {
	Owner       string `yaml:"owner"`
	Repo        string `yaml:"repo"`
	IssueNumber int    `yaml:"issue_number"`
	Comment     string `yaml:"comment"`
	Token       string `yaml:"token"`
}

func newIssueCloseStep(name string, raw map[string]any) (*issueCloseStep, error) {
	var cfg issueCloseConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_issue_close %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_issue_close %q: config.repo is required", name)
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
		return nil, fmt.Errorf("step.gh_issue_close %q: config.issue_number is required", name)
	}
	cfg.Comment, _ = raw["comment"].(string)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &issueCloseStep{name: name, config: cfg}, nil
}

func (s *issueCloseStep) Execute(
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

	// Add comment before closing if configured.
	if s.config.Comment != "" {
		comment := resolveField(s.config.Comment, triggerData, stepOutputs, current)
		_, _, err := client.GH.Issues.CreateComment(ctx, owner, repo, s.config.IssueNumber,
			&github.IssueComment{Body: github.Ptr(comment)})
		if err != nil {
			return errorResult(fmt.Sprintf("add close comment: %v", err)), nil
		}
	}

	state := "closed"
	issue, _, err := client.GH.Issues.Edit(ctx, owner, repo, s.config.IssueNumber,
		&github.IssueRequest{State: &state})
	if err != nil {
		return errorResult(fmt.Sprintf("close issue: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"number": issue.GetNumber(),
			"state":  issue.GetState(),
			"url":    issue.GetHTMLURL(),
		},
	}, nil
}
