package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// prCommentStep implements sdk.StepInstance.
// It adds a comment to a pull request.
//
// Config:
//
//	owner:     "GoCodeAlone"
//	repo:      "workflow"
//	pr_number: 123
//	body:      "LGTM!"
//	token:     "${GITHUB_TOKEN}"
type prCommentStep struct {
	name   string
	config prCommentConfig
}

type prCommentConfig struct {
	Owner    string `yaml:"owner"`
	Repo     string `yaml:"repo"`
	PRNumber int    `yaml:"pr_number"`
	Body     string `yaml:"body"`
	Token    string `yaml:"token"`
}

func newPRCommentStep(name string, raw map[string]any) (*prCommentStep, error) {
	var cfg prCommentConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_pr_comment %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_pr_comment %q: config.repo is required", name)
	}
	switch v := raw["pr_number"].(type) {
	case int:
		cfg.PRNumber = v
	case int64:
		cfg.PRNumber = int(v)
	case float64:
		cfg.PRNumber = int(v)
	}
	if cfg.PRNumber == 0 {
		return nil, fmt.Errorf("step.gh_pr_comment %q: config.pr_number is required", name)
	}
	cfg.Body, _ = raw["body"].(string)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &prCommentStep{name: name, config: cfg}, nil
}

func (s *prCommentStep) Execute(
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
	body := resolveField(s.config.Body, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	comment, _, err := client.GH.Issues.CreateComment(ctx, owner, repo, s.config.PRNumber,
		&github.IssueComment{Body: github.Ptr(body)})
	if err != nil {
		return errorResult(fmt.Sprintf("add PR comment: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"comment_id": comment.GetID(),
			"url":        comment.GetHTMLURL(),
		},
	}, nil
}
