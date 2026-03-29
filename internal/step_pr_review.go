package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// prReviewStep implements sdk.StepInstance.
// It submits or requests a review on a pull request.
//
// Config:
//
//	owner:     "GoCodeAlone"
//	repo:      "workflow"
//	pr_number: 123
//	event:     "APPROVE"   # APPROVE, REQUEST_CHANGES, COMMENT
//	body:      "LGTM"
//	token:     "${GITHUB_TOKEN}"
type prReviewStep struct {
	name   string
	config prReviewConfig
}

type prReviewConfig struct {
	Owner    string `yaml:"owner"`
	Repo     string `yaml:"repo"`
	PRNumber int    `yaml:"pr_number"`
	Event    string `yaml:"event"`
	Body     string `yaml:"body"`
	Token    string `yaml:"token"`
}

func newPRReviewStep(name string, raw map[string]any) (*prReviewStep, error) {
	var cfg prReviewConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_pr_review %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_pr_review %q: config.repo is required", name)
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
		return nil, fmt.Errorf("step.gh_pr_review %q: config.pr_number is required", name)
	}
	cfg.Event, _ = raw["event"].(string)
	if cfg.Event == "" {
		cfg.Event = "COMMENT"
	}
	cfg.Body, _ = raw["body"].(string)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &prReviewStep{name: name, config: cfg}, nil
}

func (s *prReviewStep) Execute(
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
	event := resolveField(s.config.Event, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	review, _, err := client.GH.PullRequests.CreateReview(ctx, owner, repo, s.config.PRNumber,
		&github.PullRequestReviewRequest{
			Body:  github.Ptr(body),
			Event: github.Ptr(event),
		})
	if err != nil {
		return errorResult(fmt.Sprintf("submit PR review: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"review_id": review.GetID(),
			"state":     review.GetState(),
			"url":       review.GetHTMLURL(),
		},
	}, nil
}
