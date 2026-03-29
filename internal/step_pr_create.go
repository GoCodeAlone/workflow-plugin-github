package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// prCreateStep implements sdk.StepInstance.
// It creates a pull request in a GitHub repository.
//
// Config:
//
//	owner: "GoCodeAlone"
//	repo:  "workflow"
//	title: "My PR"
//	body:  "Description"
//	head:  "feature/my-branch"   # source branch
//	base:  "main"                 # target branch
//	draft: false
//	token: "${GITHUB_TOKEN}"
type prCreateStep struct {
	name   string
	config prCreateConfig
}

type prCreateConfig struct {
	Owner string `yaml:"owner"`
	Repo  string `yaml:"repo"`
	Title string `yaml:"title"`
	Body  string `yaml:"body"`
	Head  string `yaml:"head"`
	Base  string `yaml:"base"`
	Draft bool   `yaml:"draft"`
	Token string `yaml:"token"`
}

func newPRCreateStep(name string, raw map[string]any) (*prCreateStep, error) {
	var cfg prCreateConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_pr_create %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_pr_create %q: config.repo is required", name)
	}
	cfg.Title, _ = raw["title"].(string)
	cfg.Body, _ = raw["body"].(string)
	cfg.Head, _ = raw["head"].(string)
	if cfg.Head == "" {
		return nil, fmt.Errorf("step.gh_pr_create %q: config.head is required", name)
	}
	cfg.Base, _ = raw["base"].(string)
	if cfg.Base == "" {
		cfg.Base = "main"
	}
	cfg.Draft, _ = raw["draft"].(bool)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &prCreateStep{name: name, config: cfg}, nil
}

func (s *prCreateStep) Execute(
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
	head := resolveField(s.config.Head, triggerData, stepOutputs, current)
	base := resolveField(s.config.Base, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	pr, _, err := client.GH.PullRequests.Create(ctx, owner, repo, &github.NewPullRequest{
		Title: github.Ptr(title),
		Body:  github.Ptr(body),
		Head:  github.Ptr(head),
		Base:  github.Ptr(base),
		Draft: github.Ptr(s.config.Draft),
	})
	if err != nil {
		return errorResult(fmt.Sprintf("create PR: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"number": pr.GetNumber(),
			"url":    pr.GetHTMLURL(),
			"id":     pr.GetID(),
			"state":  pr.GetState(),
		},
	}, nil
}
