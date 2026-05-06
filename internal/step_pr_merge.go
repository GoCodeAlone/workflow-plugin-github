package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// prMergeStep implements sdk.StepInstance.
// It merges a pull request in a GitHub repository.
//
// Config:
//
//	owner:        "GoCodeAlone"
//	repo:         "workflow"
//	pr_number:    123
//	commit_title: "Merge PR"
//	method:       "merge"   # merge, squash, rebase
//	token:        "${GITHUB_TOKEN}"
type prMergeStep struct {
	name   string
	config prMergeConfig
}

type prMergeConfig struct {
	Owner       string `yaml:"owner"`
	Repo        string `yaml:"repo"`
	PRNumber    int    `yaml:"pr_number"`
	CommitTitle string `yaml:"commit_title"`
	Method      string `yaml:"method"`
	Token       string `yaml:"token"`
}

func newPRMergeStep(name string, raw map[string]any) (*prMergeStep, error) {
	var cfg prMergeConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_pr_merge %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_pr_merge %q: config.repo is required", name)
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
		return nil, fmt.Errorf("step.gh_pr_merge %q: config.pr_number is required", name)
	}
	cfg.CommitTitle, _ = raw["commit_title"].(string)
	cfg.Method, _ = raw["method"].(string)
	if cfg.Method == "" {
		cfg.Method = "merge"
	}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &prMergeStep{name: name, config: cfg}, nil
}

func (s *prMergeStep) Execute(
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
	commitTitle := resolveField(s.config.CommitTitle, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	method := resolveField(s.config.Method, triggerData, stepOutputs, current)
	result, _, err := client.GH.PullRequests.Merge(ctx, owner, repo, s.config.PRNumber,
		commitTitle, &github.PullRequestOptions{MergeMethod: method})
	if err != nil {
		return errorResult(fmt.Sprintf("merge PR: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"merged":  result.GetMerged(),
			"message": result.GetMessage(),
			"sha":     result.GetSHA(),
		},
	}, nil
}
