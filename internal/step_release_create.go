package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// releaseCreateStep implements sdk.StepInstance.
// It creates a GitHub release.
//
// Config:
//
//	owner:      "GoCodeAlone"
//	repo:       "workflow"
//	tag:        "v1.2.0"
//	name:       "Release v1.2.0"
//	body:       "Changelog..."
//	draft:      false
//	prerelease: false
//	token:      "${GITHUB_TOKEN}"
type releaseCreateStep struct {
	name   string
	config releaseCreateConfig
}

type releaseCreateConfig struct {
	Owner      string `yaml:"owner"`
	Repo       string `yaml:"repo"`
	Tag        string `yaml:"tag"`
	Name       string `yaml:"name"`
	Body       string `yaml:"body"`
	Draft      bool   `yaml:"draft"`
	Prerelease bool   `yaml:"prerelease"`
	Token      string `yaml:"token"`
}

func newReleaseCreateStep(name string, raw map[string]any) (*releaseCreateStep, error) {
	var cfg releaseCreateConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_release_create %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_release_create %q: config.repo is required", name)
	}
	cfg.Tag, _ = raw["tag"].(string)
	if cfg.Tag == "" {
		return nil, fmt.Errorf("step.gh_release_create %q: config.tag is required", name)
	}
	cfg.Name, _ = raw["name"].(string)
	cfg.Body, _ = raw["body"].(string)
	cfg.Draft, _ = raw["draft"].(bool)
	cfg.Prerelease, _ = raw["prerelease"].(bool)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &releaseCreateStep{name: name, config: cfg}, nil
}

func (s *releaseCreateStep) Execute(
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
	tag := resolveField(s.config.Tag, triggerData, stepOutputs, current)
	relName := resolveField(s.config.Name, triggerData, stepOutputs, current)
	body := resolveField(s.config.Body, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	rel, _, err := client.GH.Repositories.CreateRelease(ctx, owner, repo, &github.RepositoryRelease{
		TagName:    github.Ptr(tag),
		Name:       github.Ptr(relName),
		Body:       github.Ptr(body),
		Draft:      github.Ptr(s.config.Draft),
		Prerelease: github.Ptr(s.config.Prerelease),
	})
	if err != nil {
		return errorResult(fmt.Sprintf("create release: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"release_id":  rel.GetID(),
			"url":         rel.GetHTMLURL(),
			"upload_url":  rel.GetUploadURL(),
			"tag":         rel.GetTagName(),
			"draft":       rel.GetDraft(),
			"prerelease":  rel.GetPrerelease(),
		},
	}, nil
}
