package internal

import (
	"context"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// deploymentCreateStep implements sdk.StepInstance.
// It creates a GitHub deployment.
//
// Config:
//
//	owner:       "GoCodeAlone"
//	repo:        "workflow"
//	ref:         "main"
//	environment: "production"
//	description: "Deploy v1.2.0"
//	auto_merge:  false
//	token:       "${GITHUB_TOKEN}"
type deploymentCreateStep struct {
	name   string
	config deploymentCreateConfig
}

type deploymentCreateConfig struct {
	Owner       string `yaml:"owner"`
	Repo        string `yaml:"repo"`
	Ref         string `yaml:"ref"`
	Environment string `yaml:"environment"`
	Description string `yaml:"description"`
	AutoMerge   bool   `yaml:"auto_merge"`
	Token       string `yaml:"token"`
}

func newDeploymentCreateStep(name string, raw map[string]any) (*deploymentCreateStep, error) {
	var cfg deploymentCreateConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_deployment_create %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_deployment_create %q: config.repo is required", name)
	}
	cfg.Ref, _ = raw["ref"].(string)
	if cfg.Ref == "" {
		cfg.Ref = "main"
	}
	cfg.Environment, _ = raw["environment"].(string)
	if cfg.Environment == "" {
		cfg.Environment = "production"
	}
	cfg.Description, _ = raw["description"].(string)
	cfg.AutoMerge, _ = raw["auto_merge"].(bool)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &deploymentCreateStep{name: name, config: cfg}, nil
}

func (s *deploymentCreateStep) Execute(
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
	ref := resolveField(s.config.Ref, triggerData, stepOutputs, current)
	env := resolveField(s.config.Environment, triggerData, stepOutputs, current)
	desc := resolveField(s.config.Description, triggerData, stepOutputs, current)

	client := NewSDKClient(token)
	dep, _, err := client.GH.Repositories.CreateDeployment(ctx, owner, repo, &github.DeploymentRequest{
		Ref:              github.Ptr(ref),
		Environment:      github.Ptr(env),
		Description:      github.Ptr(desc),
		AutoMerge:        github.Ptr(s.config.AutoMerge),
		RequiredContexts: &[]string{},
	})
	if err != nil {
		return errorResult(fmt.Sprintf("create deployment: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"deployment_id": dep.GetID(),
			"environment":   dep.GetEnvironment(),
			"ref":           dep.GetRef(),
			"sha":           dep.GetSHA(),
			"url":           dep.GetURL(),
		},
	}, nil
}
