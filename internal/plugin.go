package internal

import (
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// githubPlugin implements sdk.PluginProvider, sdk.ModuleProvider, and sdk.StepProvider.
type githubPlugin struct{}

// NewGitHubPlugin returns a new githubPlugin instance.
func NewGitHubPlugin() sdk.PluginProvider {
	return &githubPlugin{}
}

// Manifest returns plugin metadata.
func (p *githubPlugin) Manifest() sdk.PluginManifest {
	return sdk.PluginManifest{
		Name:        "workflow-plugin-github",
		Version:     "1.0.0",
		Author:      "GoCodeAlone",
		Description: "GitHub integration plugin: webhook handling and GitHub Actions workflow management",
	}
}

// ModuleTypes returns the module type names this plugin provides.
func (p *githubPlugin) ModuleTypes() []string {
	return []string{"git.webhook"}
}

// CreateModule creates a module instance of the given type.
func (p *githubPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "git.webhook":
		return newWebhookModule(name, config)
	default:
		return nil, fmt.Errorf("github plugin: unknown module type %q", typeName)
	}
}

// StepTypes returns the step type names this plugin provides.
func (p *githubPlugin) StepTypes() []string {
	return []string{
		"step.gh_action_trigger",
		"step.gh_action_status",
		"step.gh_create_check",
	}
}

// CreateStep creates a step instance of the given type.
func (p *githubPlugin) CreateStep(typeName, name string, config map[string]any) (sdk.StepInstance, error) {
	switch typeName {
	case "step.gh_action_trigger":
		return newActionTriggerStep(name, config, nil)
	case "step.gh_action_status":
		return newActionStatusStep(name, config, nil)
	case "step.gh_create_check":
		return newCreateCheckStep(name, config, nil)
	default:
		return nil, fmt.Errorf("github plugin: unknown step type %q", typeName)
	}
}
