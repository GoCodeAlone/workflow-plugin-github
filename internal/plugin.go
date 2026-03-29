package internal

import (
	"fmt"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// webhookRouteConfig is the config fragment YAML that declares the GitHub
// webhook HTTP route so the engine's HTTP server registers it via the normal
// config pipeline instead of the unreachable global DefaultServeMux.
const webhookRouteConfig = `
workflows:
  github-webhook-receiver:
    triggers:
      - type: http
        config:
          path: /webhooks/github
          method: POST
    steps: []
`

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
		Version:     "1.0.1",
		Author:      "GoCodeAlone",
		Description: "GitHub integration plugin: webhook handling, GitHub Actions, PRs, issues, releases, and deployments",
	}
}

// ModuleTypes returns the module type names this plugin provides.
func (p *githubPlugin) ModuleTypes() []string {
	return []string{
		"git.webhook",
		"github.app",
	}
}

// CreateModule creates a module instance of the given type.
func (p *githubPlugin) CreateModule(typeName, name string, config map[string]any) (sdk.ModuleInstance, error) {
	switch typeName {
	case "git.webhook":
		return newWebhookModule(name, config)
	case "github.app":
		return newGitHubAppModule(name, config)
	default:
		return nil, fmt.Errorf("github plugin: unknown module type %q", typeName)
	}
}

// StepTypes returns the step type names this plugin provides.
func (p *githubPlugin) StepTypes() []string {
	return []string{
		// Existing steps
		"step.gh_action_trigger",
		"step.gh_action_status",
		"step.gh_create_check",
		// Pull request steps
		"step.gh_pr_create",
		"step.gh_pr_merge",
		"step.gh_pr_comment",
		"step.gh_pr_review",
		// Issue steps
		"step.gh_issue_create",
		"step.gh_issue_close",
		"step.gh_issue_label",
		// Release steps
		"step.gh_release_create",
		"step.gh_release_upload",
		// Repository steps
		"step.gh_repo_dispatch",
		"step.gh_deployment_create",
		"step.gh_secret_set",
		// GraphQL
		"step.gh_graphql",
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
	case "step.gh_pr_create":
		return newPRCreateStep(name, config)
	case "step.gh_pr_merge":
		return newPRMergeStep(name, config)
	case "step.gh_pr_comment":
		return newPRCommentStep(name, config)
	case "step.gh_pr_review":
		return newPRReviewStep(name, config)
	case "step.gh_issue_create":
		return newIssueCreateStep(name, config)
	case "step.gh_issue_close":
		return newIssueCloseStep(name, config)
	case "step.gh_issue_label":
		return newIssueLabelStep(name, config)
	case "step.gh_release_create":
		return newReleaseCreateStep(name, config)
	case "step.gh_release_upload":
		return newReleaseUploadStep(name, config)
	case "step.gh_repo_dispatch":
		return newRepoDispatchStep(name, config)
	case "step.gh_deployment_create":
		return newDeploymentCreateStep(name, config)
	case "step.gh_secret_set":
		return newSecretSetStep(name, config)
	case "step.gh_graphql":
		return newGraphQLStep(name, config)
	default:
		return nil, fmt.Errorf("github plugin: unknown step type %q", typeName)
	}
}

// ConfigFragment implements sdk.ConfigProvider.
// It returns a config fragment that declares the /webhooks/github HTTP route
// so the engine registers it through the normal config pipeline rather than
// through the global DefaultServeMux (which is unreachable in a gRPC plugin).
func (p *githubPlugin) ConfigFragment() ([]byte, error) {
	return []byte(webhookRouteConfig), nil
}
