package internal

import sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"

// Ensure githubPlugin satisfies sdk.SchemaProvider at compile time.
var _ sdk.SchemaProvider = (*githubPlugin)(nil)

// ModuleSchemas returns schema descriptors for all module types provided by
// this plugin. Implementing sdk.SchemaProvider allows the engine to surface
// module configuration fields and I/O contracts at startup and in the UI.
func (p *githubPlugin) ModuleSchemas() []sdk.ModuleSchemaData {
	return []sdk.ModuleSchemaData{
		{
			Type:        "git.webhook",
			Label:       "GitHub Webhook",
			Category:    "github",
			Description: "Receives GitHub webhook events via HTTP, verifies HMAC-SHA256 signatures, and publishes normalised GitEvent messages to a configurable topic.",
			ConfigFields: []sdk.ConfigField{
				{
					Name:        "secret",
					Type:        "string",
					Description: "Webhook secret used to verify the X-Hub-Signature-256 header. Leave empty to skip signature verification.",
					Required:    false,
				},
				{
					Name:        "events",
					Type:        "array",
					Description: "Event types to accept (e.g. push, pull_request). An empty list accepts all event types.",
					Required:    false,
				},
				{
					Name:         "topic",
					Type:         "string",
					Description:  "Message-bus topic to which normalised GitEvent payloads are published.",
					DefaultValue: "git.events",
					Required:     false,
				},
			},
			Outputs: []sdk.ServiceIO{
				{Name: "provider", Type: "string", Description: "Webhook provider (always 'github')"},
				{Name: "event_type", Type: "string", Description: "GitHub event type (e.g. push, pull_request)"},
				{Name: "repository", Type: "string", Description: "Repository full name (owner/repo)"},
				{Name: "branch", Type: "string", Description: "Branch or ref name"},
				{Name: "commit", Type: "string", Description: "Commit SHA"},
				{Name: "author", Type: "string", Description: "Event author username"},
				{Name: "message", Type: "string", Description: "Commit message or PR title"},
				{Name: "url", Type: "string", Description: "URL to the commit or PR"},
				{Name: "raw_payload", Type: "object", Description: "Raw JSON webhook payload"},
				{Name: "timestamp", Type: "string", Description: "Event timestamp in RFC3339 format"},
			},
		},
		{
			Type:        "github.app",
			Label:       "GitHub App",
			Category:    "github",
			Description: "Authenticates as a GitHub App installation, generating short-lived installation access tokens from an App private key. Tokens are cached and refreshed automatically.",
			ConfigFields: []sdk.ConfigField{
				{
					Name:        "app_id",
					Type:        "number",
					Description: "GitHub App ID",
					Required:    true,
				},
				{
					Name:        "installation_id",
					Type:        "number",
					Description: "GitHub App installation ID",
					Required:    true,
				},
				{
					Name:        "private_key",
					Type:        "string",
					Description: "PEM-encoded RSA private key for the GitHub App (supports env var references e.g. ${GITHUB_APP_PRIVATE_KEY})",
					Required:    true,
				},
			},
		},
	}
}
