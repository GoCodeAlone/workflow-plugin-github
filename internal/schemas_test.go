package internal

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/GoCodeAlone/workflow/plugin"
)

// TestModuleSchemas verifies that the plugin's SchemaProvider returns schema
// descriptors for both advertised module types.
func TestModuleSchemas(t *testing.T) {
	p := &githubPlugin{}
	schemas := p.ModuleSchemas()

	if len(schemas) != 2 {
		t.Fatalf("expected 2 module schemas, got %d", len(schemas))
	}

	byType := make(map[string]int, len(schemas))
	for i, s := range schemas {
		byType[s.Type] = i
	}

	for _, wantType := range []string{"git.webhook", "github.app"} {
		if _, ok := byType[wantType]; !ok {
			t.Errorf("missing module schema for type %q", wantType)
		}
	}

	// git.webhook should have at least the four documented config fields.
	webhookIdx, ok := byType["git.webhook"]
	if !ok {
		t.Fatalf("git.webhook schema not found")
	}
	webhook := schemas[webhookIdx]
	if webhook.Label == "" {
		t.Error("git.webhook schema: Label must not be empty")
	}
	if webhook.Description == "" {
		t.Error("git.webhook schema: Description must not be empty")
	}
	if len(webhook.ConfigFields) < 4 {
		t.Errorf("git.webhook schema: expected at least 4 config fields, got %d", len(webhook.ConfigFields))
	}
	if len(webhook.Outputs) == 0 {
		t.Error("git.webhook schema: expected at least one output")
	}

	// github.app should declare the three required config fields.
	appIdx, ok := byType["github.app"]
	if !ok {
		t.Fatalf("github.app schema not found")
	}
	app := schemas[appIdx]
	if app.Label == "" {
		t.Error("github.app schema: Label must not be empty")
	}
	if app.Description == "" {
		t.Error("github.app schema: Description must not be empty")
	}
	requiredFields := map[string]bool{}
	for _, f := range app.ConfigFields {
		if f.Required {
			requiredFields[f.Name] = true
		}
	}
	for _, want := range []string{"app_id", "installation_id", "private_key"} {
		if !requiredFields[want] {
			t.Errorf("github.app schema: field %q should be marked required", want)
		}
	}
}

// TestPluginStepSchemasJSON verifies that plugin.json can be parsed and that
// it declares a stepSchemas entry for every step type the plugin advertises.
func TestPluginStepSchemasJSON(t *testing.T) {
	// Locate plugin.json relative to the repository root (one level up from internal/).
	data, err := os.ReadFile("../plugin.json")
	if err != nil {
		t.Fatalf("plugin.json not found — every build must ship a contract manifest: %v", err)
	}

	var manifest struct {
		StepTypes   []string `json:"stepTypes"`
		StepSchemas []struct {
			Type string `json:"type"`
		} `json:"stepSchemas"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse plugin.json: %v", err)
	}

	if len(manifest.StepTypes) == 0 {
		t.Fatal("plugin.json: stepTypes must not be empty")
	}

	schemaSet := make(map[string]bool, len(manifest.StepSchemas))
	for _, s := range manifest.StepSchemas {
		schemaSet[s.Type] = true
	}

	for _, stepType := range manifest.StepTypes {
		if !schemaSet[stepType] {
			t.Errorf("plugin.json: stepType %q has no corresponding stepSchema entry", stepType)
		}
	}

	if len(manifest.StepSchemas) != len(manifest.StepTypes) {
		t.Errorf("plugin.json: stepSchemas count (%d) does not match stepTypes count (%d)",
			len(manifest.StepSchemas), len(manifest.StepTypes))
	}
}

// TestPluginManifestEngineValidation verifies that plugin.json is parseable as a
// workflow engine PluginManifest and that Validate() passes required-field checks.
func TestPluginManifestEngineValidation(t *testing.T) {
	m, err := plugin.LoadManifest("../plugin.json")
	if err != nil {
		t.Fatalf("plugin.LoadManifest: %v", err)
	}
	if err := m.Validate(); err != nil {
		t.Fatalf("plugin.json fails engine manifest validation: %v", err)
	}
	// Strict contract requirements: must declare at least one module or step type.
	if len(m.ModuleTypes) == 0 && len(m.StepTypes) == 0 {
		t.Error("plugin.json: must advertise at least one moduleType or stepType for strict contracts")
	}
	// StepSchemas must be present when step types are declared.
	if len(m.StepTypes) > 0 && len(m.StepSchemas) == 0 {
		t.Error("plugin.json: stepSchemas is required when stepTypes are declared (missing_step_contract_descriptor)")
	}
	// Every step type must have a schema entry.
	schemaSet := make(map[string]bool, len(m.StepSchemas))
	for _, s := range m.StepSchemas {
		if s != nil {
			schemaSet[s.Type] = true
		}
	}
	for _, st := range m.StepTypes {
		if !schemaSet[st] {
			t.Errorf("plugin.json: stepType %q has no stepSchema (missing_step_contract_descriptor)", st)
		}
	}
}
