package internal

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/GoCodeAlone/workflow/plugin"
)

// TestModuleSchemas verifies that the plugin's SchemaProvider returns schema
// descriptors for both advertised module types and stays in sync with
// ModuleTypes() and plugin.json moduleTypes.
func TestModuleSchemas(t *testing.T) {
	p := &githubPlugin{}
	schemas := p.ModuleSchemas()
	runtimeTypes := p.ModuleTypes()

	if len(schemas) != 2 {
		t.Fatalf("expected 2 module schemas, got %d", len(schemas))
	}

	// Every schema type must appear in the runtime ModuleTypes() list.
	runtimeSet := make(map[string]bool, len(runtimeTypes))
	for _, mt := range runtimeTypes {
		runtimeSet[mt] = true
	}
	for _, s := range schemas {
		if !runtimeSet[s.Type] {
			t.Errorf("schema type %q is not in githubPlugin.ModuleTypes()", s.Type)
		}
	}

	// Every runtime module type must have a schema entry.
	schemaSet := make(map[string]int, len(schemas))
	for i, s := range schemas {
		schemaSet[s.Type] = i
	}
	for _, mt := range runtimeTypes {
		if _, ok := schemaSet[mt]; !ok {
			t.Errorf("ModuleTypes() returns %q but no corresponding ModuleSchema exists", mt)
		}
	}

	for _, wantType := range []string{"git.webhook", "github.app"} {
		if _, ok := schemaSet[wantType]; !ok {
			t.Errorf("missing module schema for type %q", wantType)
		}
	}

	// git.webhook should have the core documented config fields.
	webhookIdx, ok := schemaSet["git.webhook"]
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
	webhookFieldNames := make(map[string]bool, len(webhook.ConfigFields))
	for _, f := range webhook.ConfigFields {
		webhookFieldNames[f.Name] = true
	}
	for _, want := range []string{"secret", "events", "topic"} {
		if !webhookFieldNames[want] {
			t.Errorf("git.webhook schema: required config field %q is missing", want)
		}
	}
	if len(webhook.Outputs) == 0 {
		t.Error("git.webhook schema: expected at least one output")
	}
	// raw_payload must be declared as an object (json.RawMessage), not string.
	for _, f := range webhook.Outputs {
		if f.Name == "raw_payload" && f.Type != "object" {
			t.Errorf("git.webhook schema: raw_payload output type should be %q, got %q", "object", f.Type)
		}
	}

	// github.app should declare the three required config fields.
	appIdx, ok := schemaSet["github.app"]
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

	// Cross-check against plugin.json moduleTypes.
	data, err := os.ReadFile("../plugin.json")
	if err != nil {
		t.Fatalf("plugin.json not found: %v", err)
	}
	var manifest struct {
		ModuleTypes []string `json:"moduleTypes"`
	}
	if err := json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse plugin.json: %v", err)
	}
	jsonModuleSet := make(map[string]bool, len(manifest.ModuleTypes))
	for _, mt := range manifest.ModuleTypes {
		jsonModuleSet[mt] = true
	}
	for _, mt := range runtimeTypes {
		if !jsonModuleSet[mt] {
			t.Errorf("githubPlugin.ModuleTypes() returns %q but plugin.json moduleTypes does not include it", mt)
		}
	}
	for _, mt := range manifest.ModuleTypes {
		if !runtimeSet[mt] {
			t.Errorf("plugin.json moduleTypes includes %q but githubPlugin.ModuleTypes() does not", mt)
		}
	}
}

// TestPluginStepSchemasJSON verifies that plugin.json can be parsed and that
// it declares a stepSchemas entry for every step type the plugin advertises,
// and that both are in sync with the runtime StepTypes() list.
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

	// Cross-check JSON manifest against the runtime StepTypes() list.
	p := &githubPlugin{}
	runtimeTypes := p.StepTypes()

	runtimeSet := make(map[string]bool, len(runtimeTypes))
	for _, st := range runtimeTypes {
		runtimeSet[st] = true
	}
	jsonTypeSet := make(map[string]bool, len(manifest.StepTypes))
	for _, st := range manifest.StepTypes {
		jsonTypeSet[st] = true
	}

	for _, rt := range runtimeTypes {
		if !jsonTypeSet[rt] {
			t.Errorf("githubPlugin.StepTypes() returns %q but plugin.json stepTypes does not include it", rt)
		}
	}
	for _, jt := range manifest.StepTypes {
		if !runtimeSet[jt] {
			t.Errorf("plugin.json stepTypes includes %q but githubPlugin.StepTypes() does not return it", jt)
		}
	}

	// Verify both lists have the same length (no duplicates or gaps).
	sortedRuntime := append([]string(nil), runtimeTypes...)
	sortedJSON := append([]string(nil), manifest.StepTypes...)
	sort.Strings(sortedRuntime)
	sort.Strings(sortedJSON)
	if len(sortedRuntime) != len(sortedJSON) {
		t.Errorf("runtime StepTypes() count (%d) != plugin.json stepTypes count (%d)",
			len(sortedRuntime), len(sortedJSON))
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
