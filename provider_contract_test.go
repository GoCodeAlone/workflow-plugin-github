package githubplugin_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	compute "github.com/GoCodeAlone/workflow-plugin-compute-core/protocol"
	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
	githubprovider "github.com/GoCodeAlone/workflow-plugin-github/internal"
	providercontract "github.com/GoCodeAlone/workflow-plugin-github/providercontract"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	githubRunnerConfigSchemaRef = "schema://providers/workflow-plugin-github/github-runner/v1"
	githubRunnerInputSchemaRef  = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/input/v1"
	githubRunnerOutputSchemaRef = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/output/v1"
)

func TestGitHubRunnerProviderContractUsesReleasedTypedSchemas(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("contracts", "github-runner-provider.json"))
	if err != nil {
		t.Fatalf("read GitHub runner provider contract: %v", err)
	}
	var contract compute.ProviderContract
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		t.Fatalf("strict-decode GitHub runner provider contract: %v", err)
	}
	if err := contract.Validate(); err != nil {
		t.Fatalf("GitHub runner provider contract should validate: %v", err)
	}

	if contract.PluginID != "workflow-plugin-github" ||
		contract.ProviderID != "github-runner" ||
		contract.ContractID != "github.runner_provider.v1" ||
		contract.Version != "v0.0.0" {
		t.Fatalf("provider identity drifted: %+v", contract)
	}
	if contract.ConfigSchemaRef != githubRunnerConfigSchemaRef {
		t.Fatalf("config schema ref = %q, want %q", contract.ConfigSchemaRef, githubRunnerConfigSchemaRef)
	}
	if len(contract.Operations) != 1 {
		t.Fatalf("operations = %d, want 1", len(contract.Operations))
	}
	operation := contract.Operations[0]
	if operation.ID != "ephemeral_runner_job" ||
		operation.InputSchemaRef != githubRunnerInputSchemaRef ||
		operation.OutputSchemaRef != githubRunnerOutputSchemaRef {
		t.Fatalf("provider operation drifted: %+v", operation)
	}
	if len(operation.ArtifactSpecs) != 2 ||
		operation.ArtifactSpecs[0].Name != "github-runner-proof.json" ||
		!operation.ArtifactSpecs[0].Required ||
		operation.ArtifactSpecs[0].ContentType != "application/json" ||
		operation.ArtifactSpecs[1].Name != "github-workload-outputs.tar.gz" ||
		operation.ArtifactSpecs[1].Required ||
		operation.ArtifactSpecs[1].ContentType != "application/gzip" {
		t.Fatalf("provider proof artifact drifted: %+v", operation.ArtifactSpecs)
	}
	if len(contract.RuntimeContract.Profiles) != 1 ||
		contract.RuntimeContract.Profiles[0].ExecutorProvider != "github-actions-runner" ||
		contract.RuntimeContract.Profiles[0].RuntimeProfile != compute.RuntimeProfileSandboxedOCI {
		t.Fatalf("provider runtime profile drifted: %+v", contract.RuntimeContract.Profiles)
	}

	assertSchemaDigest(t, contract.ConfigSchemaDigest, "github-runner-provider.schema.json")
	assertSchemaDigest(t, operation.InputSchemaDigest, "github-runner-ephemeral-job-input.schema.json")
	assertSchemaDigest(t, operation.OutputSchemaDigest, "github-runner-ephemeral-job-output.schema.json")
}

func TestGitHubRunnerProviderSchemasAreStrictAndMatchRuntimeEnvelope(t *testing.T) {
	configSchema := compileSchema(t, "github-runner-provider.schema.json")
	journaledConfig := decodeJSON(t, `{
		"organizations":["GoCodeAlone"],
		"repositories":["GoCodeAlone/workflow-compute"],
		"state_dir":"/var/lib/workflow-github-runner-provider",
		"token":"${GITHUB_RUNNER_PROVIDER_TOKEN}",
		"provider_token":"${GITHUB_RUNNER_SIDECAR_TOKEN}"
	}`)
	if err := configSchema.Validate(journaledConfig); err != nil {
		t.Fatalf("journaled provider config rejected: %v", err)
	}
	unJournaledConfig := decodeJSON(t, `{
		"organizations":["GoCodeAlone"],
		"repositories":["GoCodeAlone/workflow-compute"],
		"token":"${GITHUB_RUNNER_PROVIDER_TOKEN}",
		"provider_token":"${GITHUB_RUNNER_SIDECAR_TOKEN}"
	}`)
	if err := configSchema.Validate(unJournaledConfig); err == nil {
		t.Fatal("provider config schema must require durable JIT ownership state_dir")
	}
	validConfig := schemaValue(t, providercontract.Config{
		Organizations: []string{"GoCodeAlone"},
		Repositories:  []string{"GoCodeAlone/workflow-compute"},
		RunnerGroups:  []string{"ephemeral"},
		APIBaseURL:    "https://api.github.com",
		StateDir:      "/var/lib/workflow-github-runner-provider",
		Token:         "${GITHUB_RUNNER_PROVIDER_TOKEN}",
		ProviderToken: "${GITHUB_RUNNER_SIDECAR_TOKEN}",
	})
	if err := configSchema.Validate(validConfig); err != nil {
		t.Fatalf("valid provider config rejected: %v", err)
	}
	compatibleLiteralConfig := providercontract.Config{
		Organizations: []string{"GoCodeAlone"},
		Repositories:  []string{"GoCodeAlone/workflow-compute"},
		StateDir:      "/var/lib/workflow-github-runner-provider",
		Token:         "github-token",
		ProviderToken: "provider-token",
	}
	if err := configSchema.Validate(schemaValue(t, compatibleLiteralConfig)); err != nil {
		t.Fatalf("backward-compatible literal token config rejected: %v", err)
	}
	invalidConfig := decodeJSON(t, `{
		"organizations":["GoCodeAlone"],
		"token":"contains whitespace",
		"provider_token":"${GITHUB_RUNNER_SIDECAR_TOKEN}"
	}`)
	if err := configSchema.Validate(invalidConfig); err == nil {
		t.Fatal("provider config schema must reject malformed token values")
	}
	for name, config := range map[string]providercontract.Config{
		"organization scoped": {
			Organizations: []string{"GoCodeAlone"},
			Token:         "${GITHUB_RUNNER_PROVIDER_TOKEN}",
			ProviderToken: "${GITHUB_RUNNER_SIDECAR_TOKEN}",
		},
		"repository scoped": {
			Repositories:  []string{"GoCodeAlone/workflow-compute"},
			Token:         "${GITHUB_RUNNER_PROVIDER_TOKEN}",
			ProviderToken: "${GITHUB_RUNNER_SIDECAR_TOKEN}",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := configSchema.Validate(schemaValue(t, config)); err == nil {
				t.Fatal("single-scope config cannot execute the contracted ephemeral runner operation")
			}
		})
	}
	missingScope := providercontract.Config{
		Token:         "${GITHUB_RUNNER_PROVIDER_TOKEN}",
		ProviderToken: "${GITHUB_RUNNER_SIDECAR_TOKEN}",
	}
	if err := configSchema.Validate(schemaValue(t, missingScope)); err == nil {
		t.Fatal("provider config schema must require organization or repository scope")
	}
	wrapper := compute.ProviderConfig{
		PluginID:     "workflow-plugin-github",
		ProviderID:   "github-runner",
		ContractID:   "github.runner_provider.v1",
		Version:      "v1.0.29",
		ConfigRef:    "config://network-products/github-runner-dogfood/github-runner",
		ConfigDigest: "sha256:" + strings.Repeat("a", 64),
	}
	if err := wrapper.Validate(); err != nil {
		t.Fatalf("compute-core provider config wrapper rejected: %v", err)
	}

	inputSchema := compileSchema(t, "github-runner-ephemeral-job-input.schema.json")
	caseCollidingInputs := decodeJSON(t, `{
		"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1",
		"organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml",
		"runner_group":"ephemeral","require_preflight":true,"workflow_inputs":{"DEPLOY":"production","deploy":"staging"}
	}`)
	if err := inputSchema.Validate(caseCollidingInputs); err == nil {
		t.Fatal("workflow input schema must require canonical lowercase keys")
	}
	request := githubprovider.EphemeralRunnerJobRequest{
		Mode:             githubprovider.EphemeralRunnerJobModeDispatchThenWait,
		Environment:      "stg",
		OS:               "linux",
		WorkerID:         "github-runner-linux-stg-am5-2-20260629",
		TaskID:           "github-provider-dogfood-linux-1",
		Organization:     "GoCodeAlone",
		Repository:       "GoCodeAlone/workflow-compute",
		Workflow:         "dogfood-provider-target.yml",
		Ref:              "main",
		RunnerGroup:      "ephemeral",
		RequirePreflight: true,
		TimeoutSeconds:   1800,
	}
	validInput := schemaValue(t, request)
	if err := inputSchema.Validate(validInput); err != nil {
		t.Fatalf("valid ephemeral runner input rejected: %v", err)
	}
	request.Mode = githubprovider.EphemeralRunnerJobModeAttachToQueued
	request.WorkflowRunID = 28460000001
	request.WorkflowJobID = 84330000001
	if err := inputSchema.Validate(schemaValue(t, request)); err != nil {
		t.Fatalf("valid attach-to-queued input rejected: %v", err)
	}
	request.WorkflowJobID = 0
	if err := inputSchema.Validate(schemaValue(t, request)); err == nil {
		t.Fatal("attach-to-queued input without exact workflow job ID accepted")
	}
	request.Mode = githubprovider.EphemeralRunnerJobModeDispatchThenWait
	request.WorkflowRunID = 0
	request.WorkflowJobID = 0
	request.WorkflowRunID = 28460000001
	request.WorkflowJobID = 84330000001
	if err := inputSchema.Validate(schemaValue(t, request)); err == nil {
		t.Fatal("dispatch_then_wait input must reject attach-only workflow IDs")
	}
	request.WorkflowRunID = 0
	request.WorkflowJobID = 0
	request.OS = "macos"
	if err := inputSchema.Validate(schemaValue(t, request)); err == nil {
		t.Fatal("Linux runner bundle schema must reject a macOS runner label")
	}
	missingWorkflow := decodeJSON(t, `{
		"mode":"dispatch_then_wait",
		"environment":"stg",
		"os":"linux",
		"worker_id":"github-runner-linux-stg-am5-2-20260629",
		"task_id":"github-provider-dogfood-linux-1",
		"organization":"GoCodeAlone",
		"repository":"GoCodeAlone/workflow-compute"
	}`)
	if err := inputSchema.Validate(missingWorkflow); err == nil {
		t.Fatal("dispatch_then_wait input must require a workflow")
	}
	unknownInput := decodeJSON(t, `{
		"mode":"dispatch_then_wait",
		"environment":"stg",
		"os":"linux",
		"worker_id":"github-runner-linux-stg-am5-2-20260629",
		"task_id":"github-provider-dogfood-linux-1",
		"organization":"GoCodeAlone",
		"repository":"GoCodeAlone/workflow-compute",
		"workflow":"dogfood-provider-target.yml",
		"product_workload":true
	}`)
	if err := inputSchema.Validate(unknownInput); err == nil {
		t.Fatal("provider input schema must reject provider-specific leakage")
	}
	selfAssertedCapabilities := decodeJSON(t, `{
		"mode":"dispatch_then_wait",
		"environment":"stg",
		"os":"linux",
		"worker_id":"github-runner-linux-stg-am5-2-20260629",
		"task_id":"github-provider-dogfood-linux-1",
		"organization":"GoCodeAlone",
		"repository":"GoCodeAlone/workflow-compute",
		"workflow":"dogfood-provider-target.yml",
		"runner_group":"ephemeral",
		"require_preflight":true,
		"advertised_caps":["docker","iac"]
	}`)
	if err := inputSchema.Validate(selfAssertedCapabilities); err == nil {
		t.Fatal("provider input schema must reject workload-supplied advertised capabilities")
	}
	request.OS = "linux"
	request.WorkflowInputs = make(map[string]string, 23)
	for i := range 23 {
		request.WorkflowInputs[fmt.Sprintf("input_%d", i)] = "value"
	}
	if err := inputSchema.Validate(schemaValue(t, request)); err == nil {
		t.Fatal("provider input schema must reserve three of GitHub's 25 workflow inputs for provider-owned dispatch keys")
	}

	outputSchema := compileSchema(t, "github-runner-ephemeral-job-output.schema.json")
	if err := outputSchema.Validate(decodeJSON(t, `{"artifacts":["github-runner-proof.json"]}`)); err != nil {
		t.Fatalf("valid provider output rejected: %v", err)
	}
	if err := outputSchema.Validate(decodeJSON(t, `{"artifacts":["github-runner-proof.json","github-workload-outputs.tar.gz"]}`)); err != nil {
		t.Fatalf("valid workload artifact archive rejected: %v", err)
	}
	if err := outputSchema.Validate(decodeJSON(t, `{"artifacts":["github-runner-proof.json","github-workload-output/build/result.txt"]}`)); err == nil {
		t.Fatal("provider output schema must reject undeclared per-file artifact names")
	}
	if err := outputSchema.Validate(decodeJSON(t, `{"artifacts":["github-workflow-artifact.zip"]}`)); err == nil {
		t.Fatal("provider output schema must reject GitHub-hosted artifact substitution")
	}
	if err := githubplugin.ValidateEphemeralRunnerJobOutput(map[string]any{"artifacts": []string{"github-runner-proof.json"}}); err != nil {
		t.Fatalf("runtime output validation rejected canonical proof artifact: %v", err)
	}
	if err := githubplugin.ValidateEphemeralRunnerJobOutput(map[string]any{"artifacts": []string{"github-workflow-artifact.zip"}}); err == nil {
		t.Fatal("runtime output validation must reject GitHub-hosted artifact substitution")
	}
}

func TestProviderContractCatalogDiscoversGitHubRunnerContract(t *testing.T) {
	catalog, err := providercontract.LoadCatalog(os.DirFS("."))
	if err != nil {
		t.Fatalf("load provider contract catalog: %v", err)
	}
	if catalog.Version != "1" || len(catalog.Contracts) != 1 {
		t.Fatalf("provider contract catalog = %+v", catalog)
	}
	ref := catalog.Contracts[0]
	if ref.ID != providercontract.GitHubRunnerContractID ||
		ref.Path != providercontract.GitHubRunnerContractPath ||
		len(ref.Schemas) != 3 {
		t.Fatalf("GitHub runner contract ref drifted: %+v", ref)
	}
}

func assertSchemaDigest(t *testing.T, got, name string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("schemas", name))
	if err != nil {
		t.Fatalf("read schema %s: %v", name, err)
	}
	sum := sha256.Sum256(data)
	want := "sha256:" + hex.EncodeToString(sum[:])
	if got != want {
		t.Fatalf("schema digest for %s = %q, want %q", name, got, want)
	}
}

func compileSchema(t *testing.T, name string) *jsonschema.Schema {
	t.Helper()
	schema, err := jsonschema.NewCompiler().Compile(filepath.Join("schemas", name))
	if err != nil {
		t.Fatalf("compile schema %s: %v", name, err)
	}
	return schema
}

func decodeJSON(t *testing.T, document string) any {
	t.Helper()
	var value any
	if err := json.Unmarshal([]byte(document), &value); err != nil {
		t.Fatalf("decode JSON test document: %v", err)
	}
	return value
}

func schemaValue(t *testing.T, value any) any {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal typed schema value: %v", err)
	}
	return decodeJSON(t, string(data))
}
