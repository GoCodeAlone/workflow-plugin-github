package githubplugin

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	providercontract "github.com/GoCodeAlone/workflow-plugin-github/providercontract"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const githubRunnerInputSchemaRef = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/input/v1"
const githubRunnerConfigSchemaRef = "schema://providers/workflow-plugin-github/github-runner/v1"
const githubRunnerOutputSchemaRef = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/output/v1"

//go:embed schemas/github-runner-ephemeral-job-input.schema.json
var githubRunnerInputSchemaDocument []byte

//go:embed schemas/github-runner-provider.schema.json
var githubRunnerConfigSchemaDocument []byte

//go:embed schemas/github-runner-ephemeral-job-output.schema.json
var githubRunnerOutputSchemaDocument []byte

var (
	githubRunnerInputSchemaOnce  sync.Once
	githubRunnerInputSchema      *jsonschema.Schema
	githubRunnerInputSchemaErr   error
	githubRunnerConfigSchemaOnce sync.Once
	githubRunnerConfigSchema     *jsonschema.Schema
	githubRunnerConfigSchemaErr  error
	githubRunnerOutputSchemaOnce sync.Once
	githubRunnerOutputSchema     *jsonschema.Schema
	githubRunnerOutputSchemaErr  error
)

func ValidateGitHubRunnerProviderConfig(config providercontract.Config) error {
	return ValidateGitHubRunnerProviderConfigValue(config)
}

func ValidateGitHubRunnerProviderConfigValue(config any) error {
	githubRunnerConfigSchemaOnce.Do(func() {
		githubRunnerConfigSchema, githubRunnerConfigSchemaErr = compileEmbeddedSchema(githubRunnerConfigSchemaRef, githubRunnerConfigSchemaDocument)
	})
	if githubRunnerConfigSchemaErr != nil {
		return githubRunnerConfigSchemaErr
	}
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("encode typed provider config: %w", err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode typed provider config: %w", err)
	}
	return githubRunnerConfigSchema.Validate(value)
}

func ValidateEphemeralRunnerJobInput(input json.RawMessage) error {
	githubRunnerInputSchemaOnce.Do(func() {
		githubRunnerInputSchema, githubRunnerInputSchemaErr = compileEmbeddedSchema(githubRunnerInputSchemaRef, githubRunnerInputSchemaDocument)
	})
	if githubRunnerInputSchemaErr != nil {
		return githubRunnerInputSchemaErr
	}
	var value any
	if err := json.Unmarshal(input, &value); err != nil {
		return fmt.Errorf("decode input: %w", err)
	}
	return githubRunnerInputSchema.Validate(value)
}

func ValidateEphemeralRunnerJobOutput(output any) error {
	githubRunnerOutputSchemaOnce.Do(func() {
		githubRunnerOutputSchema, githubRunnerOutputSchemaErr = compileEmbeddedSchema(githubRunnerOutputSchemaRef, githubRunnerOutputSchemaDocument)
	})
	if githubRunnerOutputSchemaErr != nil {
		return githubRunnerOutputSchemaErr
	}
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("encode provider output: %w", err)
	}
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("decode provider output: %w", err)
	}
	return githubRunnerOutputSchema.Validate(value)
}

func compileEmbeddedSchema(ref string, data []byte) (*jsonschema.Schema, error) {
	var document any
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, fmt.Errorf("decode embedded schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(ref, document); err != nil {
		return nil, fmt.Errorf("add embedded schema: %w", err)
	}
	return compiler.Compile(ref)
}
