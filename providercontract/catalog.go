package providercontract

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"strings"

	compute "github.com/GoCodeAlone/workflow-plugin-compute-core/protocol"
)

const (
	CatalogPath                  = "provider.contracts.json"
	GitHubRunnerContractID       = "github.runner_provider.v1"
	GitHubRunnerContractPath     = "contracts/github-runner-provider.json"
	GitHubRunnerConfigSchemaRef  = "schema://providers/workflow-plugin-github/github-runner/v1"
	GitHubRunnerConfigSchemaPath = "schemas/github-runner-provider.schema.json"
	GitHubRunnerInputSchemaRef   = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/input/v1"
	GitHubRunnerInputSchemaPath  = "schemas/github-runner-ephemeral-job-input.schema.json"
	GitHubRunnerOutputSchemaRef  = "schema://providers/workflow-plugin-github/github-runner/operations/ephemeral_runner_job/output/v1"
	GitHubRunnerOutputSchemaPath = "schemas/github-runner-ephemeral-job-output.schema.json"
)

type Catalog struct {
	Version   string        `json:"version"`
	Contracts []ContractRef `json:"contracts"`
}

type ContractRef struct {
	ID      string      `json:"id"`
	Path    string      `json:"path"`
	Schemas []SchemaRef `json:"schemas"`
}

type SchemaRef struct {
	Ref  string `json:"ref"`
	Path string `json:"path"`
}

func LoadCatalog(filesystem fs.FS) (Catalog, error) {
	data, err := readRegularAsset(filesystem, CatalogPath)
	if err != nil {
		return Catalog{}, fmt.Errorf("read provider contract catalog: %w", err)
	}
	var catalog Catalog
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return Catalog{}, fmt.Errorf("decode provider contract catalog: %w", err)
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return Catalog{}, err
	}
	if err := catalog.Validate(filesystem); err != nil {
		return Catalog{}, err
	}
	return catalog, nil
}

func (c Catalog) Validate(filesystem fs.FS) error {
	if c.Version != "1" {
		return fmt.Errorf("provider contract catalog version must be %q", "1")
	}
	if len(c.Contracts) == 0 {
		return errors.New("provider contract catalog contracts is required")
	}
	seen := make(map[string]struct{}, len(c.Contracts))
	for i, ref := range c.Contracts {
		if strings.TrimSpace(ref.ID) == "" {
			return fmt.Errorf("provider contract catalog contracts[%d].id is required", i)
		}
		if _, exists := seen[ref.ID]; exists {
			return fmt.Errorf("provider contract catalog contracts[%d].id %q is duplicated", i, ref.ID)
		}
		seen[ref.ID] = struct{}{}
		if err := validateContractRef(filesystem, ref); err != nil {
			return fmt.Errorf("provider contract catalog contracts[%d]: %w", i, err)
		}
	}
	return nil
}

func validateContractRef(filesystem fs.FS, ref ContractRef) error {
	contractData, err := readRegularAsset(filesystem, ref.Path)
	if err != nil {
		return fmt.Errorf("contract path: %w", err)
	}
	var contract compute.ProviderContract
	decoder := json.NewDecoder(bytes.NewReader(contractData))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		return fmt.Errorf("decode contract: %w", err)
	}
	if err := rejectTrailingJSON(decoder); err != nil {
		return fmt.Errorf("decode contract: %w", err)
	}
	if err := contract.Validate(); err != nil {
		return fmt.Errorf("validate contract: %w", err)
	}
	if contract.ContractID != ref.ID {
		return fmt.Errorf("catalog id %q does not match contract_id %q", ref.ID, contract.ContractID)
	}

	expected := map[string]string{contract.ConfigSchemaRef: contract.ConfigSchemaDigest}
	for _, operation := range contract.Operations {
		expected[operation.InputSchemaRef] = operation.InputSchemaDigest
		expected[operation.OutputSchemaRef] = operation.OutputSchemaDigest
	}
	assets := make(map[string]string, len(ref.Schemas))
	for i, schema := range ref.Schemas {
		if strings.TrimSpace(schema.Ref) == "" {
			return fmt.Errorf("schemas[%d].ref is required", i)
		}
		if _, exists := assets[schema.Ref]; exists {
			return fmt.Errorf("schemas[%d].ref %q is duplicated", i, schema.Ref)
		}
		assets[schema.Ref] = schema.Path
	}
	if len(assets) != len(expected) {
		return fmt.Errorf("catalog schemas count %d does not match contract schema count %d", len(assets), len(expected))
	}
	for schemaRef, digest := range expected {
		assetPath, ok := assets[schemaRef]
		if !ok {
			return fmt.Errorf("contract schema ref %q is missing from catalog", schemaRef)
		}
		data, err := readRegularAsset(filesystem, assetPath)
		if err != nil {
			return fmt.Errorf("schema %q: %w", schemaRef, err)
		}
		var document struct {
			ID string `json:"$id"`
		}
		if err := json.Unmarshal(data, &document); err != nil {
			return fmt.Errorf("decode schema %q: %w", schemaRef, err)
		}
		if document.ID != schemaRef {
			return fmt.Errorf("catalog schema ref %q does not match schema $id %q", schemaRef, document.ID)
		}
		sum := sha256.Sum256(data)
		actualDigest := "sha256:" + hex.EncodeToString(sum[:])
		if actualDigest != digest {
			return fmt.Errorf("schema %q digest %q does not match contract digest %q", schemaRef, actualDigest, digest)
		}
	}
	return nil
}

func readRegularAsset(filesystem fs.FS, assetPath string) ([]byte, error) {
	if !validAssetPath(assetPath) {
		return nil, fmt.Errorf("asset path %q is invalid", assetPath)
	}
	if links, ok := filesystem.(fs.ReadLinkFS); ok {
		var current string
		parts := strings.Split(assetPath, "/")
		for i, part := range parts {
			current = path.Join(current, part)
			info, err := links.Lstat(current)
			if err != nil {
				return nil, err
			}
			if info.Mode()&fs.ModeSymlink != 0 {
				return nil, fmt.Errorf("asset path component %q must not be a symbolic link", current)
			}
			if i < len(parts)-1 && !info.IsDir() {
				return nil, fmt.Errorf("asset path component %q must be a directory", current)
			}
		}
	}
	info, err := fs.Stat(filesystem, assetPath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("asset path %q must be a regular file", assetPath)
	}
	return fs.ReadFile(filesystem, assetPath)
}

func validAssetPath(value string) bool {
	return value != "" &&
		path.Clean(value) == value &&
		!path.IsAbs(value) &&
		value != "." &&
		!strings.HasPrefix(value, "../") &&
		!strings.ContainsAny(value, "\\\x00\r\n\t")
}

func rejectTrailingJSON(decoder *json.Decoder) error {
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("provider contract catalog must contain one JSON document")
		}
		return fmt.Errorf("decode provider contract catalog trailing data: %w", err)
	}
	return nil
}
