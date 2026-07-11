package providercontract

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadCatalogDiscoversReleasedGitHubRunnerContract(t *testing.T) {
	catalog, err := LoadCatalog(os.DirFS(".."))
	if err != nil {
		t.Fatalf("load provider contract catalog: %v", err)
	}
	if catalog.Version != "1" || len(catalog.Contracts) != 1 {
		t.Fatalf("provider contract catalog = %+v", catalog)
	}
	ref := catalog.Contracts[0]
	if ref.ID != GitHubRunnerContractID ||
		ref.Path != GitHubRunnerContractPath || len(ref.Schemas) != 3 {
		t.Fatalf("GitHub runner contract ref drifted: %+v", ref)
	}
	wantSchemas := map[string]string{
		GitHubRunnerConfigSchemaRef: GitHubRunnerConfigSchemaPath,
		GitHubRunnerInputSchemaRef:  GitHubRunnerInputSchemaPath,
		GitHubRunnerOutputSchemaRef: GitHubRunnerOutputSchemaPath,
	}
	for _, schema := range ref.Schemas {
		if wantSchemas[schema.Ref] != schema.Path {
			t.Fatalf("unexpected schema asset: %+v", schema)
		}
		delete(wantSchemas, schema.Ref)
	}
	if len(wantSchemas) != 0 {
		t.Fatalf("missing schema assets: %+v", wantSchemas)
	}
}

func TestLoadCatalogRejectsUnknownAndTraversalFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		catalog string
	}{
		{
			name:    "unknown field",
			catalog: `{"version":"1","unknown":true,"contracts":[]}`,
		},
		{
			name:    "traversal path",
			catalog: `{"version":"1","contracts":[{"id":"github.runner_provider.v1","path":"../contract.json","schemas":[]}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := fstest.MapFS{
				CatalogPath: &fstest.MapFile{Data: []byte(tc.catalog)},
			}
			if _, err := LoadCatalog(filesystem); err == nil {
				t.Fatal("invalid provider contract catalog accepted")
			}
		})
	}
}

func TestLoadCatalogRejectsContractAndSchemaIntegrityDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(fstest.MapFS)
	}{
		{
			name: "contract identity",
			mutate: func(filesystem fstest.MapFS) {
				file := filesystem[GitHubRunnerContractPath]
				file.Data = []byte(strings.Replace(string(file.Data), `"contract_id": "github.runner_provider.v1"`, `"contract_id": "github.other.v1"`, 1))
			},
		},
		{
			name: "schema digest",
			mutate: func(filesystem fstest.MapFS) {
				file := filesystem[GitHubRunnerInputSchemaPath]
				file.Data = append(file.Data, '\n')
			},
		},
		{
			name: "contract directory",
			mutate: func(filesystem fstest.MapFS) {
				filesystem[GitHubRunnerContractPath] = &fstest.MapFile{Mode: fs.ModeDir}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filesystem := releasedCatalogFS(t)
			tc.mutate(filesystem)
			if _, err := LoadCatalog(filesystem); err == nil {
				t.Fatal("provider contract catalog integrity drift accepted")
			}
		})
	}
}

func TestLoadCatalogRejectsSymlinkedAssets(t *testing.T) {
	for _, target := range []string{CatalogPath, GitHubRunnerContractPath, "contracts", "schemas"} {
		t.Run(target, func(t *testing.T) {
			filesystem := releasedCatalogFS(t)
			root := t.TempDir()
			writeReleasedCatalogFS(t, root, filesystem)
			insidePath := filepath.Join(root, filepath.FromSlash(target))
			outsideRoot := t.TempDir()
			outsidePath := filepath.Join(outsideRoot, filepath.Base(target))
			info, err := os.Stat(insidePath)
			if err != nil {
				t.Fatalf("stat packaged path: %v", err)
			}
			if info.IsDir() {
				if err := copyCatalogDirectory(insidePath, outsidePath); err != nil {
					t.Fatalf("copy outside catalog directory: %v", err)
				}
				if err := os.RemoveAll(insidePath); err != nil {
					t.Fatalf("remove packaged directory: %v", err)
				}
			} else {
				data, err := os.ReadFile(insidePath)
				if err != nil {
					t.Fatalf("read packaged asset: %v", err)
				}
				if err := os.WriteFile(outsidePath, data, 0o600); err != nil {
					t.Fatalf("write outside asset: %v", err)
				}
				if err := os.Remove(insidePath); err != nil {
					t.Fatalf("remove packaged asset: %v", err)
				}
			}
			if err := os.Symlink(outsidePath, insidePath); err != nil {
				t.Skipf("symlinks unavailable: %v", err)
			}
			if _, err := LoadCatalog(os.DirFS(root)); err == nil {
				t.Fatalf("symlinked provider catalog path %q accepted", target)
			}
		})
	}
}

func releasedCatalogFS(t *testing.T) fstest.MapFS {
	t.Helper()
	filesystem := make(fstest.MapFS, 5)
	for _, name := range []string{
		CatalogPath,
		GitHubRunnerContractPath,
		GitHubRunnerConfigSchemaPath,
		GitHubRunnerInputSchemaPath,
		GitHubRunnerOutputSchemaPath,
	} {
		data, err := os.ReadFile(filepath.Join("..", filepath.FromSlash(name)))
		if err != nil {
			t.Fatalf("read released catalog asset %s: %v", name, err)
		}
		filesystem[name] = &fstest.MapFile{Data: data, Mode: 0o600}
	}
	return filesystem
}

func writeReleasedCatalogFS(t *testing.T, root string, filesystem fstest.MapFS) {
	t.Helper()
	for name, file := range filesystem {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatalf("create catalog asset directory: %v", err)
		}
		if err := os.WriteFile(target, file.Data, 0o600); err != nil {
			t.Fatalf("write catalog asset: %v", err)
		}
	}
}

func copyCatalogDirectory(source, target string) error {
	return filepath.WalkDir(source, func(sourcePath string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, sourcePath)
		if err != nil {
			return err
		}
		targetPath := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(targetPath, 0o700)
		}
		data, err := os.ReadFile(sourcePath)
		if err != nil {
			return err
		}
		return os.WriteFile(targetPath, data, 0o600)
	})
}
