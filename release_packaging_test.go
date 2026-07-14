package githubplugin_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/GoCodeAlone/workflow-plugin-github/internal/retainedprovider"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

func TestReleaseArchiveIncludesGitHubRunnerProvider(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	text := string(data)

	if !goreleaserBuildIncludes(text, "github-runner-provider", []string{
		"main: ./cmd/github-runner-provider",
		"binary: github-runner-provider",
	}) {
		t.Fatal("release config must build the versioned github-runner-provider binary")
	}

	if !goreleaserArchiveIncludesBuild(text, "workflow-plugin-github", "github-runner-provider") {
		t.Fatal("release archive must include github-runner-provider so wfctl plugin install/fetch can provide it")
	}

	if _, err := os.Stat("cmd/github-runner-provider/main.go"); err != nil {
		t.Fatalf("github-runner-provider command must exist: %v", err)
	}
}

func TestReleaseArchiveIncludesRetainedProviderOperations(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	archive := listItemWithID(topLevelSection(string(data), "archives:"), "workflow-plugin-github")
	if !strings.Contains(archive, "- README.md") {
		t.Fatal("release archive must include retained-provider operator documentation")
	}

	readmeData, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatalf("read README.md: %v", err)
	}
	readme := string(readmeData)
	for _, want := range []string{
		"loginctl enable-linger",
		"podman info --format '{{.Host.Security.Rootless}}'",
		"examples/github-runner-retained-config.json",
		"schemas/github-runner-retained-config.schema.json",
		"github-runner-provider retained install -config",
		"GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN",
		"GITHUB_RUNNER_PROVIDER_TOKEN",
		"github-runner-provider retained status -config",
		"github-runner-provider retained uninstall -config",
		"github-runner-provider retained uninstall -config <absolute-config-path> --purge",
		"credential rotation",
		"github-runner-provider retained recover -config",
		"autonomous refresh",
		"GitHub workflow output is orchestration evidence only",
		"STG task, proof, log, and artifact APIs",
	} {
		if !strings.Contains(readme, want) {
			t.Fatalf("retained-provider operations documentation is missing %q", want)
		}
	}
}

func TestReleaseArchiveIncludesRetainedProviderConfigContract(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	archive := listItemWithID(topLevelSection(string(data), "archives:"), "workflow-plugin-github")
	for _, path := range []string{
		"schemas/github-runner-retained-config.schema.json",
		"examples/github-runner-retained-config.json",
	} {
		if !strings.Contains(archive, "- "+path) {
			t.Fatalf("release archive must include retained-provider config contract %q", path)
		}
	}

	example, err := os.ReadFile("examples/github-runner-retained-config.json")
	if err != nil {
		t.Fatalf("read retained-provider config example: %v", err)
	}
	home := t.TempDir()
	runtimeExample := bytes.ReplaceAll(example, []byte("/home/wfcompute"), []byte(filepath.ToSlash(home)))
	config, err := retainedprovider.DecodeConfig(bytes.NewReader(runtimeExample), home)
	if err != nil {
		t.Fatalf("retained-provider config example must pass the shipped runtime decoder: %v", err)
	}
	if config.ProtocolVersion != retainedprovider.ConfigProtocolVersion || config.PluginID != retainedprovider.GitHubPluginID {
		t.Fatalf("retained-provider config example has wrong identity: %+v", config)
	}
	schema, err := os.ReadFile("schemas/github-runner-retained-config.schema.json")
	if err != nil {
		t.Fatalf("read retained-provider config schema: %v", err)
	}
	if !json.Valid(schema) {
		t.Fatal("retained-provider config schema must be valid JSON")
	}
	compiled, err := jsonschema.NewCompiler().Compile("schemas/github-runner-retained-config.schema.json")
	if err != nil {
		t.Fatalf("compile retained-provider config schema: %v", err)
	}
	var exampleDocument any
	if err := json.Unmarshal(example, &exampleDocument); err != nil {
		t.Fatalf("decode retained-provider config example for schema validation: %v", err)
	}
	if err := compiled.Validate(exampleDocument); err != nil {
		t.Fatalf("retained-provider config example must pass the shipped schema: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(example, &fields); err != nil {
		t.Fatalf("decode retained-provider config fields: %v", err)
	}
	validateFields := func(fields map[string]json.RawMessage) error {
		data, err := json.Marshal(fields)
		if err != nil {
			t.Fatalf("marshal retained-provider schema fixture: %v", err)
		}
		var document any
		if err := json.Unmarshal(data, &document); err != nil {
			t.Fatalf("decode retained-provider schema fixture: %v", err)
		}
		return compiled.Validate(document)
	}
	delete(fields, "worker_id")
	if err := validateFields(fields); err == nil {
		t.Fatal("retained-provider config schema accepted an example without worker_id")
	}
	if err := json.Unmarshal(example, &fields); err != nil {
		t.Fatalf("reset retained-provider config fields: %v", err)
	}
	delete(fields, "systemctl_path")
	if err := validateFields(fields); err == nil {
		t.Fatal("retained-provider config schema accepted an example without systemctl_path")
	}
	if err := json.Unmarshal(example, &fields); err != nil {
		t.Fatalf("reset retained-provider config fields: %v", err)
	}
	labels := make([]string, 64)
	for index := range labels {
		labels[index] = fmt.Sprintf("label-%d", index)
	}
	fields["labels"], _ = json.Marshal(labels)
	if err := validateFields(fields); err != nil {
		t.Fatalf("retained-provider config schema rejected 64 labels: %v", err)
	}
	labels = append(labels, "label-64")
	fields["labels"], _ = json.Marshal(labels)
	if err := validateFields(fields); err == nil {
		t.Fatal("retained-provider config schema accepted 65 labels")
	}
	fields["labels"] = json.RawMessage(`["self-hosted"]`)
	fields["podman_path"], _ = json.Marshal("/podman")
	if err := validateFields(fields); err != nil {
		t.Fatalf("retained-provider config schema rejected runtime-valid root Podman path: %v", err)
	}
	fields["podman_path"], _ = json.Marshal("/usr/bin/podman\t")
	if err := validateFields(fields); err == nil {
		t.Fatal("retained-provider config schema accepted a control character in an absolute path")
	}
	for _, tc := range []struct {
		name      string
		field     string
		value     string
		stableURL string
	}{
		{name: "stable underscore", field: "stable_container", value: "provider_name", stableURL: "https://provider_name:18090"},
		{name: "candidate uppercase", field: "candidate_container", value: "Provider-Candidate"},
		{name: "stable trailing dot", field: "stable_container", value: "provider.", stableURL: "https://provider.:18090"},
		{name: "probe suffix exceeds label", field: "candidate_container", value: strings.Repeat("c", 63)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal(example, &fields); err != nil {
				t.Fatalf("reset retained-provider config fields: %v", err)
			}
			fields[tc.field], _ = json.Marshal(tc.value)
			if tc.stableURL != "" {
				fields["provider_url"], _ = json.Marshal(tc.stableURL)
			}
			if err := validateFields(fields); err == nil {
				t.Fatalf("retained-provider config schema accepted non-DNS %s %q", tc.field, tc.value)
			}
		})
	}
	for _, field := range []string{"protocol_version", "worker_id", "provider_marker_path", "podman_path", "systemctl_path", "loginctl_path", "ref", "refresh_interval_seconds"} {
		if !bytes.Contains(schema, []byte(`"`+field+`"`)) {
			t.Fatalf("retained-provider config schema is missing %q", field)
		}
	}
}

func TestGitHubRunnerProviderReleaseBuildInjectsVersion(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	build := listItemWithID(topLevelSection(string(data), "builds:"), "github-runner-provider")
	if !strings.Contains(build, "-X github.com/GoCodeAlone/workflow-plugin-github/internal.Version={{.Version}}") {
		t.Fatalf("github-runner-provider release build must inject internal.Version:\n%s", build)
	}
}

func TestReleaseArchiveIncludesGitHubActionsRunnerJob(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	text := string(data)

	if !goreleaserBuildIncludes(text, "github-actions-runner-job", []string{
		"main: ./cmd/github-actions-runner-job",
		"binary: github-actions-runner-job",
	}) {
		t.Fatal("release config must build the versioned github-actions-runner-job binary")
	}

	if !goreleaserArchiveIncludesBuild(text, "workflow-plugin-github", "github-actions-runner-job") {
		t.Fatal("release archive must include github-actions-runner-job so workflow-compute agents receive it through provider package delivery")
	}

	if _, err := os.Stat("cmd/github-actions-runner-job/main.go"); err != nil {
		t.Fatalf("github-actions-runner-job command must exist: %v", err)
	}
	build := listItemWithID(topLevelSection(string(data), "builds:"), "github-actions-runner-job")
	if !strings.Contains(build, "goos:\n      - linux") || strings.Contains(build, "      - darwin") || strings.Contains(build, "      - windows") {
		t.Fatalf("github-actions-runner-job release targets must be Linux-only:\n%s", build)
	}
	archive := listItemWithID(topLevelSection(string(data), "archives:"), "workflow-plugin-github")
	if !strings.Contains(archive, "allow_different_binary_count: true") {
		t.Fatal("release archive must explicitly allow the Linux-only runner job to be absent from other platform archives")
	}
}

func TestReleaseArchiveIncludesProviderContractsAndSchemas(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	archive := listItemWithID(topLevelSection(string(data), "archives:"), "workflow-plugin-github")
	for _, want := range []string{
		"provider.contracts.json",
		"schemas/github-runner-provider.schema.json",
		"schemas/github-runner-ephemeral-job-input.schema.json",
		"schemas/github-runner-ephemeral-job-output.schema.json",
	} {
		if !strings.Contains(archive, "- "+want) {
			t.Fatalf("release archive must include %q so consumers receive provider contract metadata", want)
		}
	}
	if !strings.Contains(archive, "GORELEASER_RENDER_DIR }}/contracts/github-runner-provider.json") || !strings.Contains(archive, "dst: contracts/github-runner-provider.json") {
		t.Fatal("release archive must map the rendered provider contract to its canonical archive path")
	}
}

func TestReleaseRendersProviderContractVersion(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	config := string(data)
	if !strings.Contains(config, "contracts/github-runner-provider.json") || !strings.Contains(config, `v{{ .Version }}`) {
		t.Fatal("release hooks must render the provider contract version from the release tag")
	}
}

func TestReleaseVersionRenderingDoesNotMutateTrackedManifests(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	config := string(data)
	for _, forbidden := range []string{"/' plugin.json", "/' contracts/github-runner-provider.json"} {
		if strings.Contains(config, forbidden) {
			t.Fatalf("release hooks mutate tracked source manifest via %q", forbidden)
		}
	}
	for _, want := range []string{"GORELEASER_RENDER_DIR"} {
		if !strings.Contains(config, want) {
			t.Fatalf("hermetic release rendering missing %q", want)
		}
	}
	workflowData, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	workflow := string(workflowData)
	for _, want := range []string{"GORELEASER_RENDER_DIR", ".goreleaser-rendered-${{ github.run_id }}-${{ github.run_attempt }}", "if: always()"} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("release workflow must clean hermetic render staging and is missing %q", want)
		}
	}
	ignoreData, err := os.ReadFile(".gitignore")
	if err != nil {
		t.Fatalf("read .gitignore: %v", err)
	}
	if !strings.Contains(string(ignoreData), ".goreleaser-rendered-*") {
		t.Fatal("unique repo-relative release render directories must remain ignored while GoReleaser assembles archives")
	}
}

func TestReleaseDependencyCheckDoesNotMutateTrackedModules(t *testing.T) {
	data, err := os.ReadFile(".goreleaser.yaml")
	if err != nil {
		t.Fatalf("read .goreleaser.yaml: %v", err)
	}
	hooks := topLevelSection(string(data), "before:")
	if !strings.Contains(hooks, "go mod tidy -diff") {
		t.Fatal("release hooks must check module tidiness without rewriting go.mod or go.sum")
	}
	if strings.Contains(hooks, "- go mod tidy\n") {
		t.Fatal("release hooks must not mutate tracked module files")
	}
}

func TestReleaseValidatesRenderedArchiveInsteadOfUnversionedSource(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	workflow := string(data)
	for _, want := range []string{
		"workflow-plugin-github-linux-amd64.tar.gz",
		`tar -xzf "${archive}" -C "${release_dir}"`,
		`--release-dir "${release_dir}"`,
	} {
		if !strings.Contains(workflow, want) {
			t.Fatalf("post-build contract verification must inspect the rendered archive and is missing %q", want)
		}
	}
	if strings.Contains(workflow, "--release-dir . .") {
		t.Fatal("post-build contract verification must not fall back to unversioned source manifests")
	}
}

func TestReleasePublishesGitHubActionsRunnerJobImage(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	workflow := string(data)
	topPermissions := topLevelSection(workflow, "permissions:")
	if strings.Contains(topPermissions, "packages:") {
		t.Fatal("release workflow must not grant packages permissions at workflow scope")
	}

	job := workflowJobSection(workflow, "publish-runner-job-image")
	if job == "" {
		t.Fatal("release workflow must define publish-runner-job-image job")
	}

	for _, want := range []string{
		"needs: release",
		"!contains(github.ref_name, '-')",
		"github.repository == 'GoCodeAlone/workflow-plugin-github'",
		"packages: write",
		"GOOS=linux GOARCH=amd64 go build -o dist/github-actions-runner-job-linux-amd64 ./cmd/github-actions-runner-job",
		"GOOS=linux GOARCH=arm64 go build -o dist/github-actions-runner-job-linux-arm64 ./cmd/github-actions-runner-job",
		"docker/login-action@",
		"docker/setup-buildx-action@",
		"docker/build-push-action@",
		"context: .",
		"file: cmd/github-actions-runner-job/Dockerfile",
		"platforms: linux/amd64,linux/arm64",
		"push: true",
		"ghcr.io/gocodealone/workflow-plugin-github/github-actions-runner-job:${{ github.ref_name }}",
		"ghcr.io/gocodealone/workflow-plugin-github/github-actions-runner-job:latest",
	} {
		if !strings.Contains(job, want) {
			t.Fatalf("publish-runner-job-image job must include %q so the runner-job image is available to workflow-compute agents", want)
		}
	}
}

func TestGitHubActionsRunnerJobImageCarriesCompressedRunnerArchive(t *testing.T) {
	data, err := os.ReadFile("cmd/github-actions-runner-job/Dockerfile")
	if err != nil {
		t.Fatalf("read runner job Dockerfile: %v", err)
	}
	dockerfile := string(data)
	if strings.Contains(dockerfile, "ghcr.io/actions/actions-runner") {
		t.Fatal("runner job image must not inherit the expanded actions-runner image; docker save exceeds workflow-compute package limits")
	}
	for _, want := range []string{
		"ARG ACTIONS_RUNNER_VERSION=",
		"curl --fail --show-error --location --retry 5 --retry-all-errors --retry-delay 2 --connect-timeout 15",
		"actions-runner-linux-${runner_arch}-${ACTIONS_RUNNER_VERSION}.tar.gz",
		`x64) runner_sha256="18f8f68ed1892854ff2ab1bab4fcaa2f5abeedc98093b6cb13638991725cab74"`,
		`arm64) runner_sha256="69ac7e5692f877189e7dddf4a1bb16cbbd6425568cd69a0359895fac48b9ad3b"`,
		`echo "${runner_sha256}  /opt/actions-runner/actions-runner.tar.gz" | sha256sum -c -`,
		"tar -tzf /opt/actions-runner/actions-runner.tar.gz ./config.sh ./run.sh >/dev/null",
		"mkdir -p /workspace",
		"chown runner:runner /workspace",
		"GITHUB_ACTIONS_RUNNER_ARCHIVE=/opt/actions-runner/actions-runner.tar.gz",
		"GITHUB_ACTIONS_RUNNER_DIR=/workspace/.github-actions-runner",
		"WORKDIR /workspace",
		"COPY --chmod=0755 cmd/github-actions-runner-job/entrypoint.sh /usr/local/bin/github-actions-runner-job-entrypoint",
		`ENTRYPOINT ["/usr/local/bin/github-actions-runner-job-entrypoint"]`,
	} {
		if !strings.Contains(dockerfile, want) {
			t.Fatalf("runner job Dockerfile must include %q", want)
		}
	}

	entrypoint, err := os.ReadFile("cmd/github-actions-runner-job/entrypoint.sh")
	if err != nil {
		t.Fatalf("read runner job entrypoint: %v", err)
	}
	entrypointText := string(entrypoint)
	for _, want := range []string{
		"${GITHUB_ACTIONS_RUNNER_ARCHIVE:-/opt/actions-runner/actions-runner.tar.gz}",
		"${GITHUB_ACTIONS_RUNNER_DIR:-/workspace/.github-actions-runner}",
		`[ ! -f "$archive" ]`,
		"runner archive not found",
		`mkdir -p "$(dirname "$runner_dir")"`,
		`tar --no-same-owner -xzf "$archive" -C "$runner_dir"`,
		`exec /usr/local/bin/github-actions-runner-job "$@"`,
	} {
		if !strings.Contains(entrypointText, want) {
			t.Fatalf("runner job entrypoint must include %q", want)
		}
	}
	if strings.Contains(dockerfile, "WORKDIR /home/runner") ||
		strings.Contains(dockerfile, "GITHUB_ACTIONS_RUNNER_DIR=/home/runner/actions-runner") ||
		strings.Contains(entrypointText, "GITHUB_ACTIONS_RUNNER_DIR:-/home/runner/actions-runner") {
		t.Fatal("runner job image must extract the runner under the writable /workspace mount, not /home/runner")
	}
}

func TestReleaseArchiveCheckRejectsProviderBuildOutsideArchive(t *testing.T) {
	config := `
builds:
  - id: workflow-plugin-github
    main: ./cmd/workflow-plugin-github
  - id: github-runner-provider
    main: ./cmd/github-runner-provider
    binary: github-runner-provider

archives:
  - id: workflow-plugin-github
    ids:
      - workflow-plugin-github
`

	if goreleaserArchiveIncludesBuild(config, "workflow-plugin-github", "github-runner-provider") {
		t.Fatal("archive check must reject configs that build github-runner-provider without packaging it")
	}
}

func goreleaserBuildIncludes(config, id string, required []string) bool {
	builds := topLevelSection(config, "builds:")
	build := listItemWithID(builds, id)
	if build == "" {
		return false
	}
	for _, want := range required {
		if !strings.Contains(build, want) {
			return false
		}
	}
	return true
}

func goreleaserArchiveIncludesBuild(config, archiveID, buildID string) bool {
	archives := topLevelSection(config, "archives:")
	archive := listItemWithID(archives, archiveID)
	return strings.Contains(archive, "ids:") && strings.Contains(archive, "- "+buildID)
}

func topLevelSection(config, header string) string {
	lines := strings.Split(config, "\n")
	for i, line := range lines {
		if line != header {
			continue
		}

		var section []string
		for _, next := range lines[i+1:] {
			if next != "" && !strings.HasPrefix(next, " ") && !strings.HasPrefix(next, "\t") {
				break
			}
			section = append(section, next)
		}
		return strings.Join(section, "\n")
	}
	return ""
}

func workflowJobSection(config, jobName string) string {
	lines := strings.Split(config, "\n")
	marker := "  " + jobName + ":"
	for i, line := range lines {
		if line != marker {
			continue
		}

		item := []string{line}
		for _, next := range lines[i+1:] {
			if strings.HasPrefix(next, "  ") && !strings.HasPrefix(next, "    ") {
				break
			}
			item = append(item, next)
		}
		return strings.Join(item, "\n")
	}
	return ""
}

func listItemWithID(section, id string) string {
	lines := strings.Split(section, "\n")
	marker := "- id: " + id
	for i, line := range lines {
		if strings.TrimSpace(line) != marker {
			continue
		}

		item := []string{line}
		for _, next := range lines[i+1:] {
			if strings.HasPrefix(next, "  - id: ") {
				break
			}
			item = append(item, next)
		}
		return strings.Join(item, "\n")
	}
	return ""
}
