package githubplugin_test

import (
	"os"
	"strings"
	"testing"
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
}

func TestReleasePublishesGitHubActionsRunnerJobImage(t *testing.T) {
	data, err := os.ReadFile(".github/workflows/release.yml")
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	workflow := string(data)

	for _, want := range []string{
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
		if !strings.Contains(workflow, want) {
			t.Fatalf("release workflow must include %q so the runner-job image is available to workflow-compute agents", want)
		}
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
