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
