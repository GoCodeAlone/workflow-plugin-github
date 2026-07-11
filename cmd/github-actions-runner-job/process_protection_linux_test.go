//go:build linux

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

const providerProtectionHelperMode = "GO_WANT_GITHUB_PROVIDER_PROTECTION_HELPER"

func TestT915ProviderProcessSecretsAreNotReadableByRunnerChildren(t *testing.T) {
	switch os.Getenv(providerProtectionHelperMode) {
	case "parent":
		if _, err := newSidecarClientFromEnv(); err != nil {
			os.Exit(40)
		}
		command := exec.Command(os.Args[0], "-test.run=^TestT915ProviderProcessSecretsAreNotReadableByRunnerChildren$")
		command.Env = append(environmentWithout(providerProtectionHelperMode), providerProtectionHelperMode+"=probe")
		if err := command.Run(); err != nil {
			os.Exit(41)
		}
		os.Exit(0)
	case "probe":
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", os.Getppid()))
		if err != nil {
			os.Exit(0)
		}
		if bytes.Contains(data, []byte("provider-parent-secret")) {
			os.Exit(42)
		}
		os.Exit(43)
	}

	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "https://provider.invalid")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-parent-secret")
	command := exec.Command(os.Args[0], "-test.run=^TestT915ProviderProcessSecretsAreNotReadableByRunnerChildren$")
	command.Env = append(os.Environ(), providerProtectionHelperMode+"=parent")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("runner child could inspect provider parent: %v: %s", err, strings.TrimSpace(string(output)))
	}
}

func environmentWithout(name string) []string {
	prefix := strings.ToUpper(name) + "="
	filtered := make([]string, 0, len(os.Environ()))
	for _, entry := range runnerProcessEnvironment() {
		if strings.HasPrefix(strings.ToUpper(entry), prefix) {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}
