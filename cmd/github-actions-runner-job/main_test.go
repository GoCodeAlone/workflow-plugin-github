package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestT915CommandRunsDynamicProviderEnvelopeThroughSidecarAndRunner(t *testing.T) {
	var tokenCalls, dispatchCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/registration-token":
			tokenCalls++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"runner-registration-token","expires_at":"2026-06-26T22:00:00Z"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			dispatchCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/config.args\"\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nprintf 'runner executed\\n' > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.log\"\n")
	workspace := t.TempDir()
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-actions-runner"},
	  "operation":"ephemeral_runner_job",
	  "input":{
	    "mode":"dispatch_then_wait",
	    "environment":"stg",
	    "os":"linux",
	    "worker_id":"worker-0123456789abcdef",
	    "task_id":"task-abcdef9876543210",
	    "organization":"GoCodeAlone",
	    "repository":"GoCodeAlone/workflow-compute",
	    "workflow":"dogfood.yml",
	    "ref":"main",
	    "runner_group":"ephemeral"
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if tokenCalls != 1 || dispatchCalls != 1 {
		t.Fatalf("sidecar calls: token=%d dispatch=%d", tokenCalls, dispatchCalls)
	}
	configArgs := readFile(t, filepath.Join(workspace, "config.args"))
	for _, want := range []string{
		"--url\nhttps://github.com/GoCodeAlone",
		"--token\nrunner-registration-token",
		"--name\nwfc-stg-ghp-linux-01234567-abcdef98",
		"--runnergroup\nephemeral",
		"--labels\nself-hosted,linux,wfc-stg-ghp-linux-01234567-abcdef98,wfc-ghp-stg,wfc-ghp-ephemeral",
		"--ephemeral",
	} {
		if !strings.Contains(configArgs, want) {
			t.Fatalf("config args missing %q:\n%s", want, configArgs)
		}
	}
	if got := readFile(t, filepath.Join(workspace, "run.log")); !strings.Contains(got, "runner executed") {
		t.Fatalf("run script did not execute: %q", got)
	}
	var result struct {
		Artifacts []string `json:"artifacts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	if len(result.Artifacts) != 1 || result.Artifacts[0] != "github-runner-proof.json" {
		t.Fatalf("artifacts = %#v", result.Artifacts)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"wfc-stg-ghp-linux-01234567-abcdef98", "task-abcdef9876543210", "removed"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915CommandRequiresSidecarEnvironmentForDynamicEnvelope(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-1",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-actions-runner"},
	  "operation":"ephemeral_runner_job",
	  "input":{"environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone"}
	}`), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "COMPUTE_GITHUB_RUNNER_PROVIDER_URL") {
		t.Fatalf("error = %v, want missing sidecar env", err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
