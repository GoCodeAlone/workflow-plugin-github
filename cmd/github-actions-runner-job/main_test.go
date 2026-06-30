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
	"time"
)

func TestT915CommandRunsDynamicProviderEnvelopeThroughSidecarAndRunner(t *testing.T) {
	var tokenCalls, dispatchCalls, deleteCalls int
	workspace := t.TempDir()
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
			runStarted := filepath.Join(workspace, "run.started")
			for range 20 {
				if _, err := os.Stat(runStarted); err == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if _, err := os.Stat(runStarted); err != nil {
				http.Error(w, "runner listener was not started before dispatch", http.StatusConflict)
				return
			}
			var body struct {
				Ref    string            `json:"ref"`
				Inputs map[string]string `json:"inputs"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode dispatch body: %v", err)
			}
			if body.Ref != "main" {
				t.Fatalf("dispatch ref: got %q", body.Ref)
			}
			wantLabels := `["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]`
			for key, want := range map[string]string{
				"runner_profile":                 "provider",
				"allow_github_hosted_fallback":   "false",
				"runner_labels_json":             wantLabels,
				"stg_task_id":                    "task-abcdef9876543210",
				"workflow_compute_provider_task": "task-abcdef9876543210",
				"custom":                         "kept",
			} {
				if got := body.Inputs[key]; got != want {
					t.Fatalf("dispatch input %s: got %q want %q; body=%#v", key, got, want, body.Inputs)
				}
			}
			for _, forbidden := range []string{"Runner_Profile", "ALLOW_GITHUB_HOSTED_FALLBACK", "Custom"} {
				if _, ok := body.Inputs[forbidden]; ok {
					t.Fatalf("dispatch inputs must normalize caller keys, found %q in %#v", forbidden, body.Inputs)
				}
			}
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/config.args\"\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.args\"\ntouch \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.started\"\nwhile [ ! -f \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/dispatch.seen\" ]; do sleep 0.1; done\n(for i in $(seq 1 50); do printf 'stdout-%s\\n' \"$i\"; done) &\n(for i in $(seq 1 50); do printf 'stderr-%s\\n' \"$i\" >&2; done) &\nwait\nprintf 'runner executed\\n' > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.log\"\n")
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
	    "runner_group":"ephemeral",
	    "workflow_inputs":{"Custom":"kept","Runner_Profile":"manual","ALLOW_GITHUB_HOSTED_FALLBACK":"true"}
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if tokenCalls != 1 || dispatchCalls != 1 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: token=%d dispatch=%d delete=%d", tokenCalls, dispatchCalls, deleteCalls)
	}
	configArgs := readFile(t, filepath.Join(workspace, "config.args"))
	for _, want := range []string{
		"--url\nhttps://github.com/GoCodeAlone",
		"--token\nrunner-registration-token",
		"--name\nwfc-stg-ghp-linux-abcdef987249-543210f71ee4",
		"--runnergroup\nephemeral",
		"--labels\nself-hosted,linux,wfc-stg-ghp-linux-abcdef987249-543210f71ee4,wfc-ghp-stg,wfc-ghp-ephemeral",
		"--ephemeral",
	} {
		if !strings.Contains(configArgs, want) {
			t.Fatalf("config args missing %q:\n%s", want, configArgs)
		}
	}
	if got := readFile(t, filepath.Join(workspace, "run.log")); !strings.Contains(got, "runner executed") {
		t.Fatalf("run script did not execute: %q", got)
	}
	if got := readFile(t, filepath.Join(workspace, "run.args")); strings.TrimSpace(got) != "--once" {
		t.Fatalf("run args: got %q want --once", got)
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
	for _, want := range []string{"wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "task-abcdef9876543210", "removed"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915CommandTreatsRunnerSuccessMarkerAsCompletion(t *testing.T) {
	var tokenCalls, dispatchCalls, deleteCalls int
	workspace := t.TempDir()
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
			for range 20 {
				if _, err := os.Stat(filepath.Join(workspace, "run.started")); err == nil {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if _, err := os.Stat(filepath.Join(workspace, "run.started")); err != nil {
				http.Error(w, "runner listener was not started before dispatch", http.StatusConflict)
				return
			}
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\ntouch \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.started\"\nwhile [ ! -f \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/dispatch.seen\" ]; do sleep 0.1; done\nprintf '2026-06-30T12:58:48Z: Job provider-target completed with result: Succeeded\\n'\ntouch \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/success.marker\"\nexec sleep 1000\n")
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
	    "runner_group":"ephemeral",
	    "timeout_seconds":2
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if tokenCalls != 1 || dispatchCalls != 1 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: token=%d dispatch=%d delete=%d", tokenCalls, dispatchCalls, deleteCalls)
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
	for _, want := range []string{"wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "task-abcdef9876543210", "removed"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915CommandTreatsGitHubJobAPIAsCompletion(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	var tokenCalls, dispatchCalls, runsCalls, jobsCalls, deleteCalls int
	workspace := t.TempDir()
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
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			runsCalls++
			if r.URL.Query().Get("created_after") == "" {
				t.Fatalf("created_after query is required")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28449657934,"status":"completed","conclusion":"success","html_url":"https://github.com/GoCodeAlone/workflow-compute/actions/runs/28449657934"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934/jobs":
			jobsCalls++
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84308154551,"run_id":28449657934,"status":"completed","conclusion":"success","runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-ghp-stg"]}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\ntouch \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.started\"\nexec sleep 1000\n")
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
	    "runner_group":"ephemeral",
	    "timeout_seconds":2
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if tokenCalls != 1 || dispatchCalls != 1 || runsCalls == 0 || jobsCalls == 0 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: token=%d dispatch=%d runs=%d jobs=%d delete=%d", tokenCalls, dispatchCalls, runsCalls, jobsCalls, deleteCalls)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28449657934", "84308154551", "removed", "wfc-stg-ghp-linux-abcdef987249-543210f71ee4"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915CommandRejectsFailedGitHubJobAPICompletion(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/registration-token":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"token":"runner-registration-token","expires_at":"2026-06-26T22:00:00Z"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28449657935,"status":"completed","conclusion":"failure"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657935/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84308154552,"run_id":28449657935,"status":"completed","conclusion":"failure","runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nexit 0\n")
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
	    "runner_group":"ephemeral",
	    "timeout_seconds":2
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "github workflow job failed") {
		t.Fatalf("expected failed GitHub job error, got %v\nstderr:\n%s", err, stderr.String())
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28449657935", "84308154552"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915RunnerCompletionMarkerRejectsFailedJob(t *testing.T) {
	for _, line := range []string{
		"2026-06-30T12:58:48Z: Job provider-target completed with result: Failed",
		"Job provider-target completed with result: Cancelled",
	} {
		result, ok := parseRunnerCompletion(line)
		if !ok {
			t.Fatalf("completion marker was not detected for %q", line)
		}
		if result.success {
			t.Fatalf("failed marker treated as success: %+v", result)
		}
	}
	result, ok := parseRunnerCompletion("Job provider-target completed with result: Succeeded")
	if !ok || !result.success {
		t.Fatalf("success marker not accepted: result=%+v ok=%v", result, ok)
	}
}

func TestT915CommandRejectsUnknownDynamicInputFields(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-1",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-actions-runner"},
	  "operation":"ephemeral_runner_job",
	  "input":{"environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","typo":true}
	}`), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want strict unknown field rejection", err)
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
