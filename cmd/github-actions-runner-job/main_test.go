package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

type testRoundTripFunc func(*http.Request) (*http.Response, error)

func (f testRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestT915CommandRunsDynamicProviderEnvelopeThroughSidecarAndRunner(t *testing.T) {
	var jitCalls, ackCalls, onlineCalls, dispatchCalls, deleteCalls int
	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			jitCalls++
			var body struct {
				RunnerGroup string   `json:"runner_group"`
				Labels      []string `json:"labels"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode preflight body: %v", err)
			}
			if body.RunnerGroup != "ephemeral" || !slices.Contains(body.Labels, "wfc-ghp-ephemeral") {
				t.Fatalf("preflight body = %+v", body)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","encoded_jit_config":"encoded-jit-config","ownership_token":"ownership-token","preflight":{"organization":"GoCodeAlone","runner_group":"ephemeral","runner_group_id":5,"ref":"main","resolved_workflow_path":".github/workflows/dogfood.yml","resolved_ref_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","runner_count_checked":4,"actions_enabled":true,"self_hosted_allowed":true}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42/ack":
			ackCalls++
			if _, err := os.Stat(filepath.Join(workspace, "run.started")); err == nil {
				t.Fatal("runner listener started before JIT ownership ACK")
			}
			var body struct {
				OwnershipToken string `json:"ownership_token"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OwnershipToken != "ownership-token" {
				t.Fatalf("ownership ACK body=%+v err=%v", body, err)
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			onlineCalls++
			status := "offline"
			if _, err := os.Stat(filepath.Join(workspace, "run.started")); err == nil {
				status = "online"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": 42, "name": "wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "status": status,
				"labels": []string{"self-hosted", "linux", "wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			dispatchCalls++
			if onlineCalls == 0 {
				http.Error(w, "runner online status was not checked before dispatch", http.StatusConflict)
				return
			}
			if _, err := os.Stat(filepath.Join(workspace, "run.started")); err != nil {
				http.Error(w, "runner listener was not started before dispatch", http.StatusConflict)
				return
			}
			var body struct {
				Ref                  string            `json:"ref"`
				Inputs               map[string]string `json:"inputs"`
				ExpectedWorkflowPath string            `json:"expected_workflow_path"`
				ExpectedHeadSHA      string            `json:"expected_head_sha"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode dispatch body: %v", err)
			}
			if body.Ref != "main" {
				t.Fatalf("dispatch ref: got %q", body.Ref)
			}
			if body.ExpectedHeadSHA != strings.Repeat("a", 40) {
				t.Fatalf("dispatch expected head SHA: got %q", body.ExpectedHeadSHA)
			}
			if body.ExpectedWorkflowPath != ".github/workflows/dogfood.yml" {
				t.Fatalf("dispatch expected workflow path: got %q", body.ExpectedWorkflowPath)
			}
			wantLabels := `["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]`
			for key, want := range map[string]string{
				"runner_profile":               "provider",
				"allow_github_hosted_fallback": "false",
				"runner_labels_json":           wantLabels,
				"custom":                       "kept",
			} {
				if got := body.Inputs[key]; got != want {
					t.Fatalf("dispatch input %s: got %q want %q; body=%#v", key, got, want, body.Inputs)
				}
			}
			for _, forbidden := range []string{"Runner_Profile", "ALLOW_GITHUB_HOSTED_FALLBACK", "Custom", "stg_task_id", "workflow_compute_provider_task"} {
				if _, ok := body.Inputs[forbidden]; ok {
					t.Fatalf("dispatch inputs must normalize caller keys, found %q in %#v", forbidden, body.Inputs)
				}
			}
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			writeWorkflowDispatchResponse(w, 28449657934)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":28449657934,"path":".github/workflows/dogfood.yml@refs/heads/main","head_branch":"main","head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28449657934,"status":"completed","conclusion":"success"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84308154551,"run_id":28449657934,"status":"completed","conclusion":"success","runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.args\"\ntouch \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.started\"\nwhile [ ! -f \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/dispatch.seen\" ]; do sleep 0.1; done\n(for i in $(seq 1 50); do printf 'stdout-%s\\n' \"$i\"; done) &\n(for i in $(seq 1 50); do printf 'stderr-%s\\n' \"$i\" >&2; done) &\nwait\nmkdir -p \"$GITHUB_ACTIONS_RUNNER_DIR/_work/workflow-compute/workflow-compute/build\"\nprintf 'real workload output\\n' > \"$GITHUB_ACTIONS_RUNNER_DIR/_work/workflow-compute/workflow-compute/build/result.txt\"\nprintf 'runner executed\\n' > \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/run.log\"\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "workflow_inputs":{"custom":"kept","runner_profile":"manual","allow_github_hosted_fallback":"true"},
	    "artifact_paths":["build/result.txt"]
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if jitCalls != 1 || ackCalls != 1 || onlineCalls == 0 || dispatchCalls != 1 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: jit=%d ack=%d online=%d dispatch=%d delete=%d", jitCalls, ackCalls, onlineCalls, dispatchCalls, deleteCalls)
	}
	if got := readFile(t, filepath.Join(workspace, "run.log")); !strings.Contains(got, "runner executed") {
		t.Fatalf("run script did not execute: %q", got)
	}
	if got := readFile(t, filepath.Join(workspace, "run.args")); strings.TrimSpace(got) != "--jitconfig\nencoded-jit-config" {
		t.Fatalf("run args: got %q want JIT config", got)
	}
	var result struct {
		Artifacts []string `json:"artifacts"`
	}
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode stdout: %v\n%s", err, stdout.String())
	}
	if !slices.Equal(result.Artifacts, []string{"github-runner-proof.json", "github-workload-outputs.tar.gz"}) {
		t.Fatalf("artifacts = %#v", result.Artifacts)
	}
	archiveFiles := readTarGzFiles(t, filepath.Join(workspace, "github-workload-outputs.tar.gz"))
	if got := string(archiveFiles["build/result.txt"]); got != "real workload output\n" {
		t.Fatalf("archived workload output = %q", got)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "task-abcdef9876543210", "28449657934", "84308154551", "completed", "removed", `"runner_count_checked": 4`, `"actions_enabled": true`, `"self_hosted_allowed": true`} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestV918CommandPreservesRejectedJITPreflightAndCleansExactRunner(t *testing.T) {
	workspace := t.TempDir()
	var deleteCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","encoded_jit_config":"encoded-jit-config","ownership_token":"ownership-token","preflight":{"organization":"GoCodeAlone","runner_group":"default","runner_group_id":5,"runner_count_checked":4,"actions_enabled":true,"self_hosted_allowed":true}}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\ntouch started\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "JIT preflight response did not match organization runner request") {
		t.Fatalf("run error = %v, want mismatched preflight rejection", err)
	}
	if deleteCalls != 1 {
		t.Fatalf("exact JIT runner cleanup calls = %d, want 1", deleteCalls)
	}
	if _, err := os.Stat(filepath.Join(runnerDir, "started")); !os.IsNotExist(err) {
		t.Fatalf("runner started after rejected JIT preflight: %v", err)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{`"runner_group": "default"`, `"runner_count_checked": 4`, `"actions_enabled": true`, `"self_hosted_allowed": true`} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing rejected preflight %q:\n%s", want, proof)
		}
	}
}

func TestT916ProviderSidecarReturnsOwnedJITRunnerIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/actions/orgs/GoCodeAlone/runners/jitconfig" {
			t.Fatalf("JIT sidecar request = %s %s", r.Method, r.URL.Path)
		}
		var body struct {
			Repository  string   `json:"repository"`
			Workflow    string   `json:"workflow"`
			Ref         string   `json:"ref"`
			RunnerName  string   `json:"runner_name"`
			RunnerGroup string   `json:"runner_group"`
			Labels      []string `json:"labels"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode JIT sidecar request: %v", err)
		}
		if body.Repository != "GoCodeAlone/workflow-compute" || body.Workflow != "dogfood.yml" || body.Ref != "main" || body.RunnerGroup != "ephemeral" {
			t.Fatalf("JIT sidecar body = %+v", body)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"runner_id":42,"runner_name":"wfc-stg-ghp-linux-target","encoded_jit_config":"encoded-jit-config","ownership_token":"ownership-token","preflight":{"organization":"GoCodeAlone","runner_group":"ephemeral","runner_group_id":5,"actions_enabled":true,"self_hosted_allowed":true}}`))
	}))
	defer server.Close()
	client := &providerSidecarClient{baseURL: server.URL, token: "provider-token", http: server.Client()}
	registration, err := client.orgJITConfig(t.Context(), internal.EphemeralRunnerJobRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
	}, internal.EphemeralRunnerJobSpec{
		RunnerName:  "wfc-stg-ghp-linux-target",
		RunnerGroup: "ephemeral",
		Labels:      []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"},
	})
	if err != nil {
		t.Fatalf("JIT sidecar config: %v", err)
	}
	if registration.RunnerID != 42 || registration.EncodedJITConfig != "encoded-jit-config" || registration.Preflight == nil || registration.Preflight.RunnerGroupID != 5 {
		t.Fatalf("JIT registration = %+v", registration)
	}
}

func TestT916ProviderSidecarRejectsTrailingJSONResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}{"trailing":true}`))
	}))
	defer server.Close()
	client := &providerSidecarClient{baseURL: server.URL, token: "provider-token", http: server.Client()}
	var response struct {
		Status string `json:"status"`
	}
	if err := client.do(t.Context(), http.MethodGet, "/test", nil, http.StatusOK, &response); err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("trailing sidecar JSON error = %v", err)
	}
}

func TestT916ProviderSidecarRejectsUnknownResponseFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok","unexpected":true}`))
	}))
	defer server.Close()
	client := &providerSidecarClient{baseURL: server.URL, token: "provider-token", http: server.Client()}
	var response struct {
		Status string `json:"status"`
	}
	if err := client.do(t.Context(), http.MethodGet, "/test", nil, http.StatusOK, &response); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown sidecar field error = %v", err)
	}
}

func TestT916ProviderSidecarRejectsBodyOnNoContentResponse(t *testing.T) {
	client := &providerSidecarClient{
		baseURL: "https://provider.example",
		token:   "provider-token",
		http: &http.Client{Transport: testRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusNoContent,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("unexpected")),
			}, nil
		})},
	}
	if err := client.do(t.Context(), http.MethodDelete, "/test", nil, http.StatusNoContent, nil); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("204 sidecar body error = %v", err)
	}
}

func TestT916JITPreflightSemanticValidationFailsClosed(t *testing.T) {
	req := internal.EphemeralRunnerJobRequest{Organization: "GoCodeAlone", Ref: "main"}
	spec := internal.EphemeralRunnerJobSpec{RunnerGroup: "ephemeral"}
	valid := internal.GitHubRunnerProviderPreflight{
		Organization: "GoCodeAlone", RunnerGroup: "ephemeral", RunnerGroupID: 5, Ref: "main",
		ResolvedWorkflowPath: ".github/workflows/dogfood.yml", ResolvedRefSHA: strings.Repeat("a", 40),
		ActionsEnabled: true, SelfHostedAllowed: true,
	}
	for _, tc := range []struct {
		name   string
		mutate func(*internal.GitHubRunnerProviderPreflight)
	}{
		{name: "runner_group_id", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.RunnerGroupID = 0 }},
		{name: "ref", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.Ref = "other" }},
		{name: "resolved_sha", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.ResolvedRefSHA = "short" }},
		{name: "actions_disabled", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.ActionsEnabled = false }},
		{name: "self_hosted_disabled", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.SelfHostedAllowed = false }},
		{name: "conflicts", mutate: func(p *internal.GitHubRunnerProviderPreflight) { p.ConflictingLabels = []string{"wfc-ghp-stg"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preflight := valid
			tc.mutate(&preflight)
			if err := validateJITPreflight(req, spec, &preflight); err == nil {
				t.Fatalf("invalid preflight accepted: %+v", preflight)
			}
		})
	}
}

func TestT916ProviderSidecarBoundsSuccessfulResponseBody(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"payload": strings.Repeat("x", (1<<20)+1)})
	}))
	t.Cleanup(sidecar.Close)
	client := &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()}
	var output map[string]any
	err := client.do(t.Context(), http.MethodGet, "/oversized", nil, http.StatusOK, &output)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized successful sidecar response: output keys=%d err=%v", len(output), err)
	}
}

func TestT916ProviderSidecarPreservesUncertainDispatchedRun(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"workflow_run_id":     28449657934,
			"run_url":             "https://api.github.com/runs/28449657934",
			"html_url":            "https://github.com/runs/28449657934",
			"verification_status": "uncertain",
		})
	}))
	t.Cleanup(sidecar.Close)
	client := &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()}
	dispatch, err := client.dispatchWorkflow(t.Context(), "GoCodeAlone/workflow-compute", "dogfood.yml", "main", nil, ".github/workflows/dogfood.yml", strings.Repeat("a", 40))
	if err != nil {
		t.Fatalf("preserve uncertain dispatch: %v", err)
	}
	if dispatch.WorkflowRunID != 28449657934 || dispatch.Verification != "uncertain" {
		t.Fatalf("uncertain dispatch = %+v", dispatch)
	}
}

func TestT916UncertainDispatchRevalidatesRunIdentityBeforeObservingJobs(t *testing.T) {
	var jobsCalls int
	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/dispatches"):
			markRunnerTestDispatch(t, workspace)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"workflow_run_id": 28449657934, "run_url": "https://api.github.com/runs/28449657934",
				"html_url": "https://github.com/runs/28449657934", "verification_status": "uncertain",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": 28449657934, "path": ".github/workflows/dogfood.yml@refs/heads/main", "head_sha": strings.Repeat("b", 40),
			})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/jobs"):
			jobsCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{{
				"id": 77, "run_id": 28449657934, "status": "completed", "conclusion": "success", "runner_id": 42,
				"runner_name": "wfc-stg-ghp-linux-orker113029f-task17afaa3", "labels": testEphemeralRunnerSpec("wfc-stg-ghp-linux-orker113029f-task17afaa3").Labels,
			}}})
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)
	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nwhile [ ! -f \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/dispatch.seen\" ]; do sleep 0.01; done\nexec sleep 1000\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")
	req := internal.EphemeralRunnerJobRequest{
		Mode: internal.EphemeralRunnerJobModeDispatchThenWait, Environment: "stg", OS: "linux", WorkerID: "worker-1", TaskID: "task-1",
		Organization: "GoCodeAlone", Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood.yml", Ref: "main",
		RunnerGroup: "ephemeral", RequirePreflight: true, RuntimeCaps: []string{"github-actions-runner"},
	}
	driver := &runnerDriver{req: req, sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()}, runnerDir: runnerDir}
	result, err := internal.NewEphemeralRunnerJob(driver).Run(t.Context(), req)
	if err == nil || !strings.Contains(err.Error(), "head_sha") {
		t.Fatalf("uncertain dispatch identity error = %v", err)
	}
	if jobsCalls != 0 {
		t.Fatalf("observed %d jobs before immutable run identity was verified", jobsCalls)
	}
	if result.WorkflowVerificationStatus != "uncertain" {
		t.Fatalf("uncertain dispatch proof status = %q", result.WorkflowVerificationStatus)
	}
}

func TestV917CommandDoesNotAcceptRunnerSuccessMarkerWithoutGitHubCompletion(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldGracePolls := githubJobExitGracePolls
	githubJobExitGracePolls = 3
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubJobExitGracePolls = oldGracePolls
	})

	var jitCalls, dispatchCalls, deleteCalls int
	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			jitCalls++
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			dispatchCalls++
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			writeWorkflowDispatchResponse(w, 28449657933)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657933/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[]}`))
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
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":3
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
		t.Fatalf("run error = %v, want missing terminal GitHub completion", err)
	}
	if _, statErr := os.Stat(filepath.Join(workspace, "success.marker")); statErr != nil {
		t.Fatalf("runner success marker was not emitted before rejection: %v", statErr)
	}
	if jitCalls != 1 || dispatchCalls != 1 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: jit=%d dispatch=%d delete=%d", jitCalls, dispatchCalls, deleteCalls)
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

	var jitCalls, dispatchCalls, runsCalls, jobsCalls, deleteCalls int
	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			jitCalls++
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			dispatchCalls++
			if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
				t.Fatalf("write dispatch marker: %v", err)
			}
			writeWorkflowDispatchResponse(w, 28449657934)
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
			_, _ = w.Write([]byte(`{"jobs":[{"id":84308154551,"run_id":28449657934,"status":"completed","conclusion":"success","runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
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
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if jitCalls != 1 || dispatchCalls != 1 || runsCalls != 0 || jobsCalls == 0 || deleteCalls != 1 {
		t.Fatalf("sidecar calls: jit=%d dispatch=%d runs=%d jobs=%d delete=%d", jitCalls, dispatchCalls, runsCalls, jobsCalls, deleteCalls)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28449657934", "84308154551", "removed", "wfc-stg-ghp-linux-abcdef987249-543210f71ee4"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT915AttachModeCorrelatesTerminalGitHubAssignment(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	var jobsCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 52)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000001":
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"id":28460000001,"path":".github/workflows/dogfood.yml@refs/heads/main","head_branch":"main","head_sha":%q}`, strings.Repeat("a", 40))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000001/jobs":
			jobsCalls++
			w.WriteHeader(http.StatusOK)
			if jobsCalls == 1 {
				_, _ = w.Write([]byte(`{"jobs":[{"id":84330000001,"run_id":28460000001,"status":"queued","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"jobs":[{"id":84330000001,"run_id":28460000001,"status":"completed","conclusion":"success","runner_id":52,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4"}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":52}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nexit 0\n")
	writeFunctionalRunnerListener(t, runnerDir)
	req := internal.EphemeralRunnerJobRequest{
		Mode:             internal.EphemeralRunnerJobModeAttachToQueued,
		Environment:      "stg",
		OS:               "linux",
		WorkerID:         "worker-0123456789abcdef",
		TaskID:           "task-abcdef9876543210",
		Organization:     "GoCodeAlone",
		Repository:       "GoCodeAlone/workflow-compute",
		Workflow:         "dogfood.yml",
		Ref:              "main",
		WorkflowRunID:    28460000001,
		WorkflowJobID:    84330000001,
		RunnerGroup:      "ephemeral",
		RequirePreflight: true,
	}
	spec, err := internal.BuildEphemeralRunnerJobSpec(req)
	if err != nil {
		t.Fatalf("build runner spec: %v", err)
	}
	driver := &runnerDriver{
		req:       req,
		sidecar:   &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
		runnerDir: runnerDir,
	}
	result, err := driver.RunGitHubJob(context.Background(), internal.EphemeralRunnerJobModeAttachToQueued, spec)
	if err != nil {
		t.Fatalf("run attach-to-queued job: %v", err)
	}
	if result.WorkflowRunID != 28460000001 || result.WorkflowJobID != 84330000001 || result.WorkflowJobStatus != "completed" || result.RunnerID != 52 {
		t.Fatalf("attach assignment evidence = %+v", result)
	}
	if jobsCalls < 2 {
		t.Fatalf("attach mode did not validate queued and terminal job states: jobsCalls=%d", jobsCalls)
	}
}

func TestT915AttachModePreservesLocalRunnerIDOnAssignmentMismatch(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	var jobsCalls, deleteCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 52)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000002":
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"id":28460000002,"path":".github/workflows/dogfood.yml@refs/heads/main","head_branch":"main","head_sha":%q}`, strings.Repeat("a", 40))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000002/jobs":
			jobsCalls++
			w.WriteHeader(http.StatusOK)
			if jobsCalls == 1 {
				_, _ = w.Write([]byte(`{"jobs":[{"id":84330000002,"run_id":28460000002,"status":"queued","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"jobs":[{"id":84330000002,"run_id":28460000002,"status":"completed","conclusion":"success","runner_id":99,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/52":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":52}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nexit 0\n")
	writeFunctionalRunnerListener(t, runnerDir)
	req := internal.EphemeralRunnerJobRequest{
		Mode:             internal.EphemeralRunnerJobModeAttachToQueued,
		Environment:      "stg",
		OS:               "linux",
		WorkerID:         "worker-0123456789abcdef",
		TaskID:           "task-abcdef9876543210",
		Organization:     "GoCodeAlone",
		Repository:       "GoCodeAlone/workflow-compute",
		Workflow:         "dogfood.yml",
		Ref:              "main",
		WorkflowRunID:    28460000002,
		WorkflowJobID:    84330000002,
		RunnerGroup:      "ephemeral",
		RequirePreflight: true,
		RuntimeCaps:      []string{"github-actions-runner"},
	}
	driver := &runnerDriver{
		req:       req,
		sidecar:   &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
		runnerDir: runnerDir,
	}
	result, err := internal.NewEphemeralRunnerJob(driver).Run(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "runner ID") {
		t.Fatalf("assignment mismatch error = %v", err)
	}
	if result.RunnerID != 52 {
		t.Fatalf("assignment mismatch replaced local runner ID: got %d want 52", result.RunnerID)
	}
	if deleteCalls != 1 {
		t.Fatalf("owned runner cleanup calls = %d, want 1", deleteCalls)
	}
}

func TestT916ObserveAttachedGitHubJobUsesExactRunConclusionFallback(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	const runID = int64(28460000003)
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000003/jobs":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{{
				"id": 77, "run_id": runID, "status": "completed", "runner_id": 42, "runner_name": runnerName,
			}}})
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28460000003":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": runID, "status": "completed", "conclusion": "success"})
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)
	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Mode: internal.EphemeralRunnerJobModeAttachToQueued, Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: runID, WorkflowJobID: 77,
		},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	completion, err := driver.observeAttachedGitHubJob(t.Context(), runnerName)
	if err != nil {
		t.Fatalf("observe attached job: %v", err)
	}
	if !completion.Assigned || !completion.Terminal || !completion.Success {
		t.Fatalf("attached run conclusion fallback = %+v", completion)
	}
}

func TestT915WaitForGitHubCompletionDoesNotBlockOnRunnerShutdownAfterTerminalSuccess(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldShutdownGrace := githubRunnerShutdownGrace
	githubRunnerShutdownGrace = 10 * time.Millisecond
	oldForceKillWait := runnerForceKillWait
	runnerForceKillWait = 10 * time.Millisecond
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubRunnerShutdownGrace = oldShutdownGrace
		runnerForceKillWait = oldForceKillWait
	})

	var canceled bool
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28466630619,"status":"completed","conclusion":"success"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28466630619/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84367972241,"run_id":28466630619,"status":"completed","conclusion":"success","runner_name":"wfc-stg-ghp-linux-260629fabb6f-1820455d6b7c","labels":["self-hosted","linux","wfc-stg-ghp-linux-260629fabb6f-1820455d6b7c","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Repository:    "GoCodeAlone/workflow-compute",
			Workflow:      "dogfood.yml",
			WorkflowRunID: 28466630619,
		},
		sidecar: &providerSidecarClient{
			baseURL: sidecar.URL,
			token:   "provider-token",
			http:    sidecar.Client(),
		},
	}
	runnerDone := make(chan error, 1)
	runner := &runningCommand{
		path: "run.sh",
		cancel: func() {
			canceled = true
			runnerDone <- errors.New("signal: killed")
		},
		result: make(chan runnerCompletion, 1),
		done:   runnerDone,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		completion, err := driver.waitForGitHubCompletion(ctx, runner, testEphemeralRunnerSpec("wfc-stg-ghp-linux-260629fabb6f-1820455d6b7c"))
		if err != nil {
			done <- err
			return
		}
		if !completion.Success || !completion.Terminal || completion.WorkflowRunID != 28466630619 || completion.WorkflowJobID != 84367972241 {
			done <- fmt.Errorf("completion = %+v", completion)
			return
		}
		done <- nil
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waitForGitHubCompletion blocked on runner shutdown after terminal success")
	}
	if !canceled {
		t.Fatal("runner was not asked to stop")
	}
}

func TestT915ObserveGitHubJobRejectsBlankRunnerNameAfterTerminalSuccess(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldShutdownGrace := githubRunnerShutdownGrace
	githubRunnerShutdownGrace = 10 * time.Millisecond
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubRunnerShutdownGrace = oldShutdownGrace
	})

	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28788166953,"status":"completed","conclusion":"success"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28788166953/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":85359800000,"run_id":28788166953,"status":"completed","conclusion":"skipped","runner_name":""},{"id":85359812345,"run_id":28788166953,"status":"completed","conclusion":"success","runner_name":""}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Repository:    "GoCodeAlone/workflow-compute",
			Workflow:      "dogfood.yml",
			WorkflowRunID: 28788166953,
		},
		sidecar: &providerSidecarClient{
			baseURL: sidecar.URL,
			token:   "provider-token",
			http:    sidecar.Client(),
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	completion, err := driver.observeGitHubJob(ctx, testEphemeralRunnerSpec("wfc-stg-ghp-linux-260629fabb6f-12640z85f6ed"))
	if err != nil {
		t.Fatalf("observe GitHub job: %v", err)
	}
	if completion.Assigned || completion.Terminal || completion.Success || completion.WorkflowRunID != 0 {
		t.Fatalf("blank runner identity accepted: %+v", completion)
	}
}

func TestT916ObserveDispatchedGitHubJobRequiresExactRunnerLabels(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	required := []string{"self-hosted", "linux", runnerName, "wfc-ghp-stg", "wfc-ghp-ephemeral"}
	for _, tc := range []struct {
		name   string
		labels []string
	}{
		{name: "subset", labels: []string{"self-hosted", runnerName}},
		{name: "extra", labels: append(append([]string(nil), required...), "unexpected-routing-label")},
		{name: "duplicate", labels: append(append([]string(nil), required...), required[0])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if err := json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{{
					"id": 84367972241, "run_id": 28466630619, "status": "in_progress", "runner_id": 42, "runner_name": runnerName, "labels": tc.labels,
				}}}); err != nil {
					t.Fatalf("encode jobs response: %v", err)
				}
			}))
			t.Cleanup(sidecar.Close)
			driver := &runnerDriver{
				req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: 28466630619},
				sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
			}
			completion, err := driver.observeGitHubJob(t.Context(), testEphemeralRunnerSpec(runnerName))
			if err == nil || !strings.Contains(err.Error(), "labels do not match") || completion.Assigned {
				t.Fatalf("dispatch %s labels: completion=%+v err=%v", tc.name, completion, err)
			}
		})
	}
}

func TestT916ObserveDispatchedGitHubJobIgnoresUnassignedWrongLabelSet(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	required := []string{"self-hosted", "linux", runnerName, "wfc-ghp-stg", "wfc-ghp-ephemeral"}
	for _, tc := range []struct {
		name   string
		labels []string
	}{
		{name: "subset", labels: []string{"self-hosted", runnerName}},
		{name: "extra", labels: append(append([]string(nil), required...), "unexpected-routing-label")},
		{name: "duplicate", labels: append(append([]string(nil), required...), required[0])},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if err := json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{{
					"id": 84367972241, "run_id": 28466630619, "status": "queued", "labels": tc.labels,
				}}}); err != nil {
					t.Fatalf("encode jobs response: %v", err)
				}
			}))
			t.Cleanup(sidecar.Close)
			driver := &runnerDriver{
				req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: 28466630619},
				sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
			}
			completion, err := driver.observeGitHubJob(t.Context(), testEphemeralRunnerSpec(runnerName))
			if err != nil || completion.WorkflowRunID != 0 {
				t.Fatalf("unassigned dispatch %s labels: completion=%+v err=%v", tc.name, completion, err)
			}
		})
	}
}

func TestT916ObserveDispatchedGitHubJobRejectsMismatchedRunID(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	for _, assigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("assigned_%t", assigned), func(t *testing.T) {
			job := map[string]any{
				"id": 84367972241, "run_id": 999, "status": "in_progress", "labels": testEphemeralRunnerSpec(runnerName).Labels,
			}
			if assigned {
				job["runner_id"] = 42
				job["runner_name"] = runnerName
			}
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if err := json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{job}}); err != nil {
					t.Fatalf("encode jobs response: %v", err)
				}
			}))
			t.Cleanup(sidecar.Close)
			driver := &runnerDriver{
				req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: 28466630619},
				sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
			}
			completion, err := driver.observeGitHubJob(t.Context(), testEphemeralRunnerSpec(runnerName))
			if err == nil || !strings.Contains(err.Error(), "run_id") || completion.WorkflowRunID != 0 {
				t.Fatalf("mismatched dispatch run: completion=%+v err=%v", completion, err)
			}
		})
	}
}

func TestT916ObserveDispatchedGitHubJobRejectsMultipleExactLabelCandidates(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	labels := testEphemeralRunnerSpec(runnerName).Labels
	for _, tc := range []struct {
		name string
		jobs []map[string]any
	}{
		{
			name: "two_unassigned",
			jobs: []map[string]any{
				{"id": 1, "run_id": 28466630619, "status": "queued", "labels": labels},
				{"id": 2, "run_id": 28466630619, "status": "queued", "labels": labels},
			},
		},
		{
			name: "assigned_and_unassigned",
			jobs: []map[string]any{
				{"id": 1, "run_id": 28466630619, "status": "queued", "labels": labels},
				{"id": 2, "run_id": 28466630619, "status": "in_progress", "runner_id": 42, "runner_name": runnerName, "labels": labels},
			},
		},
		{
			name: "two_assigned",
			jobs: []map[string]any{
				{"id": 1, "run_id": 28466630619, "status": "in_progress", "runner_id": 42, "runner_name": runnerName, "labels": labels},
				{"id": 2, "run_id": 28466630619, "status": "in_progress", "runner_id": 43, "runner_name": runnerName, "labels": labels},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if err := json.NewEncoder(w).Encode(map[string]any{"jobs": tc.jobs}); err != nil {
					t.Fatalf("encode jobs response: %v", err)
				}
			}))
			t.Cleanup(sidecar.Close)
			driver := &runnerDriver{
				req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: 28466630619},
				sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
			}
			completion, err := driver.observeGitHubJob(t.Context(), testEphemeralRunnerSpec(runnerName))
			if err == nil || !strings.Contains(err.Error(), "multiple GitHub workflow jobs") || completion.WorkflowRunID != 0 {
				t.Fatalf("ambiguous dispatch candidates: completion=%+v err=%v", completion, err)
			}
		})
	}
}

func TestT916ObserveDispatchedGitHubJobUsesExactRunConclusionFallback(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	const runID = int64(28466630619)
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28466630619/jobs":
			_ = json.NewEncoder(w).Encode(map[string]any{"jobs": []map[string]any{{
				"id": 1, "run_id": runID, "status": "completed", "runner_id": 42, "runner_name": runnerName,
				"labels": testEphemeralRunnerSpec(runnerName).Labels,
			}}})
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28466630619":
			_ = json.NewEncoder(w).Encode(map[string]any{"id": runID, "status": "completed", "conclusion": "success"})
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.Path)
		}
	}))
	t.Cleanup(sidecar.Close)
	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", WorkflowRunID: runID},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	completion, err := driver.observeGitHubJob(t.Context(), testEphemeralRunnerSpec(runnerName))
	if err != nil {
		t.Fatalf("observe dispatched job: %v", err)
	}
	if !completion.Assigned || !completion.Terminal || !completion.Success {
		t.Fatalf("run conclusion fallback = %+v", completion)
	}
}

func TestT915ObserveGitHubJobDoesNotUseBlankRunnerFallbackAcrossMultipleRuns(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28788166953,"status":"completed","conclusion":"success"},{"id":28788166954,"status":"completed","conclusion":"success"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28788166953/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":85359812345,"run_id":28788166953,"status":"completed","conclusion":"success","runner_name":""}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28788166954/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":85359812346,"run_id":28788166954,"status":"completed","conclusion":"success","runner_name":""}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Repository:    "GoCodeAlone/workflow-compute",
			Workflow:      "dogfood.yml",
			WorkflowRunID: 28788166953,
		},
		sidecar: &providerSidecarClient{
			baseURL: sidecar.URL,
			token:   "provider-token",
			http:    sidecar.Client(),
		},
	}
	completion, err := driver.observeGitHubJob(context.Background(), testEphemeralRunnerSpec("wfc-stg-ghp-linux-260629fabb6f-12640z85f6ed"))
	if err != nil {
		t.Fatalf("observe GitHub job: %v", err)
	}
	if completion.WorkflowRunID != 0 {
		t.Fatalf("blank runner fallback must fail closed across multiple runs, got %+v", completion)
	}
}

func TestV917WaitForGitHubCompletionWaitsForTerminalAfterRunnerSuccessMarker(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldGracePolls := githubJobExitGracePolls
	githubJobExitGracePolls = 3
	oldShutdownGrace := githubRunnerShutdownGrace
	githubRunnerShutdownGrace = 10 * time.Millisecond
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubJobExitGracePolls = oldGracePolls
		githubRunnerShutdownGrace = oldShutdownGrace
	})

	var jobsCalls int
	firstInProgressObserved := make(chan struct{})
	allowTerminalResponse := make(chan struct{})
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":29088740554,"status":"in_progress"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554/jobs":
			jobsCalls++
			if jobsCalls == 1 {
				close(firstInProgressObserved)
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"jobs":[{"id":86348700852,"run_id":29088740554,"status":"in_progress","runner_id":277,"runner_name":"wfc-stg-ghp-linux-worker-task","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			<-allowTerminalResponse
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":86348700852,"run_id":29088740554,"status":"completed","conclusion":"success","runner_id":277,"runner_name":"wfc-stg-ghp-linux-worker-task","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood.yml", WorkflowRunID: 29088740554},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	runner := &runningCommand{
		path:   "run.sh",
		cancel: func() {},
		result: make(chan runnerCompletion, 1),
		done:   make(chan error, 1),
	}
	cancelled := make(chan struct{})
	var cancelOnce sync.Once
	runner.cancel = func() { cancelOnce.Do(func() { close(cancelled) }) }
	type waitResult struct {
		completion githubJobCompletion
		err        error
	}
	waitDone := make(chan waitResult, 1)
	go func() {
		completion, err := driver.waitForGitHubCompletion(t.Context(), runner, testEphemeralRunnerSpec("wfc-stg-ghp-linux-worker-task"))
		waitDone <- waitResult{completion: completion, err: err}
	}()
	go func() {
		<-cancelled
		runner.done <- nil
	}()
	runner.result <- runnerCompletion{success: true, line: "Job completed with result: Succeeded"}
	select {
	case <-firstInProgressObserved:
	case <-time.After(time.Second):
		t.Fatal("GitHub in-progress state was not observed")
	}
	select {
	case <-cancelled:
		t.Fatal("success marker cancelled runner while GitHub still reported in-progress")
	default:
	}
	close(allowTerminalResponse)

	waited := <-waitDone
	if waited.err != nil {
		t.Fatalf("wait for GitHub completion: %v", waited.err)
	}
	completion := waited.completion
	if jobsCalls < 2 || completion.RunnerID != 277 || !completion.Terminal || !completion.Success || completion.WorkflowJobStatus != "completed" {
		t.Fatalf("completion returned before terminal GitHub state: calls=%d completion=%+v", jobsCalls, completion)
	}
}

func TestT916WorkflowDispatchInputPayloadIsBounded(t *testing.T) {
	inputs := map[string]string{"custom": ""}
	encoded, err := json.Marshal(inputs)
	if err != nil {
		t.Fatalf("marshal baseline inputs: %v", err)
	}
	inputs["custom"] = strings.Repeat("x", maxWorkflowDispatchInputPayloadBytes-len(encoded))
	if err := validateWorkflowDispatchInputPayload(inputs); err != nil {
		t.Fatalf("maximum workflow input payload rejected: %v", err)
	}
	inputs["custom"] += "x"
	if err := validateWorkflowDispatchInputPayload(inputs); err == nil || !strings.Contains(err.Error(), "65,535") {
		t.Fatalf("oversized workflow input payload error = %v", err)
	}
}

func TestV917WaitForGitHubCompletionPreservesObservedIDsOnPostSuccessAPIError(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldGracePolls := githubJobExitGracePolls
	githubJobExitGracePolls = 3
	oldShutdownGrace := githubRunnerShutdownGrace
	githubRunnerShutdownGrace = 10 * time.Millisecond
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubJobExitGracePolls = oldGracePolls
		githubRunnerShutdownGrace = oldShutdownGrace
	})

	var jobsCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":29088740554,"status":"in_progress"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554/jobs":
			jobsCalls++
			if jobsCalls == 1 {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"jobs":[{"id":86348700852,"run_id":29088740554,"status":"in_progress","runner_id":277,"runner_name":"wfc-stg-ghp-linux-worker-task","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			http.Error(w, "temporary observation failure", http.StatusServiceUnavailable)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood.yml", WorkflowRunID: 29088740554},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	runner := &runningCommand{
		path:   "run.sh",
		cancel: func() {},
		result: make(chan runnerCompletion, 1),
		done:   make(chan error, 1),
	}
	runner.result <- runnerCompletion{success: true, line: "Job completed with result: Succeeded"}
	runner.done <- nil

	completion, err := driver.waitForGitHubCompletion(t.Context(), runner, testEphemeralRunnerSpec("wfc-stg-ghp-linux-worker-task"))
	if err == nil || !strings.Contains(err.Error(), "temporary observation failure") {
		t.Fatalf("wait error = %v, want post-success observation failure", err)
	}
	if completion.WorkflowRunID != 29088740554 || completion.WorkflowJobID != 86348700852 || completion.RunnerID != 277 {
		t.Fatalf("completion lost observed identifiers: %+v", completion)
	}
}

func TestT915WaitForGitHubCompletionDoesNotBlockOnRunnerShutdownAfterTimeout(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = time.Hour
	oldShutdownGrace := githubRunnerShutdownGrace
	githubRunnerShutdownGrace = 10 * time.Millisecond
	oldForceKillWait := runnerForceKillWait
	runnerForceKillWait = 10 * time.Millisecond
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubRunnerShutdownGrace = oldShutdownGrace
		runnerForceKillWait = oldForceKillWait
	})

	var canceled bool
	driver := &runnerDriver{}
	runner := &runningCommand{
		path: "run.sh",
		cancel: func() {
			canceled = true
		},
		result: make(chan runnerCompletion, 1),
		done:   make(chan error),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := driver.waitForGitHubCompletion(ctx, runner, testEphemeralRunnerSpec("wfc-stg-ghp-linux-timeout"))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "timed out waiting for GitHub job completion") {
			t.Fatalf("err = %v, want timeout", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("waitForGitHubCompletion blocked on runner shutdown after timeout")
	}
	if !canceled {
		t.Fatal("runner was not asked to stop")
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
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			markRunnerTestDispatch(t, workspace)
			writeWorkflowDispatchResponse(w, 28449657935)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28449657935,"status":"completed","conclusion":"failure"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28449657935/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84308154552,"run_id":28449657935,"status":"completed","conclusion":"failure","runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), runnerWaitForDispatchScript())
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
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

func TestT915CommandRejectsRunnerExitBeforeGitHubJobTerminal(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			markRunnerTestDispatch(t, workspace)
			writeWorkflowDispatchResponse(w, 28454875323)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28454875323,"status":"in_progress"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28454875323/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84326797950,"run_id":28454875323,"status":"in_progress","runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), runnerWaitForDispatchScript())
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "exited before GitHub workflow job completed") {
		t.Fatalf("expected non-terminal runner exit error, got %v\nstderr:\n%s", err, stderr.String())
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28454875323", "84326797950"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT916CommandRejectsRunnerExitBeforeGitHubJobAssignment(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			markRunnerTestDispatch(t, workspace)
			writeWorkflowDispatchResponse(w, 28469568301)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28469568301,"status":"queued"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28469568301/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84378204611,"run_id":28469568301,"status":"queued","runner_name":"","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), runnerWaitForDispatchScript())
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "exited before GitHub workflow job was assigned") {
		t.Fatalf("expected unassigned runner exit error, got %v\nstderr:\n%s", err, stderr.String())
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28469568301", "84378204611", "queued"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT916CommandPropagatesObservationErrorAfterRunnerExit(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = time.Hour
	t.Cleanup(func() { githubJobPollInterval = oldPoll })

	workspace := t.TempDir()
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			markRunnerTestDispatch(t, workspace)
			writeWorkflowDispatchResponse(w, 28469568302)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28469568302/jobs":
			http.Error(w, "sidecar unavailable provider-token github_pat_example_secret", http.StatusBadGateway)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), runnerWaitForDispatchScript())
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "observe GitHub workflow job after runner exit") || !strings.Contains(err.Error(), "status 502") {
		t.Fatalf("expected observation error, got %v\nstderr:\n%s", err, stderr.String())
	}
	if strings.Contains(err.Error(), "provider-token") || strings.Contains(err.Error(), "github_pat_example_secret") {
		t.Fatalf("returned error leaked a credential: %v", err)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "removed", "redacted_error", "status 502"} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
	for _, secret := range []string{"provider-token", "github_pat_example_secret"} {
		if strings.Contains(proof, secret) {
			t.Fatalf("proof leaked %q:\n%s", secret, proof)
		}
	}
	var proofResult internal.EphemeralRunnerJobResult
	if err := json.Unmarshal([]byte(proof), &proofResult); err != nil {
		t.Fatalf("decode failure proof: %v", err)
	}
	if !strings.Contains(proofResult.RedactedError, "<redacted>") {
		t.Fatalf("proof redacted error = %q", proofResult.RedactedError)
	}
}

func TestT916ProofWriteFailurePreservesExecutionError(t *testing.T) {
	executionErr := errors.New("execution failed")
	proofErr := errors.New("proof write failed")
	err := mergeExecutionAndProofError(executionErr, proofErr)
	if !errors.Is(err, executionErr) || !errors.Is(err, proofErr) {
		t.Fatalf("joined error = %v", err)
	}
}

func TestT916ProofWriterEnforcesContractMaximum(t *testing.T) {
	t.Chdir(t.TempDir())
	result := internal.EphemeralRunnerJobResult{
		Preflight: &internal.GitHubRunnerProviderPreflight{
			ExistingLabels: []string{strings.Repeat("x", maxProofArtifactBytes)},
		},
	}
	err := writeProofArtifact(result)
	if err == nil || !strings.Contains(err.Error(), "exceeds contract maximum") {
		t.Fatalf("oversized proof error = %v", err)
	}
	if _, statErr := os.Stat(proofArtifactName); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("oversized proof artifact should not exist: %v", statErr)
	}
}

func TestT916ProofWriterRejectsPreexistingSymlink(t *testing.T) {
	workspace := t.TempDir()
	target := filepath.Join(workspace, "target.json")
	if err := os.WriteFile(target, []byte("unchanged\n"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(workspace, proofArtifactName)); err != nil {
		t.Fatalf("create proof symlink: %v", err)
	}
	t.Chdir(workspace)
	err := writeProofArtifact(internal.EphemeralRunnerJobResult{RunnerID: 42})
	if err == nil {
		t.Fatal("proof writer accepted a preexisting symlink")
	}
	if got := readFile(t, target); got != "unchanged\n" {
		t.Fatalf("proof symlink target changed to %q", got)
	}
}

func TestT916JITRunnerIdentityDoesNotDependOnLocalMetadata(t *testing.T) {
	workspace := t.TempDir()
	var deleteCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			http.Error(w, "dispatch rejected", http.StatusInternalServerError)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nsleep 10\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, input, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("dispatch error = %v", err)
	}
	if deleteCalls != 1 {
		t.Fatalf("exact runner cleanup calls: delete=%d", deleteCalls)
	}
	proof := readFile(t, filepath.Join(workspace, proofArtifactName))
	for _, want := range []string{`"runner_id": 42`, `"cleanup_status": "removed"`} {
		if !strings.Contains(proof, want) {
			t.Fatalf("proof missing %q:\n%s", want, proof)
		}
	}
}

func TestT916RunnerStartErrorRedactsJITConfigAndCleansExactRunner(t *testing.T) {
	var dispatchCalls, removeCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			var body struct {
				RunnerName string `json:"runner_name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode JIT request: %v", err)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprintf(w, `{"runner_id":42,"runner_name":%q,"encoded_jit_config":"jit-config-must-not-leak","ownership_token":"ownership-token-must-not-leak","preflight":{"organization":"GoCodeAlone","runner_group":"ephemeral","runner_group_id":5,"ref":"main","resolved_workflow_path":".github/workflows/dogfood.yml","resolved_ref_sha":%q,"actions_enabled":true,"self_hosted_allowed":true}}`, body.RunnerName, strings.Repeat("a", 40))
		case writeAcceptedRunnerJITACKWithToken(t, w, r, "ownership-token-must-not-leak"):
		case writeRunnerStatusFromRequest(w, r, "offline"):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			dispatchCalls++
			writeWorkflowDispatchResponse(w, 29088740556)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			removeCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)
	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nprintf '%s\\n' \"$@\" >&2\nprintf 'customer-private-value-12345\\n' >&2\nexit 1\n")
	writeFunctionalRunnerListener(t, runnerDir)
	req := internal.EphemeralRunnerJobRequest{
		Mode:             internal.EphemeralRunnerJobModeDispatchThenWait,
		Environment:      "stg",
		OS:               "linux",
		WorkerID:         "worker-1",
		TaskID:           "task-1",
		Organization:     "GoCodeAlone",
		Repository:       "GoCodeAlone/workflow-compute",
		Workflow:         "dogfood.yml",
		Ref:              "main",
		RunnerGroup:      "ephemeral",
		RequirePreflight: true,
		RuntimeCaps:      []string{"github-actions-runner"},
	}
	driver := &runnerDriver{
		req:       req,
		sidecar:   &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
		runnerDir: runnerDir,
	}
	result, err := internal.NewEphemeralRunnerJob(driver).Run(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "run.sh failed") {
		t.Fatalf("runner start error = %v", err)
	}
	for _, secret := range []string{"jit-config-must-not-leak", "ownership-token-must-not-leak", "customer-private-value-12345"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("runner start error leaked JIT credential: %v", err)
		}
	}
	if result.CleanupStatus != "removed" || removeCalls != 1 {
		t.Fatalf("runner cleanup: status=%q calls=%d", result.CleanupStatus, removeCalls)
	}
	if dispatchCalls != 0 {
		t.Fatalf("workflow dispatched %d times before runner startup failed", dispatchCalls)
	}
}

func TestT916RuntimeCapabilitiesComeFromLocalExecutors(t *testing.T) {
	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nexit 0\n")
	writeFunctionalRunnerListener(t, runnerDir)
	if err := os.MkdirAll(filepath.Join(runnerDir, "bin"), 0o755); err != nil {
		t.Fatalf("create runner bin: %v", err)
	}
	writeExecutable(t, filepath.Join(runnerDir, "bin", "Runner.Listener"), "#!/bin/sh\nexit 0\n")
	var probed []string
	caps := detectRuntimeCapabilities(context.Background(), runnerDir, func(name string) (string, error) {
		switch name {
		case "docker", "terraform":
			return "/usr/local/bin/" + name, nil
		default:
			return "", exec.ErrNotFound
		}
	}, func(_ context.Context, executable, _ string) error {
		probed = append(probed, executable)
		if executable == "docker" {
			return errors.New("daemon unavailable")
		}
		return nil
	})
	for _, want := range []string{"github-actions-runner", "terraform", "iac"} {
		if !slices.Contains(caps, want) {
			t.Fatalf("runtime capabilities %v missing %q", caps, want)
		}
	}
	if !slices.Contains(probed, "github-actions-runner") {
		t.Fatalf("runner listener was not functionally probed: %v", probed)
	}
	for _, absent := range []string{"docker", "podman", "nerdctl", "tofu"} {
		if slices.Contains(caps, absent) {
			t.Fatalf("runtime capabilities %v falsely advertised %q", caps, absent)
		}
	}
}

func TestT916RuntimeCapabilitiesRejectBrokenRunnerListener(t *testing.T) {
	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nexit 0\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\nexit 0\n")
	writeFunctionalRunnerListener(t, runnerDir)
	if err := os.MkdirAll(filepath.Join(runnerDir, "bin"), 0o755); err != nil {
		t.Fatalf("create runner bin: %v", err)
	}
	listener := filepath.Join(runnerDir, "bin", "Runner.Listener")
	writeExecutable(t, listener, "#!/bin/sh\nexit 1\n")
	caps := detectRuntimeCapabilities(context.Background(), runnerDir, func(string) (string, error) {
		return "", exec.ErrNotFound
	}, func(_ context.Context, executable, path string) error {
		if executable == "github-actions-runner" && path == listener {
			return errors.New("listener failed")
		}
		return nil
	})
	if slices.Contains(caps, "github-actions-runner") {
		t.Fatalf("broken runner listener was advertised: %v", caps)
	}
}

func TestT916WorkflowDispatchInputsReserveProviderKeys(t *testing.T) {
	workflowInputs := make(map[string]string, 23)
	for i := range 23 {
		workflowInputs[fmt.Sprintf("input_%d", i)] = "value"
	}
	driver := &runnerDriver{req: internal.EphemeralRunnerJobRequest{WorkflowInputs: workflowInputs}}
	_, err := driver.workflowDispatchInputs(internal.EphemeralRunnerJobSpec{Labels: []string{"self-hosted", "linux"}})
	if err == nil || !strings.Contains(err.Error(), "25") {
		t.Fatalf("workflow input limit error = %v", err)
	}
}

func TestT916WorkflowDispatchInputsRejectCaseCollisions(t *testing.T) {
	driver := &runnerDriver{req: internal.EphemeralRunnerJobRequest{WorkflowInputs: map[string]string{
		"DEPLOY": "production",
		"deploy": "staging",
	}}}
	_, err := driver.workflowDispatchInputs(internal.EphemeralRunnerJobSpec{Labels: []string{"self-hosted", "linux"}})
	if err == nil || !strings.Contains(err.Error(), "canonical input key collision") {
		t.Fatalf("case-colliding workflow inputs error = %v", err)
	}
}

func TestT916AttachToQueuedBindsRunToAuthorizedWorkflowAndRef(t *testing.T) {
	const runID = 29088740554
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id":29088740554,"path":".github/workflows/other.yml@refs/heads/main","head_branch":"main"}`))
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554/jobs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"jobs":[{"id":84330000001,"run_id":29088740554,"status":"queued","labels":["self-hosted","linux","wfc-stg-ghp-linux-target"]}]}`))
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)
	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Mode:          internal.EphemeralRunnerJobModeAttachToQueued,
			Repository:    "GoCodeAlone/workflow-compute",
			Workflow:      "dogfood.yml",
			Ref:           "main",
			WorkflowRunID: runID,
			WorkflowJobID: 84330000001,
		},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	err := driver.validateQueuedAttachedGitHubJob(t.Context(), internal.EphemeralRunnerJobSpec{Labels: []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"}}, ".github/workflows/dogfood.yml", strings.Repeat("a", 40))
	if err == nil || !strings.Contains(err.Error(), "workflow") {
		t.Fatalf("mismatched attached workflow error = %v", err)
	}
}

func TestT916AttachToQueuedRequiresExactRunnerLabelSet(t *testing.T) {
	required := []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"}
	if labelsExactlyMatch(append(append([]string(nil), required...), "unexpected-routing-label"), required) {
		t.Fatal("attach accepted a queued job with an extra routing label")
	}
	if labelsExactlyMatch(append(append([]string(nil), required...), required[0]), required) {
		t.Fatal("attach accepted a queued job with a duplicate label")
	}
	if !labelsExactlyMatch([]string{"LINUX", "self-hosted", "wfc-stg-ghp-linux-target"}, required) {
		t.Fatal("attach rejected the same canonical label set in a different order")
	}
}

func TestT916AttachToQueuedRejectsJobWithExtraRunnerLabel(t *testing.T) {
	const runID = 29088740554
	writeResponse := func(w http.ResponseWriter, value any) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(value); err != nil {
			t.Fatalf("encode sidecar response: %v", err)
		}
	}
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554":
			writeResponse(w, map[string]any{
				"id": runID, "path": ".github/workflows/dogfood.yml@refs/heads/main", "head_sha": strings.Repeat("a", 40),
			})
		case "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/29088740554/jobs":
			writeResponse(w, map[string]any{"jobs": []map[string]any{{
				"id": 84330000001, "run_id": runID, "status": "queued",
				"labels": []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target", "unexpected-routing-label"},
			}}})
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)
	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Repository: "GoCodeAlone/workflow-compute", Workflow: "dogfood.yml", Ref: "main",
			WorkflowRunID: runID, WorkflowJobID: 84330000001,
		},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	err := driver.validateQueuedAttachedGitHubJob(t.Context(), internal.EphemeralRunnerJobSpec{
		Labels: []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"},
	}, ".github/workflows/dogfood.yml", strings.Repeat("a", 40))
	if err == nil || !strings.Contains(err.Error(), "labels do not match") {
		t.Fatalf("extra queued label error = %v", err)
	}
}

func TestT916AttachToQueuedRejectsRunPathRefDespiteInconsistentBranchMetadata(t *testing.T) {
	run := internal.GitHubWorkflowRun{
		ID:         29088740554,
		Path:       ".github/workflows/dogfood.yml@refs/heads/release",
		HeadBranch: "main",
		HeadSHA:    strings.Repeat("a", 40),
	}
	if err := validateAttachedWorkflowRun(run, "dogfood.yml", "main", strings.Repeat("a", 40)); err == nil || !strings.Contains(err.Error(), "ref") {
		t.Fatalf("inconsistent attached run ref error = %v", err)
	}
}

func TestT916AttachToQueuedAcceptsRealGitHubBareRunPathRef(t *testing.T) {
	run := internal.GitHubWorkflowRun{
		ID:         29088740554,
		Path:       ".github/workflows/dogfood.yml@main",
		HeadBranch: "main",
		HeadSHA:    strings.Repeat("a", 40),
	}
	if err := validateAttachedWorkflowRun(run, "dogfood.yml", "main", strings.Repeat("a", 40)); err != nil {
		t.Fatalf("real GitHub bare run path ref rejected: %v", err)
	}
}

func TestT916AttachToQueuedNormalizesExplicitGitHubRefs(t *testing.T) {
	sha := strings.Repeat("a", 40)
	for _, tc := range []struct {
		name string
		run  internal.GitHubWorkflowRun
		ref  string
	}{
		{
			name: "branch",
			run:  internal.GitHubWorkflowRun{Path: ".github/workflows/dogfood.yml@main", HeadBranch: "main", HeadSHA: sha},
			ref:  "refs/heads/main",
		},
		{
			name: "tag",
			run:  internal.GitHubWorkflowRun{Path: ".github/workflows/dogfood.yml@v1.2.3", HeadBranch: "v1.2.3", HeadSHA: sha},
			ref:  "refs/tags/v1.2.3",
		},
		{
			name: "commit",
			run:  internal.GitHubWorkflowRun{Path: ".github/workflows/dogfood.yml@" + sha, HeadSHA: sha},
			ref:  sha,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateAttachedWorkflowRun(tc.run, "dogfood.yml", tc.ref, sha); err != nil {
				t.Fatalf("explicit ref %q rejected for run %+v: %v", tc.ref, tc.run, err)
			}
		})
	}
}

func TestT916AttachToQueuedRejectsSameNamedRefWithDifferentResolvedSHA(t *testing.T) {
	run := internal.GitHubWorkflowRun{
		Path:       ".github/workflows/dogfood.yml@release",
		HeadBranch: "release",
		HeadSHA:    strings.Repeat("b", 40),
	}
	if err := validateAttachedWorkflowRun(run, "dogfood.yml", "refs/tags/release", strings.Repeat("a", 40)); err == nil || !strings.Contains(err.Error(), "head_sha") {
		t.Fatalf("same-named ref SHA mismatch error = %v", err)
	}
}

func TestT916CallerCancellationStopsRunnerAndUnregisters(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = time.Hour
	t.Cleanup(func() { githubJobPollInterval = oldPoll })
	workspace := t.TempDir()
	runnerStarted := filepath.Join(workspace, "runner.started")
	var deleteCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			writeWorkflowDispatchResponse(w, 29088740554)
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
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\ntouch \"$RUNNER_CANCEL_TEST_MARKER\"\nexec sleep 1000\n")
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("RUNNER_CANCEL_TEST_MARKER", runnerStarted)
	t.Setenv("PATH", "/usr/bin:/bin")
	setRunnerProcessEnvironmentExtras(t, "RUNNER_CANCEL_TEST_MARKER")
	if !slices.Contains(runnerProcessEnvironment(), "RUNNER_CANCEL_TEST_MARKER="+runnerStarted) {
		t.Fatal("runner cancellation marker was not allowlisted into the test process")
	}

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1","task_id":"task-1","lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	  "operation":"ephemeral_runner_job",
	  "input":{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}
	}`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var stdout, stderr bytes.Buffer
		done <- runWithIOContext(ctx, []string{}, input, &stdout, &stderr)
	}()
	for range 300 {
		if _, err := os.Stat(runnerStarted); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("execution exited before runner start: %v", err)
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(runnerStarted); err != nil {
		t.Fatal("runner did not start before cancellation")
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) && (err == nil || !strings.Contains(err.Error(), "context canceled")) {
		t.Fatalf("cancellation error = %v", err)
	}
	if deleteCalls != 1 {
		t.Fatalf("runner unregister calls = %d", deleteCalls)
	}
	proof := readFile(t, filepath.Join(workspace, proofArtifactName))
	if !strings.Contains(proof, `"cleanup_status": "removed"`) {
		t.Fatalf("cancellation proof missing cleanup evidence:\n%s", proof)
	}
}

func TestT916TerminationSignalStopsRunnerAndUnregisters(t *testing.T) {
	if _, err := exec.LookPath("/bin/sh"); err != nil {
		t.Skip("requires POSIX signals")
	}
	workspace := t.TempDir()
	runnerStarted := filepath.Join(workspace, "signal-runner.started")
	deleteObserved := make(chan struct{}, 1)
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			writeWorkflowDispatchResponse(w, 29088740555)
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			select {
			case deleteObserved <- struct{}{}:
			default:
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)
	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), "#!/bin/sh\ntouch \"$RUNNER_SIGNAL_TEST_MARKER\"\nexec sleep 1000\n")
	writeFunctionalRunnerListener(t, runnerDir)

	cmd := exec.Command(os.Args[0], "-test.run=^TestT916TerminationSignalHelper$")
	cmd.Dir = workspace
	cmd.Env = append(os.Environ(),
		"GO_WANT_RUNNER_SIGNAL_HELPER=1",
		"COMPUTE_GITHUB_RUNNER_PROVIDER_URL="+sidecar.URL,
		"COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN=provider-token",
		"GITHUB_ACTIONS_RUNNER_DIR="+runnerDir,
		"RUNNER_SIGNAL_TEST_MARKER="+runnerStarted,
		"PATH=/usr/bin:/bin",
	)
	cmd.Stdin = strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1","task_id":"task-1","lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	  "operation":"ephemeral_runner_job",
	  "input":{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}
	}`)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start signal helper: %v", err)
	}
	for range 500 {
		if _, err := os.Stat(runnerStarted); err == nil {
			break
		}
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(runnerStarted); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("signal helper runner did not start:\n%s", output.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal helper: %v", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("signal helper exit: %v\n%s", err, output.String())
	}
	select {
	case <-deleteObserved:
	case <-time.After(5 * time.Second):
		t.Fatalf("termination signal did not unregister runner:\n%s", output.String())
	}
}

func TestT916TerminationSignalHelper(t *testing.T) {
	if os.Getenv("GO_WANT_RUNNER_SIGNAL_HELPER") != "1" {
		return
	}
	os.Args = []string{os.Args[0]}
	runnerProcessExtraEnvironmentNames = []string{"RUNNER_SIGNAL_TEST_MARKER"}
	err := run()
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("signal cancellation error = %v", err)
	}
}

func TestT916RunnerEnvironmentDoesNotInheritHostSecrets(t *testing.T) {
	for name, value := range map[string]string{
		"AWS_ACCESS_KEY_ID":                    "aws-access-key",
		"AWS_SECRET_ACCESS_KEY":                "aws-secret-key",
		"GITHUB_TOKEN":                         "github-token",
		"GH_TOKEN":                             "gh-token",
		"DIGITALOCEAN_ACCESS_TOKEN":            "do-token",
		"COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN": "provider-token",
		"UNRELATED_AGENT_SECRET":               "agent-secret",
	} {
		t.Setenv(name, value)
	}
	environment := runnerProcessEnvironment()
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "GITHUB_TOKEN", "GH_TOKEN", "DIGITALOCEAN_ACCESS_TOKEN", "COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "UNRELATED_AGENT_SECRET":
			t.Fatalf("runner inherited host secret variable %q", name)
		}
	}
	if !slices.ContainsFunc(environment, func(entry string) bool { return strings.HasPrefix(entry, "PATH=") }) {
		t.Fatal("runner environment must retain PATH")
	}
}

func TestT916CopyWorkloadOutputsRejectsTraversalAndSymlinks(t *testing.T) {
	runnerDir := t.TempDir()
	workspace := filepath.Join(runnerDir, "_work", "workflow-compute", "workflow-compute")
	if err := os.MkdirAll(workspace, 0o700); err != nil {
		t.Fatalf("create runner workspace: %v", err)
	}
	driver := &runnerDriver{
		req:       internal.EphemeralRunnerJobRequest{Repository: "GoCodeAlone/workflow-compute"},
		runnerDir: runnerDir,
	}
	if _, err := driver.copyWorkloadOutputs([]string{"../secret.txt"}); err == nil || !strings.Contains(err.Error(), "canonical relative") {
		t.Fatalf("traversal artifact error = %v", err)
	}
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside secret: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "result.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := driver.copyWorkloadOutputs([]string{"result.txt"}); err == nil || !strings.Contains(err.Error(), "symbolic links") {
		t.Fatalf("symlink artifact error = %v", err)
	}
}

func TestT916RunnerOutputCaptureIsBounded(t *testing.T) {
	running := &runningCommand{result: make(chan runnerCompletion, 1)}
	input := strings.Repeat("0123456789abcdef\n", maxRunnerOutputBytes/8)
	if err := running.captureOutput(strings.NewReader(input), &running.stdout); err != nil {
		t.Fatalf("capture output: %v", err)
	}
	if got := len(running.stdout.String()); got > maxRunnerOutputBytes {
		t.Fatalf("captured output bytes = %d, want <= %d", got, maxRunnerOutputBytes)
	}
}

func TestT916RunnerCancellationStopsDescendantProcess(t *testing.T) {
	if _, err := exec.LookPath("/bin/sh"); err != nil {
		t.Skip("requires a POSIX shell")
	}
	runnerDir := t.TempDir()
	pidFile := filepath.Join(runnerDir, "child.pid")
	script := filepath.Join(runnerDir, "runner.sh")
	writeExecutable(t, script, "#!/bin/sh\nsleep 1000 &\necho $! > \"$RUNNER_CHILD_PID_FILE\"\nwait\n")
	t.Setenv("RUNNER_CHILD_PID_FILE", pidFile)
	setRunnerProcessEnvironmentExtras(t, "RUNNER_CHILD_PID_FILE")
	running, err := startCommand(context.Background(), script, runnerDir)
	if err != nil {
		t.Fatalf("start runner process tree: %v", err)
	}
	var childPID int
	for range 100 {
		data, readErr := os.ReadFile(pidFile)
		if readErr == nil {
			if strings.TrimSpace(string(data)) == "" {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &childPID); scanErr != nil {
				t.Fatalf("parse child pid: %v", scanErr)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if childPID <= 0 {
		t.Fatal("runner child process did not start")
	}
	child, err := os.FindProcess(childPID)
	if err != nil {
		t.Fatalf("find child process: %v", err)
	}
	t.Cleanup(func() { _ = child.Kill() })

	running.cancel()
	if waitErr, stopped := running.waitAfterCancel(2 * time.Second); !stopped || waitErr == nil {
		t.Fatalf("runner process tree stop: stopped=%t err=%v", stopped, waitErr)
	}
	for range 100 {
		if !processIsRunning(child, childPID) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("runner descendant process %d survived cancellation", childPID)
}

func processIsRunning(process *os.Process, pid int) bool {
	if err := process.Signal(syscall.Signal(0)); err != nil {
		return false
	}
	status, err := exec.Command("ps", "-o", "stat=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return false
	}
	return !strings.HasPrefix(strings.TrimSpace(string(status)), "Z")
}

func TestT916RunnerOutputCaptureTruncatesOversizedLineAndKeepsDraining(t *testing.T) {
	running := &runningCommand{result: make(chan runnerCompletion, 1)}
	input := strings.Repeat("x", maxRunnerOutputLineBytes+4096) + "\nafter-long-line\n"
	if err := running.captureOutput(strings.NewReader(input), &running.stdout); err != nil {
		t.Fatalf("capture oversized line: %v", err)
	}
	output := running.stdout.String()
	if !strings.Contains(output, "[line truncated]") || !strings.Contains(output, "after-long-line") {
		t.Fatalf("capture did not drain after oversized line: %q", output)
	}
}

func TestT916RunnerShutdownRejectsCaptureFailureOrLiveProcess(t *testing.T) {
	captureErr := &runnerOutputCaptureError{err: errors.New("scanner failed")}
	if err := validateRunnerShutdown(captureErr, true); !errors.Is(err, captureErr) {
		t.Fatalf("capture failure swallowed: %v", err)
	}
	if err := validateRunnerShutdown(nil, false); err == nil || !strings.Contains(err.Error(), "did not stop") {
		t.Fatalf("live runner accepted: %v", err)
	}
	if err := validateRunnerShutdown(errors.New("signal: killed"), true); err != nil {
		t.Fatalf("expected cancellation exit rejected: %v", err)
	}
}

func TestT915CommandAllowsBriefGitHubJobAPICompletionLagAfterRunnerExit(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = 10 * time.Millisecond
	oldGracePolls := githubJobExitGracePolls
	githubJobExitGracePolls = 3
	t.Cleanup(func() {
		githubJobPollInterval = oldPoll
		githubJobExitGracePolls = oldGracePolls
	})

	workspace := t.TempDir()
	var jobsCalls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/jitconfig":
			writeAcceptedRunnerJIT(t, w, r, 42)
		case writeAcceptedRunnerJITACK(t, w, r):
		case writeOnlineRunnerStatus(w, r):
		case r.Method == http.MethodPost && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches":
			markRunnerTestDispatch(t, workspace)
			writeWorkflowDispatchResponse(w, 28454875324)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/runs":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"workflow_runs":[{"id":28454875324,"status":"in_progress"}]}`))
		case r.Method == http.MethodGet && r.URL.Path == "/v1/actions/repos/GoCodeAlone/workflow-compute/actions/runs/28454875324/jobs":
			jobsCalls++
			w.WriteHeader(http.StatusOK)
			if jobsCalls == 1 {
				_, _ = w.Write([]byte(`{"jobs":[{"id":84326797951,"run_id":28454875324,"status":"in_progress","runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
				return
			}
			_, _ = w.Write([]byte(`{"jobs":[{"id":84326797951,"run_id":28454875324,"status":"completed","conclusion":"success","runner_id":42,"runner_name":"wfc-stg-ghp-linux-abcdef987249-543210f71ee4","labels":["self-hosted","linux","wfc-stg-ghp-linux-abcdef987249-543210f71ee4","wfc-ghp-stg","wfc-ghp-ephemeral"]}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/actions/orgs/GoCodeAlone/runners/42":
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected sidecar request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(sidecar.Close)

	runnerDir := t.TempDir()
	writeExecutable(t, filepath.Join(runnerDir, "config.sh"), "#!/bin/sh\nprintf '{\"agentId\":42}\\n' > .runner\n")
	writeExecutable(t, filepath.Join(runnerDir, "run.sh"), runnerWaitForDispatchScript())
	writeFunctionalRunnerListener(t, runnerDir)
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", sidecar.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("GITHUB_ACTIONS_RUNNER_DIR", runnerDir)
	t.Setenv("GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR", workspace)
	setRunnerProcessEnvironmentExtras(t, "GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR")

	input := strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-abcdef9876543210",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
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
	    "require_preflight":true,
	    "timeout_seconds":10
	  }
	}`)
	var stdout, stderr bytes.Buffer
	if err := runWithIO([]string{}, input, &stdout, &stderr); err != nil {
		t.Fatalf("run dynamic provider: %v\nstderr:\n%s", err, stderr.String())
	}
	if jobsCalls < 2 {
		t.Fatalf("expected retry after non-terminal observation, jobsCalls=%d", jobsCalls)
	}
	proof := readFile(t, filepath.Join(workspace, "github-runner-proof.json"))
	for _, want := range []string{"28454875324", "84326797951"} {
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
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	  "operation":"ephemeral_runner_job",
	  "input":{"environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","typo":true}
	}`), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("error = %v, want strict unknown field rejection", err)
	}
}

func TestT916CommandRejectsTrailingJSONDocuments(t *testing.T) {
	input := `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone"}{}`
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{"--spec"}, strings.NewReader(input), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "exactly one JSON") {
		t.Fatalf("trailing JSON error = %v stdout=%s", err, stdout.String())
	}
}

func TestT916SpecCommandAppliesOperationInputSchema(t *testing.T) {
	input := `{"mode":"dispatch_then_wait","environment":"stg","os":"windows","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{"--spec"}, strings.NewReader(input), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), `os must be "linux"`) {
		t.Fatalf("contract-invalid spec input: stdout=%s err=%v", stdout.String(), err)
	}
}

func TestT915CommandRequiresSidecarEnvironmentForDynamicEnvelope(t *testing.T) {
	workspace := t.TempDir()
	t.Chdir(workspace)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "")
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"task-1",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	  "operation":"ephemeral_runner_job",
	  "input":{"mode":"attach_to_queued","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","workflow_run_id":28460000001,"workflow_job_id":84330000001,"runner_group":"ephemeral","require_preflight":true}
	}`), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "COMPUTE_GITHUB_RUNNER_PROVIDER_URL") {
		t.Fatalf("error = %v, want missing sidecar env", err)
	}
	var proof internal.EphemeralRunnerJobResult
	data, readErr := os.ReadFile(filepath.Join(workspace, proofArtifactName))
	if readErr != nil {
		t.Fatalf("security setup failure proof: %v", readErr)
	}
	if decodeErr := json.Unmarshal(data, &proof); decodeErr != nil {
		t.Fatalf("decode security setup failure proof: %v", decodeErr)
	}
	if proof.RunnerName == "" || len(proof.Labels) != 5 || proof.WorkerID != "worker-1" || proof.TaskID != "task-1" || proof.CleanupStatus != "skipped" || !strings.Contains(proof.RedactedError, "COMPUTE_GITHUB_RUNNER_PROVIDER_URL") {
		t.Fatalf("security setup failure proof identity = %+v", proof)
	}
}

func TestT916CommandRejectsEnvelopeTaskIdentityMismatch(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runWithIO([]string{}, strings.NewReader(`{
	  "protocol_version":"compute.v1alpha1",
	  "task_id":"authoritative-task",
	  "lease_id":"lease-1",
	  "provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	  "operation":"ephemeral_runner_job",
	  "input":{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"forged-task","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}
	}`), &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "task_id must match") {
		t.Fatalf("task identity mismatch error = %v", err)
	}
}

func TestT916CommandRejectsWrongProviderContractIdentity(t *testing.T) {
	for name, providerConfig := range map[string]string{
		"null":  `null`,
		"wrong": `{"plugin_id":"workflow-plugin-other","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner"}`,
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			input := `{"protocol_version":"compute.v1alpha1","task_id":"task-1","lease_id":"lease-1","provider_config":` + providerConfig + `,"operation":"ephemeral_runner_job","input":{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}}`
			err := runWithIO([]string{}, strings.NewReader(input), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), "provider_config") {
				t.Fatalf("provider identity error = %v", err)
			}
		})
	}
}

func TestT916ObservationFailureStopsRunningProcess(t *testing.T) {
	oldPoll := githubJobPollInterval
	githubJobPollInterval = time.Millisecond
	t.Cleanup(func() { githubJobPollInterval = oldPoll })
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "observation unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(sidecar.Close)
	cancelled := make(chan struct{})
	runner := &runningCommand{
		path:   "run.sh",
		result: make(chan runnerCompletion, 1),
		done:   make(chan error, 1),
	}
	runner.cancel = func() {
		select {
		case <-cancelled:
		default:
			close(cancelled)
			runner.done <- errors.New("runner terminated")
		}
	}
	driver := &runnerDriver{
		req: internal.EphemeralRunnerJobRequest{
			Repository:    "GoCodeAlone/workflow-compute",
			Workflow:      "dogfood.yml",
			WorkflowRunID: 29088740554,
		},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	_, err := driver.waitForGitHubCompletion(t.Context(), runner, testEphemeralRunnerSpec("wfc-stg-ghp-linux-worker-task"))
	if err == nil || !strings.Contains(err.Error(), "observation unavailable") {
		t.Fatalf("observation error = %v", err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("observation failure left runner process active")
	}
}

func TestT915RunnerChildProcessesDoNotInheritProviderControlPlaneEnvironment(t *testing.T) {
	runnerDir := t.TempDir()
	marker := filepath.Join(runnerDir, "safe-env.txt")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "https://provider.invalid")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-control-token")
	t.Setenv("GO_WANT_GITHUB_RUNNER_ENV_HELPER", "1")
	t.Setenv("RUNNER_SAFE_TEST_VALUE", "preserved")
	t.Setenv("RUNNER_SAFE_TEST_MARKER", marker)
	setRunnerProcessEnvironmentExtras(t, "GO_WANT_GITHUB_RUNNER_ENV_HELPER", "RUNNER_SAFE_TEST_VALUE", "RUNNER_SAFE_TEST_MARKER")
	args := []string{"-test.run=^TestT915RunnerChildProcessEnvironmentHelper$"}

	t.Run("started", func(t *testing.T) {
		running, err := startCommand(context.Background(), os.Args[0], runnerDir, args...)
		if err != nil {
			t.Fatalf("start child process with scrubbed environment: %v", err)
		}
		if err := running.wait(); err != nil {
			t.Fatalf("wait for child process with scrubbed environment: %v", err)
		}
	})
	if got := strings.TrimSpace(readFile(t, marker)); got != "preserved" {
		t.Fatalf("non-control environment = %q, want preserved", got)
	}
}

func TestT915RunnerChildProcessEnvironmentHelper(t *testing.T) {
	if os.Getenv("GO_WANT_GITHUB_RUNNER_ENV_HELPER") != "1" {
		return
	}
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(name), "COMPUTE_GITHUB_RUNNER_PROVIDER_") {
			os.Exit(42)
		}
	}
	marker := os.Getenv("RUNNER_SAFE_TEST_MARKER")
	if marker == "" {
		os.Exit(43)
	}
	if err := os.WriteFile(marker, []byte(os.Getenv("RUNNER_SAFE_TEST_VALUE")+"\n"), 0o600); err != nil {
		os.Exit(44)
	}
	os.Exit(0)
}

func TestT915CommandRejectsUnsafeContractInputBeforeSidecarAccess(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "preflight omitted",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml"}`,
			want:  "require_preflight must be true",
		},
		{
			name:  "preflight false",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","require_preflight":false}`,
			want:  "require_preflight must be true",
		},
		{
			name:  "non Linux label",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"macos","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","require_preflight":true}`,
			want:  `os must be "linux"`,
		},
		{
			name:  "invalid environment label",
			input: `{"mode":"dispatch_then_wait","environment":"stg,unsafe","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "validate ephemeral runner input schema",
		},
		{
			name:  "workflow with surrounding whitespace",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":" dogfood.yml ","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "validate ephemeral runner input schema",
		},
		{
			name:  "ref with surrounding whitespace",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":" main ","runner_group":"ephemeral","require_preflight":true}`,
			want:  "validate ephemeral runner input schema",
		},
		{
			name:  "unknown mode",
			input: `{"mode":"delete","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "mode must be",
		},
		{
			name:  "mode omitted",
			input: `{"environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","runner_group":"ephemeral","require_preflight":true}`,
			want:  "mode must be",
		},
		{
			name:  "runner group omitted",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","require_preflight":true}`,
			want:  "runner_group is required",
		},
		{
			name:  "attach IDs omitted",
			input: `{"mode":"attach_to_queued","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "workflow_run_id and workflow_job_id are required",
		},
		{
			name:  "timeout above contract maximum",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true,"timeout_seconds":21601}`,
			want:  "timeout_seconds must not exceed 21600",
		},
		{
			name:  "worker id above contract maximum",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"` + strings.Repeat("w", 257) + `","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "worker_id must not exceed 256 bytes",
		},
		{
			name:  "task id above contract maximum",
			input: `{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"` + strings.Repeat("t", 257) + `","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true}`,
			want:  "task_id must not exceed 256 bytes",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			envelope := `{"protocol_version":"compute.v1alpha1","task_id":"task-1","lease_id":"lease-1","provider_config":{"plugin_id":"workflow-plugin-github","provider_id":"github-runner","contract_id":"github.runner_provider.v1","version":"v1.0.29","config_ref":"config://network-products/github-runner-dogfood/github-runner","config_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"operation":"ephemeral_runner_job","input":` + tc.input + `}`
			err := runWithIO([]string{}, strings.NewReader(envelope), &stdout, &stderr)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
			if strings.Contains(err.Error(), "COMPUTE_GITHUB_RUNNER_PROVIDER_URL") {
				t.Fatalf("unsafe input reached sidecar environment lookup: %v", err)
			}
		})
	}
}

func TestT915EphemeralRunnerTimeoutRejectsIntegerOverflow(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	input := fmt.Sprintf(`{"mode":"dispatch_then_wait","environment":"stg","os":"linux","worker_id":"worker-1","task_id":"task-1","organization":"GoCodeAlone","repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_group":"ephemeral","require_preflight":true,"timeout_seconds":%d}`, maxInt)
	if _, err := decodeEphemeralRunnerRequest(json.RawMessage(input)); err == nil || !strings.Contains(err.Error(), "timeout_seconds must not exceed 21600") {
		t.Fatalf("maximum integer timeout error = %v", err)
	}
	input = strings.Replace(input, fmt.Sprintf("%d", maxInt), "21600", 1)
	request, err := decodeEphemeralRunnerRequest(json.RawMessage(input))
	if err != nil {
		t.Fatalf("contract maximum timeout rejected: %v", err)
	}
	if request.Timeout != 21600*time.Second {
		t.Fatalf("contract maximum timeout = %s, want %s", request.Timeout, 21600*time.Second)
	}
}

func TestT915ProviderSidecarURLRejectsInsecureBearerTransport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		baseURL string
		valid   bool
	}{
		{name: "https", baseURL: "https://provider.example.invalid", valid: true},
		{name: "loopback IPv4", baseURL: "http://127.0.0.1:8090", valid: true},
		{name: "loopback hostname", baseURL: "http://localhost:8090", valid: false},
		{name: "external plaintext", baseURL: "http://provider.example.invalid", valid: false},
		{name: "credentials", baseURL: "https://user@provider.example.invalid", valid: false},
		{name: "query", baseURL: "https://provider.example.invalid?token=unsafe", valid: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", tc.baseURL)
			t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
			_, err := newSidecarClientFromEnv()
			if tc.valid && err != nil {
				t.Fatalf("safe sidecar URL rejected: %v", err)
			}
			if !tc.valid && (err == nil || !strings.Contains(err.Error(), "provider URL")) {
				t.Fatalf("unsafe sidecar URL error = %v", err)
			}
		})
	}
}

func TestT915ProviderSidecarRejectsRedirects(t *testing.T) {
	var redirected bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected = true
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		}
		http.Redirect(w, r, "/redirected", http.StatusFound)
	}))
	defer server.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", server.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", base64.StdEncoding.EncodeToString(caPEM))

	client, err := newSidecarClientFromEnv()
	if err != nil {
		t.Fatalf("create sidecar client: %v", err)
	}
	if err := client.do(context.Background(), http.MethodGet, "/healthz", nil, http.StatusOK, nil); err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("redirect error = %v", err)
	}
	if redirected {
		t.Fatal("provider client followed redirect")
	}
}

func TestT915ProviderSidecarHTTPSAcceptsExplicitCertificateAuthority(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", server.URL)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", base64.StdEncoding.EncodeToString(caPEM))

	client, err := newSidecarClientFromEnv()
	if err != nil {
		t.Fatalf("create sidecar client with explicit CA: %v", err)
	}
	var health map[string]string
	if err := client.do(context.Background(), http.MethodGet, "/healthz", nil, http.StatusOK, &health); err != nil {
		t.Fatalf("call sidecar using explicit CA: %v", err)
	}
	if health["status"] != "ok" {
		t.Fatalf("health = %+v", health)
	}
}

func TestT915ProviderCommandAndRunnerClientCompleteTLSHealthRoundTrip(t *testing.T) {
	tlsFixture := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := tlsFixture.TLS.Certificates[0]
	tlsFixture.Close()

	tlsDir := t.TempDir()
	certFile := filepath.Join(tlsDir, "provider.crt")
	keyFile := filepath.Join(tlsDir, "provider.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})
	keyDER, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
	if err != nil {
		t.Fatalf("marshal provider private key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certFile, certPEM, 0o600); err != nil {
		t.Fatalf("write provider certificate: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("write provider key: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve provider address: %v", err)
	}
	providerAddress := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release provider address: %v", err)
	}

	providerBinary := filepath.Join(t.TempDir(), "github-runner-provider")
	build := exec.Command("go", "build", "-o", providerBinary, "../github-runner-provider")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build real provider command: %v\n%s", err, output)
	}
	providerLogPath := filepath.Join(t.TempDir(), "provider.log")
	providerLog, err := os.OpenFile(providerLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open provider log: %v", err)
	}
	provider := exec.Command(providerBinary, providerAddress)
	provider.Stdout = providerLog
	provider.Stderr = providerLog
	provider.Env = append(os.Environ(),
		"GITHUB_RUNNER_PROVIDER_GITHUB_TOKEN=test-github-token",
		"GITHUB_RUNNER_PROVIDER_TOKEN=test-provider-token",
		"GITHUB_RUNNER_PROVIDER_REPOSITORIES=GoCodeAlone/workflow-compute",
		"GITHUB_RUNNER_PROVIDER_ORGANIZATIONS=GoCodeAlone",
		"GITHUB_RUNNER_PROVIDER_RUNNER_GROUPS=ephemeral",
		"GITHUB_RUNNER_PROVIDER_STATE_DIR="+filepath.Join(tlsDir, "state"),
		"GITHUB_RUNNER_PROVIDER_TLS_CERT_FILE="+certFile,
		"GITHUB_RUNNER_PROVIDER_TLS_KEY_FILE="+keyFile,
	)
	if err := provider.Start(); err != nil {
		_ = providerLog.Close()
		t.Fatalf("start real provider command: %v", err)
	}
	t.Cleanup(func() {
		if provider.ProcessState == nil {
			_ = provider.Process.Kill()
			_ = provider.Wait()
		}
		_ = providerLog.Close()
	})

	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "https://"+providerAddress)
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "test-provider-token")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", base64.StdEncoding.EncodeToString(certPEM))
	client, err := newSidecarClientFromEnv()
	if err != nil {
		t.Fatalf("create production sidecar client: %v", err)
	}
	var health map[string]string
	for attempt := 0; attempt < 50; attempt++ {
		err = client.do(context.Background(), http.MethodGet, "/healthz", nil, http.StatusOK, &health)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		_ = providerLog.Close()
		output, _ := os.ReadFile(providerLogPath)
		t.Fatalf("real provider TLS health request: %v\n%s", err, output)
	}
	if health["status"] != "ok" {
		t.Fatalf("real provider health = %+v", health)
	}
}

func TestT915ProviderSidecarCertificateAuthorityKeepsHostnameVerification(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()

	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", strings.Replace(server.URL, "127.0.0.1", "localhost", 1))
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", base64.StdEncoding.EncodeToString(caPEM))

	client, err := newSidecarClientFromEnv()
	if err != nil {
		t.Fatalf("create sidecar client with explicit CA: %v", err)
	}
	if err := client.do(context.Background(), http.MethodGet, "/healthz", nil, http.StatusOK, nil); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("hostname mismatch error = %v", err)
	}
}

func TestT915ProviderSidecarCertificateAuthorityRequiresHTTPS(t *testing.T) {
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "http://127.0.0.1:8090")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
	t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", base64.StdEncoding.EncodeToString([]byte("unused")))
	if _, err := newSidecarClientFromEnv(); err == nil || !strings.Contains(err.Error(), "requires an HTTPS provider URL") {
		t.Fatalf("plaintext provider CA error = %v", err)
	}
}

func TestT915ProviderSidecarRejectsMalformedCertificateAuthority(t *testing.T) {
	for _, encoded := range []string{
		"not-base64",
		base64.StdEncoding.EncodeToString([]byte("not a PEM certificate")),
	} {
		t.Run(encoded, func(t *testing.T) {
			t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL", "https://provider.example.invalid")
			t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN", "provider-token")
			t.Setenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64", encoded)
			if _, err := newSidecarClientFromEnv(); err == nil || !strings.Contains(err.Error(), "provider CA certificate") {
				t.Fatalf("malformed provider CA error = %v", err)
			}
		})
	}
}

func TestT915ProviderCertificateAuthorityWorksWithoutSystemRoots(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer server.Close()
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})

	pool, err := providerCertificatePool(caPEM, func() (*x509.CertPool, error) {
		return nil, errors.New("system roots unavailable")
	})
	if err != nil {
		t.Fatalf("build explicit provider certificate pool: %v", err)
	}
	certificate, err := x509.ParseCertificate(server.Certificate().Raw)
	if err != nil {
		t.Fatalf("parse explicit provider certificate: %v", err)
	}
	if _, err := certificate.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		t.Fatalf("verify explicit provider certificate: %v", err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write executable %s: %v", path, err)
	}
}

func markRunnerTestDispatch(t *testing.T, workspace string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(workspace, "dispatch.seen"), []byte("1\n"), 0o600); err != nil {
		t.Fatalf("write dispatch marker: %v", err)
	}
}

func runnerWaitForDispatchScript() string {
	return "#!/bin/sh\nwhile [ ! -f \"$GITHUB_ACTIONS_RUNNER_JOB_TEST_DIR/dispatch.seen\" ]; do sleep 0.01; done\nexit 0\n"
}

func writeFunctionalRunnerListener(t *testing.T, runnerDir string) {
	t.Helper()
	binDir := filepath.Join(runnerDir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatalf("create runner bin directory: %v", err)
	}
	writeExecutable(t, filepath.Join(binDir, "Runner.Listener"), "#!/bin/sh\nexit 0\n")
}

func setRunnerProcessEnvironmentExtras(t *testing.T, names ...string) {
	t.Helper()
	previous := append([]string(nil), runnerProcessExtraEnvironmentNames...)
	runnerProcessExtraEnvironmentNames = append([]string(nil), names...)
	t.Cleanup(func() { runnerProcessExtraEnvironmentNames = previous })
}

func TestT916WriteWorkloadArtifactCapsActualCopiedBytes(t *testing.T) {
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	_, err := writeWorkloadArtifact(writer, "result.txt", strings.NewReader(strings.Repeat("x", int(maxWorkloadArtifactFileBytes)+1)), maxWorkloadArtifactFileBytes)
	_ = writer.Close()
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized actual copy error = %v", err)
	}
}

func readTarGzFiles(t *testing.T, archivePath string) map[string][]byte {
	t.Helper()
	file, err := os.Open(archivePath)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	defer func() { _ = file.Close() }()
	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("open gzip stream: %v", err)
	}
	defer func() { _ = gzipReader.Close() }()
	tarReader := tar.NewReader(gzipReader)
	files := map[string][]byte{}
	for {
		header, err := tarReader.Next()
		if errors.Is(err, io.EOF) {
			return files
		}
		if err != nil {
			t.Fatalf("read tar header: %v", err)
		}
		data, err := io.ReadAll(tarReader)
		if err != nil {
			t.Fatalf("read tar file %s: %v", header.Name, err)
		}
		files[header.Name] = data
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

func testEphemeralRunnerSpec(runnerName string) internal.EphemeralRunnerJobSpec {
	return internal.EphemeralRunnerJobSpec{
		RunnerName: runnerName,
		Labels:     []string{"self-hosted", "linux", runnerName, "wfc-ghp-stg", "wfc-ghp-ephemeral"},
	}
}

func TestT916WaitForRunnerOnlineRejectsBusyJITRunner(t *testing.T) {
	spec := testEphemeralRunnerSpec("wfc-stg-ghp-linux-busy")
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer provider-token" {
			t.Fatalf("authorization = %q", got)
		}
		if r.Method != http.MethodGet || r.URL.Path != "/v1/actions/orgs/GoCodeAlone/runners/42" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 42, "name": spec.RunnerName, "status": "online", "busy": true, "labels": spec.Labels,
		})
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Organization: "GoCodeAlone"},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	running := &runningCommand{path: "run.sh", cancel: func() {}, done: make(chan error, 1)}
	err := driver.waitForRunnerOnline(context.Background(), running, 42, spec)
	if err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("waitForRunnerOnline error = %v, want busy JIT runner rejection", err)
	}
}

func TestT916WaitForRunnerOnlineRejectsPermanentProviderAuthFailure(t *testing.T) {
	var calls int
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		http.Error(w, "invalid provider token", http.StatusUnauthorized)
	}))
	t.Cleanup(sidecar.Close)

	spec := testEphemeralRunnerSpec("wfc-stg-ghp-linux-auth")
	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Organization: "GoCodeAlone"},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	running := &runningCommand{path: "run.sh", cancel: func() {}, done: make(chan error, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := driver.waitForRunnerOnline(ctx, running, 42, spec)
	if err == nil || !strings.Contains(err.Error(), "status 401") {
		t.Fatalf("waitForRunnerOnline error = %v, want immediate provider auth failure", err)
	}
	if calls != 1 {
		t.Fatalf("permanent provider auth failure calls = %d, want 1", calls)
	}
}

func TestT916WaitForRunnerOnlineRetriesRegistrationVisibilityNotFound(t *testing.T) {
	oldPoll := githubRunnerOnlinePollInterval
	githubRunnerOnlinePollInterval = time.Millisecond
	t.Cleanup(func() { githubRunnerOnlinePollInterval = oldPoll })

	var calls int
	spec := testEphemeralRunnerSpec("wfc-stg-ghp-linux-visible")
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			http.Error(w, "runner not visible yet", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": 42, "name": spec.RunnerName, "status": "online", "busy": false, "labels": spec.Labels,
		})
	}))
	t.Cleanup(sidecar.Close)

	driver := &runnerDriver{
		req:     internal.EphemeralRunnerJobRequest{Organization: "GoCodeAlone"},
		sidecar: &providerSidecarClient{baseURL: sidecar.URL, token: "provider-token", http: sidecar.Client()},
	}
	running := &runningCommand{path: "run.sh", cancel: func() {}, done: make(chan error, 1)}
	if err := driver.waitForRunnerOnline(context.Background(), running, 42, spec); err != nil {
		t.Fatalf("waitForRunnerOnline after registration visibility delay: %v", err)
	}
	if calls != 2 {
		t.Fatalf("registration visibility calls = %d, want 2", calls)
	}
}

func writeAcceptedRunnerJIT(t *testing.T, w http.ResponseWriter, r *http.Request, runnerID int64) {
	t.Helper()
	var body struct {
		RunnerName string `json:"runner_name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode JIT request: %v", err)
	}
	if body.RunnerName == "" {
		t.Fatal("JIT request omitted runner_name")
	}
	w.WriteHeader(http.StatusCreated)
	_, _ = fmt.Fprintf(w, `{"runner_id":%d,"runner_name":%q,"encoded_jit_config":"encoded-jit-config","ownership_token":"ownership-token","preflight":{"organization":"GoCodeAlone","runner_group":"ephemeral","runner_group_id":5,"ref":"main","resolved_workflow_path":".github/workflows/dogfood.yml","resolved_ref_sha":%q,"runner_count_checked":1,"actions_enabled":true,"self_hosted_allowed":true}}`, runnerID, body.RunnerName, strings.Repeat("a", 40))
}

func writeAcceptedRunnerJITACK(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	return writeAcceptedRunnerJITACKWithToken(t, w, r, "ownership-token")
}

func writeAcceptedRunnerJITACKWithToken(t *testing.T, w http.ResponseWriter, r *http.Request, wantToken string) bool {
	t.Helper()
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/ack") {
		return false
	}
	var body struct {
		OwnershipToken string `json:"ownership_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode JIT ownership ACK: %v", err)
	}
	if body.OwnershipToken != wantToken {
		t.Fatalf("JIT ownership token = %q", body.OwnershipToken)
	}
	w.WriteHeader(http.StatusNoContent)
	return true
}

func writeOnlineRunnerStatus(w http.ResponseWriter, r *http.Request) bool {
	return writeRunnerStatusFromRequest(w, r, "online")
}

func writeRunnerStatusFromRequest(w http.ResponseWriter, r *http.Request, status string) bool {
	if r.Method != http.MethodGet || !strings.Contains(r.URL.Path, "/v1/actions/orgs/") || r.URL.Query().Get("runner_name") == "" {
		return false
	}
	runnerID, err := strconv.ParseInt(path.Base(r.URL.Path), 10, 64)
	if err != nil || runnerID <= 0 {
		return false
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": runnerID, "name": r.URL.Query().Get("runner_name"), "status": status, "labels": r.URL.Query()["label"],
	})
	return true
}

func writeWorkflowDispatchResponse(w http.ResponseWriter, workflowRunID int64) {
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"workflow_run_id":%d,"run_url":"https://api.github.com/runs/%d","html_url":"https://github.com/runs/%d","head_sha":%q}`, workflowRunID, workflowRunID, workflowRunID, strings.Repeat("a", 40))
}
