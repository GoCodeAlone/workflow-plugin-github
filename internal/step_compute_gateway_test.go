package internal

import (
	"context"
	"testing"
)

type fakeComputeGatewayClient struct {
	submitted []computeGatewayWorkloadRequest
	statuses  []computeGatewayWorkloadStatus
}

func (c *fakeComputeGatewayClient) SubmitWorkload(_ context.Context, serverURL, token string, req computeGatewayWorkloadRequest) (computeGatewayWorkloadResponse, error) {
	c.submitted = append(c.submitted, req)
	return computeGatewayWorkloadResponse{
		Task: computeGatewayTask{
			ID:     "github-gateway-task-1",
			Status: "queued",
			Labels: map[string]string{
				"adapter": "github-gateway",
			},
		},
	}, nil
}

func (c *fakeComputeGatewayClient) WorkloadStatus(_ context.Context, serverURL, token, taskID string) (computeGatewayStatusResponse, error) {
	if len(c.statuses) == 0 {
		return computeGatewayStatusResponse{Status: computeGatewayWorkloadStatus{TaskID: taskID, Status: "queued"}}, nil
	}
	status := c.statuses[0]
	c.statuses = c.statuses[1:]
	return computeGatewayStatusResponse{Status: status}, nil
}

func TestComputeGatewayStepSubmitsProtectedWorkload(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			proofBackedComputeGatewayStatus(),
		},
	}
	step, err := newComputeGatewayStep("compute", map[string]any{
		"server_url":        "https://compute.example.test",
		"token":             "compute-token",
		"repository":        "GoCodeAlone/workflow-compute",
		"oidc_token":        "oidc",
		"workflow_run_id":   "42",
		"workflow_job_id":   "99",
		"workflow_job_name": "build",
		"ref":               "refs/heads/main",
		"sha":               "abc123",
		"org_id":            "org-1",
		"pool_id":           "pool-1",
		"policy_id":         "policy-1",
		"command_args":      []any{"go", "test", "./..."},
		"wait":              true,
		"poll_interval":     "1ms",
		"timeout":           "100ms",
	}, client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Fatalf("unexpected stop: %+v", result.Output)
	}
	if len(client.submitted) != 1 {
		t.Fatalf("submissions: got %d want 1", len(client.submitted))
	}
	got := client.submitted[0]
	if got.Requirements.ExecutionSecurityTier != "sandboxed-container" || got.Requirements.ProofTier != "artifact-hash" {
		t.Fatalf("gateway placement defaults: %+v", got.Requirements)
	}
	if got.Repository != "GoCodeAlone/workflow-compute" || got.WorkflowRunID != 42 || got.WorkflowJobID != 99 {
		t.Fatalf("gateway request: %+v", got)
	}
	if result.StopPipeline {
		t.Fatalf("unexpected stop: %+v", result.Output)
	}
	if taskID, _ := result.Output["task_id"].(string); taskID != "github-gateway-task-1" {
		t.Fatalf("task_id output: got %q", taskID)
	}
}

func TestComputeGatewayStepWaitsForProofBackedStatus(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			{TaskID: "github-gateway-task-1", Status: "queued"},
			proofBackedComputeGatewayStatus(),
		},
	}
	step, err := newComputeGatewayStep("compute", map[string]any{
		"server_url":        "https://compute.example.test",
		"token":             "compute-token",
		"repository":        "GoCodeAlone/workflow-compute",
		"oidc_token":        "oidc",
		"workflow_run_id":   int64(42),
		"workflow_job_id":   int64(99),
		"workflow_job_name": "build",
		"ref":               "refs/heads/main",
		"sha":               "abc123",
		"org_id":            "org-1",
		"pool_id":           "pool-1",
		"policy_id":         "policy-1",
		"command_args":      []string{"true"},
		"wait":              true,
		"poll_interval":     "1ms",
		"timeout":           "100ms",
	}, client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Fatalf("unexpected stop: %+v", result.Output)
	}
	if result.Output["conclusion"] != "success" || result.Output["proof_id"] != "proof-1" || result.Output["contribution_id"] != "contrib-1" {
		t.Fatalf("proof-backed output: %+v", result.Output)
	}
}

func TestComputeGatewayStepRejectsSuccessWithoutProofBacking(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			{TaskID: "github-gateway-task-1", Status: "succeeded", Conclusion: "success", Labels: verifiedGitHubLabels()},
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"poll_interval": "1ms",
		"timeout":       "100ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Fatalf("unbacked success must stop pipeline: %+v", result.Output)
	}
}

func TestComputeGatewayStepWritesFailureCheckForUnbackedSuccess(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			{TaskID: "github-gateway-task-1", Status: "succeeded", Conclusion: "success", Labels: verifiedGitHubLabels()},
		},
	}
	var capturedConclusion string
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, req *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			capturedConclusion = req.Conclusion
			return &CheckRun{ID: 79, HTMLURL: "https://github.example/check/79", Status: "completed"}, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "GoCodeAlone",
		"check_repo":    "workflow-compute",
		"check_sha":     "abc123",
		"check_name":    "workflow-compute",
		"check_token":   "gh-token",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline || capturedConclusion == "success" {
		t.Fatalf("unbacked success check/stop: conclusion=%q output=%+v", capturedConclusion, result.Output)
	}
}

func TestComputeGatewayStepWritesGitHubCheckForProofBackedStatus(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			proofBackedComputeGatewayStatus(),
		},
	}
	var capturedReq *CreateCheckRunRequest
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, owner, repo string, req *CreateCheckRunRequest, token string) (*CheckRun, error) {
			if owner != "GoCodeAlone" || repo != "workflow-compute" || token != "gh-token-from-trigger" {
				t.Fatalf("check target: owner=%s repo=%s token=%s", owner, repo, token)
			}
			capturedReq = req
			return &CheckRun{ID: 77, HTMLURL: "https://github.example/check/77", Status: "completed"}, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "GoCodeAlone",
		"check_repo":    "workflow-compute",
		"check_sha":     "abc123",
		"check_name":    "workflow-compute",
		"check_token":   "{{.github_token}}",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), map[string]any{"github_token": "gh-token-from-trigger"}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Fatalf("unexpected stop: %+v", result.Output)
	}
	if capturedReq == nil || capturedReq.Conclusion != "success" || capturedReq.HeadSHA != "abc123" {
		t.Fatalf("check request: %+v", capturedReq)
	}
	if result.Output["check_run_id"] != int64(77) {
		t.Fatalf("check output: %+v", result.Output)
	}
}

func TestComputeGatewayStepCheckTargetBindingIsCaseInsensitive(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			proofBackedComputeGatewayStatus(),
		},
	}
	var called bool
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, _ *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			called = true
			return &CheckRun{ID: 80, HTMLURL: "https://github.example/check/80", Status: "completed"}, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "gocodealone",
		"check_repo":    "Workflow-Compute",
		"check_sha":     "ABC123",
		"check_name":    "workflow-compute",
		"check_token":   "gh-token",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline || !called {
		t.Fatalf("case-insensitive target should write check: called=%v output=%+v", called, result.Output)
	}
}

func TestComputeGatewayStepRejectsCheckTargetMismatch(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			proofBackedComputeGatewayStatus(),
		},
	}
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, _ *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			t.Fatal("mismatched check target must not call GitHub")
			return nil, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "GoCodeAlone",
		"check_repo":    "other",
		"check_sha":     "abc123",
		"check_name":    "workflow-compute",
		"check_token":   "gh-token",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Fatalf("mismatched check target must stop pipeline: %+v", result.Output)
	}
}

func TestComputeGatewayStepStopsOnFailedTerminalStatus(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			{TaskID: "github-gateway-task-1", Status: "failed", Labels: verifiedGitHubLabels()},
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"poll_interval": "1ms",
		"timeout":       "100ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Fatalf("failed compute status must stop pipeline: %+v", result.Output)
	}
}

func TestComputeGatewayStepWritesFailureCheckBeforeStopping(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			{TaskID: "github-gateway-task-1", Status: "failed", Labels: verifiedGitHubLabels()},
		},
	}
	var capturedConclusion string
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, req *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			capturedConclusion = req.Conclusion
			return &CheckRun{ID: 78, HTMLURL: "https://github.example/check/78", Status: "completed"}, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "GoCodeAlone",
		"check_repo":    "workflow-compute",
		"check_sha":     "abc123",
		"check_name":    "workflow-compute",
		"check_token":   "gh-token",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline || capturedConclusion != "failure" {
		t.Fatalf("failure check/stop: conclusion=%q output=%+v", capturedConclusion, result.Output)
	}
}

func TestComputeGatewayStepRejectsUnresolvedCheckToken(t *testing.T) {
	client := &fakeComputeGatewayClient{
		statuses: []computeGatewayWorkloadStatus{
			proofBackedComputeGatewayStatus(),
		},
	}
	gh := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, _ *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			t.Fatal("unresolved check token must not call GitHub")
			return nil, nil
		},
	}
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait":          true,
		"write_check":   true,
		"check_owner":   "GoCodeAlone",
		"check_repo":    "workflow-compute",
		"check_sha":     "abc123",
		"check_name":    "workflow-compute",
		"check_token":   "{{.missing_github_token}}",
		"poll_interval": "1ms",
	}), client)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	step.github = gh

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Fatalf("unresolved check token must stop pipeline: %+v", result.Output)
	}
}

func TestComputeGatewayStepRejectsDowngradedPlacement(t *testing.T) {
	_, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"execution_security_tier": "trusted-native",
		"proof_tier":              "receipt-only",
	}), &fakeComputeGatewayClient{})
	if err == nil {
		t.Fatal("downgraded compute gateway placement must fail")
	}
}

func TestComputeGatewayStepRequiresOIDCProvenanceToken(t *testing.T) {
	_, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"oidc_token": "",
	}), &fakeComputeGatewayClient{})
	if err == nil {
		t.Fatal("compute gateway step must require oidc_token")
	}
}

func TestComputeGatewayStepRequiresWaitForProofBackedStatus(t *testing.T) {
	_, err := newComputeGatewayStep("compute", validComputeGatewayConfig(map[string]any{
		"wait": false,
	}), &fakeComputeGatewayClient{})
	if err == nil {
		t.Fatal("compute gateway step must require wait=true")
	}
}

func TestComputeGatewayStepDefaultsWaitToTrue(t *testing.T) {
	_, err := newComputeGatewayStep("compute", validComputeGatewayConfig(nil), &fakeComputeGatewayClient{})
	if err != nil {
		t.Fatalf("default wait should be true: %v", err)
	}
}

func TestComputeGatewayStepDefaultHTTPClientHasTimeout(t *testing.T) {
	step, err := newComputeGatewayStep("compute", validComputeGatewayConfig(nil), nil)
	if err != nil {
		t.Fatalf("newComputeGatewayStep: %v", err)
	}
	client, ok := step.client.(httpComputeGatewayClient)
	if !ok {
		t.Fatalf("client type: got %T", step.client)
	}
	if client.http == nil || client.http.Timeout != computeGatewayHTTPTimeout {
		t.Fatalf("http timeout: got %+v want %s", client.http, computeGatewayHTTPTimeout)
	}
}

func TestComputeGatewayStepRejectsUnsafeNumericConfig(t *testing.T) {
	cases := map[string]map[string]any{
		"fractional timeout": {"timeout_seconds": 1.9},
		"negative timeout":   {"timeout_seconds": -1},
		"zero poll interval": {"poll_interval": "0s"},
		"negative wait":      {"timeout": "-1s"},
		"numeric duration":   {"poll_interval": 1000},
		"string wait":        {"wait": "true"},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := newComputeGatewayStep("compute", validComputeGatewayConfig(override), &fakeComputeGatewayClient{})
			if err == nil {
				t.Fatal("unsafe numeric config must fail")
			}
		})
	}
}

func validComputeGatewayConfig(overrides map[string]any) map[string]any {
	cfg := map[string]any{
		"server_url":        "https://compute.example.test",
		"token":             "compute-token",
		"repository":        "GoCodeAlone/workflow-compute",
		"oidc_token":        "oidc",
		"workflow_run_id":   "42",
		"workflow_job_id":   "99",
		"workflow_job_name": "build",
		"org_id":            "org-1",
		"pool_id":           "pool-1",
		"policy_id":         "policy-1",
		"command_args":      []any{"true"},
	}
	for key, value := range overrides {
		cfg[key] = value
	}
	return cfg
}

func proofBackedComputeGatewayStatus() computeGatewayWorkloadStatus {
	return computeGatewayWorkloadStatus{
		TaskID:         "github-gateway-task-1",
		Status:         "succeeded",
		Conclusion:     "success",
		ProofID:        "proof-1",
		ContributionID: "contrib-1",
		Labels:         verifiedGitHubLabels(),
	}
}

func verifiedGitHubLabels() map[string]string {
	return map[string]string{
		"github.provenance.repository": "GoCodeAlone/workflow-compute",
		"github.provenance.sha":        "abc123",
	}
}
