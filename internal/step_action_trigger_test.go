package internal

import (
	"context"
	"errors"
	"testing"
)

// --- mock GitHub client ---

type mockGitHubClient struct {
	triggerWorkflowFunc  func(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, token string) error
	getWorkflowRunFunc   func(ctx context.Context, owner, repo string, runID int64, token string) (*WorkflowRun, error)
	createCheckRunFunc   func(ctx context.Context, owner, repo string, req *CreateCheckRunRequest, token string) (*CheckRun, error)
}

func (m *mockGitHubClient) TriggerWorkflow(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, token string) error {
	if m.triggerWorkflowFunc != nil {
		return m.triggerWorkflowFunc(ctx, owner, repo, workflow, ref, inputs, token)
	}
	return nil
}

func (m *mockGitHubClient) GetWorkflowRun(ctx context.Context, owner, repo string, runID int64, token string) (*WorkflowRun, error) {
	if m.getWorkflowRunFunc != nil {
		return m.getWorkflowRunFunc(ctx, owner, repo, runID, token)
	}
	return &WorkflowRun{ID: runID, Status: "completed", Conclusion: "success"}, nil
}

func (m *mockGitHubClient) CreateCheckRun(ctx context.Context, owner, repo string, req *CreateCheckRunRequest, token string) (*CheckRun, error) {
	if m.createCheckRunFunc != nil {
		return m.createCheckRunFunc(ctx, owner, repo, req, token)
	}
	return &CheckRun{ID: 42, Status: "completed"}, nil
}

// --- step.gh_action_trigger tests ---

func TestActionTriggerStep_Success(t *testing.T) {
	var capturedOwner, capturedRepo, capturedWorkflow, capturedRef string
	var capturedInputs map[string]string

	client := &mockGitHubClient{
		triggerWorkflowFunc: func(_ context.Context, owner, repo, workflow, ref string, inputs map[string]string, _ string) error {
			capturedOwner = owner
			capturedRepo = repo
			capturedWorkflow = workflow
			capturedRef = ref
			capturedInputs = inputs
			return nil
		},
	}

	step, err := newActionTriggerStep("test", map[string]any{
		"owner":    "GoCodeAlone",
		"repo":     "workflow",
		"workflow": "ci.yml",
		"ref":      "main",
		"inputs":   map[string]any{"env": "staging"},
		"token":    "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newActionTriggerStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Errorf("expected StopPipeline=false on success")
	}

	if capturedOwner != "GoCodeAlone" {
		t.Errorf("expected owner=GoCodeAlone, got %q", capturedOwner)
	}
	if capturedRepo != "workflow" {
		t.Errorf("expected repo=workflow, got %q", capturedRepo)
	}
	if capturedWorkflow != "ci.yml" {
		t.Errorf("expected workflow=ci.yml, got %q", capturedWorkflow)
	}
	if capturedRef != "main" {
		t.Errorf("expected ref=main, got %q", capturedRef)
	}
	if capturedInputs["env"] != "staging" {
		t.Errorf("expected inputs.env=staging, got %q", capturedInputs["env"])
	}

	if triggered, _ := result.Output["triggered"].(bool); !triggered {
		t.Error("expected output.triggered=true")
	}
}

func TestActionTriggerStep_APIError(t *testing.T) {
	client := &mockGitHubClient{
		triggerWorkflowFunc: func(_ context.Context, _, _, _, _ string, _ map[string]string, _ string) error {
			return errors.New("API rate limit exceeded")
		},
	}

	step, err := newActionTriggerStep("test", map[string]any{
		"owner":    "GoCodeAlone",
		"repo":     "workflow",
		"workflow": "ci.yml",
		"token":    "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newActionTriggerStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true on API error")
	}
}

func TestActionTriggerStep_MissingToken(t *testing.T) {
	client := &mockGitHubClient{}

	step, err := newActionTriggerStep("test", map[string]any{
		"owner":    "GoCodeAlone",
		"repo":     "workflow",
		"workflow": "ci.yml",
		"token":    "", // empty token
	}, client)
	if err != nil {
		t.Fatalf("newActionTriggerStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true when token is missing")
	}
}

func TestActionTriggerStep_DefaultRef(t *testing.T) {
	var capturedRef string
	client := &mockGitHubClient{
		triggerWorkflowFunc: func(_ context.Context, _, _, _, ref string, _ map[string]string, _ string) error {
			capturedRef = ref
			return nil
		},
	}

	step, err := newActionTriggerStep("test", map[string]any{
		"owner":    "GoCodeAlone",
		"repo":     "workflow",
		"workflow": "ci.yml",
		"token":    "gh-token",
		// No ref specified; should default to "main".
	}, client)
	if err != nil {
		t.Fatalf("newActionTriggerStep: %v", err)
	}

	_, err = step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if capturedRef != "main" {
		t.Errorf("expected default ref=main, got %q", capturedRef)
	}
}

// --- config validation tests ---

func TestParseActionTriggerConfig_MissingOwner(t *testing.T) {
	_, err := parseActionTriggerConfig(map[string]any{
		"repo":     "workflow",
		"workflow": "ci.yml",
	})
	if err == nil {
		t.Error("expected error for missing owner")
	}
}

func TestParseActionTriggerConfig_MissingRepo(t *testing.T) {
	_, err := parseActionTriggerConfig(map[string]any{
		"owner":    "GoCodeAlone",
		"workflow": "ci.yml",
	})
	if err == nil {
		t.Error("expected error for missing repo")
	}
}

func TestParseActionTriggerConfig_MissingWorkflow(t *testing.T) {
	_, err := parseActionTriggerConfig(map[string]any{
		"owner": "GoCodeAlone",
		"repo":  "workflow",
	})
	if err == nil {
		t.Error("expected error for missing workflow")
	}
}
