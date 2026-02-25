package internal

import (
	"context"
	"errors"
	"testing"
)

// --- step.gh_create_check tests ---

func TestCreateCheckStep_Success(t *testing.T) {
	var capturedReq *CreateCheckRunRequest

	client := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, req *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			capturedReq = req
			return &CheckRun{
				ID:      42,
				Status:  "completed",
				HTMLURL: "https://github.com/owner/repo/runs/42",
			}, nil
		},
	}

	step, err := newCreateCheckStep("test", map[string]any{
		"owner":      "GoCodeAlone",
		"repo":       "workflow",
		"sha":        "abc123",
		"name":       "workflow-ci",
		"status":     "completed",
		"conclusion": "success",
		"title":      "CI Pipeline",
		"summary":    "All tests passed",
		"token":      "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newCreateCheckStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Error("expected StopPipeline=false on success")
	}

	if capturedReq.Name != "workflow-ci" {
		t.Errorf("expected name=workflow-ci, got %q", capturedReq.Name)
	}
	if capturedReq.HeadSHA != "abc123" {
		t.Errorf("expected sha=abc123, got %q", capturedReq.HeadSHA)
	}
	if capturedReq.Status != "completed" {
		t.Errorf("expected status=completed, got %q", capturedReq.Status)
	}
	if capturedReq.Conclusion != "success" {
		t.Errorf("expected conclusion=success, got %q", capturedReq.Conclusion)
	}
	if capturedReq.Output == nil || capturedReq.Output.Title != "CI Pipeline" {
		t.Errorf("expected output.title=CI Pipeline, got %v", capturedReq.Output)
	}
	if capturedReq.Output.Summary != "All tests passed" {
		t.Errorf("expected output.summary='All tests passed', got %q", capturedReq.Output.Summary)
	}

	if checkID, _ := result.Output["check_run_id"].(int64); checkID != 42 {
		t.Errorf("expected check_run_id=42, got %v", result.Output["check_run_id"])
	}
}

func TestCreateCheckStep_InProgress(t *testing.T) {
	client := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, req *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			return &CheckRun{ID: 1, Status: req.Status}, nil
		},
	}

	step, err := newCreateCheckStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc123",
		"name":   "workflow-ci",
		"status": "in_progress",
		"token":  "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newCreateCheckStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Error("expected StopPipeline=false")
	}
}

func TestCreateCheckStep_APIError(t *testing.T) {
	client := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, _ *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			return nil, errors.New("check run creation failed")
		},
	}

	step, err := newCreateCheckStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc123",
		"name":   "workflow-ci",
		"status": "queued",
		"token":  "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newCreateCheckStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true on API error")
	}
}

func TestCreateCheckStep_MissingToken(t *testing.T) {
	client := &mockGitHubClient{}

	step, err := newCreateCheckStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc123",
		"name":   "workflow-ci",
		"status": "queued",
		"token":  "",
	}, client)
	if err != nil {
		t.Fatalf("newCreateCheckStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true when token is missing")
	}
}

func TestCreateCheckStep_NoOutputWhenTitleAndSummaryEmpty(t *testing.T) {
	var capturedReq *CreateCheckRunRequest
	client := &mockGitHubClient{
		createCheckRunFunc: func(_ context.Context, _, _ string, req *CreateCheckRunRequest, _ string) (*CheckRun, error) {
			capturedReq = req
			return &CheckRun{ID: 1, Status: "queued"}, nil
		},
	}

	step, err := newCreateCheckStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc123",
		"name":   "workflow-ci",
		"status": "queued",
		"token":  "gh-token",
		// No title or summary.
	}, client)
	if err != nil {
		t.Fatalf("newCreateCheckStep: %v", err)
	}

	_, err = step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if capturedReq.Output != nil {
		t.Error("expected nil output when title and summary are empty")
	}
}

// --- config validation tests ---

func TestParseCreateCheckConfig_MissingOwner(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"repo":   "workflow",
		"sha":    "abc",
		"name":   "ci",
		"status": "queued",
	})
	if err == nil {
		t.Error("expected error for missing owner")
	}
}

func TestParseCreateCheckConfig_MissingRepo(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"sha":    "abc",
		"name":   "ci",
		"status": "queued",
	})
	if err == nil {
		t.Error("expected error for missing repo")
	}
}

func TestParseCreateCheckConfig_MissingSHA(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"name":   "ci",
		"status": "queued",
	})
	if err == nil {
		t.Error("expected error for missing sha")
	}
}

func TestParseCreateCheckConfig_MissingName(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc",
		"status": "queued",
	})
	if err == nil {
		t.Error("expected error for missing name")
	}
}

func TestParseCreateCheckConfig_InvalidStatus(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc",
		"name":   "ci",
		"status": "unknown-status",
	})
	if err == nil {
		t.Error("expected error for invalid status")
	}
}

func TestParseCreateCheckConfig_CompletedRequiresConclusion(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"sha":    "abc",
		"name":   "ci",
		"status": "completed",
		// No conclusion.
	})
	if err == nil {
		t.Error("expected error when status=completed but conclusion is missing")
	}
}

func TestParseCreateCheckConfig_InvalidConclusion(t *testing.T) {
	_, err := parseCreateCheckConfig(map[string]any{
		"owner":      "GoCodeAlone",
		"repo":       "workflow",
		"sha":        "abc",
		"name":       "ci",
		"status":     "completed",
		"conclusion": "bad-conclusion",
	})
	if err == nil {
		t.Error("expected error for invalid conclusion")
	}
}

func TestParseCreateCheckConfig_DefaultStatus(t *testing.T) {
	cfg, err := parseCreateCheckConfig(map[string]any{
		"owner": "GoCodeAlone",
		"repo":  "workflow",
		"sha":   "abc",
		"name":  "ci",
		// No status — should default to "queued".
	})
	if err != nil {
		t.Fatalf("parseCreateCheckConfig: %v", err)
	}
	if cfg.Status != "queued" {
		t.Errorf("expected default status=queued, got %q", cfg.Status)
	}
}
