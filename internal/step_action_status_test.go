package internal

import (
	"context"
	"errors"
	"testing"
	"time"
)

// --- step.gh_action_status tests ---

func TestActionStatusStep_SingleFetch(t *testing.T) {
	client := &mockGitHubClient{
		getWorkflowRunFunc: func(_ context.Context, _, _ string, runID int64, _ string) (*WorkflowRun, error) {
			return &WorkflowRun{
				ID:         runID,
				Status:     "completed",
				Conclusion: "success",
				HTMLURL:    "https://github.com/owner/repo/actions/runs/123",
			}, nil
		},
	}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"run_id": 123,
		"token":  "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Error("expected StopPipeline=false on success")
	}
	if result.Output["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", result.Output["status"])
	}
	if result.Output["conclusion"] != "success" {
		t.Errorf("expected conclusion=success, got %v", result.Output["conclusion"])
	}
}

func TestActionStatusStep_APIError(t *testing.T) {
	client := &mockGitHubClient{
		getWorkflowRunFunc: func(_ context.Context, _, _ string, _ int64, _ string) (*WorkflowRun, error) {
			return nil, errors.New("not found")
		},
	}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"run_id": 999,
		"token":  "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true on API error")
	}
}

func TestActionStatusStep_MissingToken(t *testing.T) {
	client := &mockGitHubClient{}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"run_id": 123,
		"token":  "",
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true when token is missing")
	}
}

func TestActionStatusStep_WaitUntilComplete(t *testing.T) {
	callCount := 0
	client := &mockGitHubClient{
		getWorkflowRunFunc: func(_ context.Context, _, _ string, _ int64, _ string) (*WorkflowRun, error) {
			callCount++
			if callCount < 3 {
				return &WorkflowRun{ID: 1, Status: "in_progress"}, nil
			}
			return &WorkflowRun{ID: 1, Status: "completed", Conclusion: "success"}, nil
		},
	}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":         "GoCodeAlone",
		"repo":          "workflow",
		"run_id":        1,
		"token":         "gh-token",
		"wait":          true,
		"poll_interval": "1ms",
		"timeout":       "5s",
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Error("expected StopPipeline=false on completion")
	}
	if callCount < 3 {
		t.Errorf("expected at least 3 polls, got %d", callCount)
	}
	if result.Output["status"] != "completed" {
		t.Errorf("expected status=completed, got %v", result.Output["status"])
	}
}

func TestActionStatusStep_WaitTimeout(t *testing.T) {
	client := &mockGitHubClient{
		getWorkflowRunFunc: func(_ context.Context, _, _ string, _ int64, _ string) (*WorkflowRun, error) {
			return &WorkflowRun{ID: 1, Status: "in_progress"}, nil // never completes
		},
	}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":         "GoCodeAlone",
		"repo":          "workflow",
		"run_id":        1,
		"token":         "gh-token",
		"wait":          true,
		"poll_interval": "1ms",
		"timeout":       "50ms", // very short timeout
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true on timeout")
	}
}

func TestActionStatusStep_ContextCancelled(t *testing.T) {
	client := &mockGitHubClient{
		getWorkflowRunFunc: func(_ context.Context, _, _ string, _ int64, _ string) (*WorkflowRun, error) {
			return &WorkflowRun{ID: 1, Status: "in_progress"}, nil
		},
	}

	step, err := newActionStatusStep("test", map[string]any{
		"owner":         "GoCodeAlone",
		"repo":          "workflow",
		"run_id":        1,
		"token":         "gh-token",
		"wait":          true,
		"poll_interval": "50ms",
		"timeout":       "10s",
	}, client)
	if err != nil {
		t.Fatalf("newActionStatusStep: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a brief delay.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	result, err := step.Execute(ctx, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Error("expected StopPipeline=true on context cancellation")
	}
}

// --- config validation tests ---

func TestParseActionStatusConfig_MissingOwner(t *testing.T) {
	_, err := parseActionStatusConfig(map[string]any{
		"repo":   "workflow",
		"run_id": 1,
	})
	if err == nil {
		t.Error("expected error for missing owner")
	}
}

func TestParseActionStatusConfig_MissingRepo(t *testing.T) {
	_, err := parseActionStatusConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"run_id": 1,
	})
	if err == nil {
		t.Error("expected error for missing repo")
	}
}

func TestParseActionStatusConfig_MissingRunID(t *testing.T) {
	_, err := parseActionStatusConfig(map[string]any{
		"owner": "GoCodeAlone",
		"repo":  "workflow",
	})
	if err == nil {
		t.Error("expected error for missing run_id")
	}
}

func TestParseActionStatusConfig_RunIDAsString(t *testing.T) {
	cfg, err := parseActionStatusConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"run_id": "456",
	})
	if err != nil {
		t.Fatalf("parseActionStatusConfig: %v", err)
	}
	if cfg.RunID != 456 {
		t.Errorf("expected run_id=456, got %d", cfg.RunID)
	}
}

func TestParseActionStatusConfig_RunIDAsFloat(t *testing.T) {
	cfg, err := parseActionStatusConfig(map[string]any{
		"owner":  "GoCodeAlone",
		"repo":   "workflow",
		"run_id": float64(789),
	})
	if err != nil {
		t.Fatalf("parseActionStatusConfig: %v", err)
	}
	if cfg.RunID != 789 {
		t.Errorf("expected run_id=789, got %d", cfg.RunID)
	}
}

func TestParseActionStatusConfig_InvalidPollInterval(t *testing.T) {
	_, err := parseActionStatusConfig(map[string]any{
		"owner":         "GoCodeAlone",
		"repo":          "workflow",
		"run_id":        1,
		"poll_interval": "not-a-duration",
	})
	if err == nil {
		t.Error("expected error for invalid poll_interval")
	}
}

func TestParseActionStatusConfig_InvalidTimeout(t *testing.T) {
	_, err := parseActionStatusConfig(map[string]any{
		"owner":   "GoCodeAlone",
		"repo":    "workflow",
		"run_id":  1,
		"timeout": "not-a-duration",
	})
	if err == nil {
		t.Error("expected error for invalid timeout")
	}
}
