package internal_test

import (
	"testing"

	"github.com/GoCodeAlone/workflow/wftest"
)

// TestIntegration_ActionTrigger verifies that step.gh_action_trigger can be
// mocked and that subsequent pipeline steps see its output.
func TestIntegration_ActionTrigger(t *testing.T) {
	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  trigger-pipeline:
    steps:
      - name: trigger
        type: step.gh_action_trigger
        config:
          owner: "GoCodeAlone"
          repo: "workflow"
          workflow: "ci.yml"
          ref: "main"
          token: "fake-token"
      - name: confirm
        type: step.set
        config:
          values:
            dispatched: true
`),
		wftest.MockStep("step.gh_action_trigger", wftest.Returns(map[string]any{
			"triggered": true,
			"owner":     "GoCodeAlone",
			"repo":      "workflow",
			"workflow":  "ci.yml",
			"ref":       "main",
		})),
	)

	result := h.ExecutePipeline("trigger-pipeline", nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output["dispatched"] != true {
		t.Errorf("expected dispatched=true, got %v", result.Output["dispatched"])
	}
	triggerOut := result.StepResults["trigger"]
	if triggerOut["triggered"] != true {
		t.Errorf("expected trigger.triggered=true, got %v", triggerOut["triggered"])
	}
}

// TestIntegration_ActionStatus verifies that step.gh_action_status can be
// mocked to return a completed run and that downstream steps can use the output.
func TestIntegration_ActionStatus(t *testing.T) {
	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  status-pipeline:
    steps:
      - name: status
        type: step.gh_action_status
        config:
          owner: "GoCodeAlone"
          repo: "workflow"
          run_id: 12345
          token: "fake-token"
      - name: confirm
        type: step.set
        config:
          values:
            checked: true
`),
		wftest.MockStep("step.gh_action_status", wftest.Returns(map[string]any{
			"run_id":     int64(12345),
			"status":     "completed",
			"conclusion": "success",
			"url":        "https://github.com/GoCodeAlone/workflow/actions/runs/12345",
		})),
	)

	result := h.ExecutePipeline("status-pipeline", nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output["checked"] != true {
		t.Errorf("expected checked=true, got %v", result.Output["checked"])
	}
	statusOut := result.StepResults["status"]
	if statusOut["conclusion"] != "success" {
		t.Errorf("expected status.conclusion=success, got %v", statusOut["conclusion"])
	}
}

// TestIntegration_CreateCheck verifies that step.gh_create_check can be mocked
// and records its invocation via a Recorder.
func TestIntegration_CreateCheck(t *testing.T) {
	rec := wftest.RecordStep("step.gh_create_check")
	rec.WithOutput(map[string]any{
		"check_run_id": int64(99),
		"status":       "completed",
		"url":          "https://github.com/GoCodeAlone/workflow/runs/99",
	})

	h := wftest.New(t, wftest.WithYAML(`
pipelines:
  check-pipeline:
    steps:
      - name: create_check
        type: step.gh_create_check
        config:
          owner: "GoCodeAlone"
          repo: "workflow"
          sha: "abc123"
          name: "workflow-ci"
          status: "completed"
          conclusion: "success"
          title: "CI Pipeline"
          summary: "All tests passed"
          token: "fake-token"
      - name: done
        type: step.set
        config:
          values:
            reported: true
`),
		rec,
	)

	result := h.ExecutePipeline("check-pipeline", nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output["reported"] != true {
		t.Errorf("expected reported=true, got %v", result.Output["reported"])
	}
	if rec.CallCount() != 1 {
		t.Errorf("expected step.gh_create_check to be called once, got %d", rec.CallCount())
	}
	checkOut := result.StepResults["create_check"]
	if checkOut["check_run_id"] != int64(99) {
		t.Errorf("expected check_run_id=99, got %v", checkOut["check_run_id"])
	}
}
