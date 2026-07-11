package internal

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestT594EphemeralRunnerJobBuildsDeterministicNameAndLabels(t *testing.T) {
	spec, err := BuildEphemeralRunnerJobSpec(EphemeralRunnerJobRequest{
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-0123456789abcdef",
		TaskID:       "task-abcdef9876543210",
		Organization: "GoCodeAlone",
		RunnerGroup:  "workflow-compute-stg",
	})
	if err != nil {
		t.Fatalf("build spec: %v", err)
	}

	if spec.RunnerName != "wfc-stg-ghp-linux-abcdef987249-543210f71ee4" {
		t.Fatalf("runner name: got %q", spec.RunnerName)
	}
	wantLabels := []string{
		"self-hosted",
		"linux",
		"wfc-stg-ghp-linux-abcdef987249-543210f71ee4",
		"wfc-ghp-stg",
		"wfc-ghp-ephemeral",
	}
	if !reflect.DeepEqual(spec.Labels, wantLabels) {
		t.Fatalf("labels:\n got %#v\nwant %#v", spec.Labels, wantLabels)
	}
	if spec.RunnerGroup != "workflow-compute-stg" {
		t.Fatalf("runner group: got %q", spec.RunnerGroup)
	}
}

func TestT594EphemeralRunnerJobSpecKeepsTimestampedDogfoodTasksUnique(t *testing.T) {
	base := EphemeralRunnerJobRequest{
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "github-runner-linux-stg-am5-2-20260629",
		Organization: "GoCodeAlone",
		RunnerGroup:  "ephemeral",
	}
	first := base
	first.TaskID = "github-provider-dogfood-linux-20260630061427"
	second := base
	second.TaskID = "github-provider-dogfood-linux-20260630064133"

	firstSpec, err := BuildEphemeralRunnerJobSpec(first)
	if err != nil {
		t.Fatalf("build first spec: %v", err)
	}
	secondSpec, err := BuildEphemeralRunnerJobSpec(second)
	if err != nil {
		t.Fatalf("build second spec: %v", err)
	}

	if firstSpec.RunnerName == secondSpec.RunnerName {
		t.Fatalf("runner names must be unique across timestamped dogfood tasks, both got %q", firstSpec.RunnerName)
	}
	if firstSpec.Labels[2] == secondSpec.Labels[2] {
		t.Fatalf("runner dispatch labels must be unique across timestamped dogfood tasks, both got %q", firstSpec.Labels[2])
	}
	if firstSpec.RunnerName != "wfc-stg-ghp-linux-260629fabb6f-061427fb2c0e" {
		t.Fatalf("first runner name: got %q", firstSpec.RunnerName)
	}
	if secondSpec.RunnerName != "wfc-stg-ghp-linux-260629fabb6f-064133cf86b4" {
		t.Fatalf("second runner name: got %q", secondSpec.RunnerName)
	}
}

func TestT594ShortEphemeralIDKeepsSanitizedShortIDsUnique(t *testing.T) {
	if got := shortEphemeralID("abcd"); got != "abcd" {
		t.Fatalf("unchanged short ID: got %q", got)
	}
	if got := shortEphemeralID("ab-cd"); got != "abcd5db3d8" {
		t.Fatalf("sanitized short ID: got %q", got)
	}
	if shortEphemeralID("abcd") == shortEphemeralID("ab-cd") {
		t.Fatal("sanitized short IDs must not collide with already-safe short IDs")
	}
}

func TestT594EphemeralRunnerJobUsesExactLabelsForDispatchAndAttachModes(t *testing.T) {
	for _, mode := range []EphemeralRunnerJobMode{EphemeralRunnerJobModeDispatchThenWait, EphemeralRunnerJobModeAttachToQueued} {
		t.Run(string(mode), func(t *testing.T) {
			driver := &fakeEphemeralRunnerDriver{
				result: EphemeralRunnerJobResult{
					RunnerID:      42,
					RunnerName:    "wfc-stg-ghp-linux-abcdef987249-543210f71ee4",
					Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
					WorkflowRunID: 1001,
					WorkflowJobID: 2002,
					CleanupStatus: "removed",
				},
			}
			job := NewEphemeralRunnerJob(driver)
			result, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
				Mode:         mode,
				Environment:  "stg",
				OS:           "linux",
				WorkerID:     "worker-0123456789abcdef",
				TaskID:       "task-abcdef9876543210",
				Organization: "GoCodeAlone",
				Repository:   "GoCodeAlone/workflow-compute",
				Workflow:     "dogfood.yml",
				Ref:          "main",
				RuntimeCaps:  []string{"github-actions-runner"},
			})
			if err != nil {
				t.Fatalf("run job: %v", err)
			}
			if driver.mode != mode {
				t.Fatalf("mode: got %q want %q", driver.mode, mode)
			}
			if !reflect.DeepEqual(driver.spec.Labels, result.Labels) {
				t.Fatalf("driver labels:\n got %#v\nwant %#v", driver.spec.Labels, result.Labels)
			}
		})
	}
}

func TestT916EphemeralRunnerJobAlwaysRequiresRunnerExecutorCapability(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{}
	job := NewEphemeralRunnerJob(driver)
	result, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeDispatchThenWait,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-1",
		TaskID:       "task-1",
		Organization: "GoCodeAlone",
	})
	if !errors.Is(err, ErrEphemeralRunnerCapabilityUnsupported) || !strings.Contains(err.Error(), "github-actions-runner") {
		t.Fatalf("missing mandatory runner capability error = %v", err)
	}
	if driver.mode != "" {
		t.Fatalf("driver ran without mandatory runner capability: mode=%q", driver.mode)
	}
	if result.RunnerName == "" || len(result.Labels) != 5 || result.WorkerID != "worker-1" || result.TaskID != "task-1" || result.CleanupStatus != "skipped" {
		t.Fatalf("capability failure proof identity = %+v", result)
	}
}

func TestT594EphemeralRunnerJobCleansUpRunnerOnTimeout(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{
		result: EphemeralRunnerJobResult{
			RunnerID:      42,
			RunnerName:    "wfc-stg-ghp-linux-abcdef987249-543210f71ee4",
			CleanupStatus: "pending",
		},
		blockUntilDone: true,
	}
	job := NewEphemeralRunnerJob(driver)
	result, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeDispatchThenWait,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-0123456789abcdef",
		TaskID:       "task-abcdef9876543210",
		Organization: "GoCodeAlone",
		Timeout:      10 * time.Millisecond,
		RuntimeCaps:  []string{"github-actions-runner"},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error: got %v", err)
	}
	if !driver.runDeadlineSet {
		t.Fatal("run context did not receive request timeout deadline")
	}
	if driver.cleanupContextDone {
		t.Fatal("cleanup used the already-expired run context")
	}
	if driver.removedOrganization != "GoCodeAlone" || driver.removedRunnerID != 42 {
		t.Fatalf("cleanup: org=%q runner=%d", driver.removedOrganization, driver.removedRunnerID)
	}
	if result.CleanupStatus != "removed" {
		t.Fatalf("cleanup status: got %q", result.CleanupStatus)
	}
}

func TestT594EphemeralRunnerJobProofIncludesAssignmentAndCleanup(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{
		result: EphemeralRunnerJobResult{
			RunnerID:      42,
			RunnerName:    "wfc-stg-ghp-linux-abcdef987249-543210f71ee4",
			Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-abcdef987249-543210f71ee4", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
			WorkflowRunID: 1001,
			WorkflowJobID: 2002,
			WorkerID:      "worker-0123456789abcdef",
			TaskID:        "task-abcdef9876543210",
			CleanupStatus: "removed",
			RedactedError: "<redacted>",
		},
	}
	job := NewEphemeralRunnerJob(driver)
	result, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeAttachToQueued,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-0123456789abcdef",
		TaskID:       "task-abcdef9876543210",
		Organization: "GoCodeAlone",
		RuntimeCaps:  []string{"github-actions-runner"},
	})
	if err != nil {
		t.Fatalf("run job: %v", err)
	}

	if result.RunnerID == 0 || result.RunnerName == "" || len(result.Labels) == 0 {
		t.Fatalf("runner assignment missing from proof: %+v", result)
	}
	if result.WorkflowRunID == 0 || result.WorkflowJobID == 0 {
		t.Fatalf("workflow assignment missing from proof: %+v", result)
	}
	if result.WorkerID == "" || result.TaskID == "" {
		t.Fatalf("worker/task identity missing from proof: %+v", result)
	}
	if result.CleanupStatus != "removed" || result.RedactedError == "" {
		t.Fatalf("cleanup/redaction missing from proof: %+v", result)
	}
}

func TestT916EphemeralRunnerJobPreservesIdentityWhenJITAcquisitionFails(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{runErr: errors.New("JIT config unavailable")}
	result, err := NewEphemeralRunnerJob(driver).Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeDispatchThenWait,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-1",
		TaskID:       "task-1",
		Organization: "GoCodeAlone",
		RuntimeCaps:  []string{"github-actions-runner"},
	})
	if err == nil || !strings.Contains(err.Error(), "JIT config unavailable") {
		t.Fatalf("JIT config error = %v", err)
	}
	if result.RunnerName == "" || len(result.Labels) == 0 || result.WorkerID != "worker-1" || result.TaskID != "task-1" {
		t.Fatalf("failure proof identity = %+v", result)
	}
	if result.CleanupStatus != "skipped" {
		t.Fatalf("cleanup status = %q, want skipped", result.CleanupStatus)
	}
}

func TestT916EphemeralRunnerJobPreservesRunAndCleanupFailures(t *testing.T) {
	runErr := errors.New("runner execution failed")
	cleanupErr := errors.New("runner cleanup failed")
	driver := &fakeEphemeralRunnerDriver{
		result:    EphemeralRunnerJobResult{RunnerID: 42},
		runErr:    runErr,
		removeErr: cleanupErr,
	}
	previousRetryInterval := ephemeralRunnerCleanupRetryInterval
	ephemeralRunnerCleanupRetryInterval = time.Millisecond
	t.Cleanup(func() { ephemeralRunnerCleanupRetryInterval = previousRetryInterval })

	result, err := NewEphemeralRunnerJob(driver).Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeDispatchThenWait,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-1",
		TaskID:       "task-1",
		Organization: "GoCodeAlone",
		RuntimeCaps:  []string{"github-actions-runner"},
	})
	if !errors.Is(err, runErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("combined error = %v, want run and cleanup failures", err)
	}
	if result.CleanupStatus != "remove_failed" {
		t.Fatalf("cleanup status = %q", result.CleanupStatus)
	}
}

func TestT594EphemeralRunnerJobRejectsDockerOrIaCWithoutCapabilityExtras(t *testing.T) {
	job := NewEphemeralRunnerJob(&fakeEphemeralRunnerDriver{})
	_, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:                EphemeralRunnerJobModeDispatchThenWait,
		Environment:         "stg",
		OS:                  "linux",
		WorkerID:            "worker-0123456789abcdef",
		TaskID:              "task-abcdef9876543210",
		Organization:        "GoCodeAlone",
		RequiredRuntimeCaps: []string{"docker", "iac"},
		RuntimeCaps:         []string{"github-actions-runner"},
	})
	if err == nil {
		t.Fatal("docker/iac workload must fail closed without advertised runtime extras")
	}
	if !errors.Is(err, ErrEphemeralRunnerCapabilityUnsupported) {
		t.Fatalf("error: got %v", err)
	}
}

func TestT594RunnerProviderInvokesEphemeralRunnerJobSpec(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("ephemeral_runner_job", map[string]any{
		"provider_token":    "provider-token",
		"mode":              "dispatch_then_wait",
		"environment":       "stg",
		"os":                "linux",
		"worker_id":         "worker-0123456789abcdef",
		"task_id":           "task-abcdef9876543210",
		"organization":      "GoCodeAlone",
		"repository":        "GoCodeAlone/workflow-compute",
		"workflow":          "dogfood.yml",
		"ref":               "main",
		"runner_group":      "workflow-compute-stg",
		"require_preflight": true,
	})
	if err != nil {
		t.Fatalf("invoke ephemeral_runner_job: %v", err)
	}
	if result["runner_name"] != "wfc-stg-ghp-linux-abcdef987249-543210f71ee4" {
		t.Fatalf("runner name: got %+v", result)
	}
	if labels, ok := result["labels"].([]string); !ok || len(labels) != 5 || labels[3] != "wfc-ghp-stg" {
		t.Fatalf("labels: got %#v", result["labels"])
	}
}

func TestT916RunnerProviderModuleAppliesEphemeralJobInputContract(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	base := map[string]any{
		"provider_token":    "provider-token",
		"mode":              "dispatch_then_wait",
		"environment":       "stg",
		"os":                "linux",
		"worker_id":         "worker-0123456789abcdef",
		"task_id":           "task-abcdef9876543210",
		"organization":      "GoCodeAlone",
		"repository":        "GoCodeAlone/workflow-compute",
		"workflow":          "dogfood.yml",
		"ref":               "main",
		"runner_group":      "workflow-compute-stg",
		"require_preflight": true,
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing_mode", mutate: func(input map[string]any) { delete(input, "mode") }},
		{name: "non_linux", mutate: func(input map[string]any) { input["os"] = "windows" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := make(map[string]any, len(base))
			for key, value := range base {
				input[key] = value
			}
			tc.mutate(input)
			result, err := module.InvokeMethod("ephemeral_runner_job", input)
			if err == nil || !strings.Contains(err.Error(), "input schema") {
				t.Fatalf("contract-invalid module input: result=%+v err=%v", result, err)
			}
		})
	}
}

type fakeEphemeralRunnerDriver struct {
	mode                EphemeralRunnerJobMode
	spec                EphemeralRunnerJobSpec
	result              EphemeralRunnerJobResult
	runErr              error
	removeErr           error
	blockUntilDone      bool
	runDeadlineSet      bool
	cleanupContextDone  bool
	removedOrganization string
	removedRunnerID     int64
}

func (f *fakeEphemeralRunnerDriver) RunGitHubJob(ctx context.Context, mode EphemeralRunnerJobMode, spec EphemeralRunnerJobSpec) (EphemeralRunnerJobResult, error) {
	f.mode = mode
	f.spec = spec
	_, f.runDeadlineSet = ctx.Deadline()
	if f.blockUntilDone {
		<-ctx.Done()
		return f.result, ctx.Err()
	}
	return f.result, f.runErr
}

func (f *fakeEphemeralRunnerDriver) RemoveOrgRunner(ctx context.Context, organization string, runnerID int64) error {
	select {
	case <-ctx.Done():
		f.cleanupContextDone = true
	default:
	}
	f.removedOrganization = organization
	f.removedRunnerID = runnerID
	return f.removeErr
}
