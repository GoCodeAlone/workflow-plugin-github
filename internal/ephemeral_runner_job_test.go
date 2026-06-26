package internal

import (
	"context"
	"errors"
	"reflect"
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

	if spec.RunnerName != "wfc-stg-ghp-linux-01234567-abcdef98" {
		t.Fatalf("runner name: got %q", spec.RunnerName)
	}
	wantLabels := []string{
		"self-hosted",
		"linux",
		"wfc-stg-ghp-linux-01234567-abcdef98",
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

func TestT594EphemeralRunnerJobUsesExactLabelsForDispatchAndAttachModes(t *testing.T) {
	for _, mode := range []EphemeralRunnerJobMode{EphemeralRunnerJobModeDispatchThenWait, EphemeralRunnerJobModeAttachToQueued} {
		t.Run(string(mode), func(t *testing.T) {
			driver := &fakeEphemeralRunnerDriver{
				result: EphemeralRunnerJobResult{
					RunnerID:      42,
					RunnerName:    "wfc-stg-ghp-linux-01234567-abcdef98",
					Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-01234567-abcdef98", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
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

func TestT594EphemeralRunnerJobCleansUpRunnerOnTimeout(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{
		result: EphemeralRunnerJobResult{
			RunnerID:      42,
			RunnerName:    "wfc-stg-ghp-linux-01234567-abcdef98",
			CleanupStatus: "pending",
		},
		runErr: context.DeadlineExceeded,
	}
	job := NewEphemeralRunnerJob(driver)
	result, err := job.Run(context.Background(), EphemeralRunnerJobRequest{
		Mode:         EphemeralRunnerJobModeDispatchThenWait,
		Environment:  "stg",
		OS:           "linux",
		WorkerID:     "worker-0123456789abcdef",
		TaskID:       "task-abcdef9876543210",
		Organization: "GoCodeAlone",
		Timeout:      time.Millisecond,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error: got %v", err)
	}
	if driver.removedOrganization != "GoCodeAlone" || driver.removedRunnerID != 42 {
		t.Fatalf("cleanup: org=%q runner=%d", driver.removedOrganization, driver.removedRunnerID)
	}
	if result.CleanupStatus != "removed" {
		t.Fatalf("cleanup status: got %q", result.CleanupStatus)
	}
}

func TestT594EphemeralRunnerJobProofIncludesAssignmentCleanupAndArtifacts(t *testing.T) {
	driver := &fakeEphemeralRunnerDriver{
		result: EphemeralRunnerJobResult{
			RunnerID:      42,
			RunnerName:    "wfc-stg-ghp-linux-01234567-abcdef98",
			Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-01234567-abcdef98", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
			WorkflowRunID: 1001,
			WorkflowJobID: 2002,
			WorkerID:      "worker-0123456789abcdef",
			TaskID:        "task-abcdef9876543210",
			ArtifactRefs:  []string{"stg://proofs/proof-123/artifacts/result.json"},
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
	if result.WorkerID == "" || result.TaskID == "" || len(result.ArtifactRefs) == 0 {
		t.Fatalf("STG proof/artifact refs missing from proof: %+v", result)
	}
	if result.CleanupStatus != "removed" || result.RedactedError == "" {
		t.Fatalf("cleanup/redaction missing from proof: %+v", result)
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
		AdvertisedCaps:      []string{"github-actions-runner"},
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
		"provider_token": "provider-token",
		"environment":    "stg",
		"os":             "linux",
		"worker_id":      "worker-0123456789abcdef",
		"task_id":        "task-abcdef9876543210",
		"organization":   "GoCodeAlone",
		"runner_group":   "workflow-compute-stg",
	})
	if err != nil {
		t.Fatalf("invoke ephemeral_runner_job: %v", err)
	}
	if result["runner_name"] != "wfc-stg-ghp-linux-01234567-abcdef98" {
		t.Fatalf("runner name: got %+v", result)
	}
	if labels, ok := result["labels"].([]string); !ok || len(labels) != 5 || labels[3] != "wfc-ghp-stg" {
		t.Fatalf("labels: got %#v", result["labels"])
	}
}

type fakeEphemeralRunnerDriver struct {
	mode                EphemeralRunnerJobMode
	spec                EphemeralRunnerJobSpec
	result              EphemeralRunnerJobResult
	runErr              error
	removedOrganization string
	removedRunnerID     int64
}

func (f *fakeEphemeralRunnerDriver) OrgRegistrationToken(_ context.Context, _ string) (GitHubRunnerRegistrationToken, error) {
	return GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (f *fakeEphemeralRunnerDriver) RunGitHubJob(_ context.Context, mode EphemeralRunnerJobMode, spec EphemeralRunnerJobSpec, _ GitHubRunnerRegistrationToken) (EphemeralRunnerJobResult, error) {
	f.mode = mode
	f.spec = spec
	return f.result, f.runErr
}

func (f *fakeEphemeralRunnerDriver) RemoveOrgRunner(_ context.Context, organization string, runnerID int64) error {
	f.removedOrganization = organization
	f.removedRunnerID = runnerID
	return nil
}
