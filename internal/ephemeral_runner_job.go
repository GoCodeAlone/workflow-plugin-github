package internal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrEphemeralRunnerCapabilityUnsupported = errors.New("ephemeral runner capability unsupported")

const ephemeralRunnerCleanupTimeout = 30 * time.Second

var ephemeralRunnerCleanupRetryInterval = 250 * time.Millisecond

type EphemeralRunnerJobMode string

const (
	EphemeralRunnerJobModeDispatchThenWait EphemeralRunnerJobMode = "dispatch_then_wait"
	EphemeralRunnerJobModeAttachToQueued   EphemeralRunnerJobMode = "attach_to_queued"
)

type EphemeralRunnerJobRequest struct {
	Mode                EphemeralRunnerJobMode `json:"mode"`
	Environment         string                 `json:"environment"`
	OS                  string                 `json:"os"`
	WorkerID            string                 `json:"worker_id"`
	TaskID              string                 `json:"task_id"`
	Organization        string                 `json:"organization"`
	Repository          string                 `json:"repository,omitempty"`
	Workflow            string                 `json:"workflow,omitempty"`
	Ref                 string                 `json:"ref,omitempty"`
	WorkflowRunID       int64                  `json:"workflow_run_id,omitempty"`
	WorkflowJobID       int64                  `json:"workflow_job_id,omitempty"`
	WorkflowInputs      map[string]string      `json:"workflow_inputs,omitempty"`
	ArtifactPaths       []string               `json:"artifact_paths,omitempty"`
	RunnerGroup         string                 `json:"runner_group,omitempty"`
	RequirePreflight    bool                   `json:"require_preflight,omitempty"`
	TimeoutSeconds      int                    `json:"timeout_seconds,omitempty"`
	Timeout             time.Duration          `json:"-"`
	RequiredRuntimeCaps []string               `json:"required_runtime_caps,omitempty"`
	RuntimeCaps         []string               `json:"-"`
}

type EphemeralRunnerJobSpec struct {
	RunnerName  string   `json:"runner_name"`
	Labels      []string `json:"labels"`
	RunnerGroup string   `json:"runner_group,omitempty"`
}

type EphemeralRunnerJobResult struct {
	RunnerID                   int64                          `json:"runner_id"`
	RunnerName                 string                         `json:"runner_name"`
	Labels                     []string                       `json:"labels"`
	WorkflowRunID              int64                          `json:"workflow_run_id"`
	WorkflowJobID              int64                          `json:"workflow_job_id"`
	WorkflowJobStatus          string                         `json:"workflow_job_status,omitempty"`
	WorkflowVerificationStatus string                         `json:"workflow_verification_status,omitempty"`
	WorkerID                   string                         `json:"worker_id"`
	TaskID                     string                         `json:"task_id"`
	WorkloadArtifacts          []string                       `json:"workload_artifacts,omitempty"`
	CleanupStatus              string                         `json:"cleanup_status"`
	Preflight                  *GitHubRunnerProviderPreflight `json:"preflight,omitempty"`
	RedactedError              string                         `json:"redacted_error,omitempty"`
}

type EphemeralRunnerJobDriver interface {
	RunGitHubJob(ctx context.Context, mode EphemeralRunnerJobMode, spec EphemeralRunnerJobSpec) (EphemeralRunnerJobResult, error)
	RemoveOrgRunner(ctx context.Context, organization string, runnerID int64) error
}

type EphemeralRunnerJob struct {
	driver EphemeralRunnerJobDriver
}

func NewEphemeralRunnerJob(driver EphemeralRunnerJobDriver) *EphemeralRunnerJob {
	return &EphemeralRunnerJob{driver: driver}
}

func BuildEphemeralRunnerJobSpec(req EphemeralRunnerJobRequest) (EphemeralRunnerJobSpec, error) {
	environment := strings.ToLower(strings.TrimSpace(req.Environment))
	if environment == "" {
		return EphemeralRunnerJobSpec{}, errors.New("environment is required")
	}
	osName := strings.ToLower(strings.TrimSpace(req.OS))
	if osName == "" {
		return EphemeralRunnerJobSpec{}, errors.New("os is required")
	}
	worker := shortEphemeralID(req.WorkerID)
	if worker == "" {
		return EphemeralRunnerJobSpec{}, errors.New("worker_id is required")
	}
	task := shortEphemeralID(req.TaskID)
	if task == "" {
		return EphemeralRunnerJobSpec{}, errors.New("task_id is required")
	}
	name := fmt.Sprintf("wfc-%s-ghp-%s-%s-%s", environment, osName, worker, task)
	return EphemeralRunnerJobSpec{
		RunnerName:  name,
		RunnerGroup: strings.TrimSpace(req.RunnerGroup),
		Labels: []string{
			"self-hosted",
			osName,
			name,
			"wfc-ghp-" + environment,
			"wfc-ghp-ephemeral",
		},
	}, nil
}

func (j *EphemeralRunnerJob) Run(ctx context.Context, req EphemeralRunnerJobRequest) (EphemeralRunnerJobResult, error) {
	spec, err := BuildEphemeralRunnerJobSpec(req)
	if err != nil {
		return EphemeralRunnerJobResult{}, err
	}
	failureResult := EphemeralRunnerJobResult{CleanupStatus: "skipped"}
	fillEphemeralRunnerResult(&failureResult, req, spec)
	requiredCapabilities := make([]string, 0, len(req.RequiredRuntimeCaps)+1)
	requiredCapabilities = append(requiredCapabilities, "github-actions-runner")
	requiredCapabilities = append(requiredCapabilities, req.RequiredRuntimeCaps...)
	if err := requireEphemeralRunnerCapabilities(requiredCapabilities, req.RuntimeCaps); err != nil {
		return failureResult, err
	}
	if j == nil || j.driver == nil {
		return failureResult, errors.New("ephemeral runner job driver is required")
	}
	jobCtx := ctx
	cancel := func() {}
	if req.Timeout > 0 {
		jobCtx, cancel = context.WithTimeout(ctx, req.Timeout)
	}
	defer cancel()
	result, runErr := j.driver.RunGitHubJob(jobCtx, req.Mode, spec)
	if result.RunnerID > 0 {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), ephemeralRunnerCleanupTimeout)
		defer cleanupCancel()
		var cleanupErr error
		for attempt := 0; attempt < 5; attempt++ {
			cleanupErr = j.driver.RemoveOrgRunner(cleanupCtx, req.Organization, result.RunnerID)
			if cleanupErr == nil {
				break
			}
			if attempt == 4 {
				break
			}
			timer := time.NewTimer(ephemeralRunnerCleanupRetryInterval)
			select {
			case <-timer.C:
			case <-cleanupCtx.Done():
				timer.Stop()
				cleanupErr = errors.Join(cleanupErr, cleanupCtx.Err())
				attempt = 4
			}
		}
		if cleanupErr != nil {
			result.CleanupStatus = "remove_failed"
			runErr = errors.Join(runErr, cleanupErr)
		} else {
			result.CleanupStatus = "removed"
		}
	} else if result.CleanupStatus == "" {
		result.CleanupStatus = "skipped"
	}
	fillEphemeralRunnerResult(&result, req, spec)
	return result, runErr
}

func fillEphemeralRunnerResult(result *EphemeralRunnerJobResult, req EphemeralRunnerJobRequest, spec EphemeralRunnerJobSpec) {
	if result.RunnerName == "" {
		result.RunnerName = spec.RunnerName
	}
	if len(result.Labels) == 0 {
		result.Labels = append([]string(nil), spec.Labels...)
	}
	if result.WorkerID == "" {
		result.WorkerID = req.WorkerID
	}
	if result.TaskID == "" {
		result.TaskID = req.TaskID
	}
}

func requireEphemeralRunnerCapabilities(required, advertised []string) error {
	available := map[string]struct{}{}
	for _, cap := range advertised {
		cap = strings.ToLower(strings.TrimSpace(cap))
		if cap != "" {
			available[cap] = struct{}{}
		}
	}
	for _, cap := range required {
		cap = strings.ToLower(strings.TrimSpace(cap))
		if cap == "" {
			continue
		}
		if _, ok := available[cap]; !ok {
			return fmt.Errorf("%w: %s", ErrEphemeralRunnerCapabilityUnsupported, cap)
		}
	}
	return nil
}

func shortEphemeralID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	canonical := strings.ToLower(value)
	var safe strings.Builder
	for _, r := range canonical {
		switch {
		case r >= 'a' && r <= 'z':
			safe.WriteRune(r)
		case r >= '0' && r <= '9':
			safe.WriteRune(r)
		}
	}
	token := safe.String()
	if token == "" {
		return ""
	}
	if len(token) <= 8 && token == canonical {
		return token
	}
	sum := sha256.Sum256([]byte(canonical))
	hash := hex.EncodeToString(sum[:])[:6]
	tail := token
	if len(tail) > 6 {
		tail = tail[len(tail)-6:]
	}
	return tail + hash
}
