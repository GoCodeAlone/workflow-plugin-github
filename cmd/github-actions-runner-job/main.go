package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

const (
	computeProtocolVersion  = "compute.v1alpha1"
	providerOperation       = "ephemeral_runner_job"
	proofArtifactName       = "github-runner-proof.json"
	defaultRunnerJobTimeout = 30 * time.Minute
)

var githubJobPollInterval = 5 * time.Second
var githubJobExitGracePolls = 3
var githubRunnerShutdownGrace = 10 * time.Second

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "github-actions-runner-job failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	return runWithIO(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}

func runWithIO(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("github-actions-runner-job", flag.ContinueOnError)
	fs.SetOutput(stderr)
	specOnly := fs.Bool("spec", false, "emit the deterministic runner spec without starting a runner")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runParsed(*specOnly, stdin, stdout)
}

func runParsed(specOnly bool, stdin io.Reader, stdout io.Writer) error {
	if specOnly {
		var req internal.EphemeralRunnerJobRequest
		decoder := json.NewDecoder(stdin)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			return fmt.Errorf("decode request: %w", err)
		}
		spec, err := internal.BuildEphemeralRunnerJobSpec(req)
		if err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(map[string]any{
			"runner_name":  spec.RunnerName,
			"labels":       spec.Labels,
			"runner_group": spec.RunnerGroup,
		})
	}

	var envelope dynamicProviderEnvelope
	decoder := json.NewDecoder(stdin)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return fmt.Errorf("decode dynamic provider envelope: %w", err)
	}
	if err := envelope.validate(); err != nil {
		return err
	}
	req, err := decodeEphemeralRunnerRequest(envelope.Input)
	if err != nil {
		return err
	}
	sidecar, err := newSidecarClientFromEnv()
	if err != nil {
		return err
	}
	driver, err := newRunnerDriver(req, sidecar)
	if err != nil {
		return err
	}
	result, err := internal.NewEphemeralRunnerJob(driver).Run(context.Background(), req)
	if proofErr := writeProofArtifact(result); proofErr != nil && err == nil {
		err = proofErr
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(map[string]any{"artifacts": []string{proofArtifactName}})
}

type dynamicProviderEnvelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	TaskID          string          `json:"task_id"`
	LeaseID         string          `json:"lease_id"`
	ProviderConfig  json.RawMessage `json:"provider_config"`
	Operation       string          `json:"operation"`
	Input           json.RawMessage `json:"input"`
}

func (e dynamicProviderEnvelope) validate() error {
	if e.ProtocolVersion != computeProtocolVersion {
		return fmt.Errorf("protocol_version must be %q", computeProtocolVersion)
	}
	if strings.TrimSpace(e.TaskID) == "" {
		return fmt.Errorf("task_id is required")
	}
	if strings.TrimSpace(e.LeaseID) == "" {
		return fmt.Errorf("lease_id is required")
	}
	if strings.TrimSpace(e.Operation) != providerOperation {
		return fmt.Errorf("operation must be %q", providerOperation)
	}
	if len(e.ProviderConfig) == 0 || !json.Valid(e.ProviderConfig) {
		return fmt.Errorf("provider_config must be valid JSON")
	}
	if len(e.Input) == 0 || !json.Valid(e.Input) {
		return fmt.Errorf("input must be valid JSON")
	}
	return nil
}

func decodeEphemeralRunnerRequest(input json.RawMessage) (internal.EphemeralRunnerJobRequest, error) {
	var req internal.EphemeralRunnerJobRequest
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("decode ephemeral runner request: %w", err)
	}
	if req.TimeoutSeconds < 0 {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("timeout_seconds must not be negative")
	}
	if req.TimeoutSeconds > 0 {
		req.Timeout = time.Duration(req.TimeoutSeconds) * time.Second
	} else {
		req.Timeout = defaultRunnerJobTimeout
	}
	return req, nil
}

type providerSidecarClient struct {
	baseURL string
	token   string
	http    *http.Client
}

func newSidecarClientFromEnv() (*providerSidecarClient, error) {
	baseURL := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL"))
	if baseURL == "" {
		return nil, fmt.Errorf("COMPUTE_GITHUB_RUNNER_PROVIDER_URL is required")
	}
	token := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN"))
	if token == "" {
		return nil, fmt.Errorf("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN is required")
	}
	return &providerSidecarClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (c *providerSidecarClient) orgRegistrationToken(ctx context.Context, organization string) (internal.GitHubRunnerRegistrationToken, error) {
	var out internal.GitHubRunnerRegistrationToken
	if err := c.do(ctx, http.MethodPost, "/v1/actions/orgs/"+organization+"/runners/registration-token", nil, http.StatusCreated, &out); err != nil {
		return internal.GitHubRunnerRegistrationToken{}, err
	}
	return out, nil
}

func (c *providerSidecarClient) dispatchWorkflow(ctx context.Context, repository, workflow, ref string, inputs map[string]string) error {
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return err
	}
	path := "/v1/actions/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/workflows/" + url.PathEscape(workflow) + "/dispatches"
	body := map[string]any{"ref": ref}
	if len(inputs) > 0 {
		body["inputs"] = inputs
	}
	return c.do(ctx, http.MethodPost, path, body, http.StatusNoContent, nil)
}

func (c *providerSidecarClient) workflowRuns(ctx context.Context, repository, workflow string, createdAfter time.Time) ([]internal.GitHubWorkflowRun, error) {
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return nil, err
	}
	path := "/v1/actions/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/workflows/" + url.PathEscape(workflow) + "/runs"
	query := url.Values{}
	if !createdAfter.IsZero() {
		query.Set("created_after", createdAfter.UTC().Format(time.RFC3339))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var out struct {
		WorkflowRuns []internal.GitHubWorkflowRun `json:"workflow_runs"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, http.StatusOK, &out); err != nil {
		return nil, err
	}
	return out.WorkflowRuns, nil
}

func (c *providerSidecarClient) workflowRunJobs(ctx context.Context, repository string, runID int64) ([]internal.GitHubWorkflowJob, error) {
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/actions/repos/%s/%s/actions/runs/%d/jobs", url.PathEscape(owner), url.PathEscape(repo), runID)
	var out struct {
		Jobs []internal.GitHubWorkflowJob `json:"jobs"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, http.StatusOK, &out); err != nil {
		return nil, err
	}
	return out.Jobs, nil
}

func (c *providerSidecarClient) removeOrgRunner(ctx context.Context, organization string, runnerID int64) error {
	path := fmt.Sprintf("/v1/actions/orgs/%s/runners/%d", url.PathEscape(organization), runnerID)
	return c.do(ctx, http.MethodDelete, path, nil, http.StatusNoContent, nil)
}

func (c *providerSidecarClient) do(ctx context.Context, method, path string, body any, wantStatus int, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal sidecar request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("provider sidecar request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("provider sidecar %s %s: status %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode provider sidecar response: %w", err)
		}
	}
	return nil
}

type runnerDriver struct {
	req       internal.EphemeralRunnerJobRequest
	sidecar   *providerSidecarClient
	runnerDir string
}

func newRunnerDriver(req internal.EphemeralRunnerJobRequest, sidecar *providerSidecarClient) (*runnerDriver, error) {
	runnerDir := strings.TrimSpace(os.Getenv("GITHUB_ACTIONS_RUNNER_DIR"))
	if runnerDir == "" {
		runnerDir = "/opt/actions-runner"
	}
	return &runnerDriver{req: req, sidecar: sidecar, runnerDir: runnerDir}, nil
}

func (d *runnerDriver) OrgRegistrationToken(ctx context.Context, organization string) (internal.GitHubRunnerRegistrationToken, error) {
	return d.sidecar.orgRegistrationToken(ctx, organization)
}

func (d *runnerDriver) RunGitHubJob(ctx context.Context, mode internal.EphemeralRunnerJobMode, spec internal.EphemeralRunnerJobSpec, token internal.GitHubRunnerRegistrationToken) (internal.EphemeralRunnerJobResult, error) {
	if mode == "" {
		mode = internal.EphemeralRunnerJobModeAttachToQueued
	}
	var dispatchRef string
	var dispatchInputs map[string]string
	if mode == internal.EphemeralRunnerJobModeDispatchThenWait {
		if strings.TrimSpace(d.req.Workflow) == "" || strings.TrimSpace(d.req.Repository) == "" {
			return internal.EphemeralRunnerJobResult{}, fmt.Errorf("repository and workflow are required for %s", mode)
		}
		dispatchRef = strings.TrimSpace(d.req.Ref)
		if dispatchRef == "" {
			dispatchRef = "main"
		}
		inputs, err := d.workflowDispatchInputs(spec)
		if err != nil {
			return internal.EphemeralRunnerJobResult{}, err
		}
		dispatchInputs = inputs
	}
	if err := d.configureRunner(ctx, spec, token.Token); err != nil {
		return internal.EphemeralRunnerJobResult{}, err
	}
	result := internal.EphemeralRunnerJobResult{
		RunnerID:   d.runnerID(),
		RunnerName: spec.RunnerName,
		Labels:     append([]string(nil), spec.Labels...),
	}
	if mode == internal.EphemeralRunnerJobModeDispatchThenWait {
		runner, err := d.startRunner(ctx)
		if err != nil {
			return result, err
		}
		dispatchedAfter := time.Now().UTC().Add(-10 * time.Second)
		if err := d.sidecar.dispatchWorkflow(ctx, d.req.Repository, d.req.Workflow, dispatchRef, dispatchInputs); err != nil {
			runner.cancel()
			_, _ = runner.waitAfterCancel(githubRunnerShutdownGrace)
			return result, err
		}
		completion, err := d.waitForGitHubCompletion(ctx, runner, spec.RunnerName, dispatchedAfter)
		if completion.WorkflowRunID != 0 {
			result.WorkflowRunID = completion.WorkflowRunID
		}
		if completion.RunnerID != 0 {
			result.RunnerID = completion.RunnerID
		}
		if completion.WorkflowJobID != 0 {
			result.WorkflowJobID = completion.WorkflowJobID
		}
		if completion.WorkflowJobStatus != "" {
			result.WorkflowJobStatus = completion.WorkflowJobStatus
		}
		if err != nil {
			return result, err
		}
		return result, nil
	}
	if err := d.runRunner(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func (d *runnerDriver) workflowDispatchInputs(spec internal.EphemeralRunnerJobSpec) (map[string]string, error) {
	inputs := make(map[string]string, len(d.req.WorkflowInputs)+5)
	for key, value := range d.req.WorkflowInputs {
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			continue
		}
		inputs[key] = value
	}
	labels, err := json.Marshal(spec.Labels)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow runner labels: %w", err)
	}
	inputs["runner_profile"] = "provider"
	inputs["allow_github_hosted_fallback"] = "false"
	inputs["runner_labels_json"] = string(labels)
	inputs["stg_task_id"] = d.req.TaskID
	inputs["workflow_compute_provider_task"] = d.req.TaskID
	return inputs, nil
}

type githubJobCompletion struct {
	RunnerID          int64
	WorkflowRunID     int64
	WorkflowJobID     int64
	WorkflowJobStatus string
	Assigned          bool
	Success           bool
	Terminal          bool
	Message           string
}

func (d *runnerDriver) waitForGitHubCompletion(ctx context.Context, runner *runningCommand, runnerName string, dispatchedAfter time.Time) (githubJobCompletion, error) {
	ticker := time.NewTicker(githubJobPollInterval)
	defer ticker.Stop()
	var last githubJobCompletion
	for {
		select {
		case err := <-runner.done:
			runner.cancel()
			if err != nil {
				return last, fmt.Errorf("%s failed: %w: %s", filepath.Base(runner.path), err, runner.output())
			}
			completion, observeErr := d.observeTerminalGitHubJob(ctx, runnerName, dispatchedAfter, githubJobExitGracePolls)
			if observeErr != nil {
				return completion, fmt.Errorf("observe GitHub workflow job after runner exit: %w", observeErr)
			}
			if completion.WorkflowRunID != 0 {
				if !completion.Assigned {
					return completion, fmt.Errorf("%s exited before GitHub workflow job was assigned: %s", filepath.Base(runner.path), completion.Message)
				}
				if !completion.Terminal {
					return completion, fmt.Errorf("%s exited before GitHub workflow job completed: %s", filepath.Base(runner.path), completion.Message)
				}
				if completion.Terminal && !completion.Success {
					return completion, fmt.Errorf("github workflow job failed: %s", completion.Message)
				}
				return completion, nil
			}
			return last, fmt.Errorf("%s exited before GitHub workflow job was assigned to runner %s", filepath.Base(runner.path), runnerName)
		case result := <-runner.result:
			runner.cancel()
			err, _ := runner.waitAfterCancel(githubRunnerShutdownGrace)
			if result.success {
				completion, observeErr := d.observeTerminalGitHubJob(ctx, runnerName, dispatchedAfter, githubJobExitGracePolls)
				if observeErr != nil {
					return completion, fmt.Errorf("observe GitHub workflow job after success marker: %w", observeErr)
				}
				if completion.WorkflowRunID == 0 || !completion.Assigned {
					return completion, fmt.Errorf("%s reported success before GitHub workflow job was assigned", filepath.Base(runner.path))
				}
				if !completion.Terminal {
					return completion, fmt.Errorf("%s reported success before GitHub workflow job completed: %s", filepath.Base(runner.path), completion.Message)
				}
				if !completion.Success {
					return completion, fmt.Errorf("github workflow job failed after success marker: %s", completion.Message)
				}
				return completion, nil
			}
			if err != nil {
				return last, fmt.Errorf("%s reported failed job and exited with %w: %s", filepath.Base(runner.path), err, result.line)
			}
			return last, fmt.Errorf("%s reported failed job: %s", filepath.Base(runner.path), result.line)
		case <-ticker.C:
			completion, err := d.observeGitHubJob(ctx, runnerName, dispatchedAfter)
			if err != nil {
				return last, err
			}
			if completion.WorkflowRunID != 0 {
				last = completion
			}
			if !completion.Terminal {
				continue
			}
			runner.cancel()
			_, _ = runner.waitAfterCancel(githubRunnerShutdownGrace)
			if completion.Success {
				return completion, nil
			}
			return completion, fmt.Errorf("github workflow job failed: %s", completion.Message)
		case <-ctx.Done():
			runner.cancel()
			err, _ := runner.waitAfterCancel(githubRunnerShutdownGrace)
			if err != nil {
				return last, fmt.Errorf("%s timed out waiting for GitHub job completion: %w: %s", filepath.Base(runner.path), ctx.Err(), runner.output())
			}
			return last, fmt.Errorf("%s timed out waiting for GitHub job completion: %w", filepath.Base(runner.path), ctx.Err())
		}
	}
}

func (d *runnerDriver) observeTerminalGitHubJob(ctx context.Context, runnerName string, dispatchedAfter time.Time, maxPolls int) (githubJobCompletion, error) {
	if maxPolls < 1 {
		maxPolls = 1
	}
	var last githubJobCompletion
	for attempt := 0; attempt < maxPolls; attempt++ {
		completion, err := d.observeGitHubJob(ctx, runnerName, dispatchedAfter)
		if err != nil {
			return last, err
		}
		if completion.WorkflowRunID != 0 {
			last = completion
			if completion.Terminal {
				return completion, nil
			}
		}
		if attempt == maxPolls-1 {
			break
		}
		timer := time.NewTimer(githubJobPollInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return last, ctx.Err()
		}
	}
	return last, nil
}

func (d *runnerDriver) observeGitHubJob(ctx context.Context, runnerName string, dispatchedAfter time.Time) (githubJobCompletion, error) {
	runs, err := d.sidecar.workflowRuns(ctx, d.req.Repository, d.req.Workflow, dispatchedAfter)
	if err != nil {
		return githubJobCompletion{}, err
	}
	var unassigned githubJobCompletion
	var terminalUnreported githubJobCompletion
	allowTerminalUnreported := len(runs) == 1
	for _, run := range runs {
		jobs, err := d.sidecar.workflowRunJobs(ctx, d.req.Repository, run.ID)
		if err != nil {
			return githubJobCompletion{}, err
		}
		for _, job := range jobs {
			jobRunner := strings.TrimSpace(job.RunnerName)
			runTerminal := strings.EqualFold(run.Status, "completed")
			jobTerminal := strings.EqualFold(job.Status, "completed")
			if jobRunner == "" && unassigned.WorkflowRunID == 0 && d.jobLabelsMatchRunner(job.Labels, runnerName) && !strings.EqualFold(job.Status, "completed") && !strings.EqualFold(run.Status, "completed") {
				unassigned = githubJobCompletion{
					WorkflowRunID:     run.ID,
					WorkflowJobID:     job.ID,
					WorkflowJobStatus: job.Status,
					Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=unassigned", run.ID, job.ID, job.Status, job.Conclusion),
				}
				continue
			}
			if allowTerminalUnreported && jobRunner == "" && terminalUnreported.WorkflowRunID == 0 && (runTerminal || jobTerminal) {
				conclusion := strings.ToLower(strings.TrimSpace(job.Conclusion))
				if conclusion == "" {
					conclusion = strings.ToLower(strings.TrimSpace(run.Conclusion))
				}
				if conclusion == "skipped" {
					continue
				}
				completion := githubJobCompletion{
					RunnerID:          job.RunnerID,
					WorkflowRunID:     run.ID,
					WorkflowJobID:     job.ID,
					WorkflowJobStatus: job.Status,
					Assigned:          true,
					Terminal:          true,
					Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=unreported", run.ID, job.ID, job.Status, job.Conclusion),
				}
				completion.Success = conclusion == "success" || conclusion == "succeeded"
				terminalUnreported = completion
				continue
			}
			if jobRunner != runnerName {
				continue
			}
			completion := githubJobCompletion{
				RunnerID:          job.RunnerID,
				WorkflowRunID:     run.ID,
				WorkflowJobID:     job.ID,
				WorkflowJobStatus: job.Status,
				Assigned:          true,
				Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=%s", run.ID, job.ID, job.Status, job.Conclusion, job.RunnerName),
			}
			if !strings.EqualFold(job.Status, "completed") && !strings.EqualFold(run.Status, "completed") {
				return completion, nil
			}
			completion.Terminal = true
			conclusion := strings.ToLower(strings.TrimSpace(job.Conclusion))
			if conclusion == "" {
				conclusion = strings.ToLower(strings.TrimSpace(run.Conclusion))
			}
			completion.Success = conclusion == "success" || conclusion == "succeeded"
			return completion, nil
		}
	}
	if terminalUnreported.WorkflowRunID != 0 {
		return terminalUnreported, nil
	}
	return unassigned, nil
}

func (d *runnerDriver) jobLabelsMatchRunner(jobLabels []string, runnerName string) bool {
	if len(jobLabels) == 0 {
		return false
	}
	labels := make(map[string]struct{}, len(jobLabels))
	for _, label := range jobLabels {
		label = strings.ToLower(strings.TrimSpace(label))
		if label != "" {
			labels[label] = struct{}{}
		}
	}
	for _, want := range []string{"self-hosted", strings.ToLower(strings.TrimSpace(runnerName))} {
		if want == "" {
			return false
		}
		if _, ok := labels[want]; !ok {
			return false
		}
	}
	return true
}

func (d *runnerDriver) RemoveOrgRunner(ctx context.Context, organization string, runnerID int64) error {
	return d.sidecar.removeOrgRunner(ctx, organization, runnerID)
}

func (d *runnerDriver) configureRunner(ctx context.Context, spec internal.EphemeralRunnerJobSpec, token string) error {
	args := []string{
		"--url", "https://github.com/" + d.req.Organization,
		"--token", token,
		"--name", spec.RunnerName,
		"--labels", strings.Join(spec.Labels, ","),
		"--unattended",
		"--ephemeral",
		"--replace",
	}
	if strings.TrimSpace(spec.RunnerGroup) != "" {
		args = append(args, "--runnergroup", spec.RunnerGroup)
	}
	return runCommand(ctx, filepath.Join(d.runnerDir, "config.sh"), d.runnerDir, args...)
}

func (d *runnerDriver) runRunner(ctx context.Context) error {
	return runCommand(ctx, filepath.Join(d.runnerDir, "run.sh"), d.runnerDir, "--once")
}

func (d *runnerDriver) startRunner(ctx context.Context) (*runningCommand, error) {
	return startCommand(ctx, filepath.Join(d.runnerDir, "run.sh"), d.runnerDir, "--once")
}

func (d *runnerDriver) runnerID() int64 {
	data, err := os.ReadFile(filepath.Join(d.runnerDir, ".runner"))
	if err != nil {
		return 0
	}
	var metadata struct {
		AgentID int64 `json:"agentId"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return 0
	}
	return metadata.AgentID
}

func runCommand(ctx context.Context, path, dir string, args ...string) error {
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s failed: %w: %s", filepath.Base(path), err, strings.TrimSpace(string(output)))
	}
	return nil
}

type runningCommand struct {
	path   string
	cancel context.CancelFunc
	cmd    *exec.Cmd
	mu     sync.Mutex
	stdout bytes.Buffer
	stderr bytes.Buffer
	result chan runnerCompletion
	done   chan error
	once   sync.Once
}

type runnerCompletion struct {
	success bool
	line    string
}

func startCommand(ctx context.Context, path, dir string, args ...string) (*runningCommand, error) {
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, path, args...)
	cmd.Dir = dir
	cmd.Env = os.Environ()
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%s stdout pipe failed: %w", filepath.Base(path), err)
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("%s stderr pipe failed: %w", filepath.Base(path), err)
	}
	running := &runningCommand{
		path:   path,
		cancel: cancel,
		cmd:    cmd,
		result: make(chan runnerCompletion, 1),
		done:   make(chan error, 1),
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("%s start failed: %w", filepath.Base(path), err)
	}
	go running.captureOutput(stdoutPipe, &running.stdout)
	go running.captureOutput(stderrPipe, &running.stderr)
	go func() {
		running.done <- cmd.Wait()
	}()
	return running, nil
}

func (r *runningCommand) wait() error {
	err := <-r.done
	r.cancel()
	if err != nil {
		output := r.output()
		return fmt.Errorf("%s failed: %w: %s", filepath.Base(r.path), err, output)
	}
	return nil
}

func (r *runningCommand) waitAfterCancel(timeout time.Duration) (error, bool) {
	if timeout <= 0 {
		return <-r.done, true
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-r.done:
		return err, true
	case <-timer.C:
		return nil, false
	}
}

func (r *runningCommand) waitForGitHubJob(ctx context.Context) error {
	select {
	case err := <-r.done:
		r.cancel()
		if err != nil {
			return fmt.Errorf("%s failed: %w: %s", filepath.Base(r.path), err, r.output())
		}
		return nil
	case result := <-r.result:
		r.cancel()
		err := <-r.done
		if result.success {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s reported failed job and exited with %w: %s", filepath.Base(r.path), err, result.line)
		}
		return fmt.Errorf("%s reported failed job: %s", filepath.Base(r.path), result.line)
	case <-ctx.Done():
		r.cancel()
		err := <-r.done
		if err != nil {
			return fmt.Errorf("%s timed out waiting for GitHub job completion: %w: %s", filepath.Base(r.path), ctx.Err(), r.output())
		}
		return ctx.Err()
	}
}

func (r *runningCommand) captureOutput(reader io.Reader, buffer *bytes.Buffer) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		r.mu.Lock()
		_, _ = buffer.WriteString(line)
		_ = buffer.WriteByte('\n')
		r.mu.Unlock()
		if result, ok := parseRunnerCompletion(line); ok {
			r.once.Do(func() {
				r.result <- result
			})
		}
	}
}

func (r *runningCommand) output() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.TrimSpace(strings.Join([]string{r.stdout.String(), r.stderr.String()}, "\n"))
}

func parseRunnerCompletion(line string) (runnerCompletion, bool) {
	normalized := strings.ToLower(strings.TrimSpace(line))
	if !strings.Contains(normalized, "completed with result:") {
		return runnerCompletion{}, false
	}
	return runnerCompletion{
		success: strings.Contains(normalized, "completed with result: succeeded") || strings.Contains(normalized, "completed with result: success"),
		line:    strings.TrimSpace(line),
	}, true
}

func writeProofArtifact(result internal.EphemeralRunnerJobResult) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(proofArtifactName, append(data, '\n'), 0o600)
}

func splitRepository(repository string) (string, string, error) {
	owner, repo, ok := strings.Cut(strings.TrimSpace(repository), "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", fmt.Errorf("repository must be owner/name")
	}
	return owner, repo, nil
}
