package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

const computeGatewayHTTPTimeout = 30 * time.Second

type computeGatewayStep struct {
	name   string
	config computeGatewayConfig
	client computeGatewayClient
	github GitHubClient
}

type computeGatewayConfig struct {
	ServerURL             string
	Token                 string
	Repository            string
	OIDCToken             string
	WorkflowRunID         string
	WorkflowRunAttempt    string
	WorkflowJobID         string
	WorkflowJobName       string
	Ref                   string
	SHA                   string
	OrgID                 string
	PoolID                string
	PolicyID              string
	TimeoutSeconds        int
	CommandArgs           []string
	WorkingDirectory      string
	ArtifactAllowlist     []string
	Labels                map[string]string
	ExecutionSecurityTier string
	ProofTier             string
	ExecutorProvider      string
	HardwareClass         string
	Wait                  bool
	PollInterval          time.Duration
	Timeout               time.Duration
	WriteCheck            bool
	CheckOwner            string
	CheckRepo             string
	CheckSHA              string
	CheckName             string
	CheckToken            string
}

type computeGatewayPlacementRequirements struct {
	ExecutorProvider      string `json:"executor_provider,omitempty"`
	ExecutionSecurityTier string `json:"execution_security_tier,omitempty"`
	ProofTier             string `json:"proof_tier,omitempty"`
	HardwareClass         string `json:"hardware_class,omitempty"`
}

type computeGatewayWorkloadRequest struct {
	Repository         string                              `json:"repository"`
	OIDCToken          string                              `json:"oidc_token,omitempty"`
	WorkflowRunID      int64                               `json:"workflow_run_id"`
	WorkflowRunAttempt int64                               `json:"workflow_run_attempt,omitempty"`
	WorkflowJobID      int64                               `json:"workflow_job_id"`
	WorkflowJobName    string                              `json:"workflow_job_name"`
	Ref                string                              `json:"ref,omitempty"`
	SHA                string                              `json:"sha,omitempty"`
	OrgID              string                              `json:"org_id"`
	PoolID             string                              `json:"pool_id"`
	PolicyID           string                              `json:"policy_id"`
	TimeoutSeconds     int                                 `json:"timeout_seconds,omitempty"`
	CommandArgs        []string                            `json:"command_args"`
	WorkingDirectory   string                              `json:"working_directory,omitempty"`
	ArtifactAllowlist  []string                            `json:"artifact_allowlist,omitempty"`
	Requirements       computeGatewayPlacementRequirements `json:"requirements,omitempty"`
	Labels             map[string]string                   `json:"labels,omitempty"`
}

type computeGatewayTask struct {
	ID     string            `json:"id"`
	Status string            `json:"status,omitempty"`
	Labels map[string]string `json:"labels,omitempty"`
}

type computeGatewayWorkloadResponse struct {
	Task computeGatewayTask `json:"task"`
}

type computeGatewayWorkloadStatus struct {
	TaskID         string            `json:"task_id"`
	Status         string            `json:"status"`
	Conclusion     string            `json:"conclusion,omitempty"`
	ProofID        string            `json:"proof_id,omitempty"`
	ArtifactHash   string            `json:"artifact_hash,omitempty"`
	ContributionID string            `json:"contribution_id,omitempty"`
	WorkerID       string            `json:"worker_id,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

type computeGatewayStatusResponse struct {
	Status computeGatewayWorkloadStatus `json:"status"`
}

type computeGatewayClient interface {
	SubmitWorkload(context.Context, string, string, computeGatewayWorkloadRequest) (computeGatewayWorkloadResponse, error)
	WorkloadStatus(context.Context, string, string, string) (computeGatewayStatusResponse, error)
}

type httpComputeGatewayClient struct {
	http *http.Client
}

func newComputeGatewayStep(name string, config map[string]any, client computeGatewayClient) (*computeGatewayStep, error) {
	cfg, err := parseComputeGatewayConfig(config)
	if err != nil {
		return nil, fmt.Errorf("step.gh_compute_gateway %q: %w", name, err)
	}
	if client == nil {
		client = httpComputeGatewayClient{http: &http.Client{Timeout: computeGatewayHTTPTimeout}}
	}
	return &computeGatewayStep{name: name, config: cfg, client: client, github: newHTTPGitHubClient()}, nil
}

func parseComputeGatewayConfig(raw map[string]any) (computeGatewayConfig, error) {
	var cfg computeGatewayConfig
	cfg.ServerURL, _ = raw["server_url"].(string)
	cfg.ServerURL = os.ExpandEnv(cfg.ServerURL)
	if cfg.ServerURL == "" {
		return cfg, errors.New("config.server_url is required")
	}
	if err := validateComputeGatewayServerURL(cfg.ServerURL); err != nil {
		return cfg, err
	}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	if cfg.Token == "" {
		return cfg, errors.New("config.token is required")
	}
	cfg.Repository, _ = raw["repository"].(string)
	if cfg.Repository == "" {
		return cfg, errors.New("config.repository is required")
	}
	cfg.OIDCToken, _ = raw["oidc_token"].(string)
	cfg.OIDCToken = os.ExpandEnv(cfg.OIDCToken)
	if cfg.OIDCToken == "" {
		return cfg, errors.New("config.oidc_token is required")
	}
	cfg.WorkflowRunID = requiredString(raw, "workflow_run_id")
	if cfg.WorkflowRunID == "" {
		return cfg, errors.New("config.workflow_run_id is required")
	}
	cfg.WorkflowRunAttempt = optionalString(raw, "workflow_run_attempt")
	cfg.WorkflowJobID = requiredString(raw, "workflow_job_id")
	if cfg.WorkflowJobID == "" {
		return cfg, errors.New("config.workflow_job_id is required")
	}
	cfg.WorkflowJobName, _ = raw["workflow_job_name"].(string)
	if cfg.WorkflowJobName == "" {
		return cfg, errors.New("config.workflow_job_name is required")
	}
	cfg.Ref, _ = raw["ref"].(string)
	cfg.SHA, _ = raw["sha"].(string)
	cfg.OrgID, _ = raw["org_id"].(string)
	if cfg.OrgID == "" {
		return cfg, errors.New("config.org_id is required")
	}
	cfg.PoolID, _ = raw["pool_id"].(string)
	if cfg.PoolID == "" {
		return cfg, errors.New("config.pool_id is required")
	}
	cfg.PolicyID, _ = raw["policy_id"].(string)
	if cfg.PolicyID == "" {
		return cfg, errors.New("config.policy_id is required")
	}
	if timeout, ok, err := optionalInt(raw, "timeout_seconds"); err != nil {
		return cfg, err
	} else if ok {
		if timeout < 0 {
			return cfg, errors.New("config.timeout_seconds cannot be negative")
		}
		cfg.TimeoutSeconds = timeout
	}
	args, err := stringSlice(raw["command_args"])
	if err != nil {
		return cfg, fmt.Errorf("config.command_args: %w", err)
	}
	if len(args) == 0 {
		return cfg, errors.New("config.command_args is required")
	}
	cfg.CommandArgs = args
	cfg.WorkingDirectory, _ = raw["working_directory"].(string)
	if values, err := stringSlice(raw["artifact_allowlist"]); err != nil {
		return cfg, fmt.Errorf("config.artifact_allowlist: %w", err)
	} else {
		cfg.ArtifactAllowlist = values
	}
	if labels, err := stringMap(raw["labels"]); err != nil {
		return cfg, fmt.Errorf("config.labels: %w", err)
	} else {
		cfg.Labels = labels
	}
	cfg.ExecutionSecurityTier, _ = raw["execution_security_tier"].(string)
	if cfg.ExecutionSecurityTier == "" {
		cfg.ExecutionSecurityTier = "sandboxed-container"
	}
	cfg.ProofTier, _ = raw["proof_tier"].(string)
	if cfg.ProofTier == "" {
		cfg.ProofTier = "artifact-hash"
	}
	if !protectedComputeGatewayExecutionTier(cfg.ExecutionSecurityTier) {
		return cfg, fmt.Errorf("config.execution_security_tier %q is not protected for compute gateway workloads", cfg.ExecutionSecurityTier)
	}
	if !proofBackedComputeGatewayTier(cfg.ProofTier) {
		return cfg, fmt.Errorf("config.proof_tier %q is not proof-backed for compute gateway workloads", cfg.ProofTier)
	}
	cfg.ExecutorProvider, _ = raw["executor_provider"].(string)
	cfg.HardwareClass, _ = raw["hardware_class"].(string)
	if rawWait, ok := raw["wait"]; ok {
		wait, ok := rawWait.(bool)
		if !ok {
			return cfg, errors.New("config.wait must be a boolean")
		}
		cfg.Wait = wait
	} else {
		cfg.Wait = true
	}
	if !cfg.Wait {
		return cfg, errors.New("config.wait must be true for proof-backed compute gateway workloads")
	}
	cfg.WriteCheck, _ = raw["write_check"].(bool)
	cfg.CheckOwner, _ = raw["check_owner"].(string)
	cfg.CheckRepo, _ = raw["check_repo"].(string)
	cfg.CheckSHA, _ = raw["check_sha"].(string)
	cfg.CheckName, _ = raw["check_name"].(string)
	cfg.CheckToken, _ = raw["check_token"].(string)
	cfg.CheckToken = os.ExpandEnv(cfg.CheckToken)
	if cfg.CheckOwner != "" || cfg.CheckRepo != "" || cfg.CheckSHA != "" || cfg.CheckName != "" || cfg.CheckToken != "" {
		cfg.WriteCheck = true
	}
	if cfg.WriteCheck {
		if !cfg.Wait {
			return cfg, errors.New("config.write_check requires wait=true")
		}
		if cfg.CheckOwner == "" || cfg.CheckRepo == "" || cfg.CheckSHA == "" || cfg.CheckName == "" || cfg.CheckToken == "" {
			return cfg, errors.New("config.write_check requires check_owner, check_repo, check_sha, check_name, and check_token")
		}
	}
	pollInterval, err := durationField(raw, "poll_interval", "10s")
	if err != nil {
		return cfg, err
	}
	if pollInterval <= 0 {
		return cfg, errors.New("config.poll_interval must be greater than zero")
	}
	cfg.PollInterval = pollInterval
	timeout, err := durationField(raw, "timeout", "30m")
	if err != nil {
		return cfg, err
	}
	if timeout <= 0 {
		return cfg, errors.New("config.timeout must be greater than zero")
	}
	cfg.Timeout = timeout
	return cfg, nil
}

func (s *computeGatewayStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	req, err := s.workloadRequest(triggerData, stepOutputs, current)
	if err != nil {
		return errorResult(err.Error()), nil
	}
	submitted, err := s.client.SubmitWorkload(ctx, s.config.ServerURL, s.config.Token, req)
	if err != nil {
		return errorResult(fmt.Sprintf("failed to submit compute gateway workload: %v", err)), nil
	}
	output := map[string]any{
		"task_id": submitted.Task.ID,
		"status":  submitted.Task.Status,
	}
	status, err := s.waitForStatus(ctx, submitted.Task.ID)
	if err != nil {
		return errorResult(err.Error()), nil
	}
	if err := computeGatewayTerminalError(status); err != nil {
		checkOutput, checkErr := s.writeTerminalCheck(ctx, status, triggerData, stepOutputs, current)
		_ = checkOutput
		if checkErr != nil {
			return errorResult(errors.Join(err, checkErr).Error()), nil
		}
		return errorResult(err.Error()), nil
	}
	checkOutput, checkErr := s.writeTerminalCheck(ctx, status, triggerData, stepOutputs, current)
	if checkErr != nil {
		return errorResult(checkErr.Error()), nil
	}
	for key, value := range computeGatewayStatusOutput(status) {
		output[key] = value
	}
	for key, value := range checkOutput {
		output[key] = value
	}
	return &sdk.StepResult{Output: output}, nil
}

func (s *computeGatewayStep) workloadRequest(triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) (computeGatewayWorkloadRequest, error) {
	runID, err := resolveInt64(s.config.WorkflowRunID, "workflow_run_id", triggerData, stepOutputs, current)
	if err != nil {
		return computeGatewayWorkloadRequest{}, err
	}
	var attempt int64
	if s.config.WorkflowRunAttempt != "" {
		attempt, err = resolveInt64(s.config.WorkflowRunAttempt, "workflow_run_attempt", triggerData, stepOutputs, current)
		if err != nil {
			return computeGatewayWorkloadRequest{}, err
		}
	}
	jobID, err := resolveInt64(s.config.WorkflowJobID, "workflow_job_id", triggerData, stepOutputs, current)
	if err != nil {
		return computeGatewayWorkloadRequest{}, err
	}
	return computeGatewayWorkloadRequest{
		Repository:         resolveField(s.config.Repository, triggerData, stepOutputs, current),
		OIDCToken:          resolveField(s.config.OIDCToken, triggerData, stepOutputs, current),
		WorkflowRunID:      runID,
		WorkflowRunAttempt: attempt,
		WorkflowJobID:      jobID,
		WorkflowJobName:    resolveField(s.config.WorkflowJobName, triggerData, stepOutputs, current),
		Ref:                resolveField(s.config.Ref, triggerData, stepOutputs, current),
		SHA:                resolveField(s.config.SHA, triggerData, stepOutputs, current),
		OrgID:              resolveField(s.config.OrgID, triggerData, stepOutputs, current),
		PoolID:             resolveField(s.config.PoolID, triggerData, stepOutputs, current),
		PolicyID:           resolveField(s.config.PolicyID, triggerData, stepOutputs, current),
		TimeoutSeconds:     s.config.TimeoutSeconds,
		CommandArgs:        resolveStringSlice(s.config.CommandArgs, triggerData, stepOutputs, current),
		WorkingDirectory:   resolveField(s.config.WorkingDirectory, triggerData, stepOutputs, current),
		ArtifactAllowlist:  resolveStringSlice(s.config.ArtifactAllowlist, triggerData, stepOutputs, current),
		Labels:             resolveStringMap(s.config.Labels, triggerData, stepOutputs, current),
		Requirements: computeGatewayPlacementRequirements{
			ExecutorProvider:      resolveField(s.config.ExecutorProvider, triggerData, stepOutputs, current),
			ExecutionSecurityTier: s.config.ExecutionSecurityTier,
			ProofTier:             s.config.ProofTier,
			HardwareClass:         resolveField(s.config.HardwareClass, triggerData, stepOutputs, current),
		},
	}, nil
}

func (s *computeGatewayStep) waitForStatus(ctx context.Context, taskID string) (computeGatewayWorkloadStatus, error) {
	deadline := time.Now().Add(s.config.Timeout)
	for {
		resp, err := s.client.WorkloadStatus(ctx, s.config.ServerURL, s.config.Token, taskID)
		if err != nil {
			return computeGatewayWorkloadStatus{}, fmt.Errorf("failed to read compute gateway status: %w", err)
		}
		if computeGatewayTerminal(resp.Status) {
			return resp.Status, nil
		}
		if time.Now().After(deadline) {
			return computeGatewayWorkloadStatus{}, fmt.Errorf("timeout waiting for compute gateway task %s after %s", taskID, s.config.Timeout)
		}
		select {
		case <-ctx.Done():
			return computeGatewayWorkloadStatus{}, errors.New("context cancelled while waiting for compute gateway workload")
		case <-time.After(s.config.PollInterval):
		}
	}
}

func (s *computeGatewayStep) writeTerminalCheck(
	ctx context.Context,
	status computeGatewayWorkloadStatus,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
) (map[string]any, error) {
	if !s.config.WriteCheck {
		return nil, nil
	}
	owner, err := resolveRequiredCheckField("check_owner", s.config.CheckOwner, triggerData, stepOutputs, current)
	if err != nil {
		return nil, err
	}
	repo, err := resolveRequiredCheckField("check_repo", s.config.CheckRepo, triggerData, stepOutputs, current)
	if err != nil {
		return nil, err
	}
	sha, err := resolveRequiredCheckField("check_sha", s.config.CheckSHA, triggerData, stepOutputs, current)
	if err != nil {
		return nil, err
	}
	if err := validateComputeGatewayCheckTarget(status, owner, repo, sha); err != nil {
		return nil, err
	}
	name, err := resolveRequiredCheckField("check_name", s.config.CheckName, triggerData, stepOutputs, current)
	if err != nil {
		return nil, err
	}
	token, err := resolveRequiredCheckField("check_token", s.config.CheckToken, triggerData, stepOutputs, current)
	if err != nil {
		return nil, err
	}
	req := &CreateCheckRunRequest{
		Name:       name,
		HeadSHA:    sha,
		Status:     "completed",
		Conclusion: githubConclusionForComputeStatus(status),
		Output: &CheckRunOutput{
			Title:   "workflow-compute " + status.Status,
			Summary: computeGatewayCheckSummary(status),
		},
	}
	check, err := s.github.CreateCheckRun(ctx, owner, repo, req, token)
	if err != nil {
		return nil, fmt.Errorf("failed to create compute gateway check: %w", err)
	}
	return map[string]any{
		"check_run_id": check.ID,
		"check_url":    check.HTMLURL,
	}, nil
}

func validateComputeGatewayCheckTarget(status computeGatewayWorkloadStatus, owner, repo, sha string) error {
	verifiedRepository := status.Labels["github.provenance.repository"]
	verifiedSHA := status.Labels["github.provenance.sha"]
	if verifiedRepository == "" || verifiedSHA == "" {
		return fmt.Errorf("compute gateway status missing verified github repository/sha labels for check binding")
	}
	if !strings.EqualFold(owner+"/"+repo, verifiedRepository) {
		return fmt.Errorf("compute gateway check target %s/%s does not match verified repository %s", owner, repo, verifiedRepository)
	}
	if !strings.EqualFold(sha, verifiedSHA) {
		return fmt.Errorf("compute gateway check sha %s does not match verified sha %s", sha, verifiedSHA)
	}
	return nil
}

func resolveRequiredCheckField(name, value string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) (string, error) {
	resolved := resolveField(value, triggerData, stepOutputs, current)
	if resolved == "" || strings.Contains(resolved, "{{") {
		return "", fmt.Errorf("config.%s resolved to empty or unresolved value", name)
	}
	return resolved, nil
}

func (c httpComputeGatewayClient) SubmitWorkload(ctx context.Context, serverURL, token string, req computeGatewayWorkloadRequest) (computeGatewayWorkloadResponse, error) {
	var out computeGatewayWorkloadResponse
	err := c.do(ctx, http.MethodPost, serverURL, token, "/v1/adapters/github/actions/workloads", req, http.StatusCreated, &out)
	return out, err
}

func (c httpComputeGatewayClient) WorkloadStatus(ctx context.Context, serverURL, token, taskID string) (computeGatewayStatusResponse, error) {
	var out computeGatewayStatusResponse
	err := c.do(ctx, http.MethodGet, serverURL, token, "/v1/adapters/github/actions/workloads/"+url.PathEscape(taskID), nil, http.StatusOK, &out)
	return out, err
}

func (c httpComputeGatewayClient) do(ctx context.Context, method, serverURL, token, path string, body any, want int, out any) error {
	base, err := url.Parse(serverURL)
	if err != nil {
		return fmt.Errorf("parse server_url: %w", err)
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, base.JoinPath(path).String(), reader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: got HTTP %d want %d: %s", method, path, resp.StatusCode, want, strings.TrimSpace(string(data)))
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}
	return nil
}

func validateComputeGatewayServerURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse config.server_url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("config.server_url must use http or https")
	}
	if u.Host == "" {
		return errors.New("config.server_url must include host")
	}
	if u.Scheme == "http" && !computeGatewayLoopback(u.Hostname()) {
		return fmt.Errorf("config.server_url refuses bearer token over cleartext non-loopback host %q", raw)
	}
	return nil
}

func computeGatewayLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func requiredString(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func optionalString(raw map[string]any, key string) string {
	value, ok := raw[key]
	if !ok || value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func optionalInt(raw map[string]any, key string) (int, bool, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0, false, nil
	}
	switch v := value.(type) {
	case int:
		return v, true, nil
	case int64:
		if v > int64(int(^uint(0)>>1)) || v < -int64(int(^uint(0)>>1))-1 {
			return 0, true, fmt.Errorf("config.%s is outside int range", key)
		}
		return int(v), true, nil
	case float64:
		if v != float64(int64(v)) {
			return 0, true, fmt.Errorf("config.%s must be an integer", key)
		}
		if v > float64(int(^uint(0)>>1)) || v < -float64(int(^uint(0)>>1))-1 {
			return 0, true, fmt.Errorf("config.%s is outside int range", key)
		}
		return int(v), true, nil
	case string:
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, true, fmt.Errorf("config.%s is not a valid integer: %w", key, err)
		}
		return n, true, nil
	default:
		return 0, true, fmt.Errorf("config.%s must be an integer", key)
	}
}

func durationField(raw map[string]any, key, fallback string) (time.Duration, error) {
	rawValue, ok := raw[key]
	if ok && rawValue != nil {
		value, ok := rawValue.(string)
		if !ok {
			return 0, fmt.Errorf("config.%s must be a duration string", key)
		}
		return parseDurationField(key, value)
	}
	value := fallback
	return parseDurationField(key, value)
}

func parseDurationField(key, value string) (time.Duration, error) {
	if value == "" {
		return 0, fmt.Errorf("config.%s is required", key)
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("config.%s is invalid: %w", key, err)
	}
	return d, nil
}

func stringSlice(value any) ([]string, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case []string:
		return append([]string(nil), v...), nil
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, errors.New("all values must be strings")
			}
			out = append(out, s)
		}
		return out, nil
	default:
		return nil, errors.New("must be a string array")
	}
}

func stringMap(value any) (map[string]string, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, item := range v {
			out[key] = item
		}
		return out, nil
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, item := range v {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s must be a string", key)
			}
			out[key] = s
		}
		return out, nil
	default:
		return nil, errors.New("must be a string map")
	}
}

func resolveInt64(value, name string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) (int64, error) {
	resolved := resolveField(value, triggerData, stepOutputs, current)
	n, err := strconv.ParseInt(resolved, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s resolved to non-integer value %q: %w", name, resolved, err)
	}
	return n, nil
}

func resolveStringSlice(values []string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, resolveField(value, triggerData, stepOutputs, current))
	}
	return out
}

func resolveStringMap(values map[string]string, triggerData map[string]any, stepOutputs map[string]map[string]any, current map[string]any) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = resolveField(value, triggerData, stepOutputs, current)
	}
	return out
}

func protectedComputeGatewayExecutionTier(tier string) bool {
	switch tier {
	case "sandboxed-container", "microvm", "confidential-cpu", "confidential-gpu", "wasm-capability":
		return true
	default:
		return false
	}
}

func proofBackedComputeGatewayTier(tier string) bool {
	switch tier {
	case "artifact-hash", "replicated-quorum", "attested-receipt", "attested-quorum", "zk-replay":
		return true
	default:
		return false
	}
}

func computeGatewayTerminal(status computeGatewayWorkloadStatus) bool {
	if status.Conclusion != "" {
		return true
	}
	switch status.Status {
	case "failed", "stalled", "cancelled":
		return true
	default:
		return false
	}
}

func computeGatewayTerminalError(status computeGatewayWorkloadStatus) error {
	if status.Conclusion == "success" && (status.ProofID == "" || status.ContributionID == "") {
		return fmt.Errorf("compute gateway task %s reported success without proof-backed status", status.TaskID)
	}
	if status.Conclusion != "" && status.Conclusion != "success" {
		return fmt.Errorf("compute gateway task %s concluded %s", status.TaskID, status.Conclusion)
	}
	switch status.Status {
	case "failed", "stalled", "cancelled":
		return fmt.Errorf("compute gateway task %s ended with status %s", status.TaskID, status.Status)
	default:
		return nil
	}
}

func githubConclusionForComputeStatus(status computeGatewayWorkloadStatus) string {
	if status.Conclusion == "success" && (status.ProofID == "" || status.ContributionID == "") {
		return "action_required"
	}
	if status.Conclusion == "success" {
		return "success"
	}
	switch status.Status {
	case "cancelled":
		return "cancelled"
	case "stalled":
		return "timed_out"
	default:
		return "failure"
	}
}

func computeGatewayCheckSummary(status computeGatewayWorkloadStatus) string {
	parts := []string{
		"task_id=" + status.TaskID,
		"status=" + status.Status,
	}
	if status.Conclusion != "" {
		parts = append(parts, "conclusion="+status.Conclusion)
	}
	if status.ProofID != "" {
		parts = append(parts, "proof_id="+status.ProofID)
	}
	if status.ContributionID != "" {
		parts = append(parts, "contribution_id="+status.ContributionID)
	}
	if status.ArtifactHash != "" {
		parts = append(parts, "artifact_hash="+status.ArtifactHash)
	}
	return strings.Join(parts, "\n")
}

func computeGatewayStatusOutput(status computeGatewayWorkloadStatus) map[string]any {
	return map[string]any{
		"task_id":         status.TaskID,
		"status":          status.Status,
		"conclusion":      status.Conclusion,
		"proof_id":        status.ProofID,
		"artifact_hash":   status.ArtifactHash,
		"contribution_id": status.ContributionID,
		"worker_id":       status.WorkerID,
	}
}
