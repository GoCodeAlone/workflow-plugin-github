package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	compute "github.com/GoCodeAlone/workflow-plugin-compute-core/protocol"
	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

const (
	computeProtocolVersion               = "compute.v1alpha1"
	providerOperation                    = "ephemeral_runner_job"
	proofArtifactName                    = "github-runner-proof.json"
	workloadArtifactArchiveName          = "github-workload-outputs.tar.gz"
	defaultRunnerJobTimeout              = 30 * time.Minute
	maxRunnerJobTimeout                  = 6 * time.Hour
	runnerCleanupTimeout                 = 30 * time.Second
	maxRunnerOutputBytes                 = 256 * 1024
	maxRunnerOutputLineBytes             = 64 * 1024
	maxProofArtifactBytes                = 262144
	maxProviderSidecarResponseBytes      = 1 << 20
	maxWorkflowDispatchInputPayloadBytes = 65535
	maxWorkloadArtifactFiles             = 64
	maxWorkloadArtifactFileBytes         = 64 * 1024 * 1024
	maxWorkloadArtifactTotalBytes        = 256 * 1024 * 1024
)

var githubJobPollInterval = 5 * time.Second
var githubJobExitGracePolls = 3
var githubRunnerShutdownGrace = 10 * time.Second
var githubRunnerOnlinePollInterval = 250 * time.Millisecond
var githubRunnerOnlineMaxPollInterval = 2 * time.Second
var runnerForceKillWait = 2 * time.Second
var runnerProcessExtraEnvironmentNames []string

var providerCredentialPatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)bearer\s+\S+`),
	regexp.MustCompile(`github_pat_[A-Za-z0-9_]+`),
	regexp.MustCompile(`gh[pousr]_[A-Za-z0-9_]+`),
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "github-actions-runner-job failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), terminationSignals()...)
	defer stop()
	return runWithIOContext(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
}

func runWithIO(args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	return runWithIOContext(context.Background(), args, stdin, stdout, stderr)
}

func runWithIOContext(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("github-actions-runner-job", flag.ContinueOnError)
	fs.SetOutput(stderr)
	specOnly := fs.Bool("spec", false, "emit the deterministic runner spec without starting a runner")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return runParsedContext(ctx, *specOnly, stdin, stdout)
}

func runParsedContext(ctx context.Context, specOnly bool, stdin io.Reader, stdout io.Writer) error {
	if specOnly {
		var input json.RawMessage
		decoder := json.NewDecoder(stdin)
		if err := decodeExactlyOneJSON(decoder, &input); err != nil {
			return fmt.Errorf("decode request: %w", err)
		}
		req, err := decodeEphemeralRunnerRequest(input)
		if err != nil {
			return err
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
	if err := decodeExactlyOneJSON(decoder, &envelope); err != nil {
		return fmt.Errorf("decode dynamic provider envelope: %w", err)
	}
	if err := envelope.validate(); err != nil {
		return err
	}
	req, err := decodeEphemeralRunnerRequest(envelope.Input)
	if err != nil {
		return err
	}
	if req.TaskID != envelope.TaskID {
		return fmt.Errorf("input task_id must match envelope task_id")
	}
	spec, err := internal.BuildEphemeralRunnerJobSpec(req)
	if err != nil {
		return err
	}
	result := internal.EphemeralRunnerJobResult{
		RunnerName:    spec.RunnerName,
		Labels:        append([]string(nil), spec.Labels...),
		WorkerID:      req.WorkerID,
		TaskID:        req.TaskID,
		CleanupStatus: "skipped",
	}
	providerCredential := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN"))
	sidecar, err := newSidecarClientFromEnv()
	if err == nil {
		providerCredential = sidecar.token
		var driver *runnerDriver
		driver, err = newRunnerDriver(req, sidecar)
		if err == nil {
			req.RuntimeCaps = detectRuntimeCapabilities(ctx, driver.runnerDir, exec.LookPath, probeRuntimeExecutable)
			driver.req = req
			result, err = internal.NewEphemeralRunnerJob(driver).Run(ctx, req)
		}
	}
	if err != nil {
		redacted := redactProviderError(err, providerCredential)
		result.RedactedError = redacted
		err = errors.New(redacted)
	}
	err = mergeExecutionAndProofError(err, writeProofArtifact(result))
	if err != nil {
		return err
	}
	artifacts := make([]string, 0, len(result.WorkloadArtifacts)+1)
	artifacts = append(artifacts, proofArtifactName)
	artifacts = append(artifacts, result.WorkloadArtifacts...)
	output := dynamicProviderOutput{Artifacts: artifacts}
	if err := githubplugin.ValidateEphemeralRunnerJobOutput(output); err != nil {
		return fmt.Errorf("validate dynamic provider output: %w", err)
	}
	return json.NewEncoder(stdout).Encode(output)
}

func decodeExactlyOneJSON(decoder *json.Decoder, destination any) error {
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("input must contain exactly one JSON document")
		}
		return fmt.Errorf("decode trailing JSON: %w", err)
	}
	return nil
}

type dynamicProviderEnvelope struct {
	ProtocolVersion string          `json:"protocol_version"`
	TaskID          string          `json:"task_id"`
	LeaseID         string          `json:"lease_id"`
	ProviderConfig  json.RawMessage `json:"provider_config"`
	Operation       string          `json:"operation"`
	Input           json.RawMessage `json:"input"`
}

type dynamicProviderOutput struct {
	Artifacts []string `json:"artifacts"`
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
	if err := validateProviderConfig(e.ProviderConfig); err != nil {
		return err
	}
	if len(e.Input) == 0 || !json.Valid(e.Input) {
		return fmt.Errorf("input must be valid JSON")
	}
	return nil
}

func validateProviderConfig(raw json.RawMessage) error {
	if len(raw) == 0 || !json.Valid(raw) {
		return fmt.Errorf("provider_config must be valid JSON")
	}
	var config compute.ProviderConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return fmt.Errorf("decode provider_config: %w", err)
	}
	if config == (compute.ProviderConfig{}) {
		return fmt.Errorf("provider_config is required")
	}
	if err := config.Validate(); err != nil {
		return fmt.Errorf("validate provider_config: %w", err)
	}
	if config.PluginID != "workflow-plugin-github" || config.ProviderID != "github-runner" || config.ContractID != "github.runner_provider.v1" {
		return fmt.Errorf("provider_config must identify workflow-plugin-github/github-runner contract github.runner_provider.v1")
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
	if req.TimeoutSeconds > int(maxRunnerJobTimeout/time.Second) {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("timeout_seconds must not exceed %d", int(maxRunnerJobTimeout/time.Second))
	}
	if len(req.WorkerID) > 256 {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("worker_id must not exceed 256 bytes")
	}
	if len(req.TaskID) > 256 {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("task_id must not exceed 256 bytes")
	}
	if !req.RequirePreflight {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("require_preflight must be true")
	}
	if !strings.EqualFold(strings.TrimSpace(req.OS), "linux") {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("os must be %q for the Linux runner bundle", "linux")
	}
	req.OS = "linux"
	switch req.Mode {
	case internal.EphemeralRunnerJobModeDispatchThenWait:
		if req.WorkflowRunID != 0 || req.WorkflowJobID != 0 {
			return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("workflow_run_id and workflow_job_id require attach_to_queued mode")
		}
	case internal.EphemeralRunnerJobModeAttachToQueued:
		if req.WorkflowRunID <= 0 || req.WorkflowJobID <= 0 {
			return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("workflow_run_id and workflow_job_id are required for attach_to_queued mode")
		}
	default:
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("mode must be %q or %q", internal.EphemeralRunnerJobModeDispatchThenWait, internal.EphemeralRunnerJobModeAttachToQueued)
	}
	req.RunnerGroup = strings.TrimSpace(req.RunnerGroup)
	if req.RunnerGroup == "" {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("runner_group is required")
	}
	if req.TimeoutSeconds > 0 {
		req.Timeout = time.Duration(req.TimeoutSeconds) * time.Second
	} else {
		req.Timeout = defaultRunnerJobTimeout
	}
	if err := githubplugin.ValidateEphemeralRunnerJobInput(input); err != nil {
		return internal.EphemeralRunnerJobRequest{}, fmt.Errorf("validate ephemeral runner input schema: %w", err)
	}
	return req, nil
}

func detectRuntimeCapabilities(ctx context.Context, runnerDir string, lookPath func(string) (string, error), probe func(context.Context, string, string) error) []string {
	capabilities := make([]string, 0, 7)
	runnerAvailable := true
	for _, name := range []string{"run.sh"} {
		info, err := os.Stat(filepath.Join(runnerDir, name))
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
			runnerAvailable = false
		}
	}
	listenerPath := filepath.Join(runnerDir, "bin", "Runner.Listener")
	if info, err := os.Stat(listenerPath); err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		runnerAvailable = false
	}
	if runnerAvailable {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := probe(probeCtx, "github-actions-runner", listenerPath)
		cancel()
		if err != nil {
			runnerAvailable = false
		}
	}
	if runnerAvailable {
		capabilities = append(capabilities, "github-actions-runner")
	}
	iacAvailable := false
	for _, executable := range []string{"docker", "podman", "nerdctl", "terraform", "tofu"} {
		path, err := lookPath(executable)
		if err != nil {
			continue
		}
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = probe(probeCtx, executable, path)
		cancel()
		if err != nil {
			continue
		}
		capabilities = append(capabilities, executable)
		if executable == "terraform" || executable == "tofu" {
			iacAvailable = true
		}
	}
	if iacAvailable {
		capabilities = append(capabilities, "iac")
	}
	return capabilities
}

func probeRuntimeExecutable(ctx context.Context, executable, path string) error {
	var args []string
	switch executable {
	case "github-actions-runner":
		args = []string{"--version"}
	case "docker":
		args = []string{"info", "--format", "{{json .ServerVersion}}"}
	case "podman":
		args = []string{"info", "--format", "json"}
	case "nerdctl":
		args = []string{"info"}
	case "terraform", "tofu":
		args = []string{"version"}
	default:
		return fmt.Errorf("unsupported runtime probe %q", executable)
	}
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = runnerProcessEnvironment()
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	return cmd.Run()
}

type providerSidecarClient struct {
	baseURL string
	token   string
	http    *http.Client
}

type providerSidecarHTTPError struct {
	Method     string
	Path       string
	StatusCode int
	Body       string
}

func (e *providerSidecarHTTPError) Error() string {
	return fmt.Sprintf("provider sidecar %s %s: status %d: %s", e.Method, e.Path, e.StatusCode, e.Body)
}

func newSidecarClientFromEnv() (*providerSidecarClient, error) {
	baseURL := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_URL"))
	if baseURL == "" {
		return nil, fmt.Errorf("COMPUTE_GITHUB_RUNNER_PROVIDER_URL is required")
	}
	parsedURL, err := url.Parse(baseURL)
	if err != nil || parsedURL.Host == "" || parsedURL.User != nil || parsedURL.RawQuery != "" || parsedURL.Fragment != "" {
		return nil, fmt.Errorf("provider URL must be absolute and must not contain credentials, query, or fragment")
	}
	host := parsedURL.Hostname()
	loopbackHTTP := parsedURL.Scheme == "http" && net.ParseIP(host).IsLoopback()
	if parsedURL.Scheme != "https" && !loopbackHTTP {
		return nil, fmt.Errorf("provider URL must use HTTPS except for a loopback HTTP endpoint")
	}
	httpClient, err := providerSidecarHTTPClient(parsedURL)
	if err != nil {
		return nil, err
	}
	token := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN"))
	if token == "" {
		return nil, fmt.Errorf("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN is required")
	}
	if err := protectProviderProcessSecrets(); err != nil {
		return nil, fmt.Errorf("protect provider process secrets: %w", err)
	}
	if err := os.Unsetenv("COMPUTE_GITHUB_RUNNER_PROVIDER_TOKEN"); err != nil {
		return nil, fmt.Errorf("remove provider token from process environment: %w", err)
	}
	return &providerSidecarClient{
		baseURL: strings.TrimRight(parsedURL.String(), "/"),
		token:   token,
		http:    httpClient,
	}, nil
}

func providerSidecarHTTPClient(parsedURL *url.URL) (*http.Client, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("provider sidecar redirects are forbidden")
		},
	}
	encodedCA := strings.TrimSpace(os.Getenv("COMPUTE_GITHUB_RUNNER_PROVIDER_CA_CERT_B64"))
	if encodedCA == "" {
		return client, nil
	}
	if parsedURL.Scheme != "https" {
		return nil, errors.New("provider CA certificate requires an HTTPS provider URL")
	}
	caPEM, err := base64.StdEncoding.DecodeString(encodedCA)
	if err != nil {
		return nil, fmt.Errorf("decode provider CA certificate: %w", err)
	}
	roots, err := providerCertificatePool(caPEM, x509.SystemCertPool)
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    roots,
	}
	client.Transport = transport
	return client, nil
}

func providerCertificatePool(caPEM []byte, loadSystemRoots func() (*x509.CertPool, error)) (*x509.CertPool, error) {
	roots, err := loadSystemRoots()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("provider CA certificate must contain at least one PEM certificate")
	}
	return roots, nil
}

func (c *providerSidecarClient) orgJITConfig(ctx context.Context, req internal.EphemeralRunnerJobRequest, spec internal.EphemeralRunnerJobSpec) (internal.GitHubRunnerJITRegistration, error) {
	requestPath := "/v1/actions/orgs/" + url.PathEscape(req.Organization) + "/runners/jitconfig"
	body := struct {
		Repository  string   `json:"repository"`
		Workflow    string   `json:"workflow"`
		Ref         string   `json:"ref"`
		RunnerName  string   `json:"runner_name"`
		RunnerGroup string   `json:"runner_group"`
		Labels      []string `json:"labels"`
	}{
		Repository:  req.Repository,
		Workflow:    req.Workflow,
		Ref:         req.Ref,
		RunnerName:  spec.RunnerName,
		RunnerGroup: spec.RunnerGroup,
		Labels:      append([]string(nil), spec.Labels...),
	}
	var out internal.GitHubRunnerJITRegistration
	if err := c.do(ctx, http.MethodPost, requestPath, body, http.StatusCreated, &out); err != nil {
		return internal.GitHubRunnerJITRegistration{}, err
	}
	if out.RunnerID <= 0 || out.RunnerName != spec.RunnerName || strings.TrimSpace(out.EncodedJITConfig) == "" || strings.TrimSpace(out.OwnershipToken) == "" || out.Preflight == nil {
		return internal.GitHubRunnerJITRegistration{}, errors.New("provider JIT response missing exact runner identity, configuration, ownership token, or preflight evidence")
	}
	return out, nil
}

func (c *providerSidecarClient) acknowledgeOrgJIT(ctx context.Context, organization string, runnerID int64, ownershipToken string) error {
	path := fmt.Sprintf("/v1/actions/orgs/%s/runners/%d/ack", url.PathEscape(organization), runnerID)
	body := struct {
		OwnershipToken string `json:"ownership_token"`
	}{OwnershipToken: ownershipToken}
	return c.do(ctx, http.MethodPost, path, body, http.StatusNoContent, nil)
}

func (c *providerSidecarClient) orgRunner(ctx context.Context, organization string, runnerID int64, spec internal.EphemeralRunnerJobSpec) (internal.GitHubOrgRunner, error) {
	path := fmt.Sprintf("/v1/actions/orgs/%s/runners/%d", url.PathEscape(organization), runnerID)
	query := url.Values{"runner_name": []string{spec.RunnerName}}
	for _, label := range spec.Labels {
		query.Add("label", label)
	}
	path += "?" + query.Encode()
	var runner internal.GitHubOrgRunner
	if err := c.do(ctx, http.MethodGet, path, nil, http.StatusOK, &runner); err != nil {
		return internal.GitHubOrgRunner{}, err
	}
	if runner.ID != runnerID {
		return internal.GitHubOrgRunner{}, fmt.Errorf("provider organization runner id %d does not match requested runner_id %d", runner.ID, runnerID)
	}
	return runner, nil
}

func (c *providerSidecarClient) dispatchWorkflow(ctx context.Context, repository, workflow, ref string, inputs map[string]string, expectedWorkflowPath, expectedHeadSHA string) (internal.GitHubWorkflowDispatch, error) {
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return internal.GitHubWorkflowDispatch{}, err
	}
	path := "/v1/actions/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repo) + "/workflows/" + url.PathEscape(workflow) + "/dispatches"
	body := struct {
		Ref                  string            `json:"ref"`
		Inputs               map[string]string `json:"inputs,omitempty"`
		ExpectedWorkflowPath string            `json:"expected_workflow_path"`
		ExpectedHeadSHA      string            `json:"expected_head_sha"`
	}{
		Ref:                  ref,
		Inputs:               inputs,
		ExpectedWorkflowPath: expectedWorkflowPath,
		ExpectedHeadSHA:      expectedHeadSHA,
	}
	var out internal.GitHubWorkflowDispatch
	if err := c.do(ctx, http.MethodPost, path, body, http.StatusOK, &out); err != nil {
		return internal.GitHubWorkflowDispatch{}, err
	}
	if out.WorkflowRunID <= 0 {
		return internal.GitHubWorkflowDispatch{}, errors.New("provider dispatch response missing exact workflow run")
	}
	switch out.Verification {
	case "", "verified":
		if !strings.EqualFold(out.ValidatedHeadSHA, expectedHeadSHA) {
			return internal.GitHubWorkflowDispatch{}, errors.New("provider dispatch response missing validated head SHA")
		}
	case "uncertain":
		if out.ValidatedHeadSHA != "" && !strings.EqualFold(out.ValidatedHeadSHA, expectedHeadSHA) {
			return internal.GitHubWorkflowDispatch{}, errors.New("uncertain provider dispatch response has mismatched head SHA")
		}
	default:
		return internal.GitHubWorkflowDispatch{}, fmt.Errorf("provider dispatch response has unsupported verification_status %q", out.Verification)
	}
	return out, nil
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

func (c *providerSidecarClient) workflowRun(ctx context.Context, repository string, runID int64) (internal.GitHubWorkflowRun, error) {
	owner, repo, err := splitRepository(repository)
	if err != nil {
		return internal.GitHubWorkflowRun{}, err
	}
	path := fmt.Sprintf("/v1/actions/repos/%s/%s/actions/runs/%d", url.PathEscape(owner), url.PathEscape(repo), runID)
	var out internal.GitHubWorkflowRun
	if err := c.do(ctx, http.MethodGet, path, nil, http.StatusOK, &out); err != nil {
		return internal.GitHubWorkflowRun{}, err
	}
	if out.ID != runID {
		return internal.GitHubWorkflowRun{}, fmt.Errorf("provider workflow run id %d does not match requested run_id %d", out.ID, runID)
	}
	return out, nil
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
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != wantStatus {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return &providerSidecarHTTPError{Method: method, Path: path, StatusCode: resp.StatusCode, Body: strings.TrimSpace(string(data))}
	}
	if out != nil {
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxProviderSidecarResponseBytes+1))
		if err != nil {
			return fmt.Errorf("read provider sidecar response: %w", err)
		}
		if len(data) > maxProviderSidecarResponseBytes {
			return fmt.Errorf("provider sidecar response exceeds %d bytes", maxProviderSidecarResponseBytes)
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decodeExactlyOneJSON(decoder, out); err != nil {
			return fmt.Errorf("decode provider sidecar response: %w", err)
		}
	} else if wantStatus == http.StatusNoContent {
		var unexpected [1]byte
		if count, err := resp.Body.Read(unexpected[:]); count != 0 || !errors.Is(err, io.EOF) {
			return errors.New("provider sidecar 204 response body must be empty")
		}
	}
	return nil
}

type runnerDriver struct {
	req                        internal.EphemeralRunnerJobRequest
	sidecar                    *providerSidecarClient
	runnerDir                  string
	dispatchWorkflowPath       string
	dispatchHeadSHA            string
	workflowVerificationStatus string
}

func newRunnerDriver(req internal.EphemeralRunnerJobRequest, sidecar *providerSidecarClient) (*runnerDriver, error) {
	if err := prepareRunnerProcessIsolation(); err != nil {
		return nil, err
	}
	runnerDir := strings.TrimSpace(os.Getenv("GITHUB_ACTIONS_RUNNER_DIR"))
	if runnerDir == "" {
		runnerDir = "/opt/actions-runner"
	}
	return &runnerDriver{req: req, sidecar: sidecar, runnerDir: runnerDir}, nil
}

func (d *runnerDriver) RunGitHubJob(ctx context.Context, mode internal.EphemeralRunnerJobMode, spec internal.EphemeralRunnerJobSpec) (result internal.EphemeralRunnerJobResult, returnErr error) {
	switch mode {
	case internal.EphemeralRunnerJobModeDispatchThenWait, internal.EphemeralRunnerJobModeAttachToQueued:
	default:
		return internal.EphemeralRunnerJobResult{}, fmt.Errorf("unsupported ephemeral runner mode %q", mode)
	}
	if strings.TrimSpace(d.req.Workflow) == "" || strings.TrimSpace(d.req.Repository) == "" {
		return internal.EphemeralRunnerJobResult{}, fmt.Errorf("repository and workflow are required for %s", mode)
	}
	var dispatchRef string
	var dispatchInputs map[string]string
	if mode == internal.EphemeralRunnerJobModeDispatchThenWait {
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
	if dispatchRef == "" {
		dispatchRef = strings.TrimSpace(d.req.Ref)
		if dispatchRef == "" {
			dispatchRef = "main"
		}
	}
	result = internal.EphemeralRunnerJobResult{
		RunnerName: spec.RunnerName,
		Labels:     append([]string(nil), spec.Labels...),
	}
	registration, err := d.sidecar.orgJITConfig(ctx, d.req, spec)
	if err != nil {
		return result, fmt.Errorf("acquire organization runner JIT config: %w", err)
	}
	result.RunnerID = registration.RunnerID
	result.CleanupStatus = "pending"
	result.Preflight = registration.Preflight
	if err := validateJITPreflight(d.req, spec, registration.Preflight); err != nil {
		return result, err
	}
	if err := d.sidecar.acknowledgeOrgJIT(ctx, d.req.Organization, registration.RunnerID, registration.OwnershipToken); err != nil {
		return result, redactRunnerCredentialError(err, registration.OwnershipToken)
	}
	registration.OwnershipToken = ""
	if mode == internal.EphemeralRunnerJobModeAttachToQueued {
		if err := d.validateQueuedAttachedGitHubJob(ctx, spec, registration.Preflight.ResolvedWorkflowPath, registration.Preflight.ResolvedRefSHA); err != nil {
			return result, err
		}
		d.workflowVerificationStatus = "verified"
		result.WorkflowVerificationStatus = d.workflowVerificationStatus
	}
	runner, err := d.startRunner(ctx, registration.EncodedJITConfig)
	if err != nil {
		return result, redactRunnerCredentialError(err, registration.EncodedJITConfig)
	}
	cleanupPending := true
	defer func() {
		if cleanupPending {
			returnErr = errors.Join(returnErr, cleanupDetachedRunnerProcesses())
		}
	}()
	if mode == internal.EphemeralRunnerJobModeDispatchThenWait {
		if err := d.waitForRunnerOnline(ctx, runner, registration.RunnerID, spec); err != nil {
			return result, redactRunnerCredentialError(err, registration.EncodedJITConfig)
		}
		dispatch, err := d.sidecar.dispatchWorkflow(ctx, d.req.Repository, d.req.Workflow, dispatchRef, dispatchInputs, registration.Preflight.ResolvedWorkflowPath, registration.Preflight.ResolvedRefSHA)
		if err != nil {
			runner.cancel()
			exitErr, stopped := runner.waitAfterCancel(githubRunnerShutdownGrace)
			return result, redactRunnerCredentialError(errors.Join(err, validateRunnerShutdown(exitErr, stopped)), registration.EncodedJITConfig)
		}
		d.req.WorkflowRunID = dispatch.WorkflowRunID
		result.WorkflowRunID = dispatch.WorkflowRunID
		d.dispatchWorkflowPath = registration.Preflight.ResolvedWorkflowPath
		d.dispatchHeadSHA = registration.Preflight.ResolvedRefSHA
		d.workflowVerificationStatus = dispatch.Verification
		if d.workflowVerificationStatus == "" {
			d.workflowVerificationStatus = "verified"
		}
		result.WorkflowVerificationStatus = d.workflowVerificationStatus
	}
	completion, err := d.waitForGitHubCompletion(ctx, runner, spec)
	result.WorkflowVerificationStatus = d.workflowVerificationStatus
	cleanupErr := cleanupDetachedRunnerProcesses()
	cleanupPending = false
	err = errors.Join(err, cleanupErr)
	if completion.WorkflowRunID != 0 {
		result.WorkflowRunID = completion.WorkflowRunID
	}
	if completion.Assigned && completion.RunnerID != registration.RunnerID {
		err = errors.Join(err, fmt.Errorf("GitHub workflow job runner ID %d does not match JIT-owned runner ID %d", completion.RunnerID, registration.RunnerID))
	}
	if completion.WorkflowJobID != 0 {
		result.WorkflowJobID = completion.WorkflowJobID
	}
	if completion.WorkflowJobStatus != "" {
		result.WorkflowJobStatus = completion.WorkflowJobStatus
	}
	if err == nil && completion.Terminal && len(d.req.ArtifactPaths) > 0 {
		artifacts, artifactErr := d.copyWorkloadOutputs(d.req.ArtifactPaths)
		result.WorkloadArtifacts = artifacts
		err = errors.Join(err, artifactErr)
	}
	return result, redactRunnerCredentialError(err, registration.EncodedJITConfig)
}

func validateJITPreflight(req internal.EphemeralRunnerJobRequest, spec internal.EphemeralRunnerJobSpec, preflight *internal.GitHubRunnerProviderPreflight) error {
	if preflight == nil ||
		!strings.EqualFold(strings.TrimSpace(preflight.Organization), strings.TrimSpace(req.Organization)) ||
		strings.TrimSpace(preflight.RunnerGroup) != strings.TrimSpace(spec.RunnerGroup) ||
		preflight.RunnerGroupID <= 0 ||
		strings.TrimSpace(preflight.Ref) != strings.TrimSpace(req.Ref) ||
		strings.TrimSpace(preflight.ResolvedWorkflowPath) == "" ||
		!isFullGitSHA(strings.TrimSpace(preflight.ResolvedRefSHA)) ||
		!preflight.ActionsEnabled ||
		!preflight.SelfHostedAllowed ||
		len(preflight.ConflictingLabels) != 0 {
		return errors.New("JIT preflight response did not match organization runner request")
	}
	return nil
}

func (d *runnerDriver) waitForRunnerOnline(ctx context.Context, running *runningCommand, runnerID int64, spec internal.EphemeralRunnerJobSpec) error {
	pollDelay := githubRunnerOnlinePollInterval
	for {
		select {
		case err := <-running.done:
			running.cancel()
			if err != nil {
				return fmt.Errorf("%s failed before GitHub reported the runner online: %w", filepath.Base(running.path), err)
			}
			return fmt.Errorf("%s exited before GitHub reported the runner online", filepath.Base(running.path))
		default:
		}

		observed, err := d.sidecar.orgRunner(ctx, d.req.Organization, runnerID, spec)
		if err == nil {
			pollDelay = githubRunnerOnlinePollInterval
			if observed.Name != spec.RunnerName || !labelsExactlyMatch(observed.Labels, spec.Labels) {
				return errors.New("provider organization runner status did not match exact JIT runner identity")
			}
			switch strings.ToLower(strings.TrimSpace(observed.Status)) {
			case "online":
				if observed.Busy {
					return errors.New("exact JIT runner became busy before workflow dispatch")
				}
				return nil
			case "offline":
			default:
				return fmt.Errorf("provider organization runner returned unsupported status %q", observed.Status)
			}
		} else if !isRetriableRunnerOnlineError(err) {
			return fmt.Errorf("query exact JIT runner readiness: %w", err)
		}

		timer := time.NewTimer(pollDelay)
		select {
		case runnerErr := <-running.done:
			if !timer.Stop() {
				<-timer.C
			}
			running.cancel()
			if runnerErr != nil {
				return fmt.Errorf("%s failed before GitHub reported the runner online: %w", filepath.Base(running.path), runnerErr)
			}
			return fmt.Errorf("%s exited before GitHub reported the runner online", filepath.Base(running.path))
		case <-timer.C:
			if err != nil && pollDelay < githubRunnerOnlineMaxPollInterval {
				pollDelay = min(pollDelay*2, githubRunnerOnlineMaxPollInterval)
			}
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return errors.Join(ctx.Err(), err)
		}
	}
}

func isRetriableRunnerOnlineError(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var responseErr *providerSidecarHTTPError
	if errors.As(err, &responseErr) {
		return responseErr.StatusCode == http.StatusNotFound || responseErr.StatusCode == http.StatusTooManyRequests || responseErr.StatusCode >= 500
	}
	var networkErr net.Error
	return errors.As(err, &networkErr)
}

func redactRunnerCredentialError(err error, secret string) error {
	if err == nil {
		return nil
	}
	redacted := errors.New(redactProviderError(err, secret))
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w: %v", context.DeadlineExceeded, redacted)
	case errors.Is(err, context.Canceled):
		return fmt.Errorf("%w: %v", context.Canceled, redacted)
	default:
		return redacted
	}
}

func (d *runnerDriver) copyWorkloadOutputs(artifactPaths []string) ([]string, error) {
	if len(artifactPaths) > maxWorkloadArtifactFiles {
		return nil, fmt.Errorf("artifact_paths exceeds maximum of %d files", maxWorkloadArtifactFiles)
	}
	_, repositoryName, err := splitRepository(d.req.Repository)
	if err != nil {
		return nil, err
	}
	archiveFile, err := os.OpenFile(workloadArtifactArchiveName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create workload artifact archive: %w", err)
	}
	archiveComplete := false
	defer func() {
		_ = archiveFile.Close()
		if !archiveComplete {
			_ = os.Remove(workloadArtifactArchiveName)
		}
	}()
	gzipWriter := gzip.NewWriter(archiveFile)
	tarWriter := tar.NewWriter(gzipWriter)
	var totalBytes int64
	for _, artifactPath := range artifactPaths {
		if !validWorkloadArtifactPath(artifactPath) {
			return nil, fmt.Errorf("artifact path %q must be a canonical relative slash path", artifactPath)
		}
		sourceRelative := path.Join("_work", repositoryName, repositoryName, artifactPath)
		source, err := openWorkloadArtifact(d.runnerDir, sourceRelative)
		if err != nil {
			return nil, fmt.Errorf("open workload artifact %q: %w", artifactPath, err)
		}
		info, statErr := source.Stat()
		if statErr != nil || !info.Mode().IsRegular() {
			_ = source.Close()
			if statErr != nil {
				return nil, fmt.Errorf("stat workload artifact %q: %w", artifactPath, statErr)
			}
			return nil, fmt.Errorf("workload artifact %q must be a regular file", artifactPath)
		}
		if info.Size() > maxWorkloadArtifactFileBytes || totalBytes+info.Size() > maxWorkloadArtifactTotalBytes {
			_ = source.Close()
			return nil, fmt.Errorf("workload artifact %q exceeds artifact size limits", artifactPath)
		}
		remaining := maxWorkloadArtifactTotalBytes - totalBytes
		if remaining > maxWorkloadArtifactFileBytes {
			remaining = maxWorkloadArtifactFileBytes
		}
		written, copyErr := writeWorkloadArtifact(tarWriter, artifactPath, source, remaining)
		closeErr := source.Close()
		if copyErr != nil || closeErr != nil {
			return nil, fmt.Errorf("archive workload artifact %q: %w", artifactPath, errors.Join(copyErr, closeErr))
		}
		totalBytes += written
	}
	if err := errors.Join(tarWriter.Close(), gzipWriter.Close(), archiveFile.Close()); err != nil {
		return nil, fmt.Errorf("finalize workload artifact archive: %w", err)
	}
	info, err := os.Stat(workloadArtifactArchiveName)
	if err != nil {
		return nil, fmt.Errorf("stat workload artifact archive: %w", err)
	}
	if info.Size() > maxWorkloadArtifactTotalBytes {
		return nil, fmt.Errorf("workload artifact archive exceeds contract maximum of %d bytes", maxWorkloadArtifactTotalBytes)
	}
	archiveComplete = true
	return []string{workloadArtifactArchiveName}, nil
}

func writeWorkloadArtifact(writer *tar.Writer, name string, source io.Reader, maxBytes int64) (int64, error) {
	staged, err := os.CreateTemp("", "github-workload-artifact-*")
	if err != nil {
		return 0, fmt.Errorf("create bounded artifact staging file: %w", err)
	}
	stagedName := staged.Name()
	if err := os.Remove(stagedName); err != nil {
		_ = staged.Close()
		return 0, fmt.Errorf("unlink bounded artifact staging file: %w", err)
	}
	defer func() { _ = staged.Close() }()
	written, err := io.Copy(staged, io.LimitReader(source, maxBytes+1))
	if err != nil {
		return 0, fmt.Errorf("stage workload artifact: %w", err)
	}
	if written > maxBytes {
		return 0, fmt.Errorf("workload artifact exceeds maximum of %d bytes", maxBytes)
	}
	if _, err := staged.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("rewind workload artifact: %w", err)
	}
	header := &tar.Header{Name: name, Mode: 0o600, Size: written, Typeflag: tar.TypeReg}
	if err := writer.WriteHeader(header); err != nil {
		return 0, fmt.Errorf("write workload artifact header: %w", err)
	}
	if _, err := io.CopyN(writer, staged, written); err != nil {
		return 0, fmt.Errorf("write workload artifact content: %w", err)
	}
	return written, nil
}

func validWorkloadArtifactPath(value string) bool {
	return value != "" &&
		path.Clean(value) == value &&
		!path.IsAbs(value) &&
		value != "." &&
		!strings.HasPrefix(value, "../") &&
		!strings.ContainsAny(value, "\\\x00\r\n\t")
}

func (d *runnerDriver) workflowDispatchInputs(spec internal.EphemeralRunnerJobSpec) (map[string]string, error) {
	inputs := make(map[string]string, len(d.req.WorkflowInputs)+3)
	canonicalKeys := make(map[string]string, len(d.req.WorkflowInputs))
	for originalKey, value := range d.req.WorkflowInputs {
		key := strings.ToLower(strings.TrimSpace(originalKey))
		if key == "" {
			continue
		}
		if previous, exists := canonicalKeys[key]; exists && previous != originalKey {
			return nil, fmt.Errorf("canonical input key collision between %q and %q", previous, originalKey)
		}
		canonicalKeys[key] = originalKey
		inputs[key] = value
	}
	labels, err := json.Marshal(spec.Labels)
	if err != nil {
		return nil, fmt.Errorf("marshal workflow runner labels: %w", err)
	}
	inputs["runner_profile"] = "provider"
	inputs["allow_github_hosted_fallback"] = "false"
	inputs["runner_labels_json"] = string(labels)
	if len(inputs) > 25 {
		return nil, fmt.Errorf("workflow dispatch inputs exceed GitHub limit of 25: got %d", len(inputs))
	}
	if err := validateWorkflowDispatchInputPayload(inputs); err != nil {
		return nil, err
	}
	return inputs, nil
}

func validateWorkflowDispatchInputPayload(inputs map[string]string) error {
	encoded, err := json.Marshal(inputs)
	if err != nil {
		return fmt.Errorf("encode workflow dispatch inputs: %w", err)
	}
	if len(encoded) > maxWorkflowDispatchInputPayloadBytes {
		return fmt.Errorf("workflow dispatch input payload exceeds GitHub limit of 65,535 bytes: got %d", len(encoded))
	}
	return nil
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

func (d *runnerDriver) waitForGitHubCompletion(ctx context.Context, runner *runningCommand, spec internal.EphemeralRunnerJobSpec) (completion githubJobCompletion, returnErr error) {
	ticker := time.NewTicker(githubJobPollInterval)
	defer ticker.Stop()
	reaped := false
	defer func() {
		runner.cancel()
		if reaped {
			return
		}
		exitErr, stopped := runner.waitAfterCancel(githubRunnerShutdownGrace)
		if shutdownErr := validateRunnerShutdown(exitErr, stopped); shutdownErr != nil {
			returnErr = errors.Join(returnErr, shutdownErr)
		}
	}()
	var last githubJobCompletion
	for {
		select {
		case err := <-runner.done:
			reaped = true
			if err != nil {
				return last, fmt.Errorf("%s failed: %w", filepath.Base(runner.path), err)
			}
			completion, observeErr := d.observeTerminalGitHubJob(ctx, spec, githubJobExitGracePolls)
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
			return last, fmt.Errorf("%s exited before GitHub workflow job was assigned to runner %s", filepath.Base(runner.path), spec.RunnerName)
		case <-runner.result:
			// Runner output markers are diagnostic; GitHub API state remains authoritative.
			continue
		case <-ticker.C:
			completion, err := d.observeGitHubJob(ctx, spec)
			if err != nil {
				return last, err
			}
			if completion.WorkflowRunID != 0 {
				last = completion
			}
			if !completion.Terminal {
				continue
			}
			if completion.Success {
				return completion, nil
			}
			return completion, fmt.Errorf("github workflow job failed: %s", completion.Message)
		case <-ctx.Done():
			return last, fmt.Errorf("%s timed out waiting for GitHub job completion: %w", filepath.Base(runner.path), ctx.Err())
		}
	}
}

func (d *runnerDriver) observeTerminalGitHubJob(ctx context.Context, spec internal.EphemeralRunnerJobSpec, maxPolls int) (githubJobCompletion, error) {
	if maxPolls < 1 {
		maxPolls = 1
	}
	var last githubJobCompletion
	for attempt := 0; attempt < maxPolls; attempt++ {
		completion, err := d.observeGitHubJob(ctx, spec)
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

func (d *runnerDriver) observeGitHubJob(ctx context.Context, spec internal.EphemeralRunnerJobSpec) (githubJobCompletion, error) {
	if d.req.Mode == internal.EphemeralRunnerJobModeAttachToQueued {
		return d.observeAttachedGitHubJob(ctx, spec.RunnerName)
	}
	if d.req.WorkflowRunID <= 0 {
		return githubJobCompletion{}, errors.New("dispatch response did not provide an exact workflow run ID")
	}
	if d.workflowVerificationStatus == "uncertain" {
		run, err := d.sidecar.workflowRun(ctx, d.req.Repository, d.req.WorkflowRunID)
		if err != nil {
			return githubJobCompletion{}, nil
		}
		if err := validateAttachedWorkflowRun(run, d.dispatchWorkflowPath, d.req.Ref, d.dispatchHeadSHA); err != nil {
			return githubJobCompletion{}, fmt.Errorf("verify uncertain dispatched workflow run: %w", err)
		}
		d.workflowVerificationStatus = "verified"
	}
	runID := d.req.WorkflowRunID
	jobs, err := d.sidecar.workflowRunJobs(ctx, d.req.Repository, runID)
	if err != nil {
		return githubJobCompletion{}, err
	}
	matchingJobs := make([]internal.GitHubWorkflowJob, 0, 1)
	for _, job := range jobs {
		if job.RunID != 0 && job.RunID != runID {
			return githubJobCompletion{}, fmt.Errorf("GitHub workflow job %d run_id %d does not match requested run_id %d", job.ID, job.RunID, runID)
		}
		jobRunner := strings.TrimSpace(job.RunnerName)
		if !labelsExactlyMatch(job.Labels, spec.Labels) {
			if jobRunner == spec.RunnerName {
				return githubJobCompletion{}, fmt.Errorf("GitHub workflow job %d labels do not match ephemeral runner labels", job.ID)
			}
			continue
		}
		if jobRunner != "" && jobRunner != spec.RunnerName {
			return githubJobCompletion{}, fmt.Errorf("GitHub workflow job %d exact ephemeral labels were assigned to unexpected runner %q", job.ID, job.RunnerName)
		}
		matchingJobs = append(matchingJobs, job)
	}
	if len(matchingJobs) > 1 {
		return githubJobCompletion{}, fmt.Errorf("multiple GitHub workflow jobs in run %d have the exact ephemeral runner labels", runID)
	}
	if len(matchingJobs) == 0 {
		return githubJobCompletion{}, nil
	}
	job := matchingJobs[0]
	jobRunner := strings.TrimSpace(job.RunnerName)
	if jobRunner == "" {
		if strings.EqualFold(job.Status, "completed") {
			return githubJobCompletion{}, nil
		}
		return githubJobCompletion{
			WorkflowRunID:     runID,
			WorkflowJobID:     job.ID,
			WorkflowJobStatus: job.Status,
			Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=unassigned", runID, job.ID, job.Status, job.Conclusion),
		}, nil
	}
	completion := githubJobCompletion{
		RunnerID:          job.RunnerID,
		WorkflowRunID:     runID,
		WorkflowJobID:     job.ID,
		WorkflowJobStatus: job.Status,
		Assigned:          true,
		Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=%s", runID, job.ID, job.Status, job.Conclusion, job.RunnerName),
	}
	if !strings.EqualFold(job.Status, "completed") {
		return completion, nil
	}
	conclusion, err := d.resolveGitHubJobConclusion(ctx, runID, job.Conclusion)
	if err != nil {
		return githubJobCompletion{}, err
	}
	if conclusion == "" {
		return completion, nil
	}
	completion.Terminal = true
	completion.Success = conclusion == "success" || conclusion == "succeeded"
	return completion, nil
}

func (d *runnerDriver) resolveGitHubJobConclusion(ctx context.Context, runID int64, jobConclusion string) (string, error) {
	conclusion := strings.ToLower(strings.TrimSpace(jobConclusion))
	if conclusion != "" {
		return conclusion, nil
	}
	run, err := d.sidecar.workflowRun(ctx, d.req.Repository, runID)
	if err != nil {
		return "", fmt.Errorf("get exact GitHub workflow run conclusion: %w", err)
	}
	if run.ID != runID {
		return "", fmt.Errorf("GitHub workflow run ID %d does not match requested run_id %d", run.ID, runID)
	}
	return strings.ToLower(strings.TrimSpace(run.Conclusion)), nil
}

func (d *runnerDriver) validateQueuedAttachedGitHubJob(ctx context.Context, spec internal.EphemeralRunnerJobSpec, resolvedWorkflowPath, resolvedRefSHA string) error {
	run, err := d.sidecar.workflowRun(ctx, d.req.Repository, d.req.WorkflowRunID)
	if err != nil {
		return fmt.Errorf("get queued GitHub workflow run: %w", err)
	}
	if err := validateAttachedWorkflowRun(run, resolvedWorkflowPath, d.req.Ref, resolvedRefSHA); err != nil {
		return err
	}
	jobs, err := d.sidecar.workflowRunJobs(ctx, d.req.Repository, d.req.WorkflowRunID)
	if err != nil {
		return fmt.Errorf("list queued GitHub workflow job: %w", err)
	}
	for _, job := range jobs {
		if job.ID != d.req.WorkflowJobID {
			continue
		}
		if job.RunID != 0 && job.RunID != d.req.WorkflowRunID {
			return fmt.Errorf("queued GitHub workflow job run_id %d does not match requested run_id %d", job.RunID, d.req.WorkflowRunID)
		}
		status := strings.ToLower(strings.TrimSpace(job.Status))
		if status != "queued" && status != "waiting" {
			return fmt.Errorf("GitHub workflow job %d must be queued before attach, status=%q", job.ID, job.Status)
		}
		if strings.TrimSpace(job.RunnerName) != "" || job.RunnerID != 0 {
			return fmt.Errorf("GitHub workflow job %d is already assigned", job.ID)
		}
		if !labelsExactlyMatch(job.Labels, spec.Labels) {
			return fmt.Errorf("GitHub workflow job %d labels do not match ephemeral runner labels", job.ID)
		}
		return nil
	}
	return fmt.Errorf("queued GitHub workflow job %d was not found in run %d", d.req.WorkflowJobID, d.req.WorkflowRunID)
}

func validateAttachedWorkflowRun(run internal.GitHubWorkflowRun, workflow, ref, resolvedRefSHA string) error {
	wantWorkflow := strings.TrimPrefix(strings.TrimSpace(workflow), ".github/workflows/")
	wantRef := strings.TrimSpace(ref)
	if wantWorkflow == "" || wantRef == "" {
		return errors.New("workflow and ref are required for attach_to_queued mode")
	}
	runPath, runRef, _ := strings.Cut(strings.TrimSpace(run.Path), "@")
	if strings.TrimPrefix(runPath, ".github/workflows/") != wantWorkflow {
		return fmt.Errorf("attached workflow run path %q does not match requested workflow %q", run.Path, workflow)
	}
	if !isFullGitSHA(resolvedRefSHA) || !strings.EqualFold(strings.TrimSpace(run.HeadSHA), resolvedRefSHA) {
		return fmt.Errorf("attached workflow run head_sha %q does not match preflight-resolved ref SHA %q", run.HeadSHA, resolvedRefSHA)
	}
	if isFullGitSHA(wantRef) {
		if runRef != "" && !strings.EqualFold(runRef, wantRef) {
			return fmt.Errorf("attached workflow run ref %q does not match requested ref %q", runRef, ref)
		}
		if runRef == "" && !strings.EqualFold(strings.TrimSpace(run.HeadSHA), wantRef) {
			return fmt.Errorf("attached workflow run ref does not match requested ref %q", ref)
		}
		return nil
	}
	if strings.HasPrefix(wantRef, "refs/") {
		shortRef := strings.TrimPrefix(strings.TrimPrefix(wantRef, "refs/heads/"), "refs/tags/")
		if shortRef == wantRef {
			return fmt.Errorf("attached workflow run requested unsupported ref %q", ref)
		}
		if runRef != "" && runRef != wantRef && runRef != shortRef {
			return fmt.Errorf("attached workflow run ref %q does not match requested ref %q", runRef, ref)
		}
		if headBranch := strings.TrimSpace(run.HeadBranch); headBranch != "" && headBranch != shortRef {
			return fmt.Errorf("attached workflow run head_branch %q does not match requested ref %q", headBranch, ref)
		}
		if runRef == "" && strings.TrimSpace(run.HeadBranch) != shortRef {
			return fmt.Errorf("attached workflow run ref does not match requested ref %q", ref)
		}
		return nil
	}
	if runRef != "" && runRef != wantRef && runRef != "refs/heads/"+wantRef && runRef != "refs/tags/"+wantRef {
		return fmt.Errorf("attached workflow run ref %q does not match requested ref %q", runRef, ref)
	}
	if runRef == "" && strings.TrimSpace(run.HeadBranch) != wantRef {
		return fmt.Errorf("attached workflow run ref does not match requested ref %q", ref)
	}
	return nil
}

func isFullGitSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, char := range value {
		switch {
		case char >= '0' && char <= '9':
		case char >= 'a' && char <= 'f':
		case char >= 'A' && char <= 'F':
		default:
			return false
		}
	}
	return true
}

func (d *runnerDriver) observeAttachedGitHubJob(ctx context.Context, runnerName string) (githubJobCompletion, error) {
	jobs, err := d.sidecar.workflowRunJobs(ctx, d.req.Repository, d.req.WorkflowRunID)
	if err != nil {
		return githubJobCompletion{}, err
	}
	for _, job := range jobs {
		if job.ID != d.req.WorkflowJobID {
			continue
		}
		if job.RunID != 0 && job.RunID != d.req.WorkflowRunID {
			return githubJobCompletion{}, fmt.Errorf("attached GitHub workflow job run_id %d does not match requested run_id %d", job.RunID, d.req.WorkflowRunID)
		}
		completion := githubJobCompletion{
			RunnerID:          job.RunnerID,
			WorkflowRunID:     d.req.WorkflowRunID,
			WorkflowJobID:     job.ID,
			WorkflowJobStatus: job.Status,
			Message:           fmt.Sprintf("run=%d job=%d status=%s conclusion=%s runner=%s", d.req.WorkflowRunID, job.ID, job.Status, job.Conclusion, job.RunnerName),
		}
		jobRunner := strings.TrimSpace(job.RunnerName)
		if jobRunner == "" {
			return completion, nil
		}
		if jobRunner != runnerName {
			return completion, fmt.Errorf("attached GitHub workflow job %d was assigned to runner %q, want %q", job.ID, jobRunner, runnerName)
		}
		completion.Assigned = true
		if !strings.EqualFold(job.Status, "completed") {
			return completion, nil
		}
		conclusion, err := d.resolveGitHubJobConclusion(ctx, d.req.WorkflowRunID, job.Conclusion)
		if err != nil {
			return githubJobCompletion{}, err
		}
		if conclusion == "" {
			return completion, nil
		}
		completion.Terminal = true
		completion.Success = conclusion == "success" || conclusion == "succeeded"
		return completion, nil
	}
	return githubJobCompletion{}, fmt.Errorf("attached GitHub workflow job %d was not found in run %d", d.req.WorkflowJobID, d.req.WorkflowRunID)
}

func labelsExactlyMatch(actual, required []string) bool {
	canonicalSet := func(values []string) (map[string]struct{}, bool) {
		labels := make(map[string]struct{}, len(values))
		for _, value := range values {
			label := strings.ToLower(strings.TrimSpace(value))
			if label == "" {
				return nil, false
			}
			if _, exists := labels[label]; exists {
				return nil, false
			}
			labels[label] = struct{}{}
		}
		return labels, true
	}
	actualSet, actualValid := canonicalSet(actual)
	requiredSet, requiredValid := canonicalSet(required)
	if !actualValid || !requiredValid || len(actualSet) != len(requiredSet) {
		return false
	}
	for label := range requiredSet {
		if _, ok := actualSet[label]; !ok {
			return false
		}
	}
	return true
}

func (d *runnerDriver) RemoveOrgRunner(ctx context.Context, organization string, runnerID int64) error {
	return d.sidecar.removeOrgRunner(ctx, organization, runnerID)
}

func (d *runnerDriver) startRunner(ctx context.Context, encodedJITConfig string) (*runningCommand, error) {
	return startCommand(ctx, filepath.Join(d.runnerDir, "run.sh"), d.runnerDir, "--jitconfig", encodedJITConfig)
}

type runningCommand struct {
	path   string
	cancel context.CancelFunc
	cmd    *exec.Cmd
	mu     sync.Mutex
	stdout boundedOutput
	stderr boundedOutput
	result chan runnerCompletion
	done   chan error
	once   sync.Once
}

type boundedOutput struct {
	data []byte
}

func (b *boundedOutput) appendLine(line string) {
	b.appendBytes(append([]byte(line), '\n'))
}

func (b *boundedOutput) appendBytes(data []byte) {
	if len(data) >= maxRunnerOutputBytes {
		b.data = append(b.data[:0], data[len(data)-maxRunnerOutputBytes:]...)
		return
	}
	if overflow := len(b.data) + len(data) - maxRunnerOutputBytes; overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
	}
	b.data = append(b.data, data...)
}

func (b *boundedOutput) String() string {
	return string(b.data)
}

type runnerCompletion struct {
	success bool
	line    string
}

type runnerOutputCaptureError struct {
	err error
}

func (e *runnerOutputCaptureError) Error() string { return e.err.Error() }
func (e *runnerOutputCaptureError) Unwrap() error { return e.err }

func validateRunnerShutdown(err error, stopped bool) error {
	if !stopped {
		return errors.New("runner process did not stop before shutdown deadline")
	}
	var captureErr *runnerOutputCaptureError
	if errors.As(err, &captureErr) {
		return captureErr
	}
	return nil
}

func startCommand(ctx context.Context, path, dir string, args ...string) (*runningCommand, error) {
	runCtx, cancel := context.WithCancel(ctx)
	cmd := exec.CommandContext(runCtx, path, args...)
	configureRunnerProcessGroup(cmd)
	cmd.Dir = dir
	cmd.Env = runnerProcessEnvironment()
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
	captureErrors := make(chan error, 2)
	go func() {
		captureErrors <- running.captureOutput(stdoutPipe, &running.stdout)
	}()
	go func() {
		captureErrors <- running.captureOutput(stderrPipe, &running.stderr)
	}()
	go func() {
		waitErr := cmd.Wait()
		for range 2 {
			if captureErr := <-captureErrors; captureErr != nil {
				waitErr = errors.Join(waitErr, &runnerOutputCaptureError{err: captureErr})
			}
		}
		running.done <- waitErr
	}()
	return running, nil
}

func runnerProcessEnvironment() []string {
	allowed := map[string]struct{}{
		"COLORTERM":                 {},
		"HOME":                      {},
		"LANG":                      {},
		"LANGUAGE":                  {},
		"LOGNAME":                   {},
		"PATH":                      {},
		"SHELL":                     {},
		"SSL_CERT_DIR":              {},
		"SSL_CERT_FILE":             {},
		"TEMP":                      {},
		"TERM":                      {},
		"TMP":                       {},
		"TMPDIR":                    {},
		"TZ":                        {},
		"USER":                      {},
		"GITHUB_ACTIONS_RUNNER_DIR": {},
	}
	for _, name := range runnerProcessExtraEnvironmentNames {
		allowed[name] = struct{}{}
	}
	filtered := make([]string, 0, len(allowed))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		_, explicitlyAllowed := allowed[name]
		if explicitlyAllowed || strings.HasPrefix(name, "LC_") {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func (r *runningCommand) wait() error {
	err := <-r.done
	r.cancel()
	if err != nil {
		return fmt.Errorf("%s failed: %w", filepath.Base(r.path), err)
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
		killErr := forceKillRunnerProcessGroup(r.cmd)
		forceTimer := time.NewTimer(runnerForceKillWait)
		defer forceTimer.Stop()
		select {
		case err := <-r.done:
			return errors.Join(err, killErr), true
		case <-forceTimer.C:
			return killErr, false
		}
	}
}

func (r *runningCommand) captureOutput(reader io.Reader, buffer *boundedOutput) error {
	bufferedReader := bufio.NewReaderSize(reader, 4096)
	line := make([]byte, 0, 4096)
	truncated := false
	for {
		fragment, readErr := bufferedReader.ReadSlice('\n')
		if remaining := maxRunnerOutputLineBytes - len(line); remaining > 0 {
			if len(fragment) > remaining {
				line = append(line, fragment[:remaining]...)
				truncated = true
			} else {
				line = append(line, fragment...)
			}
		} else if len(fragment) > 0 {
			truncated = true
		}
		if readErr == nil || errors.Is(readErr, io.EOF) {
			captured := strings.TrimSuffix(strings.TrimSuffix(string(line), "\n"), "\r")
			if truncated {
				captured += " [line truncated]"
			}
			if captured != "" {
				r.mu.Lock()
				buffer.appendLine(captured)
				r.mu.Unlock()
				if !truncated {
					if result, ok := parseRunnerCompletion(captured); ok {
						r.once.Do(func() { r.result <- result })
					}
				}
			}
			line = line[:0]
			truncated = false
		}
		switch {
		case readErr == nil:
			continue
		case errors.Is(readErr, bufio.ErrBufferFull):
			continue
		case errors.Is(readErr, io.EOF), errors.Is(readErr, os.ErrClosed):
			return nil
		default:
			return fmt.Errorf("read runner output: %w", readErr)
		}
	}
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
	if len(data)+1 > maxProofArtifactBytes {
		return fmt.Errorf("proof artifact size %d exceeds contract maximum %d", len(data)+1, maxProofArtifactBytes)
	}
	file, err := os.OpenFile(proofArtifactName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create exclusive proof artifact: %w", err)
	}
	complete := false
	defer func() {
		_ = file.Close()
		if !complete {
			_ = os.Remove(proofArtifactName)
		}
	}()
	if _, err := file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write proof artifact: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close proof artifact: %w", err)
	}
	complete = true
	return nil
}

func redactProviderError(err error, secrets ...string) string {
	if err == nil {
		return ""
	}
	redacted := err.Error()
	for _, secret := range secrets {
		if secret = strings.TrimSpace(secret); secret != "" {
			redacted = strings.ReplaceAll(redacted, secret, "<redacted>")
		}
	}
	for _, pattern := range providerCredentialPatterns {
		redacted = pattern.ReplaceAllString(redacted, "<redacted>")
	}
	redacted = strings.Join(strings.Fields(strings.ToValidUTF8(redacted, "?")), " ")
	const maxRedactedErrorBytes = 4096
	if len(redacted) > maxRedactedErrorBytes {
		redacted = strings.ToValidUTF8(redacted[:maxRedactedErrorBytes], "?")
	}
	return redacted
}

func mergeExecutionAndProofError(executionErr, proofErr error) error {
	if proofErr == nil {
		return executionErr
	}
	proofErr = fmt.Errorf("write proof artifact: %w", proofErr)
	if executionErr == nil {
		return proofErr
	}
	return errors.Join(executionErr, proofErr)
}

func splitRepository(repository string) (string, string, error) {
	owner, repo, ok := strings.Cut(strings.TrimSpace(repository), "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", fmt.Errorf("repository must be owner/name")
	}
	return owner, repo, nil
}
