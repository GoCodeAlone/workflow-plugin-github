package main

import (
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
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
)

const (
	computeProtocolVersion = "compute.v1alpha1"
	providerOperation      = "ephemeral_runner_job"
	proofArtifactName      = "github-runner-proof.json"
)

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
	if mode == internal.EphemeralRunnerJobModeDispatchThenWait {
		if strings.TrimSpace(d.req.Workflow) == "" || strings.TrimSpace(d.req.Repository) == "" {
			return internal.EphemeralRunnerJobResult{}, fmt.Errorf("repository and workflow are required for %s", mode)
		}
		ref := strings.TrimSpace(d.req.Ref)
		if ref == "" {
			ref = "main"
		}
		inputs, err := d.workflowDispatchInputs(spec)
		if err != nil {
			return internal.EphemeralRunnerJobResult{}, err
		}
		if err := d.sidecar.dispatchWorkflow(ctx, d.req.Repository, d.req.Workflow, ref, inputs); err != nil {
			return internal.EphemeralRunnerJobResult{}, err
		}
	}
	if err := d.configureRunner(ctx, spec, token.Token); err != nil {
		return internal.EphemeralRunnerJobResult{}, err
	}
	runnerID := d.runnerID()
	if err := d.runRunner(ctx); err != nil {
		return internal.EphemeralRunnerJobResult{}, err
	}
	return internal.EphemeralRunnerJobResult{
		RunnerID:      runnerID,
		RunnerName:    spec.RunnerName,
		Labels:        append([]string(nil), spec.Labels...),
		CleanupStatus: "removed",
	}, nil
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
