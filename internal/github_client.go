// Package internal implements the workflow-plugin-github plugin, providing
// GitHub webhook handling and GitHub Actions workflow management.
package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// GitHubClient is the interface for interacting with the GitHub API.
// It is defined as an interface so tests can inject a mock.
type GitHubClient interface {
	TriggerWorkflow(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, token string) error
	GetWorkflowRun(ctx context.Context, owner, repo string, runID int64, token string) (*WorkflowRun, error)
	CreateCheckRun(ctx context.Context, owner, repo string, req *CreateCheckRunRequest, token string) (*CheckRun, error)
}

// WorkflowRun represents a GitHub Actions workflow run.
type WorkflowRun struct {
	ID         int64  `json:"id"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	HTMLURL    string `json:"html_url"`
}

// CreateCheckRunRequest holds parameters for creating a GitHub Check Run.
type CreateCheckRunRequest struct {
	Name       string          `json:"name"`
	HeadSHA    string          `json:"head_sha"`
	Status     string          `json:"status"`
	Conclusion string          `json:"conclusion,omitempty"`
	Output     *CheckRunOutput `json:"output,omitempty"`
}

// CheckRunOutput holds the title and summary for a check run.
type CheckRunOutput struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
}

// CheckRun represents a GitHub Check Run response.
type CheckRun struct {
	ID      int64  `json:"id"`
	HTMLURL string `json:"html_url"`
	Status  string `json:"status"`
}

// httpGitHubClient implements GitHubClient using net/http.
type httpGitHubClient struct {
	baseURL    string
	httpClient *http.Client
}

// newHTTPGitHubClient returns a production GitHub API client.
func newHTTPGitHubClient() GitHubClient {
	return &httpGitHubClient{
		baseURL: "https://api.github.com",
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// doRequest performs an authenticated request to the GitHub API.
func (c *httpGitHubClient) doRequest(ctx context.Context, method, url string, body any, token string) ([]byte, int, error) {
	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, 0, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, 0, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read response body: %w", err)
	}

	return respBody, resp.StatusCode, nil
}

// TriggerWorkflow triggers a GitHub Actions workflow via workflow_dispatch.
func (c *httpGitHubClient) TriggerWorkflow(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, token string) error {
	url := fmt.Sprintf("%s/repos/%s/%s/actions/workflows/%s/dispatches", c.baseURL, owner, repo, workflow)

	payload := map[string]any{
		"ref": ref,
	}
	if len(inputs) > 0 {
		payload["inputs"] = inputs
	}

	_, status, err := c.doRequest(ctx, http.MethodPost, url, payload, token)
	if err != nil {
		return fmt.Errorf("trigger workflow: %w", err)
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("trigger workflow: unexpected status %d", status)
	}
	return nil
}

// GetWorkflowRun fetches the status of a GitHub Actions workflow run.
func (c *httpGitHubClient) GetWorkflowRun(ctx context.Context, owner, repo string, runID int64, token string) (*WorkflowRun, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/actions/runs/%d", c.baseURL, owner, repo, runID)

	body, status, err := c.doRequest(ctx, http.MethodGet, url, nil, token)
	if err != nil {
		return nil, fmt.Errorf("get workflow run: %w", err)
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("get workflow run: unexpected status %d", status)
	}

	var run WorkflowRun
	if err := json.Unmarshal(body, &run); err != nil {
		return nil, fmt.Errorf("parse workflow run: %w", err)
	}
	return &run, nil
}

// CreateCheckRun creates a GitHub Check Run on a commit.
func (c *httpGitHubClient) CreateCheckRun(ctx context.Context, owner, repo string, req *CreateCheckRunRequest, token string) (*CheckRun, error) {
	url := fmt.Sprintf("%s/repos/%s/%s/check-runs", c.baseURL, owner, repo)

	body, status, err := c.doRequest(ctx, http.MethodPost, url, req, token)
	if err != nil {
		return nil, fmt.Errorf("create check run: %w", err)
	}
	if status != http.StatusCreated {
		return nil, fmt.Errorf("create check run: unexpected status %d", status)
	}

	var check CheckRun
	if err := json.Unmarshal(body, &check); err != nil {
		return nil, fmt.Errorf("parse check run: %w", err)
	}
	return &check, nil
}
