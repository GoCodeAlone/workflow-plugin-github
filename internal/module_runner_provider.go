package internal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

const defaultGitHubAPIBaseURL = "https://api.github.com"
const maxPreflightExistingLabels = 64
const maxGitHubPaginationPages = 100

var errRepositoryNotAllowlisted = errors.New("repository is not allowlisted")
var errOrganizationNotAllowlisted = errors.New("organization is not allowlisted")
var errRunnerGroupNotAllowlisted = errors.New("runner group is not allowlisted")
var errRepositoryOrganizationMismatch = errors.New("repository owner does not match organization")
var errInvalidJITIdentity = errors.New("invalid JIT runner identity")
var errJITOwnershipNotFound = errors.New("pending JIT runner ownership not found")
var errJITOwnershipTokenInvalid = errors.New("JIT runner ownership token is invalid")
var errJITJournalDurabilityUncertain = errors.New("JIT ownership journal durability is uncertain")
var errWorkflowDispatchVerificationUncertain = errors.New("workflow dispatch verification is uncertain")

const defaultJITOwnershipTTL = 2 * time.Minute
const defaultJITOwnedTTL = 7 * time.Hour
const jitOwnershipCleanupAttempts = 5
const defaultJITOwnershipRetryTTL = time.Minute

var jitOwnershipCleanupRetryInterval = 250 * time.Millisecond
var workflowDispatchVerificationRetryInterval = 250 * time.Millisecond

const workflowDispatchVerificationAttempts = 5

type GitHubRunnerClient interface {
	RegistrationToken(ctx context.Context, owner, repo, token string) (GitHubRunnerRegistrationToken, error)
	RemoveRunner(ctx context.Context, owner, repo string, runnerID int64, token string) error
	OrgRegistrationToken(ctx context.Context, organization, token string) (GitHubRunnerRegistrationToken, error)
	GenerateOrgJITConfig(ctx context.Context, req GitHubRunnerJITConfigRequest, token string) (GitHubRunnerJITConfig, error)
	GetOrgRunner(ctx context.Context, organization string, runnerID int64, token string) (GitHubOrgRunner, error)
	RemoveOrgRunner(ctx context.Context, organization string, runnerID int64, token string) error
	PreflightOrg(ctx context.Context, req GitHubRunnerProviderPreflightRequest, token string) (GitHubRunnerProviderPreflight, error)
	DispatchWorkflow(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, expectedWorkflowPath, expectedHeadSHA, token string) (GitHubWorkflowDispatch, error)
	GetWorkflowRun(ctx context.Context, owner, repo string, runID int64, token string) (GitHubWorkflowRun, error)
	ListWorkflowRuns(ctx context.Context, owner, repo, workflow string, createdAfter time.Time, token string) ([]GitHubWorkflowRun, error)
	ListWorkflowRunJobs(ctx context.Context, owner, repo string, runID int64, token string) ([]GitHubWorkflowJob, error)
}

type GitHubRunnerRegistrationToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type GitHubRunnerJITConfigRequest struct {
	Organization  string
	RunnerName    string
	RunnerGroupID int64
	Labels        []string
}

type GitHubRunnerJITConfig struct {
	RunnerID         int64  `json:"runner_id"`
	RunnerName       string `json:"runner_name"`
	EncodedJITConfig string `json:"encoded_jit_config"`
}

type GitHubOrgRunner struct {
	ID     int64    `json:"id"`
	Name   string   `json:"name"`
	Status string   `json:"status"`
	Busy   bool     `json:"busy"`
	Labels []string `json:"labels"`
}

type GitHubRunnerJITRegistration struct {
	RunnerID         int64                          `json:"runner_id"`
	RunnerName       string                         `json:"runner_name"`
	EncodedJITConfig string                         `json:"encoded_jit_config"`
	OwnershipToken   string                         `json:"ownership_token"`
	Preflight        *GitHubRunnerProviderPreflight `json:"preflight"`
}

type GitHubRunnerProviderPreflightRequest struct {
	Organization string
	Repository   string
	Workflow     string
	Ref          string
	RunnerName   string
	RunnerGroup  string
	Labels       []string
}

type GitHubRunnerProviderPreflight struct {
	Organization            string   `json:"organization"`
	RunnerGroup             string   `json:"runner_group,omitempty"`
	RunnerGroupID           int64    `json:"runner_group_id,omitempty"`
	Ref                     string   `json:"ref,omitempty"`
	ResolvedWorkflowPath    string   `json:"resolved_workflow_path,omitempty"`
	ResolvedRefSHA          string   `json:"resolved_ref_sha,omitempty"`
	ExistingLabels          []string `json:"existing_labels,omitempty"`
	ConflictingLabels       []string `json:"conflicting_labels,omitempty"`
	LabelsObserved          int      `json:"labels_observed,omitempty"`
	ExistingLabelsTruncated bool     `json:"existing_labels_truncated,omitempty"`
	RunnerCountChecked      int      `json:"runner_count_checked,omitempty"`
	ActionsEnabled          bool     `json:"actions_enabled"`
	SelfHostedAllowed       bool     `json:"self_hosted_allowed"`
}

type GitHubWorkflowRun struct {
	ID         int64  `json:"id"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion,omitempty"`
	Path       string `json:"path,omitempty"`
	HeadBranch string `json:"head_branch,omitempty"`
	HeadSHA    string `json:"head_sha,omitempty"`
	HTMLURL    string `json:"html_url,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

type GitHubWorkflowDispatch struct {
	WorkflowRunID    int64  `json:"workflow_run_id"`
	RunURL           string `json:"run_url"`
	HTMLURL          string `json:"html_url"`
	ValidatedHeadSHA string `json:"head_sha"`
	Verification     string `json:"verification_status,omitempty"`
}

type githubWorkflowDispatchRequest struct {
	Ref    string            `json:"ref"`
	Inputs map[string]string `json:"inputs,omitempty"`
}

type GitHubWorkflowJob struct {
	ID              int64    `json:"id"`
	RunID           int64    `json:"run_id,omitempty"`
	Status          string   `json:"status"`
	Conclusion      string   `json:"conclusion,omitempty"`
	RunnerID        int64    `json:"runner_id,omitempty"`
	RunnerName      string   `json:"runner_name,omitempty"`
	RunnerGroupID   int64    `json:"runner_group_id,omitempty"`
	RunnerGroupName string   `json:"runner_group_name,omitempty"`
	Labels          []string `json:"labels,omitempty"`
	StartedAt       string   `json:"started_at,omitempty"`
	CompletedAt     string   `json:"completed_at,omitempty"`
}

type httpGitHubRunnerClient struct {
	baseURL    string
	httpClient *http.Client
}

type githubPaginationGuard struct {
	seen  map[string]struct{}
	pages int
}

func (g *githubPaginationGuard) visit(endpoint string) error {
	if g.pages >= maxGitHubPaginationPages {
		return fmt.Errorf("GitHub pagination exceeds %d pages", maxGitHubPaginationPages)
	}
	if g.seen == nil {
		g.seen = make(map[string]struct{})
	}
	if _, exists := g.seen[endpoint]; exists {
		return errors.New("GitHub pagination cycle detected")
	}
	g.seen[endpoint] = struct{}{}
	g.pages++
	return nil
}

func newHTTPGitHubRunnerClient(baseURL string) GitHubRunnerClient {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultGitHubAPIBaseURL
	}
	return &httpGitHubRunnerClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *httpGitHubRunnerClient) RegistrationToken(ctx context.Context, owner, repo, token string) (GitHubRunnerRegistrationToken, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/runners/registration-token", c.baseURL, url.PathEscape(owner), url.PathEscape(repo))
	var out GitHubRunnerRegistrationToken
	if err := c.do(ctx, http.MethodPost, endpoint, nil, token, http.StatusCreated, &out); err != nil {
		return GitHubRunnerRegistrationToken{}, err
	}
	if out.Token == "" {
		return GitHubRunnerRegistrationToken{}, errors.New("github runner registration token response missing token")
	}
	return out, nil
}

func (c *httpGitHubRunnerClient) RemoveRunner(ctx context.Context, owner, repo string, runnerID int64, token string) error {
	if runnerID <= 0 {
		return errors.New("runner_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/runners/%d", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), runnerID)
	return c.doRunnerDelete(ctx, endpoint, token)
}

func (c *httpGitHubRunnerClient) OrgRegistrationToken(ctx context.Context, organization, token string) (GitHubRunnerRegistrationToken, error) {
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners/registration-token", c.baseURL, url.PathEscape(organization))
	var out GitHubRunnerRegistrationToken
	if err := c.do(ctx, http.MethodPost, endpoint, nil, token, http.StatusCreated, &out); err != nil {
		return GitHubRunnerRegistrationToken{}, err
	}
	if out.Token == "" {
		return GitHubRunnerRegistrationToken{}, errors.New("github runner registration token response missing token")
	}
	return out, nil
}

func (c *httpGitHubRunnerClient) GenerateOrgJITConfig(ctx context.Context, req GitHubRunnerJITConfigRequest, token string) (GitHubRunnerJITConfig, error) {
	if strings.TrimSpace(req.Organization) == "" || strings.TrimSpace(req.RunnerName) == "" {
		return GitHubRunnerJITConfig{}, errors.New("organization and runner_name are required")
	}
	if req.RunnerGroupID <= 0 {
		return GitHubRunnerJITConfig{}, errors.New("runner_group_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners/generate-jitconfig", c.baseURL, url.PathEscape(req.Organization))
	body := struct {
		Name          string   `json:"name"`
		RunnerGroupID int64    `json:"runner_group_id"`
		WorkFolder    string   `json:"work_folder"`
		Labels        []string `json:"labels"`
	}{
		Name:          req.RunnerName,
		RunnerGroupID: req.RunnerGroupID,
		WorkFolder:    "_work",
		Labels:        append([]string(nil), req.Labels...),
	}
	var response struct {
		Runner struct {
			ID   int64  `json:"id"`
			Name string `json:"name"`
		} `json:"runner"`
		EncodedJITConfig string `json:"encoded_jit_config"`
	}
	if err := c.do(ctx, http.MethodPost, endpoint, body, token, http.StatusCreated, &response); err != nil {
		return GitHubRunnerJITConfig{}, err
	}
	if response.Runner.ID <= 0 || response.Runner.Name != req.RunnerName || strings.TrimSpace(response.EncodedJITConfig) == "" {
		validationErr := errors.New("github JIT config response missing exact runner identity or encoded configuration")
		if response.Runner.ID <= 0 {
			return GitHubRunnerJITConfig{}, validationErr
		}
		return GitHubRunnerJITConfig{RunnerID: response.Runner.ID, RunnerName: response.Runner.Name}, validationErr
	}
	return GitHubRunnerJITConfig{
		RunnerID:         response.Runner.ID,
		RunnerName:       response.Runner.Name,
		EncodedJITConfig: response.EncodedJITConfig,
	}, nil
}

func (c *httpGitHubRunnerClient) RemoveOrgRunner(ctx context.Context, organization string, runnerID int64, token string) error {
	if runnerID <= 0 {
		return errors.New("runner_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners/%d", c.baseURL, url.PathEscape(organization), runnerID)
	return c.doRunnerDelete(ctx, endpoint, token)
}

func (c *httpGitHubRunnerClient) GetOrgRunner(ctx context.Context, organization string, runnerID int64, token string) (GitHubOrgRunner, error) {
	if runnerID <= 0 {
		return GitHubOrgRunner{}, errors.New("runner_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners/%d", c.baseURL, url.PathEscape(organization), runnerID)
	var response struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Status string `json:"status"`
		Busy   bool   `json:"busy"`
		Labels []struct {
			Name string `json:"name"`
		} `json:"labels"`
	}
	if err := c.do(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &response); err != nil {
		return GitHubOrgRunner{}, err
	}
	if response.ID != runnerID {
		return GitHubOrgRunner{}, fmt.Errorf("GitHub organization runner response id %d does not match requested runner_id %d", response.ID, runnerID)
	}
	runner := GitHubOrgRunner{ID: response.ID, Name: response.Name, Status: response.Status, Busy: response.Busy, Labels: make([]string, 0, len(response.Labels))}
	for _, label := range response.Labels {
		runner.Labels = append(runner.Labels, label.Name)
	}
	return runner, nil
}

func (c *httpGitHubRunnerClient) PreflightOrg(ctx context.Context, req GitHubRunnerProviderPreflightRequest, token string) (GitHubRunnerProviderPreflight, error) {
	repositoryName := ""
	resolvedWorkflowPath := ""
	resolvedRefSHA := ""
	if strings.TrimSpace(req.Repository) != "" {
		owner, repo, _, err := parseRepository(req.Repository)
		if err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
		if !strings.EqualFold(owner, strings.TrimSpace(req.Organization)) {
			return GitHubRunnerProviderPreflight{}, fmt.Errorf("%w: repository owner %q must match organization %q", errRepositoryOrganizationMismatch, owner, req.Organization)
		}
		repositoryName = repo
	}
	permissionsEndpoint := fmt.Sprintf("%s/orgs/%s/actions/permissions", c.baseURL, url.PathEscape(req.Organization))
	var permissions struct {
		EnabledRepositories string `json:"enabled_repositories"`
	}
	if err := c.do(ctx, http.MethodGet, permissionsEndpoint, nil, token, http.StatusOK, &permissions); err != nil {
		return GitHubRunnerProviderPreflight{}, fmt.Errorf("query organization Actions permissions: %w", err)
	}
	actionsEnabled := !strings.EqualFold(strings.TrimSpace(permissions.EnabledRepositories), "none") && strings.TrimSpace(permissions.EnabledRepositories) != ""
	if strings.EqualFold(strings.TrimSpace(permissions.EnabledRepositories), "selected") {
		actionsEnabled = false
		endpoint := fmt.Sprintf("%s/orgs/%s/actions/permissions/repositories?per_page=100", c.baseURL, url.PathEscape(req.Organization))
		var pagination githubPaginationGuard
		for endpoint != "" {
			if err := pagination.visit(endpoint); err != nil {
				return GitHubRunnerProviderPreflight{}, err
			}
			var selected struct {
				Repositories []struct {
					FullName string `json:"full_name"`
				} `json:"repositories"`
			}
			headers, err := c.doRaw(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &selected)
			if err != nil {
				return GitHubRunnerProviderPreflight{}, fmt.Errorf("query selected Actions repositories: %w", err)
			}
			for _, repository := range selected.Repositories {
				if strings.EqualFold(repository.FullName, req.Repository) {
					actionsEnabled = true
				}
			}
			endpoint, err = c.nextPage(headers.Get("Link"))
			if err != nil {
				return GitHubRunnerProviderPreflight{}, err
			}
		}
	}
	if repositoryName != "" {
		owner, repo, _, _ := parseRepository(req.Repository)
		repositoryPermissionsEndpoint := fmt.Sprintf("%s/repos/%s/%s/actions/permissions", c.baseURL, url.PathEscape(owner), url.PathEscape(repo))
		var repositoryPermissions struct {
			Enabled bool `json:"enabled"`
		}
		if err := c.do(ctx, http.MethodGet, repositoryPermissionsEndpoint, nil, token, http.StatusOK, &repositoryPermissions); err != nil {
			return GitHubRunnerProviderPreflight{}, fmt.Errorf("query repository Actions permissions: %w", err)
		}
		actionsEnabled = actionsEnabled && repositoryPermissions.Enabled
		if strings.TrimSpace(req.Workflow) != "" && strings.TrimSpace(req.Ref) != "" {
			workflowPath, resolvedSHA, workflowAndRefAllowed, err := c.workflowAndRefAllowed(ctx, owner, repo, req.Workflow, req.Ref, token)
			if err != nil {
				return GitHubRunnerProviderPreflight{}, err
			}
			actionsEnabled = actionsEnabled && workflowAndRefAllowed
			resolvedWorkflowPath = workflowPath
			resolvedRefSHA = resolvedSHA
		}
	}
	selfHostedEndpoint := fmt.Sprintf("%s/orgs/%s/actions/permissions/self-hosted-runners", c.baseURL, url.PathEscape(req.Organization))
	var selfHostedPermissions struct {
		EnabledRepositories string `json:"enabled_repositories"`
	}
	if err := c.do(ctx, http.MethodGet, selfHostedEndpoint, nil, token, http.StatusOK, &selfHostedPermissions); err != nil {
		return GitHubRunnerProviderPreflight{}, fmt.Errorf("query organization self-hosted runner permissions: %w", err)
	}
	selfHostedAllowed := strings.EqualFold(selfHostedPermissions.EnabledRepositories, "all")
	if strings.EqualFold(selfHostedPermissions.EnabledRepositories, "selected") {
		endpoint := selfHostedEndpoint + "/repositories?per_page=100"
		var pagination githubPaginationGuard
		for endpoint != "" {
			if err := pagination.visit(endpoint); err != nil {
				return GitHubRunnerProviderPreflight{}, err
			}
			var selected struct {
				Repositories []struct {
					FullName string `json:"full_name"`
				} `json:"repositories"`
			}
			headers, err := c.doRaw(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &selected)
			if err != nil {
				return GitHubRunnerProviderPreflight{}, fmt.Errorf("query selected self-hosted runner repositories: %w", err)
			}
			for _, repository := range selected.Repositories {
				if strings.EqualFold(repository.FullName, req.Repository) {
					selfHostedAllowed = true
				}
			}
			endpoint, err = c.nextPage(headers.Get("Link"))
			if err != nil {
				return GitHubRunnerProviderPreflight{}, err
			}
		}
	}

	runnerGroupQuery := url.Values{"per_page": []string{"100"}}
	if repositoryName != "" {
		runnerGroupQuery.Set("visible_to_repository", repositoryName)
	}
	runnerGroupEndpoint := fmt.Sprintf("%s/orgs/%s/actions/runner-groups?%s", c.baseURL, url.PathEscape(req.Organization), runnerGroupQuery.Encode())
	runnerGroupAllowed := false
	var runnerGroupID int64
	var runnerGroupPagination githubPaginationGuard
	for runnerGroupEndpoint != "" {
		if err := runnerGroupPagination.visit(runnerGroupEndpoint); err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
		var groups struct {
			RunnerGroups []struct {
				ID                    int64    `json:"id"`
				Name                  string   `json:"name"`
				RestrictedToWorkflows bool     `json:"restricted_to_workflows"`
				SelectedWorkflows     []string `json:"selected_workflows"`
			} `json:"runner_groups"`
		}
		headers, err := c.doRaw(ctx, http.MethodGet, runnerGroupEndpoint, nil, token, http.StatusOK, &groups)
		if err != nil {
			return GitHubRunnerProviderPreflight{}, fmt.Errorf("query organization runner groups: %w", err)
		}
		for _, group := range groups.RunnerGroups {
			if !strings.EqualFold(group.Name, req.RunnerGroup) {
				continue
			}
			if !group.RestrictedToWorkflows {
				runnerGroupAllowed = true
				runnerGroupID = group.ID
				continue
			}
			workflow := req.Workflow
			if resolvedWorkflowPath != "" {
				workflow = resolvedWorkflowPath
			}
			if selectedWorkflowsAllow(group.SelectedWorkflows, req.Repository, workflow, req.Ref) {
				runnerGroupAllowed = true
				runnerGroupID = group.ID
			}
		}
		runnerGroupEndpoint, err = c.nextPage(headers.Get("Link"))
		if err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
	}

	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners?per_page=100", c.baseURL, url.PathEscape(req.Organization))
	seen := map[string]string{}
	conflictSet := map[string]string{}
	checked := 0
	runnerCount := 0
	labelsObserved := 0
	labelsTruncated := false
	requestedRunnerName := strings.TrimSpace(req.RunnerName)
	var runnerPagination githubPaginationGuard
	for endpoint != "" {
		if err := runnerPagination.visit(endpoint); err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
		var out struct {
			TotalCount int `json:"total_count"`
			Runners    []struct {
				Name   string `json:"name"`
				Labels []struct {
					Name string `json:"name"`
				} `json:"labels"`
			} `json:"runners"`
		}
		headers, err := c.doRaw(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &out)
		if err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
		if out.TotalCount > checked {
			checked = out.TotalCount
		}
		runnerCount += len(out.Runners)
		for _, runner := range out.Runners {
			runnerName := strings.TrimSpace(runner.Name)
			if runnerName != "" && requestedRunnerName != "" && strings.EqualFold(runnerName, requestedRunnerName) {
				conflictSet[strings.ToLower(requestedRunnerName)] = requestedRunnerName
			}
			for _, label := range runner.Labels {
				name := strings.TrimSpace(label.Name)
				if name == "" {
					continue
				}
				labelsObserved++
				canonical := strings.ToLower(name)
				if requestedRunnerName != "" && strings.EqualFold(name, requestedRunnerName) {
					conflictSet[strings.ToLower(requestedRunnerName)] = requestedRunnerName
				}
				if _, exists := seen[canonical]; exists || len(seen) < maxPreflightExistingLabels {
					seen[canonical] = name
				} else {
					labelsTruncated = true
				}
			}
		}
		endpoint, err = c.nextPage(headers.Get("Link"))
		if err != nil {
			return GitHubRunnerProviderPreflight{}, err
		}
	}
	if checked == 0 {
		checked = runnerCount
	}
	existing := make([]string, 0, len(seen))
	for _, name := range seen {
		existing = append(existing, name)
	}
	conflicts := make([]string, 0, len(conflictSet))
	for _, name := range conflictSet {
		conflicts = append(conflicts, name)
	}
	sort.Strings(existing)
	sort.Strings(conflicts)
	return GitHubRunnerProviderPreflight{
		Organization:            req.Organization,
		RunnerGroup:             req.RunnerGroup,
		RunnerGroupID:           runnerGroupID,
		Ref:                     req.Ref,
		ResolvedWorkflowPath:    resolvedWorkflowPath,
		ResolvedRefSHA:          resolvedRefSHA,
		ExistingLabels:          existing,
		ConflictingLabels:       conflicts,
		LabelsObserved:          labelsObserved,
		ExistingLabelsTruncated: labelsTruncated,
		RunnerCountChecked:      checked,
		ActionsEnabled:          actionsEnabled,
		SelfHostedAllowed:       selfHostedAllowed && runnerGroupAllowed,
	}, nil
}

func (c *httpGitHubRunnerClient) workflowAndRefAllowed(ctx context.Context, owner, repo, workflow, ref, token string) (string, string, bool, error) {
	workflowEndpoint := fmt.Sprintf("%s/repos/%s/%s/actions/workflows/%s", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(strings.TrimSpace(workflow)))
	var workflowMetadata struct {
		ID    int64  `json:"id"`
		Path  string `json:"path"`
		State string `json:"state"`
	}
	if _, err := c.doRawAllowed(ctx, http.MethodGet, workflowEndpoint, nil, token, []int{http.StatusOK, http.StatusNotFound}, &workflowMetadata); err != nil {
		return "", "", false, fmt.Errorf("query repository workflow: %w", err)
	}
	if workflowMetadata.ID <= 0 || !strings.EqualFold(strings.TrimSpace(workflowMetadata.State), "active") {
		return "", "", false, nil
	}
	resolvedWorkflowPath := strings.TrimSpace(workflowMetadata.Path)
	requestedWorkflow := strings.TrimPrefix(strings.TrimSpace(workflow), ".github/workflows/")
	if _, err := strconv.ParseInt(requestedWorkflow, 10, 64); err != nil && strings.TrimPrefix(resolvedWorkflowPath, ".github/workflows/") != requestedWorkflow {
		return "", "", false, nil
	}
	resolvedSHA, allowed, err := c.repositoryRefExists(ctx, owner, repo, ref, token)
	return resolvedWorkflowPath, resolvedSHA, allowed, err
}

func (c *httpGitHubRunnerClient) repositoryRefExists(ctx context.Context, owner, repo, ref, token string) (string, bool, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", false, nil
	}
	if isFullGitSHA(ref) {
		endpoint := fmt.Sprintf("%s/repos/%s/%s/commits/%s", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(ref))
		var commit struct {
			SHA string `json:"sha"`
		}
		if _, err := c.doRawAllowed(ctx, http.MethodGet, endpoint, nil, token, []int{http.StatusOK, http.StatusNotFound}, &commit); err != nil {
			return "", false, fmt.Errorf("resolve repository commit: %w", err)
		}
		return commit.SHA, strings.EqualFold(commit.SHA, ref), nil
	}
	if explicit, ok := strings.CutPrefix(ref, "refs/"); ok {
		return c.gitReferenceExists(ctx, owner, repo, explicit, token)
	}
	branchSHA, branch, err := c.gitReferenceExists(ctx, owner, repo, "heads/"+ref, token)
	if err != nil {
		return "", false, err
	}
	tagSHA, tag, err := c.gitReferenceExists(ctx, owner, repo, "tags/"+ref, token)
	if err != nil {
		return "", false, err
	}
	if branch == tag {
		return "", false, nil
	}
	if branch {
		return branchSHA, true, nil
	}
	return tagSHA, true, nil
}

func (c *httpGitHubRunnerClient) gitReferenceExists(ctx context.Context, owner, repo, ref, token string) (string, bool, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/git/ref/%s", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(ref))
	var reference struct {
		Ref    string `json:"ref"`
		Object struct {
			Type string `json:"type"`
			SHA  string `json:"sha"`
		} `json:"object"`
	}
	if _, err := c.doRawAllowed(ctx, http.MethodGet, endpoint, nil, token, []int{http.StatusOK, http.StatusNotFound}, &reference); err != nil {
		return "", false, fmt.Errorf("resolve repository ref %q: %w", ref, err)
	}
	if reference.Ref == "" || reference.Object.SHA == "" {
		return "", false, nil
	}
	return c.resolveGitObjectCommitSHA(ctx, owner, repo, reference.Object.Type, reference.Object.SHA, token)
}

func (c *httpGitHubRunnerClient) resolveGitObjectCommitSHA(ctx context.Context, owner, repo, objectType, objectSHA, token string) (string, bool, error) {
	const maxAnnotatedTagDepth = 8
	seen := make(map[string]struct{}, maxAnnotatedTagDepth)
	for range maxAnnotatedTagDepth {
		objectType = strings.ToLower(strings.TrimSpace(objectType))
		objectSHA = strings.TrimSpace(objectSHA)
		if objectSHA == "" {
			return "", false, nil
		}
		if objectType == "" || objectType == "commit" {
			return objectSHA, true, nil
		}
		if objectType != "tag" {
			return "", false, fmt.Errorf("repository ref resolves to unsupported Git object type %q", objectType)
		}
		canonicalSHA := strings.ToLower(objectSHA)
		if _, exists := seen[canonicalSHA]; exists {
			return "", false, errors.New("annotated tag resolution contains a cycle")
		}
		seen[canonicalSHA] = struct{}{}
		endpoint := fmt.Sprintf("%s/repos/%s/%s/git/tags/%s", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(objectSHA))
		var tag struct {
			Object struct {
				Type string `json:"type"`
				SHA  string `json:"sha"`
			} `json:"object"`
		}
		if _, err := c.doRawAllowed(ctx, http.MethodGet, endpoint, nil, token, []int{http.StatusOK, http.StatusNotFound}, &tag); err != nil {
			return "", false, fmt.Errorf("resolve annotated tag %q: %w", objectSHA, err)
		}
		objectType = tag.Object.Type
		objectSHA = tag.Object.SHA
	}
	return "", false, fmt.Errorf("annotated tag resolution exceeds %d objects", maxAnnotatedTagDepth)
}

func (c *httpGitHubRunnerClient) GetWorkflowRun(ctx context.Context, owner, repo string, runID int64, token string) (GitHubWorkflowRun, error) {
	if runID <= 0 {
		return GitHubWorkflowRun{}, errors.New("run_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/runs/%d", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), runID)
	var out GitHubWorkflowRun
	if err := c.do(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &out); err != nil {
		return GitHubWorkflowRun{}, err
	}
	if out.ID != runID {
		return GitHubWorkflowRun{}, fmt.Errorf("GitHub workflow run response id %d does not match requested run_id %d", out.ID, runID)
	}
	return out, nil
}

func selectedWorkflowsAllow(selectedWorkflows []string, repository, workflow, ref string) bool {
	workflowPath := strings.TrimPrefix(strings.TrimSpace(workflow), ".github/workflows/")
	workflowRef := strings.TrimSpace(ref)
	targets := make(map[string]struct{}, 2)
	switch {
	case strings.HasPrefix(workflowRef, "refs/"), isFullGitSHA(workflowRef):
		targets[workflowPath+"@"+workflowRef] = struct{}{}
	default:
		targets[workflowPath+"@refs/heads/"+workflowRef] = struct{}{}
		targets[workflowPath+"@refs/tags/"+workflowRef] = struct{}{}
	}
	matches := make(map[string]struct{}, len(targets))
	for _, selected := range selectedWorkflows {
		selectedRepository, selectedTarget, ok := splitSelectedWorkflow(selected)
		if !ok || !strings.EqualFold(selectedRepository, strings.TrimSuffix(strings.TrimSpace(repository), "/")) {
			continue
		}
		if _, ok := targets[selectedTarget]; ok {
			matches[selectedTarget] = struct{}{}
		}
	}
	return len(matches) == 1
}

func splitSelectedWorkflow(selected string) (string, string, bool) {
	const workflowDirectory = "/.github/workflows/"
	selectedRepository, selectedTarget, ok := strings.Cut(strings.TrimSpace(selected), workflowDirectory)
	return selectedRepository, selectedTarget, ok
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

func (c *httpGitHubRunnerClient) DispatchWorkflow(ctx context.Context, owner, repo, workflow, ref string, inputs map[string]string, expectedWorkflowPath, expectedHeadSHA, token string) (GitHubWorkflowDispatch, error) {
	if strings.TrimSpace(ref) == "" {
		ref = "main"
	}
	if !isFullGitSHA(expectedHeadSHA) {
		return GitHubWorkflowDispatch{}, errors.New("expected_head_sha must be a full Git commit SHA")
	}
	wantWorkflow := strings.TrimPrefix(strings.TrimSpace(expectedWorkflowPath), ".github/workflows/")
	if wantWorkflow == "" {
		return GitHubWorkflowDispatch{}, errors.New("expected_workflow_path is required")
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/workflows/%s/dispatches", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(workflow))
	body := githubWorkflowDispatchRequest{Ref: ref, Inputs: inputs}
	var out GitHubWorkflowDispatch
	if err := c.do(ctx, http.MethodPost, endpoint, body, token, http.StatusOK, &out); err != nil {
		return GitHubWorkflowDispatch{}, err
	}
	if out.WorkflowRunID <= 0 || strings.TrimSpace(out.RunURL) == "" || strings.TrimSpace(out.HTMLURL) == "" {
		return GitHubWorkflowDispatch{}, errors.New("GitHub workflow dispatch response missing run identity")
	}
	var run GitHubWorkflowRun
	var verificationErr error
	for attempt := 0; attempt < workflowDispatchVerificationAttempts; attempt++ {
		run, verificationErr = c.GetWorkflowRun(ctx, owner, repo, out.WorkflowRunID, token)
		if verificationErr == nil {
			break
		}
		if attempt == workflowDispatchVerificationAttempts-1 {
			break
		}
		timer := time.NewTimer(workflowDispatchVerificationRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			verificationErr = errors.Join(verificationErr, ctx.Err())
			attempt = workflowDispatchVerificationAttempts - 1
		}
	}
	if verificationErr != nil {
		out.Verification = "uncertain"
		return out, fmt.Errorf("%w: %v", errWorkflowDispatchVerificationUncertain, verificationErr)
	}
	runPath, _, _ := strings.Cut(strings.TrimSpace(run.Path), "@")
	if strings.TrimPrefix(runPath, ".github/workflows/") != wantWorkflow || !strings.EqualFold(strings.TrimSpace(run.HeadSHA), expectedHeadSHA) {
		return GitHubWorkflowDispatch{}, fmt.Errorf("dispatched workflow run identity does not match authorized workflow and head SHA")
	}
	out.ValidatedHeadSHA = strings.ToLower(expectedHeadSHA)
	out.Verification = "verified"
	return out, nil
}

func (c *httpGitHubRunnerClient) ListWorkflowRuns(ctx context.Context, owner, repo, workflow string, createdAfter time.Time, token string) ([]GitHubWorkflowRun, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/workflows/%s/runs", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(workflow))
	query := url.Values{}
	query.Set("event", "workflow_dispatch")
	query.Set("per_page", "20")
	if !createdAfter.IsZero() {
		query.Set("created", ">="+createdAfter.UTC().Format(time.RFC3339))
	}
	endpoint += "?" + query.Encode()
	runs := make([]GitHubWorkflowRun, 0)
	var pagination githubPaginationGuard
	for endpoint != "" {
		if err := pagination.visit(endpoint); err != nil {
			return nil, err
		}
		var out struct {
			WorkflowRuns []GitHubWorkflowRun `json:"workflow_runs"`
		}
		headers, err := c.doRaw(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &out)
		if err != nil {
			return nil, err
		}
		runs = append(runs, out.WorkflowRuns...)
		endpoint, err = c.nextPage(headers.Get("Link"))
		if err != nil {
			return nil, err
		}
	}
	return runs, nil
}

func (c *httpGitHubRunnerClient) ListWorkflowRunJobs(ctx context.Context, owner, repo string, runID int64, token string) ([]GitHubWorkflowJob, error) {
	if runID <= 0 {
		return nil, errors.New("run_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/runs/%d/jobs?per_page=100", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), runID)
	jobs := make([]GitHubWorkflowJob, 0)
	var pagination githubPaginationGuard
	for endpoint != "" {
		if err := pagination.visit(endpoint); err != nil {
			return nil, err
		}
		var out struct {
			Jobs []GitHubWorkflowJob `json:"jobs"`
		}
		headers, err := c.doRaw(ctx, http.MethodGet, endpoint, nil, token, http.StatusOK, &out)
		if err != nil {
			return nil, err
		}
		for _, job := range out.Jobs {
			if job.RunID == 0 {
				job.RunID = runID
			}
			jobs = append(jobs, job)
		}
		endpoint, err = c.nextPage(headers.Get("Link"))
		if err != nil {
			return nil, err
		}
	}
	return jobs, nil
}

func (c *httpGitHubRunnerClient) do(ctx context.Context, method, endpoint string, body any, token string, wantStatus int, out any) error {
	_, err := c.doRaw(ctx, method, endpoint, body, token, wantStatus, out)
	return err
}

func (c *httpGitHubRunnerClient) doRaw(ctx context.Context, method, endpoint string, body any, token string, wantStatus int, out any) (http.Header, error) {
	return c.doRawAllowed(ctx, method, endpoint, body, token, []int{wantStatus}, out)
}

func (c *httpGitHubRunnerClient) doRunnerDelete(ctx context.Context, endpoint, token string) error {
	_, err := c.doRawAllowed(ctx, http.MethodDelete, endpoint, nil, token, []int{http.StatusNoContent, http.StatusNotFound}, nil)
	return err
}

func (c *httpGitHubRunnerClient) doRawAllowed(ctx context.Context, method, endpoint string, body any, token string, wantStatuses []int, out any) (http.Header, error) {
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("github token is required")
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal github runner request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return nil, fmt.Errorf("build github runner request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("User-Agent", "workflow-plugin-github")
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := c.httpClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("github runner request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	ok := false
	for _, status := range wantStatuses {
		if resp.StatusCode == status {
			ok = true
			break
		}
	}
	if !ok {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("github runner request returned %s", resp.Status)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.Header, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return nil, fmt.Errorf("decode github runner response: %w", err)
	}
	return resp.Header, nil
}

func githubNextLink(header string) string {
	for part := range strings.SplitSeq(header, ",") {
		part = strings.TrimSpace(part)
		if !strings.Contains(part, `rel="next"`) {
			continue
		}
		start := strings.IndexByte(part, '<')
		end := strings.IndexByte(part, '>')
		if start >= 0 && end > start+1 {
			return part[start+1 : end]
		}
	}
	return ""
}

func (c *httpGitHubRunnerClient) nextPage(linkHeader string) (string, error) {
	next := githubNextLink(linkHeader)
	if next == "" {
		return "", nil
	}
	base, err := url.Parse(c.baseURL)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return "", errors.New("configured GitHub API URL is invalid")
	}
	reference, err := url.Parse(next)
	if err != nil {
		return "", fmt.Errorf("parse GitHub pagination link: %w", err)
	}
	resolved := base.ResolveReference(reference)
	if resolved.User != nil || !strings.EqualFold(resolved.Scheme, base.Scheme) || !strings.EqualFold(resolved.Host, base.Host) {
		return "", fmt.Errorf("GitHub pagination origin %q does not match configured API origin %q", resolved.Scheme+"://"+resolved.Host, base.Scheme+"://"+base.Host)
	}
	return resolved.String(), nil
}

type githubRunnerProviderModule struct {
	name                        string
	config                      githubRunnerProviderConfig
	client                      GitHubRunnerClient
	jitOwnershipTTL             time.Duration
	jitOwnedTTL                 time.Duration
	jitRetryTTL                 time.Duration
	pendingJITMu                sync.Mutex
	pendingJIT                  map[pendingJITKey]*pendingJITOwnership
	cleanupContext              context.Context
	cancelCleanup               context.CancelFunc
	cleanupWG                   sync.WaitGroup
	stopped                     bool
	stateRoot                   *os.Root
	journalDirectorySync        func() error
	journalDirectorySyncPending bool
}

type pendingJITKey struct {
	organization string
	runnerID     int64
}

type pendingJITOwnership struct {
	organization          string
	tokenHash             [sha256.Size]byte
	acknowledged          bool
	deleting              bool
	expiresAt             time.Time
	cleanupAttempts       int
	lastCleanupStatus     string
	lastCleanupAt         time.Time
	timer                 *time.Timer
	timerGeneration       uint64
	cleanupWithoutJournal bool
}

type githubRunnerProviderConfig struct {
	Token         string
	ProviderToken string
	APIBaseURL    string
	Repositories  map[string]struct{}
	Organizations map[string]struct{}
	RunnerGroups  map[string]struct{}
	StateDir      string
}

func newGitHubRunnerProviderModule(name string, raw map[string]any, client GitHubRunnerClient) (*githubRunnerProviderModule, error) {
	if err := rejectUnknownConfig(raw, "token", "provider_token", "api_base_url", "repositories", "organizations", "runner_groups", "state_dir"); err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	cfg := githubRunnerProviderConfig{}
	rawToken, _ := raw["token"].(string)
	cfg.Token = strings.TrimSpace(os.ExpandEnv(rawToken))
	if cfg.Token == "" {
		return nil, fmt.Errorf("github.runner_provider %q: config.token is required", name)
	}
	rawProviderToken, _ := raw["provider_token"].(string)
	cfg.ProviderToken = strings.TrimSpace(os.ExpandEnv(rawProviderToken))
	if cfg.ProviderToken == "" {
		return nil, fmt.Errorf("github.runner_provider %q: config.provider_token is required", name)
	}
	cfg.APIBaseURL, _ = raw["api_base_url"].(string)
	if cfg.APIBaseURL != strings.TrimSpace(cfg.APIBaseURL) {
		return nil, fmt.Errorf("github.runner_provider %q: config.api_base_url must not contain surrounding whitespace", name)
	}
	if err := validateGitHubAPIBaseURL(cfg.APIBaseURL); err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	repositories, err := parseRunnerProviderRepositories(raw["repositories"])
	if err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	if len(repositories) == 0 {
		if _, ok := raw["organizations"]; !ok {
			return nil, fmt.Errorf("github.runner_provider %q: config.repositories requires at least one repository", name)
		}
	}
	cfg.Repositories = repositories
	organizations, err := parseRunnerProviderOrganizations(raw["organizations"])
	if err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	if len(repositories) == 0 && len(organizations) == 0 {
		return nil, fmt.Errorf("github.runner_provider %q: config.repositories or config.organizations requires at least one entry", name)
	}
	cfg.Organizations = organizations
	runnerGroups, err := parseRunnerProviderStringSet(raw["runner_groups"], "config.runner_groups")
	if err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	cfg.RunnerGroups = runnerGroups
	rawStateDir, _ := raw["state_dir"].(string)
	cfg.StateDir = strings.TrimSpace(os.ExpandEnv(rawStateDir))
	if rawStateDir != strings.TrimSpace(rawStateDir) {
		return nil, fmt.Errorf("github.runner_provider %q: config.state_dir must not contain surrounding whitespace", name)
	}
	if len(cfg.Organizations) > 0 && client == nil && cfg.StateDir == "" {
		return nil, fmt.Errorf("github.runner_provider %q: config.state_dir is required for organization JIT runner ownership", name)
	}
	if client == nil {
		client = newHTTPGitHubRunnerClient(cfg.APIBaseURL)
	}
	cleanupContext, cancelCleanup := context.WithCancel(context.Background())
	module := &githubRunnerProviderModule{
		name:            name,
		config:          cfg,
		client:          client,
		jitOwnershipTTL: defaultJITOwnershipTTL,
		jitOwnedTTL:     defaultJITOwnedTTL,
		jitRetryTTL:     defaultJITOwnershipRetryTTL,
		pendingJIT:      make(map[pendingJITKey]*pendingJITOwnership),
		cleanupContext:  cleanupContext,
		cancelCleanup:   cancelCleanup,
	}
	if err := module.initializeJITOwnershipJournal(); err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: initialize JIT ownership journal: %w", name, err)
	}
	return module, nil
}

func validateGitHubAPIBaseURL(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("config.api_base_url must be an absolute GitHub API URL without credentials, query, or fragment")
	}
	if parsed.Scheme == "https" {
		return nil
	}
	host := parsed.Hostname()
	if parsed.Scheme == "http" && (strings.EqualFold(host, "localhost") || net.ParseIP(host).IsLoopback()) {
		return nil
	}
	return errors.New("config.api_base_url must use https except for loopback development endpoints")
}

type GitHubRunnerProviderHTTPService struct {
	module  *githubRunnerProviderModule
	handler http.Handler
}

func NewGitHubRunnerProviderHTTPService(name string, raw map[string]any) (*GitHubRunnerProviderHTTPService, error) {
	if err := githubplugin.ValidateGitHubRunnerProviderConfigValue(raw); err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: raw config does not match provider contract: %w", name, err)
	}
	module, err := newGitHubRunnerProviderModule(name, raw, nil)
	if err != nil {
		return nil, err
	}
	return &GitHubRunnerProviderHTTPService{module: module, handler: module.HTTPHandler()}, nil
}

func (s *GitHubRunnerProviderHTTPService) Handler() http.Handler {
	if s == nil {
		return nil
	}
	return s.handler
}

func (s *GitHubRunnerProviderHTTPService) Stop(ctx context.Context) error {
	if s == nil || s.module == nil {
		return nil
	}
	return s.module.Stop(ctx)
}

func NewGitHubRunnerProviderHTTPHandler(name string, raw map[string]any) (http.Handler, error) {
	module, err := newGitHubRunnerProviderModule(name, raw, nil)
	if err != nil {
		return nil, err
	}
	return module.HTTPHandler(), nil
}

func (m *githubRunnerProviderModule) Init() error { return nil }

func (m *githubRunnerProviderModule) Start(_ context.Context) error {
	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	if m.stopped {
		m.cleanupContext, m.cancelCleanup = context.WithCancel(context.Background())
		m.stopped = false
	}
	for key, pending := range m.pendingJIT {
		if pending.timer == nil {
			m.scheduleJITOwnershipLocked(key, pending)
		}
	}
	return nil
}

func (m *githubRunnerProviderModule) Stop(ctx context.Context) error {
	m.pendingJITMu.Lock()
	m.stopped = true
	if m.cancelCleanup != nil {
		m.cancelCleanup()
	}
	for _, pending := range m.pendingJIT {
		m.stopJITOwnershipTimerLocked(pending)
	}
	m.pendingJITMu.Unlock()
	cleanupDone := make(chan struct{})
	go func() {
		m.cleanupWG.Wait()
		close(cleanupDone)
	}()
	var waitErr error
	select {
	case <-cleanupDone:
	case <-ctx.Done():
		waitErr = ctx.Err()
	}

	m.pendingJITMu.Lock()
	persistErr := m.persistJITOwnershipJournalLocked()
	if persistErr == nil && m.journalDirectorySyncPending {
		if err := m.syncJITOwnershipJournalDirectory(); err != nil {
			persistErr = fmt.Errorf("finalize journal directory sync: %w", err)
		} else {
			m.journalDirectorySyncPending = false
		}
	}
	m.pendingJITMu.Unlock()
	return errors.Join(waitErr, persistErr)
}

func (m *githubRunnerProviderModule) InvokeMethod(method string, args map[string]any) (map[string]any, error) {
	return m.invokeMethod(context.Background(), method, args)
}

func (m *githubRunnerProviderModule) invokeMethod(ctx context.Context, method string, args map[string]any) (map[string]any, error) {
	if err := m.authorizeProvider(args); err != nil {
		return nil, err
	}
	switch method {
	case "registration_token":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		token, err := m.client.RegistrationToken(ctx, owner, repo, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"token":      token.Token,
			"expires_at": token.ExpiresAt.Format(time.RFC3339),
		}, nil
	case "org_registration_token":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		token, err := m.client.OrgRegistrationToken(ctx, organization, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"token":      token.Token,
			"expires_at": token.ExpiresAt.Format(time.RFC3339),
		}, nil
	case "org_jit_config":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		owner, _, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(owner, organization) {
			return nil, fmt.Errorf("%w: repository owner %q must match organization %q", errRepositoryOrganizationMismatch, owner, organization)
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		runnerGroup := stringArg(args, "runner_group")
		if err := m.requireAllowedRunnerGroup(runnerGroup); err != nil {
			return nil, err
		}
		labels, err := stringListArg(args["labels"])
		if err != nil {
			return nil, fmt.Errorf("labels: %w", err)
		}
		runnerName := stringArg(args, "runner_name")
		workflow := stringArg(args, "workflow")
		ref := stringArg(args, "ref")
		if err := validateJITRunnerIdentity(workflow, ref, runnerName, runnerGroup, labels); err != nil {
			return nil, err
		}
		preflight, err := m.client.PreflightOrg(ctx, GitHubRunnerProviderPreflightRequest{
			Organization: organization,
			Repository:   repository,
			Workflow:     workflow,
			Ref:          ref,
			RunnerName:   runnerName,
			RunnerGroup:  runnerGroup,
			Labels:       labels,
		}, m.config.Token)
		if err != nil {
			return nil, err
		}
		if !preflight.ActionsEnabled || !preflight.SelfHostedAllowed || preflight.RunnerGroupID <= 0 || strings.TrimSpace(preflight.ResolvedWorkflowPath) == "" || !isFullGitSHA(preflight.ResolvedRefSHA) || len(preflight.ConflictingLabels) > 0 {
			return nil, fmt.Errorf("organization runner JIT preflight rejected: actions_enabled=%t self_hosted_allowed=%t runner_group_id=%d conflicts=%d", preflight.ActionsEnabled, preflight.SelfHostedAllowed, preflight.RunnerGroupID, len(preflight.ConflictingLabels))
		}
		if err := m.ensureJITOwnershipJournalWritable(); err != nil {
			return nil, fmt.Errorf("verify JIT ownership journal: %w", err)
		}
		config, err := m.client.GenerateOrgJITConfig(ctx, GitHubRunnerJITConfigRequest{
			Organization:  organization,
			RunnerName:    runnerName,
			RunnerGroupID: preflight.RunnerGroupID,
			Labels:        labels,
		}, m.config.Token)
		if err != nil {
			if config.RunnerID > 0 {
				return nil, errors.Join(err, m.trackJITCleanupOnly(organization, config.RunnerID))
			}
			return nil, err
		}
		ownershipToken, err := m.trackPendingJIT(organization, config.RunnerID)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"runner_id":          config.RunnerID,
			"runner_name":        config.RunnerName,
			"encoded_jit_config": config.EncodedJITConfig,
			"ownership_token":    ownershipToken,
			"preflight":          preflightMap(preflight),
		}, nil
	case "ack_org_jit_config":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		runnerID, err := int64Arg(args, "runner_id")
		if err != nil {
			return nil, err
		}
		if err := m.acknowledgePendingJIT(organization, runnerID, stringArg(args, "ownership_token")); err != nil {
			return nil, err
		}
		return map[string]any{"acknowledged": true}, nil
	case "remove_runner":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		runnerID, err := int64Arg(args, "runner_id")
		if err != nil {
			return nil, err
		}
		if err := m.client.RemoveRunner(ctx, owner, repo, runnerID, m.config.Token); err != nil {
			return nil, err
		}
		return map[string]any{"removed": true}, nil
	case "remove_org_runner":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		runnerID, err := int64Arg(args, "runner_id")
		if err != nil {
			return nil, err
		}
		if err := m.requirePendingJITOwnership(organization, runnerID); err != nil {
			return nil, err
		}
		if err := m.client.RemoveOrgRunner(ctx, organization, runnerID, m.config.Token); err != nil {
			return nil, err
		}
		if err := m.forgetPendingJIT(organization, runnerID); err != nil {
			return nil, fmt.Errorf("persist removed JIT runner ownership: %w", err)
		}
		return map[string]any{"removed": true}, nil
	case "org_runner":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		runnerID, err := int64Arg(args, "runner_id")
		if err != nil {
			return nil, err
		}
		runner, err := m.client.GetOrgRunner(ctx, organization, runnerID, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{"id": runner.ID, "name": runner.Name, "status": runner.Status, "busy": runner.Busy, "labels": runner.Labels}, nil
	case "preflight":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		owner, _, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if !strings.EqualFold(owner, organization) {
			return nil, fmt.Errorf("%w: repository owner %q must match organization %q", errRepositoryOrganizationMismatch, owner, organization)
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		runnerGroup, _ := args["runner_group"].(string)
		runnerGroup = strings.TrimSpace(runnerGroup)
		if err := m.requireAllowedRunnerGroup(runnerGroup); err != nil {
			return nil, err
		}
		labels, err := stringListArg(args["labels"])
		if err != nil {
			return nil, fmt.Errorf("labels: %w", err)
		}
		preflight, err := m.client.PreflightOrg(ctx, GitHubRunnerProviderPreflightRequest{
			Organization: organization,
			Repository:   repository,
			Workflow:     stringArg(args, "workflow"),
			Ref:          stringArg(args, "ref"),
			RunnerName:   stringArg(args, "runner_name"),
			RunnerGroup:  runnerGroup,
			Labels:       labels,
		}, m.config.Token)
		if err != nil {
			return nil, err
		}
		return preflightMap(preflight), nil
	case "dispatch_workflow":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		workflow := stringArg(args, "workflow")
		if workflow == "" {
			return nil, errors.New("workflow is required")
		}
		ref := stringArg(args, "ref")
		inputs, err := stringMapArg(args["inputs"])
		if err != nil {
			return nil, fmt.Errorf("inputs: %w", err)
		}
		dispatch, err := m.client.DispatchWorkflow(ctx, owner, repo, workflow, ref, inputs, stringArg(args, "expected_workflow_path"), stringArg(args, "expected_head_sha"), m.config.Token)
		if err != nil && (!errors.Is(err, errWorkflowDispatchVerificationUncertain) || dispatch.WorkflowRunID <= 0) {
			return nil, err
		}
		return map[string]any{
			"workflow_run_id":     dispatch.WorkflowRunID,
			"run_url":             dispatch.RunURL,
			"html_url":            dispatch.HTMLURL,
			"head_sha":            dispatch.ValidatedHeadSHA,
			"verification_status": dispatch.Verification,
		}, nil
	case "workflow_runs":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		workflow := stringArg(args, "workflow")
		if workflow == "" {
			return nil, errors.New("workflow is required")
		}
		createdAfter, err := timeArg(args, "created_after")
		if err != nil {
			return nil, err
		}
		runs, err := m.client.ListWorkflowRuns(ctx, owner, repo, workflow, createdAfter, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{"workflow_runs": runs}, nil
	case "workflow_run":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		runID, err := int64Arg(args, "run_id")
		if err != nil {
			return nil, err
		}
		run, err := m.client.GetWorkflowRun(ctx, owner, repo, runID, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"id":          run.ID,
			"status":      run.Status,
			"conclusion":  run.Conclusion,
			"path":        run.Path,
			"head_branch": run.HeadBranch,
			"head_sha":    run.HeadSHA,
			"html_url":    run.HTMLURL,
			"created_at":  run.CreatedAt,
			"updated_at":  run.UpdatedAt,
		}, nil
	case "workflow_run_jobs":
		owner, repo, repository, err := repositoryArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedRepository(repository); err != nil {
			return nil, err
		}
		runID, err := int64Arg(args, "run_id")
		if err != nil {
			return nil, err
		}
		if runID <= 0 {
			return nil, errors.New("run_id must be positive")
		}
		jobs, err := m.client.ListWorkflowRunJobs(ctx, owner, repo, runID, m.config.Token)
		if err != nil {
			return nil, err
		}
		return map[string]any{"jobs": jobs}, nil
	case "ephemeral_runner_job":
		if err := validateEphemeralRunnerJobArgs(args); err != nil {
			return nil, err
		}
		req, err := ephemeralRunnerJobRequestFromArgs(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(req.Organization); err != nil {
			return nil, err
		}
		spec, err := BuildEphemeralRunnerJobSpec(req)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"runner_name":  spec.RunnerName,
			"labels":       spec.Labels,
			"runner_group": spec.RunnerGroup,
		}, nil
	default:
		return nil, fmt.Errorf("unknown github runner provider method %q", method)
	}
}

func validateEphemeralRunnerJobArgs(args map[string]any) error {
	input := make(map[string]any, len(args))
	for key, value := range args {
		if key != "provider_token" {
			input[key] = value
		}
	}
	data, err := json.Marshal(input)
	if err != nil {
		return fmt.Errorf("encode ephemeral runner input: %w", err)
	}
	if err := githubplugin.ValidateEphemeralRunnerJobInput(data); err != nil {
		return fmt.Errorf("validate ephemeral runner input schema: %w", err)
	}
	return nil
}

func (m *githubRunnerProviderModule) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", m.handleHealth)
	mux.HandleFunc("POST /v1/actions/runners/registration-token", m.handleRegistrationToken)
	mux.HandleFunc("DELETE /v1/actions/runners/{runner_id}", m.handleRemoveRunner)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/registration-token", m.handleOrgRegistrationToken)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/jitconfig", m.handleOrgJITConfig)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/{runner_id}/ack", m.handleOrgJITOwnershipACK)
	mux.HandleFunc("GET /v1/actions/orgs/{organization}/runners/{runner_id}", m.handleOrgRunner)
	mux.HandleFunc("DELETE /v1/actions/orgs/{organization}/runners/{runner_id}", m.handleRemoveOrgRunner)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/preflight", m.handleOrgPreflight)
	mux.HandleFunc("POST /v1/actions/repos/{owner}/{repo}/workflows/{workflow}/dispatches", m.handleDispatchWorkflow)
	mux.HandleFunc("GET /v1/actions/repos/{owner}/{repo}/workflows/{workflow}/runs", m.handleWorkflowRuns)
	mux.HandleFunc("GET /v1/actions/repos/{owner}/{repo}/actions/runs/{run_id}", m.handleWorkflowRun)
	mux.HandleFunc("GET /v1/actions/repos/{owner}/{repo}/actions/runs/{run_id}/jobs", m.handleWorkflowRunJobs)
	return mux
}

func (m *githubRunnerProviderModule) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeProviderResponse(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (m *githubRunnerProviderModule) handleRegistrationToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Repository string `json:"repository"`
	}
	if err := decodeProviderRequest(w, r, &req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "registration_token", map[string]any{
		"repository":     req.Repository,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusCreated, out)
}

func (m *githubRunnerProviderModule) handleRemoveRunner(w http.ResponseWriter, r *http.Request) {
	runnerID, err := strconv.ParseInt(r.PathValue("runner_id"), 10, 64)
	if err != nil || runnerID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("runner_id must be positive"))
		return
	}
	out, err := m.invokeMethod(r.Context(), "remove_runner", map[string]any{
		"repository":     r.URL.Query().Get("repository"),
		"runner_id":      runnerID,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusNoContent, out)
}

func (m *githubRunnerProviderModule) handleOrgRegistrationToken(w http.ResponseWriter, r *http.Request) {
	out, err := m.invokeMethod(r.Context(), "org_registration_token", map[string]any{
		"organization":   r.PathValue("organization"),
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusCreated, out)
}

func (m *githubRunnerProviderModule) handleOrgJITConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Repository  string   `json:"repository"`
		Workflow    string   `json:"workflow"`
		Ref         string   `json:"ref"`
		RunnerName  string   `json:"runner_name"`
		RunnerGroup string   `json:"runner_group"`
		Labels      []string `json:"labels"`
	}
	if err := decodeProviderRequest(w, r, &req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "org_jit_config", map[string]any{
		"organization":   r.PathValue("organization"),
		"repository":     req.Repository,
		"workflow":       req.Workflow,
		"ref":            req.Ref,
		"runner_name":    req.RunnerName,
		"runner_group":   req.RunnerGroup,
		"labels":         req.Labels,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusCreated, out)
}

func (m *githubRunnerProviderModule) handleOrgJITOwnershipACK(w http.ResponseWriter, r *http.Request) {
	runnerID, err := strconv.ParseInt(r.PathValue("runner_id"), 10, 64)
	if err != nil || runnerID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("runner_id must be positive"))
		return
	}
	var req struct {
		OwnershipToken string `json:"ownership_token"`
	}
	if err := decodeProviderRequest(w, r, &req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	_, err = m.invokeMethod(r.Context(), "ack_org_jit_config", map[string]any{
		"organization":    r.PathValue("organization"),
		"runner_id":       runnerID,
		"ownership_token": req.OwnershipToken,
		"provider_token":  bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusNoContent, nil)
}

func (m *githubRunnerProviderModule) handleRemoveOrgRunner(w http.ResponseWriter, r *http.Request) {
	runnerID, err := strconv.ParseInt(r.PathValue("runner_id"), 10, 64)
	if err != nil || runnerID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("runner_id must be positive"))
		return
	}
	out, err := m.invokeMethod(r.Context(), "remove_org_runner", map[string]any{
		"organization":   r.PathValue("organization"),
		"runner_id":      runnerID,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusNoContent, out)
}

func (m *githubRunnerProviderModule) handleOrgRunner(w http.ResponseWriter, r *http.Request) {
	runnerID, err := strconv.ParseInt(r.PathValue("runner_id"), 10, 64)
	if err != nil || runnerID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("runner_id must be positive"))
		return
	}
	out, err := m.invokeMethod(r.Context(), "org_runner", map[string]any{
		"organization":   r.PathValue("organization"),
		"runner_id":      runnerID,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func (m *githubRunnerProviderModule) handleOrgPreflight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Repository  string   `json:"repository"`
		Workflow    string   `json:"workflow"`
		Ref         string   `json:"ref"`
		RunnerName  string   `json:"runner_name"`
		RunnerGroup string   `json:"runner_group"`
		Labels      []string `json:"labels"`
	}
	if err := decodeProviderRequest(w, r, &req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "preflight", map[string]any{
		"organization":   r.PathValue("organization"),
		"repository":     req.Repository,
		"workflow":       req.Workflow,
		"ref":            req.Ref,
		"runner_name":    req.RunnerName,
		"runner_group":   req.RunnerGroup,
		"labels":         req.Labels,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func (m *githubRunnerProviderModule) handleDispatchWorkflow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Ref                  string            `json:"ref"`
		Inputs               map[string]string `json:"inputs,omitempty"`
		ExpectedWorkflowPath string            `json:"expected_workflow_path"`
		ExpectedHeadSHA      string            `json:"expected_head_sha"`
	}
	if err := decodeProviderRequest(w, r, &req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "dispatch_workflow", map[string]any{
		"repository":             r.PathValue("owner") + "/" + r.PathValue("repo"),
		"workflow":               r.PathValue("workflow"),
		"ref":                    req.Ref,
		"inputs":                 req.Inputs,
		"expected_workflow_path": req.ExpectedWorkflowPath,
		"expected_head_sha":      req.ExpectedHeadSHA,
		"provider_token":         bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func (m *githubRunnerProviderModule) handleWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	createdAfter, err := parseOptionalRFC3339Query(r.URL.Query().Get("created_after"), "created_after")
	if err != nil {
		writeProviderError(w, http.StatusBadRequest, err)
		return
	}
	owner := r.PathValue("owner")
	repo := r.PathValue("repo")
	out, err := m.invokeMethod(r.Context(), "workflow_runs", map[string]any{
		"repository":     owner + "/" + repo,
		"workflow":       r.PathValue("workflow"),
		"created_after":  createdAfter,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func (m *githubRunnerProviderModule) handleWorkflowRunJobs(w http.ResponseWriter, r *http.Request) {
	runID, err := strconv.ParseInt(r.PathValue("run_id"), 10, 64)
	if err != nil || runID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("run_id must be positive"))
		return
	}
	owner := r.PathValue("owner")
	repo := r.PathValue("repo")
	out, err := m.invokeMethod(r.Context(), "workflow_run_jobs", map[string]any{
		"repository":     owner + "/" + repo,
		"run_id":         runID,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func (m *githubRunnerProviderModule) handleWorkflowRun(w http.ResponseWriter, r *http.Request) {
	runID, err := strconv.ParseInt(r.PathValue("run_id"), 10, 64)
	if err != nil || runID <= 0 {
		writeProviderError(w, http.StatusBadRequest, errors.New("run_id must be positive"))
		return
	}
	out, err := m.invokeMethod(r.Context(), "workflow_run", map[string]any{
		"repository":     r.PathValue("owner") + "/" + r.PathValue("repo"),
		"run_id":         runID,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusOK, out)
}

func decodeProviderRequest(w http.ResponseWriter, r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request must contain exactly one JSON value")
		}
		return fmt.Errorf("decode trailing request data: %w", err)
	}
	return nil
}

func parseOptionalRFC3339Query(value, name string) (time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s must be RFC3339", name)
	}
	return parsed, nil
}

func bearerToken(r *http.Request) string {
	value := r.Header.Get("Authorization")
	token, ok := strings.CutPrefix(value, "Bearer ")
	if !ok {
		return ""
	}
	return token
}

func providerErrorStatus(err error) int {
	message := err.Error()
	switch {
	case strings.Contains(message, "provider token"):
		return http.StatusUnauthorized
	case errors.Is(err, errRepositoryNotAllowlisted), errors.Is(err, errOrganizationNotAllowlisted), errors.Is(err, errRunnerGroupNotAllowlisted):
		return http.StatusForbidden
	case errors.Is(err, errRepositoryOrganizationMismatch):
		return http.StatusBadRequest
	case errors.Is(err, errInvalidJITIdentity):
		return http.StatusBadRequest
	case errors.Is(err, errJITOwnershipNotFound):
		return http.StatusNotFound
	case errors.Is(err, errJITOwnershipTokenInvalid):
		return http.StatusUnauthorized
	case message == "repository must be owner/name",
		message == "repository contains invalid characters",
		message == "organization is required",
		message == "organization contains invalid characters",
		message == "runner_name is required",
		message == "workflow is required",
		message == "runner_id is required",
		message == "runner_id must be an integer",
		message == "runner_id must be positive",
		message == "run_id is required",
		message == "run_id must be an integer",
		message == "created_after must be RFC3339":
		return http.StatusBadRequest
	default:
		return http.StatusBadGateway
	}
}

func writeProviderError(w http.ResponseWriter, status int, err error) {
	writeProviderResponse(w, status, map[string]any{"error": err.Error()})
}

func writeProviderResponse(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status == http.StatusNoContent {
		return
	}
	_ = json.NewEncoder(w).Encode(value)
}

func (m *githubRunnerProviderModule) authorizeProvider(args map[string]any) error {
	got, _ := args["provider_token"].(string)
	if got == "" || !constantTimeEqual(got, m.config.ProviderToken) {
		return errors.New("provider token is invalid")
	}
	return nil
}

func (m *githubRunnerProviderModule) requireAllowedRepository(repository string) error {
	if len(m.config.Repositories) == 0 {
		return fmt.Errorf("%w: config.repositories is required for repository-scoped runner operations", errRepositoryNotAllowlisted)
	}
	if _, ok := m.config.Repositories[canonicalRepository(repository)]; !ok {
		return fmt.Errorf("%w: %s", errRepositoryNotAllowlisted, repository)
	}
	return nil
}

func (m *githubRunnerProviderModule) requireAllowedOrganization(organization string) error {
	if len(m.config.Organizations) == 0 {
		return fmt.Errorf("%w: config.organizations is required for organization-scoped runner operations", errOrganizationNotAllowlisted)
	}
	if _, ok := m.config.Organizations[canonicalOrganization(organization)]; !ok {
		return fmt.Errorf("%w: %s", errOrganizationNotAllowlisted, organization)
	}
	return nil
}

func (m *githubRunnerProviderModule) requireAllowedRunnerGroup(runnerGroup string) error {
	runnerGroup = strings.TrimSpace(runnerGroup)
	if len(m.config.RunnerGroups) == 0 {
		return nil
	}
	if runnerGroup == "" {
		return fmt.Errorf("%w: runner_group is required when config.runner_groups is set", errRunnerGroupNotAllowlisted)
	}
	if _, ok := m.config.RunnerGroups[strings.ToLower(runnerGroup)]; !ok {
		return fmt.Errorf("%w: %s", errRunnerGroupNotAllowlisted, runnerGroup)
	}
	return nil
}

func constantTimeEqual(a, b string) bool {
	aHash := sha256.Sum256([]byte(a))
	bHash := sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aHash[:], bHash[:]) == 1
}

func (m *githubRunnerProviderModule) requirePendingJITOwnership(organization string, runnerID int64) error {
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: runnerID}
	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	if m.pendingJIT[key] == nil {
		return errJITOwnershipNotFound
	}
	return nil
}

func (m *githubRunnerProviderModule) trackPendingJIT(organization string, runnerID int64) (string, error) {
	if runnerID <= 0 {
		return "", errors.New("runner_id must be positive")
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", fmt.Errorf("generate JIT runner ownership token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: runnerID}
	pending := &pendingJITOwnership{
		organization: organization,
		tokenHash:    sha256.Sum256([]byte(token)),
		expiresAt:    time.Now().UTC().Add(m.effectiveJITOwnershipTTL()),
	}

	m.pendingJITMu.Lock()
	previous := m.pendingJIT[key]
	m.pendingJIT[key] = pending
	if persistErr := m.persistJITOwnershipJournalLocked(); persistErr != nil && !pending.cleanupWithoutJournal {
		pending.deleting = true
		pending.cleanupWithoutJournal = true
		pending.tokenHash = [sha256.Size]byte{}
		pending.lastCleanupAt = time.Now().UTC()
		pending.lastCleanupStatus = "journal_failed"
		pending.expiresAt = time.Now().UTC()
		if previous != nil {
			m.stopJITOwnershipTimerLocked(previous)
		}
		m.pendingJITMu.Unlock()
		return "", errors.Join(persistErr, m.cleanupUnjournaledJIT(key, pending))
	}
	if previous != nil {
		m.stopJITOwnershipTimerLocked(previous)
	}
	m.scheduleJITOwnershipLocked(key, pending)
	m.pendingJITMu.Unlock()
	return token, nil
}

func (m *githubRunnerProviderModule) trackJITCleanupOnly(organization string, runnerID int64) error {
	if runnerID <= 0 {
		return errors.New("runner_id must be positive")
	}
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: runnerID}
	pending := &pendingJITOwnership{
		organization:      organization,
		deleting:          true,
		expiresAt:         time.Now().UTC(),
		lastCleanupAt:     time.Now().UTC(),
		lastCleanupStatus: "cleanup_pending",
	}
	m.pendingJITMu.Lock()
	previous := m.pendingJIT[key]
	m.pendingJIT[key] = pending
	persistErr := m.persistJITOwnershipJournalLocked()
	if persistErr != nil {
		pending.cleanupWithoutJournal = true
	}
	if previous != nil {
		m.stopJITOwnershipTimerLocked(previous)
	}
	m.pendingJITMu.Unlock()
	if persistErr != nil {
		return errors.Join(persistErr, m.cleanupUnjournaledJIT(key, pending))
	}
	m.pendingJITMu.Lock()
	if m.pendingJIT[key] == pending {
		m.scheduleJITOwnershipLocked(key, pending)
	}
	m.pendingJITMu.Unlock()
	return nil
}

func (m *githubRunnerProviderModule) cleanupUnjournaledJIT(key pendingJITKey, pending *pendingJITOwnership) error {
	cleanupCtx, cancelCleanup := context.WithTimeout(m.cleanupContext, 30*time.Second)
	cleanupErr := m.client.RemoveOrgRunner(cleanupCtx, pending.organization, key.runnerID, m.config.Token)
	cancelCleanup()

	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	if m.pendingJIT[key] != pending {
		return cleanupErr
	}
	if cleanupErr == nil {
		delete(m.pendingJIT, key)
		return m.persistJITOwnershipJournalLocked()
	}
	pending.cleanupAttempts++
	pending.lastCleanupAt = time.Now().UTC()
	pending.lastCleanupStatus = "remove_failed"
	pending.expiresAt = time.Now().UTC().Add(m.effectiveJITRetryTTL())
	m.scheduleJITOwnershipLocked(key, pending)
	return cleanupErr
}

func (m *githubRunnerProviderModule) acknowledgePendingJIT(organization string, runnerID int64, token string) error {
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: runnerID}
	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	pending := m.pendingJIT[key]
	if pending == nil || pending.acknowledged || pending.deleting {
		return errJITOwnershipNotFound
	}
	gotHash := sha256.Sum256([]byte(token))
	if token == "" || subtle.ConstantTimeCompare(gotHash[:], pending.tokenHash[:]) != 1 {
		return errJITOwnershipTokenInvalid
	}
	previousHash := pending.tokenHash
	previousExpiry := pending.expiresAt
	pending.acknowledged = true
	pending.tokenHash = [sha256.Size]byte{}
	pending.expiresAt = time.Now().UTC().Add(m.effectiveJITOwnedTTL())
	persistErr := m.persistJITOwnershipJournalLocked()
	if persistErr != nil && !pending.cleanupWithoutJournal && !errors.Is(persistErr, errJITJournalDurabilityUncertain) {
		pending.acknowledged = false
		pending.tokenHash = previousHash
		pending.expiresAt = previousExpiry
		return persistErr
	}
	m.stopJITOwnershipTimerLocked(pending)
	m.scheduleJITOwnershipLocked(key, pending)
	return persistErr
}

func (m *githubRunnerProviderModule) forgetPendingJIT(organization string, runnerID int64) error {
	key := pendingJITKey{organization: canonicalOrganization(organization), runnerID: runnerID}
	m.pendingJITMu.Lock()
	pending := m.pendingJIT[key]
	delete(m.pendingJIT, key)
	if err := m.persistJITOwnershipJournalLocked(); err != nil {
		if errors.Is(err, errJITJournalDurabilityUncertain) {
			if pending != nil && pending.timer != nil {
				m.stopJITOwnershipTimerLocked(pending)
			}
			m.pendingJITMu.Unlock()
			return err
		}
		if pending != nil {
			m.pendingJIT[key] = pending
		}
		m.pendingJITMu.Unlock()
		return err
	}
	if pending != nil && pending.timer != nil {
		m.stopJITOwnershipTimerLocked(pending)
	}
	m.pendingJITMu.Unlock()
	return nil
}

func (m *githubRunnerProviderModule) expirePendingJIT(key pendingJITKey, pending *pendingJITOwnership, generation uint64) {
	m.pendingJITMu.Lock()
	if m.pendingJIT[key] != pending || m.stopped || pending.timerGeneration != generation {
		m.pendingJITMu.Unlock()
		return
	}
	pending.timer = nil
	previousDeleting := pending.deleting
	previousHash := pending.tokenHash
	pending.deleting = true
	pending.tokenHash = [sha256.Size]byte{}
	if err := m.persistJITOwnershipJournalLocked(); err != nil && !pending.cleanupWithoutJournal && !errors.Is(err, errJITJournalDurabilityUncertain) {
		pending.deleting = previousDeleting
		pending.tokenHash = previousHash
		pending.lastCleanupAt = time.Now().UTC()
		pending.lastCleanupStatus = "journal_failed"
		pending.expiresAt = time.Now().UTC().Add(m.effectiveJITRetryTTL())
		m.scheduleJITOwnershipLocked(key, pending)
		m.pendingJITMu.Unlock()
		return
	}
	cleanupContext := m.cleanupContext
	m.pendingJITMu.Unlock()

	var cleanupErr error
	for attempt := 0; attempt < jitOwnershipCleanupAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(cleanupContext, 30*time.Second)
		cleanupErr = m.client.RemoveOrgRunner(ctx, pending.organization, key.runnerID, m.config.Token)
		cancel()
		if cleanupErr == nil {
			break
		}
		if attempt+1 < jitOwnershipCleanupAttempts {
			timer := time.NewTimer(jitOwnershipCleanupRetryInterval)
			select {
			case <-timer.C:
			case <-cleanupContext.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				cleanupErr = errors.Join(cleanupErr, cleanupContext.Err())
				attempt = jitOwnershipCleanupAttempts - 1
			}
		}
	}
	m.pendingJITMu.Lock()
	defer m.pendingJITMu.Unlock()
	if m.pendingJIT[key] != pending {
		return
	}
	if cleanupErr == nil {
		delete(m.pendingJIT, key)
		if err := m.persistJITOwnershipJournalLocked(); err == nil || errors.Is(err, errJITJournalDurabilityUncertain) {
			return
		}
		m.pendingJIT[key] = pending
	}
	pending.cleanupAttempts += jitOwnershipCleanupAttempts
	pending.lastCleanupAt = time.Now().UTC()
	pending.lastCleanupStatus = "remove_failed"
	pending.expiresAt = time.Now().UTC().Add(m.effectiveJITRetryTTL())
	if err := m.persistJITOwnershipJournalLocked(); err != nil {
		pending.lastCleanupStatus = "remove_failed_journal_failed"
	}
	m.scheduleJITOwnershipLocked(key, pending)
}

func validateJITRunnerIdentity(workflow, ref, runnerName, runnerGroup string, labels []string) error {
	if strings.TrimSpace(workflow) == "" || strings.TrimSpace(ref) == "" || strings.TrimSpace(runnerName) == "" || strings.TrimSpace(runnerGroup) == "" {
		return fmt.Errorf("%w: workflow, ref, runner_name, and runner_group are required", errInvalidJITIdentity)
	}
	remainder, ok := strings.CutPrefix(runnerName, "wfc-")
	if !ok {
		return fmt.Errorf("%w: runner_name must use the wfc environment prefix", errInvalidJITIdentity)
	}
	environment, suffix, ok := strings.Cut(remainder, "-ghp-linux-")
	if !ok || environment == "" || suffix == "" {
		return fmt.Errorf("%w: runner_name must identify an environment-scoped Linux GitHub provider runner", errInvalidJITIdentity)
	}
	expected := map[string]struct{}{
		"self-hosted":            {},
		"linux":                  {},
		runnerName:               {},
		"wfc-ghp-" + environment: {},
		"wfc-ghp-ephemeral":      {},
	}
	if len(labels) != len(expected) {
		return fmt.Errorf("%w: labels must be the exact provider-owned runner label set", errInvalidJITIdentity)
	}
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if _, ok := expected[label]; !ok {
			return fmt.Errorf("%w: label %q is not provider-owned", errInvalidJITIdentity, label)
		}
		if _, duplicated := seen[label]; duplicated {
			return fmt.Errorf("%w: label %q is duplicated", errInvalidJITIdentity, label)
		}
		seen[label] = struct{}{}
	}
	return nil
}

func rejectUnknownConfig(raw map[string]any, allowed ...string) error {
	known := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		known[key] = struct{}{}
	}
	for key := range raw {
		if _, ok := known[key]; !ok {
			return fmt.Errorf("unknown config key %q", key)
		}
	}
	return nil
}

func parseRunnerProviderRepositories(value any) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	add := func(repository string) error {
		_, _, normalized, err := parseRepository(repository)
		if err != nil {
			return err
		}
		out[normalized] = struct{}{}
		return nil
	}
	switch v := value.(type) {
	case nil:
		return out, nil
	case []any:
		for _, item := range v {
			repository, ok := item.(string)
			if !ok {
				return nil, errors.New("config.repositories entries must be strings")
			}
			if err := add(repository); err != nil {
				return nil, err
			}
		}
	case []string:
		for _, repository := range v {
			if err := add(repository); err != nil {
				return nil, err
			}
		}
	case string:
		for part := range strings.SplitSeq(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if err := add(part); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.New("config.repositories must be a string list")
	}
	return out, nil
}

func parseRunnerProviderOrganizations(value any) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	add := func(organization string) error {
		normalized, err := parseOrganization(organization)
		if err != nil {
			return err
		}
		out[canonicalOrganization(normalized)] = struct{}{}
		return nil
	}
	switch v := value.(type) {
	case nil:
		return out, nil
	case []any:
		for _, item := range v {
			organization, ok := item.(string)
			if !ok {
				return nil, errors.New("config.organizations entries must be strings")
			}
			if err := add(organization); err != nil {
				return nil, err
			}
		}
	case []string:
		for _, organization := range v {
			if err := add(organization); err != nil {
				return nil, err
			}
		}
	case string:
		for part := range strings.SplitSeq(v, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			if err := add(part); err != nil {
				return nil, err
			}
		}
	default:
		return nil, errors.New("config.organizations must be a string list")
	}
	return out, nil
}

func parseRunnerProviderStringSet(value any, name string) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	add := func(item string) error {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil
		}
		if strings.ContainsAny(item, "\t\r\n") || strings.Contains(item, "..") {
			return fmt.Errorf("%s contains invalid characters", name)
		}
		out[strings.ToLower(item)] = struct{}{}
		return nil
	}
	switch v := value.(type) {
	case nil:
		return out, nil
	case []any:
		for _, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("%s entries must be strings", name)
			}
			if err := add(text); err != nil {
				return nil, err
			}
		}
	case []string:
		for _, item := range v {
			if err := add(item); err != nil {
				return nil, err
			}
		}
	case string:
		for part := range strings.SplitSeq(v, ",") {
			if err := add(part); err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf("%s must be a string list", name)
	}
	return out, nil
}

func repositoryArg(args map[string]any) (string, string, string, error) {
	repository, _ := args["repository"].(string)
	owner, repo, normalized, err := parseRepository(repository)
	if err != nil {
		return "", "", "", err
	}
	return owner, repo, normalized, nil
}

func organizationArg(args map[string]any) (string, error) {
	organization, _ := args["organization"].(string)
	return parseOrganization(organization)
}

func parseOrganization(organization string) (string, error) {
	organization = strings.TrimSpace(organization)
	if organization == "" {
		return "", errors.New("organization is required")
	}
	if strings.Contains(organization, "/") || strings.ContainsAny(organization, " \t\r\n") || strings.Contains(organization, "..") {
		return "", errors.New("organization contains invalid characters")
	}
	return organization, nil
}

func canonicalOrganization(organization string) string {
	return strings.ToLower(strings.TrimSpace(organization))
}

func parseRepository(repository string) (string, string, string, error) {
	repository = strings.TrimSpace(repository)
	owner, repo, ok := strings.Cut(repository, "/")
	if !ok || owner == "" || repo == "" || strings.Contains(repo, "/") {
		return "", "", "", errors.New("repository must be owner/name")
	}
	if strings.ContainsAny(repository, " \t\r\n") || strings.Contains(repository, "..") {
		return "", "", "", errors.New("repository contains invalid characters")
	}
	return owner, repo, canonicalRepository(repository), nil
}

func canonicalRepository(repository string) string {
	owner, repo, _ := strings.Cut(repository, "/")
	return strings.ToLower(owner) + "/" + strings.ToLower(repo)
}

func stringListArg(value any) ([]string, error) {
	var out []string
	switch v := value.(type) {
	case []string:
		out = append(out, v...)
	case []any:
		for index, item := range v {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("value at index %d must be a string", index)
			}
			out = append(out, text)
		}
	case string:
		for part := range strings.SplitSeq(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	case nil:
	default:
		return nil, fmt.Errorf("value must be a string or string array")
	}
	return out, nil
}

func preflightMap(preflight GitHubRunnerProviderPreflight) map[string]any {
	return map[string]any{
		"organization":              preflight.Organization,
		"runner_group":              preflight.RunnerGroup,
		"runner_group_id":           preflight.RunnerGroupID,
		"ref":                       preflight.Ref,
		"resolved_workflow_path":    preflight.ResolvedWorkflowPath,
		"resolved_ref_sha":          preflight.ResolvedRefSHA,
		"existing_labels":           preflight.ExistingLabels,
		"conflicting_labels":        preflight.ConflictingLabels,
		"labels_observed":           preflight.LabelsObserved,
		"existing_labels_truncated": preflight.ExistingLabelsTruncated,
		"runner_count_checked":      preflight.RunnerCountChecked,
		"actions_enabled":           preflight.ActionsEnabled,
		"self_hosted_allowed":       preflight.SelfHostedAllowed,
	}
}

func ephemeralRunnerJobRequestFromArgs(args map[string]any) (EphemeralRunnerJobRequest, error) {
	organization, err := organizationArg(args)
	if err != nil {
		return EphemeralRunnerJobRequest{}, err
	}
	requiredRuntimeCaps, err := stringListArg(args["required_runtime_caps"])
	if err != nil {
		return EphemeralRunnerJobRequest{}, fmt.Errorf("required_runtime_caps: %w", err)
	}
	workflowInputs, err := stringMapArg(args["workflow_inputs"])
	if err != nil {
		return EphemeralRunnerJobRequest{}, fmt.Errorf("workflow_inputs: %w", err)
	}
	return EphemeralRunnerJobRequest{
		Mode:                EphemeralRunnerJobMode(stringArg(args, "mode")),
		Environment:         stringArg(args, "environment"),
		OS:                  stringArg(args, "os"),
		WorkerID:            stringArg(args, "worker_id"),
		TaskID:              stringArg(args, "task_id"),
		Organization:        organization,
		Repository:          stringArg(args, "repository"),
		Workflow:            stringArg(args, "workflow"),
		Ref:                 stringArg(args, "ref"),
		WorkflowInputs:      workflowInputs,
		RunnerGroup:         stringArg(args, "runner_group"),
		RequiredRuntimeCaps: requiredRuntimeCaps,
	}, nil
}

func stringArg(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return strings.TrimSpace(value)
}

func stringMapArg(value any) (map[string]string, error) {
	add := func(out map[string]string, key, val string) error {
		originalKey := key
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return errors.New("input key must not be empty")
		}
		if _, exists := out[key]; exists {
			return fmt.Errorf("canonical input key collision for %q", originalKey)
		}
		out[key] = val
		return nil
	}
	switch v := value.(type) {
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, val := range v {
			if err := add(out, key, val); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, val := range v {
			str, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("input %q must be a string", key)
			}
			if err := add(out, key, str); err != nil {
				return nil, err
			}
		}
		return out, nil
	case nil:
		return nil, nil
	default:
		return nil, errors.New("value must be an object with string values")
	}
}

func int64Arg(args map[string]any, key string) (int64, error) {
	switch v := args[key].(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if v != float64(int64(v)) {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return int64(v), nil
	case json.Number:
		value, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s must be an integer", key)
		}
		return value, nil
	default:
		return 0, fmt.Errorf("%s is required", key)
	}
}

func timeArg(args map[string]any, key string) (time.Time, error) {
	switch v := args[key].(type) {
	case nil:
		return time.Time{}, nil
	case time.Time:
		return v, nil
	case string:
		return parseOptionalRFC3339Query(v, key)
	default:
		return time.Time{}, fmt.Errorf("%s must be RFC3339", key)
	}
}

var _ sdk.ModuleInstance = (*githubRunnerProviderModule)(nil)
var _ sdk.ServiceInvoker = (*githubRunnerProviderModule)(nil)
