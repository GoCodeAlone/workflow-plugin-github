package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

const defaultGitHubAPIBaseURL = "https://api.github.com"

var errRepositoryNotAllowlisted = errors.New("repository is not allowlisted")
var errOrganizationNotAllowlisted = errors.New("organization is not allowlisted")
var errRunnerGroupNotAllowlisted = errors.New("runner group is not allowlisted")

type GitHubRunnerClient interface {
	RegistrationToken(ctx context.Context, owner, repo, token string) (GitHubRunnerRegistrationToken, error)
	RemoveRunner(ctx context.Context, owner, repo string, runnerID int64, token string) error
	OrgRegistrationToken(ctx context.Context, organization, token string) (GitHubRunnerRegistrationToken, error)
	RemoveOrgRunner(ctx context.Context, organization string, runnerID int64, token string) error
	PreflightOrg(ctx context.Context, req GitHubRunnerProviderPreflightRequest, token string) (GitHubRunnerProviderPreflight, error)
	DispatchWorkflow(ctx context.Context, owner, repo, workflow, ref, token string) error
}

type GitHubRunnerRegistrationToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

type GitHubRunnerProviderPreflightRequest struct {
	Organization string
	RunnerGroup  string
	Labels       []string
}

type GitHubRunnerProviderPreflight struct {
	Organization       string   `json:"organization"`
	RunnerGroup        string   `json:"runner_group,omitempty"`
	ExistingLabels     []string `json:"existing_labels,omitempty"`
	ConflictingLabels  []string `json:"conflicting_labels,omitempty"`
	RunnerCountChecked int      `json:"runner_count_checked,omitempty"`
	ActionsEnabled     bool     `json:"actions_enabled"`
	SelfHostedAllowed  bool     `json:"self_hosted_allowed"`
}

type httpGitHubRunnerClient struct {
	baseURL    string
	httpClient *http.Client
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
	return c.do(ctx, http.MethodDelete, endpoint, nil, token, http.StatusNoContent, nil)
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

func (c *httpGitHubRunnerClient) RemoveOrgRunner(ctx context.Context, organization string, runnerID int64, token string) error {
	if runnerID <= 0 {
		return errors.New("runner_id must be positive")
	}
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners/%d", c.baseURL, url.PathEscape(organization), runnerID)
	return c.do(ctx, http.MethodDelete, endpoint, nil, token, http.StatusNoContent, nil)
}

func (c *httpGitHubRunnerClient) PreflightOrg(ctx context.Context, req GitHubRunnerProviderPreflightRequest, token string) (GitHubRunnerProviderPreflight, error) {
	endpoint := fmt.Sprintf("%s/orgs/%s/actions/runners?per_page=100", c.baseURL, url.PathEscape(req.Organization))
	seen := map[string]string{}
	checked := 0
	runnerCount := 0
	for endpoint != "" {
		var out struct {
			TotalCount int `json:"total_count"`
			Runners    []struct {
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
			for _, label := range runner.Labels {
				name := strings.TrimSpace(label.Name)
				if name == "" {
					continue
				}
				seen[strings.ToLower(name)] = name
			}
		}
		endpoint = githubNextLink(headers.Get("Link"))
	}
	if checked == 0 {
		checked = runnerCount
	}
	existing := make([]string, 0, len(seen))
	for _, name := range seen {
		existing = append(existing, name)
	}
	conflicts := make([]string, 0)
	for _, label := range req.Labels {
		if existingName, ok := seen[strings.ToLower(strings.TrimSpace(label))]; ok {
			conflicts = append(conflicts, existingName)
		}
	}
	sort.Strings(existing)
	sort.Strings(conflicts)
	return GitHubRunnerProviderPreflight{
		Organization:       req.Organization,
		RunnerGroup:        req.RunnerGroup,
		ExistingLabels:     existing,
		ConflictingLabels:  conflicts,
		RunnerCountChecked: checked,
		ActionsEnabled:     true,
		SelfHostedAllowed:  true,
	}, nil
}

func (c *httpGitHubRunnerClient) DispatchWorkflow(ctx context.Context, owner, repo, workflow, ref, token string) error {
	if strings.TrimSpace(ref) == "" {
		ref = "main"
	}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/actions/workflows/%s/dispatches", c.baseURL, url.PathEscape(owner), url.PathEscape(repo), url.PathEscape(workflow))
	return c.do(ctx, http.MethodPost, endpoint, map[string]any{"ref": ref}, token, http.StatusNoContent, nil)
}

func (c *httpGitHubRunnerClient) do(ctx context.Context, method, endpoint string, body any, token string, wantStatus int, out any) error {
	_, err := c.doRaw(ctx, method, endpoint, body, token, wantStatus, out)
	return err
}

func (c *httpGitHubRunnerClient) doRaw(ctx context.Context, method, endpoint string, body any, token string, wantStatus int, out any) (http.Header, error) {
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
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
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
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		limited, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("github runner request returned %s: %s", resp.Status, strings.TrimSpace(string(limited)))
	}
	if out == nil {
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

type githubRunnerProviderModule struct {
	name   string
	config githubRunnerProviderConfig
	client GitHubRunnerClient
}

type githubRunnerProviderConfig struct {
	Token         string
	ProviderToken string
	APIBaseURL    string
	Repositories  map[string]struct{}
	Organizations map[string]struct{}
	RunnerGroups  map[string]struct{}
}

func newGitHubRunnerProviderModule(name string, raw map[string]any, client GitHubRunnerClient) (*githubRunnerProviderModule, error) {
	if err := rejectUnknownConfig(raw, "token", "provider_token", "api_base_url", "repositories", "organizations", "runner_groups"); err != nil {
		return nil, fmt.Errorf("github.runner_provider %q: %w", name, err)
	}
	cfg := githubRunnerProviderConfig{}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	if cfg.Token == "" {
		return nil, fmt.Errorf("github.runner_provider %q: config.token is required", name)
	}
	cfg.ProviderToken, _ = raw["provider_token"].(string)
	cfg.ProviderToken = os.ExpandEnv(cfg.ProviderToken)
	if cfg.ProviderToken == "" {
		return nil, fmt.Errorf("github.runner_provider %q: config.provider_token is required", name)
	}
	cfg.APIBaseURL, _ = raw["api_base_url"].(string)
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
	if client == nil {
		client = newHTTPGitHubRunnerClient(cfg.APIBaseURL)
	}
	return &githubRunnerProviderModule{name: name, config: cfg, client: client}, nil
}

func NewGitHubRunnerProviderHTTPHandler(name string, raw map[string]any) (http.Handler, error) {
	module, err := newGitHubRunnerProviderModule(name, raw, nil)
	if err != nil {
		return nil, err
	}
	return module.HTTPHandler(), nil
}

func (m *githubRunnerProviderModule) Init() error { return nil }

func (m *githubRunnerProviderModule) Start(_ context.Context) error { return nil }

func (m *githubRunnerProviderModule) Stop(_ context.Context) error { return nil }

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
		if err := m.client.RemoveOrgRunner(ctx, organization, runnerID, m.config.Token); err != nil {
			return nil, err
		}
		return map[string]any{"removed": true}, nil
	case "preflight":
		organization, err := organizationArg(args)
		if err != nil {
			return nil, err
		}
		if err := m.requireAllowedOrganization(organization); err != nil {
			return nil, err
		}
		runnerGroup, _ := args["runner_group"].(string)
		runnerGroup = strings.TrimSpace(runnerGroup)
		if err := m.requireAllowedRunnerGroup(runnerGroup); err != nil {
			return nil, err
		}
		preflight, err := m.client.PreflightOrg(ctx, GitHubRunnerProviderPreflightRequest{
			Organization: organization,
			RunnerGroup:  runnerGroup,
			Labels:       stringListArg(args["labels"]),
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
		if err := m.client.DispatchWorkflow(ctx, owner, repo, workflow, ref, m.config.Token); err != nil {
			return nil, err
		}
		return map[string]any{}, nil
	case "ephemeral_runner_job":
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

func (m *githubRunnerProviderModule) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", m.handleHealth)
	mux.HandleFunc("POST /v1/actions/runners/registration-token", m.handleRegistrationToken)
	mux.HandleFunc("DELETE /v1/actions/runners/{runner_id}", m.handleRemoveRunner)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/registration-token", m.handleOrgRegistrationToken)
	mux.HandleFunc("DELETE /v1/actions/orgs/{organization}/runners/{runner_id}", m.handleRemoveOrgRunner)
	mux.HandleFunc("POST /v1/actions/orgs/{organization}/runners/preflight", m.handleOrgPreflight)
	mux.HandleFunc("POST /v1/actions/repos/{owner}/{repo}/workflows/{workflow}/dispatches", m.handleDispatchWorkflow)
	return mux
}

func (m *githubRunnerProviderModule) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeProviderResponse(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (m *githubRunnerProviderModule) handleRegistrationToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Repository string `json:"repository"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
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

func (m *githubRunnerProviderModule) handleOrgPreflight(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RunnerGroup string   `json:"runner_group"`
		Labels      []string `json:"labels"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "preflight", map[string]any{
		"organization":   r.PathValue("organization"),
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
		Ref string `json:"ref"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeProviderError(w, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}
	out, err := m.invokeMethod(r.Context(), "dispatch_workflow", map[string]any{
		"repository":     r.PathValue("owner") + "/" + r.PathValue("repo"),
		"workflow":       r.PathValue("workflow"),
		"ref":            req.Ref,
		"provider_token": bearerToken(r),
	})
	if err != nil {
		writeProviderError(w, providerErrorStatus(err), err)
		return
	}
	writeProviderResponse(w, http.StatusNoContent, out)
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
	case strings.Contains(message, "repository"):
		return http.StatusBadRequest
	case strings.Contains(message, "organization"):
		return http.StatusBadRequest
	case strings.Contains(message, "runner group"):
		return http.StatusBadRequest
	case strings.Contains(message, "runner_id"):
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

func stringListArg(value any) []string {
	var out []string
	switch v := value.(type) {
	case []string:
		out = append(out, v...)
	case []any:
		for _, item := range v {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
	case string:
		for part := range strings.SplitSeq(v, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				out = append(out, part)
			}
		}
	}
	return out
}

func preflightMap(preflight GitHubRunnerProviderPreflight) map[string]any {
	return map[string]any{
		"organization":         preflight.Organization,
		"runner_group":         preflight.RunnerGroup,
		"existing_labels":      preflight.ExistingLabels,
		"conflicting_labels":   preflight.ConflictingLabels,
		"runner_count_checked": preflight.RunnerCountChecked,
		"actions_enabled":      preflight.ActionsEnabled,
		"self_hosted_allowed":  preflight.SelfHostedAllowed,
	}
}

func ephemeralRunnerJobRequestFromArgs(args map[string]any) (EphemeralRunnerJobRequest, error) {
	organization, err := organizationArg(args)
	if err != nil {
		return EphemeralRunnerJobRequest{}, err
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
		WorkflowInputs:      stringMapArg(args["workflow_inputs"]),
		RunnerGroup:         stringArg(args, "runner_group"),
		RequiredRuntimeCaps: stringListArg(args["required_runtime_caps"]),
		AdvertisedCaps:      stringListArg(args["advertised_caps"]),
	}, nil
}

func stringArg(args map[string]any, key string) string {
	value, _ := args[key].(string)
	return strings.TrimSpace(value)
}

func stringMapArg(value any) map[string]string {
	switch v := value.(type) {
	case map[string]string:
		out := make(map[string]string, len(v))
		for key, val := range v {
			key = strings.ToLower(strings.TrimSpace(key))
			if key != "" {
				out[key] = val
			}
		}
		return out
	case map[string]any:
		out := make(map[string]string, len(v))
		for key, val := range v {
			key = strings.ToLower(strings.TrimSpace(key))
			if key == "" {
				continue
			}
			str, ok := val.(string)
			if !ok {
				continue
			}
			out[key] = str
		}
		return out
	default:
		return nil
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

var _ sdk.ModuleInstance = (*githubRunnerProviderModule)(nil)
var _ sdk.ServiceInvoker = (*githubRunnerProviderModule)(nil)
