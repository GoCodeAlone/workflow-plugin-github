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
	"strconv"
	"strings"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

const defaultGitHubAPIBaseURL = "https://api.github.com"

var errRepositoryNotAllowlisted = errors.New("repository is not allowlisted")

type GitHubRunnerClient interface {
	RegistrationToken(ctx context.Context, owner, repo, token string) (GitHubRunnerRegistrationToken, error)
	RemoveRunner(ctx context.Context, owner, repo string, runnerID int64, token string) error
}

type GitHubRunnerRegistrationToken struct {
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
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

func (c *httpGitHubRunnerClient) do(ctx context.Context, method, endpoint string, body any, token string, wantStatus int, out any) error {
	if strings.TrimSpace(token) == "" {
		return errors.New("github token is required")
	}
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal github runner request: %w", err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return fmt.Errorf("build github runner request: %w", err)
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
		return fmt.Errorf("github runner request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		limited, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("github runner request returned %s: %s", resp.Status, strings.TrimSpace(string(limited)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode github runner response: %w", err)
	}
	return nil
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
}

func newGitHubRunnerProviderModule(name string, raw map[string]any, client GitHubRunnerClient) (*githubRunnerProviderModule, error) {
	if err := rejectUnknownConfig(raw, "token", "provider_token", "api_base_url", "repositories"); err != nil {
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
		return nil, fmt.Errorf("github.runner_provider %q: config.repositories requires at least one repository", name)
	}
	cfg.Repositories = repositories
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
	default:
		return nil, fmt.Errorf("unknown github runner provider method %q", method)
	}
}

func (m *githubRunnerProviderModule) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", m.handleHealth)
	mux.HandleFunc("POST /v1/actions/runners/registration-token", m.handleRegistrationToken)
	mux.HandleFunc("DELETE /v1/actions/runners/{runner_id}", m.handleRemoveRunner)
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
	case errors.Is(err, errRepositoryNotAllowlisted):
		return http.StatusForbidden
	case strings.Contains(message, "repository"):
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
		return nil
	}
	if _, ok := m.config.Repositories[canonicalRepository(repository)]; !ok {
		return fmt.Errorf("%w: %s", errRepositoryNotAllowlisted, repository)
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

func repositoryArg(args map[string]any) (string, string, string, error) {
	repository, _ := args["repository"].(string)
	owner, repo, normalized, err := parseRepository(repository)
	if err != nil {
		return "", "", "", err
	}
	return owner, repo, normalized, nil
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
