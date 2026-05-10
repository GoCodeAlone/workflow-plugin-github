package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestT41_GitHubRunnerClientMintsRegistrationToken(t *testing.T) {
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: got %q want POST", r.Method)
		}
		if r.URL.Path != "/repos/GoCodeAlone/workflow-compute/actions/runners/registration-token" {
			t.Fatalf("path: got %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		writeRunnerProviderJSON(t, w, http.StatusCreated, GitHubRunnerRegistrationToken{
			Token:     "runner-token",
			ExpiresAt: expiresAt,
		})
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	token, err := client.RegistrationToken(context.Background(), "GoCodeAlone", "workflow-compute", "github-token")
	if err != nil {
		t.Fatalf("registration token: %v", err)
	}
	if token.Token != "runner-token" || !token.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("token: got %+v", token)
	}
}

func TestT41_GitHubRunnerProviderModuleRejectsUnknownConfig(t *testing.T) {
	_, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":      "github-token",
		"unexpected": true,
	}, nil)
	if err == nil {
		t.Fatal("unknown config key must fail")
	}
	if !strings.Contains(err.Error(), "unknown config key") {
		t.Fatalf("error: got %q", err)
	}
}

func TestT41_GitHubRunnerProviderModuleInvokesRegistrationToken(t *testing.T) {
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	fake := &fakeRunnerClient{token: GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: expiresAt}}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"provider_token": "provider-token",
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("registration_token", map[string]any{
		"repository":     "GoCodeAlone/workflow-compute",
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("invoke registration_token: %v", err)
	}
	if result["token"] != "runner-token" {
		t.Fatalf("token output: got %+v", result)
	}
	if fake.registrationRepository != "GoCodeAlone/workflow-compute" {
		t.Fatalf("repository: got %q", fake.registrationRepository)
	}
}

func TestT41_GitHubRunnerProviderModuleRejectsWrongProviderToken(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	if _, err := module.InvokeMethod("registration_token", map[string]any{
		"repository":     "GoCodeAlone/workflow-compute",
		"provider_token": "wrong",
	}); err == nil {
		t.Fatal("wrong provider token must fail")
	}
}

func TestT41_GitHubRunnerProviderModuleRequiresRepositoryAllowlist(t *testing.T) {
	_, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
	}, &fakeRunnerClient{})
	if err == nil {
		t.Fatal("missing repository allowlist must fail")
	}
	if !strings.Contains(err.Error(), "config.repositories requires at least one repository") {
		t.Fatalf("error: got %q", err)
	}
}

func TestT41_GitHubRunnerProviderModuleServesComputeProviderHTTP(t *testing.T) {
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{token: GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: expiresAt}})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/runners/registration-token", strings.NewReader(`{"repository":"GoCodeAlone/workflow-compute"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: got %d", resp.StatusCode)
	}
	var got GitHubRunnerRegistrationToken
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.Token != "runner-token" || !got.ExpiresAt.Equal(expiresAt) {
		t.Fatalf("response: got %+v", got)
	}
}

func TestT41_GitHubRunnerProviderHTTPRejectsUnallowlistedRepository(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/runners/registration-token", strings.NewReader(`{"repository":"GoCodeAlone/other"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status: got %d want 403", resp.StatusCode)
	}
}

type fakeRunnerClient struct {
	token                  GitHubRunnerRegistrationToken
	registrationRepository string
	removedRepository      string
	removedRunnerID        int64
}

func (f *fakeRunnerClient) RegistrationToken(_ context.Context, owner, repo, _ string) (GitHubRunnerRegistrationToken, error) {
	f.registrationRepository = owner + "/" + repo
	return f.token, nil
}

func (f *fakeRunnerClient) RemoveRunner(_ context.Context, owner, repo string, runnerID int64, _ string) error {
	f.removedRepository = owner + "/" + repo
	f.removedRunnerID = runnerID
	return nil
}

func writeRunnerProviderJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
