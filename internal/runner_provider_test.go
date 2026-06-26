package internal

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	githubv1 "github.com/GoCodeAlone/workflow-plugin-github/gen"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func TestT593_GitHubRunnerClientPreflightChecksPaginatedOrgRunnerLabels(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method: got %q want GET", r.Method)
		}
		if r.URL.Path != "/orgs/GoCodeAlone/actions/runners" {
			t.Fatalf("path: got %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		switch r.URL.Query().Get("page") {
		case "":
			w.Header().Set("Link", `<`+serverURL+`/orgs/GoCodeAlone/actions/runners?per_page=100&page=2>; rel="next"`)
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"total_count": 2,
				"runners": []map[string]any{
					{"labels": []map[string]string{{"name": "linux"}}},
				},
			})
		case "2":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"total_count": 2,
				"runners": []map[string]any{
					{"labels": []map[string]string{{"name": "wfc-ghp-stg"}}},
				},
			})
		default:
			t.Fatalf("unexpected page: %q", r.URL.Query().Get("page"))
		}
	}))
	defer server.Close()
	serverURL = server.URL

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Labels:       []string{"wfc-ghp-stg"},
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if result.RunnerCountChecked != 2 {
		t.Fatalf("runner count: got %d", result.RunnerCountChecked)
	}
	if len(result.ConflictingLabels) != 1 || result.ConflictingLabels[0] != "wfc-ghp-stg" {
		t.Fatalf("conflicting labels: got %+v", result.ConflictingLabels)
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

func TestT44_GitHubRunnerProviderServesHealthz(t *testing.T) {
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

	resp, err := server.Client().Get(server.URL + "/healthz")
	if err != nil {
		t.Fatalf("health request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var got map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got["status"] != "ok" {
		t.Fatalf("health response: got %+v", got)
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

func TestT593_GitHubRunnerProviderModuleRejectsRepositoryTokenWithoutRepositoryAllowlist(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, &fakeRunnerClient{token: GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: time.Now().UTC().Add(time.Hour)}})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("registration_token", map[string]any{
		"repository":     "GoCodeAlone/workflow-compute",
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errRepositoryNotAllowlisted) {
		t.Fatalf("registration_token err: got %v want %v", err, errRepositoryNotAllowlisted)
	}
}

func TestT593_GitHubRunnerProviderModuleInvokesOrgRegistrationToken(t *testing.T) {
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	fake := &fakeRunnerClient{token: GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: expiresAt}}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("org_registration_token", map[string]any{
		"organization":   "GoCodeAlone",
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("invoke org_registration_token: %v", err)
	}
	if result["token"] != "runner-token" {
		t.Fatalf("token output: got %+v", result)
	}
	if fake.orgRegistrationOrganization != "GoCodeAlone" {
		t.Fatalf("organization: got %q", fake.orgRegistrationOrganization)
	}
}

func TestT593_GitHubRunnerProviderModuleRejectsOrgRegistrationWithoutOrgAllowlist(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{token: GitHubRunnerRegistrationToken{Token: "runner-token", ExpiresAt: time.Now().UTC().Add(time.Hour)}})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("org_registration_token", map[string]any{
		"organization":   "GoCodeAlone",
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errOrganizationNotAllowlisted) {
		t.Fatalf("org_registration_token err: got %v want %v", err, errOrganizationNotAllowlisted)
	}
}

func TestT593_GitHubRunnerProviderModulePreflightChecksOrgRunners(t *testing.T) {
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:       "GoCodeAlone",
			RunnerGroup:        "workflow-compute-stg",
			ExistingLabels:     []string{"self-hosted", "linux"},
			ConflictingLabels:  []string{"wfc-ghp-stg"},
			RunnerCountChecked: 2,
			ActionsEnabled:     true,
			SelfHostedAllowed:  true,
		},
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"runner_groups":  []any{"workflow-compute-stg"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("preflight", map[string]any{
		"organization":   "GoCodeAlone",
		"runner_group":   "workflow-compute-stg",
		"labels":         []any{"wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("invoke preflight: %v", err)
	}
	if result["organization"] != "GoCodeAlone" || result["runner_group"] != "workflow-compute-stg" {
		t.Fatalf("preflight output: got %+v", result)
	}
	if result["actions_enabled"] != true || result["self_hosted_allowed"] != true {
		t.Fatalf("preflight status output: got %+v", result)
	}
	if fake.preflightOrganization != "GoCodeAlone" {
		t.Fatalf("preflight organization: got %q", fake.preflightOrganization)
	}
}

func TestT593_GitHubRunnerProviderModuleRejectsMissingAllowlistedRunnerGroup(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"runner_groups":  []any{"workflow-compute-stg"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("preflight", map[string]any{
		"organization":   "GoCodeAlone",
		"labels":         []any{"wfc-ghp-stg"},
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errRunnerGroupNotAllowlisted) {
		t.Fatalf("preflight err: got %v want %v", err, errRunnerGroupNotAllowlisted)
	}
}

func TestT593_GitHubRunnerProviderModuleRemovesOrgRunner(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("remove_org_runner", map[string]any{
		"organization":   "GoCodeAlone",
		"runner_id":      int64(42),
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("invoke remove_org_runner: %v", err)
	}
	if result["removed"] != true {
		t.Fatalf("remove output: got %+v", result)
	}
}

func TestT593_RunnerProviderSchemaDeclaresOrgPreflight(t *testing.T) {
	schemas := (&githubPlugin{}).ModuleSchemas()
	runnerIndex := -1
	for i := range schemas {
		if schemas[i].Type == "github.runner_provider" {
			runnerIndex = i
			break
		}
	}
	if runnerIndex == -1 {
		t.Fatal("github.runner_provider schema missing")
	}
	runner := schemas[runnerIndex]
	fields := map[string]bool{}
	for _, field := range runner.ConfigFields {
		fields[field.Name] = true
	}
	for _, want := range []string{"organizations", "runner_groups"} {
		if !fields[want] {
			t.Fatalf("github.runner_provider schema missing config field %q", want)
		}
	}
	inputs := map[string]bool{}
	for _, input := range runner.Inputs {
		inputs[input.Name] = true
	}
	for _, want := range []string{"preflight", "org_registration_token", "remove_org_runner"} {
		if !inputs[want] {
			t.Fatalf("github.runner_provider schema missing input %q", want)
		}
	}
}

func TestT593_RunnerProviderProtoConfigDeclaresOrgFields(t *testing.T) {
	desc := (&githubv1.RunnerProviderModuleConfig{}).ProtoReflect().Descriptor()
	for _, want := range []protoreflect.Name{"organizations", "runner_groups"} {
		if field := desc.Fields().ByName(want); field == nil {
			t.Fatalf("RunnerProviderModuleConfig missing proto field %s", want)
		}
	}
}

type fakeRunnerClient struct {
	token                       GitHubRunnerRegistrationToken
	registrationRepository      string
	orgRegistrationOrganization string
	removedRepository           string
	removedOrganization         string
	removedRunnerID             int64
	preflight                   GitHubRunnerProviderPreflight
	preflightOrganization       string
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

func (f *fakeRunnerClient) OrgRegistrationToken(_ context.Context, organization, _ string) (GitHubRunnerRegistrationToken, error) {
	f.orgRegistrationOrganization = organization
	return f.token, nil
}

func (f *fakeRunnerClient) RemoveOrgRunner(_ context.Context, organization string, runnerID int64, _ string) error {
	f.removedOrganization = organization
	f.removedRunnerID = runnerID
	return nil
}

func (f *fakeRunnerClient) PreflightOrg(_ context.Context, req GitHubRunnerProviderPreflightRequest, _ string) (GitHubRunnerProviderPreflight, error) {
	f.preflightOrganization = req.Organization
	return f.preflight, nil
}

func writeRunnerProviderJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}
