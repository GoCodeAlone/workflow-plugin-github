package internal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestT915_GitHubRunnerClientDispatchesWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method: got %q want POST", r.Method)
		}
		if r.URL.Path != "/repos/GoCodeAlone/workflow-compute/actions/workflows/dogfood.yml/dispatches" {
			t.Fatalf("path: got %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["ref"] != "main" {
			t.Fatalf("body: %+v", body)
		}
		inputs, ok := body["inputs"].(map[string]any)
		if !ok {
			t.Fatalf("inputs: got %#v", body["inputs"])
		}
		if inputs["runner_profile"] != "provider" || inputs["allow_github_hosted_fallback"] != "false" {
			t.Fatalf("inputs: got %+v", inputs)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	if err := client.DispatchWorkflow(context.Background(), "GoCodeAlone", "workflow-compute", "dogfood.yml", "main", map[string]string{
		"runner_profile":               "provider",
		"allow_github_hosted_fallback": "false",
	}, "github-token"); err != nil {
		t.Fatalf("dispatch workflow: %v", err)
	}
}

func TestT915_GitHubRunnerClientListsWorkflowRunsAndJobs(t *testing.T) {
	var sawCreatedFilter bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/workflows/dogfood.yml/runs":
			if r.URL.Query().Get("event") != "workflow_dispatch" {
				t.Fatalf("event query: %q", r.URL.RawQuery)
			}
			if strings.HasPrefix(r.URL.Query().Get("created"), ">=") {
				sawCreatedFilter = true
			}
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"workflow_runs": []map[string]any{{"id": 28449657934, "status": "completed", "conclusion": "success"}},
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934/jobs":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"jobs": []map[string]any{{"id": 84308154551, "status": "completed", "conclusion": "success", "runner_name": "wfc-stg-ghp-linux-260629fabb6f-1352329e8d5b"}},
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	runs, err := client.ListWorkflowRuns(context.Background(), "GoCodeAlone", "workflow-compute", "dogfood.yml", time.Date(2026, 6, 30, 13, 53, 30, 0, time.UTC), "github-token")
	if err != nil {
		t.Fatalf("list workflow runs: %v", err)
	}
	if !sawCreatedFilter || len(runs) != 1 || runs[0].ID != 28449657934 {
		t.Fatalf("runs=%+v sawCreatedFilter=%v", runs, sawCreatedFilter)
	}
	jobs, err := client.ListWorkflowRunJobs(context.Background(), "GoCodeAlone", "workflow-compute", 28449657934, "github-token")
	if err != nil {
		t.Fatalf("list workflow jobs: %v", err)
	}
	if len(jobs) != 1 || jobs[0].ID != 84308154551 || jobs[0].RunnerName == "" || jobs[0].RunID != 28449657934 {
		t.Fatalf("jobs=%+v", jobs)
	}
}

func TestT915_GitHubRunnerClientTreatsMissingRunnerAsRemoved(t *testing.T) {
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Fatalf("method: got %q want DELETE", r.Method)
		}
		paths = append(paths, r.URL.Path)
		http.Error(w, `{"message":"Not Found"}`, http.StatusNotFound)
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	if err := client.RemoveRunner(context.Background(), "GoCodeAlone", "workflow-compute", 42, "github-token"); err != nil {
		t.Fatalf("remove repo runner should ignore already-missing runner: %v", err)
	}
	if err := client.RemoveOrgRunner(context.Background(), "GoCodeAlone", 43, "github-token"); err != nil {
		t.Fatalf("remove org runner should ignore already-missing runner: %v", err)
	}
	want := []string{
		"/repos/GoCodeAlone/workflow-compute/actions/runners/42",
		"/orgs/GoCodeAlone/actions/runners/43",
	}
	if strings.Join(paths, "\n") != strings.Join(want, "\n") {
		t.Fatalf("delete paths:\ngot:\n%s\nwant:\n%s", strings.Join(paths, "\n"), strings.Join(want, "\n"))
	}
}

func TestT915_GitHubRunnerClientDrainsAcceptedDeleteResponseBody(t *testing.T) {
	body := &trackingReadCloser{reader: strings.NewReader(`{"message":"Not Found"}`)}
	client := &httpGitHubRunnerClient{
		baseURL: "https://api.github.invalid",
		httpClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodDelete {
				t.Fatalf("method: got %q want DELETE", r.Method)
			}
			return &http.Response{
				StatusCode: http.StatusNotFound,
				Status:     "404 Not Found",
				Header:     make(http.Header),
				Body:       body,
				Request:    r,
			}, nil
		})},
	}
	if err := client.RemoveOrgRunner(context.Background(), "GoCodeAlone", 43, "github-token"); err != nil {
		t.Fatalf("remove org runner: %v", err)
	}
	if !body.read {
		t.Fatal("accepted delete response body was not drained")
	}
	if !body.closed {
		t.Fatal("accepted delete response body was not closed")
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

func TestT915_GitHubRunnerProviderHTTPDispatchesWorkflow(t *testing.T) {
	fake := &fakeRunnerClient{}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches", strings.NewReader(`{"ref":"main","inputs":{"runner_profile":"provider","allow_github_hosted_fallback":"false"}}`))
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
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readResponseBody(t, resp))
	}
	if fake.dispatchedRepository != "GoCodeAlone/workflow-compute" || fake.dispatchedWorkflow != "dogfood.yml" || fake.dispatchedRef != "main" {
		t.Fatalf("dispatch: repo=%q workflow=%q ref=%q", fake.dispatchedRepository, fake.dispatchedWorkflow, fake.dispatchedRef)
	}
	if got := fake.dispatchedInputs["runner_profile"]; got != "provider" {
		t.Fatalf("runner_profile input: got %q", got)
	}
	if got := fake.dispatchedInputs["allow_github_hosted_fallback"]; got != "false" {
		t.Fatalf("allow_github_hosted_fallback input: got %q", got)
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
	dispatchedRepository        string
	dispatchedWorkflow          string
	dispatchedRef               string
	dispatchedInputs            map[string]string
	workflowRuns                []GitHubWorkflowRun
	workflowJobs                []GitHubWorkflowJob
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

type trackingReadCloser struct {
	reader *strings.Reader
	read   bool
	closed bool
}

func (b *trackingReadCloser) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	if n > 0 || err == io.EOF {
		b.read = true
	}
	return n, err
}

func (b *trackingReadCloser) Close() error {
	b.closed = true
	return nil
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

func (f *fakeRunnerClient) DispatchWorkflow(_ context.Context, owner, repo, workflow, ref string, inputs map[string]string, _ string) error {
	f.dispatchedRepository = owner + "/" + repo
	f.dispatchedWorkflow = workflow
	f.dispatchedRef = ref
	f.dispatchedInputs = inputs
	return nil
}

func (f *fakeRunnerClient) ListWorkflowRuns(_ context.Context, owner, repo, workflow string, _ time.Time, _ string) ([]GitHubWorkflowRun, error) {
	f.dispatchedRepository = owner + "/" + repo
	f.dispatchedWorkflow = workflow
	return f.workflowRuns, nil
}

func (f *fakeRunnerClient) ListWorkflowRunJobs(_ context.Context, owner, repo string, runID int64, _ string) ([]GitHubWorkflowJob, error) {
	f.dispatchedRepository = owner + "/" + repo
	for i := range f.workflowJobs {
		if f.workflowJobs[i].RunID == 0 {
			f.workflowJobs[i].RunID = runID
		}
	}
	return f.workflowJobs, nil
}

func writeRunnerProviderJSON(t *testing.T, w http.ResponseWriter, status int, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("encode response: %v", err)
	}
}

func readResponseBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	return string(data)
}
