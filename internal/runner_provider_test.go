package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
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
		if r.Header.Get("X-GitHub-Api-Version") != "2026-03-10" {
			t.Fatalf("API version: got %q", r.Header.Get("X-GitHub-Api-Version"))
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

func TestT916GitHubRunnerClientGetsExactOrgRunner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/orgs/GoCodeAlone/actions/runners/42" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
			"id": 42, "name": "wfc-stg-ghp-linux-worker-task", "status": "online", "busy": false,
			"labels": []map[string]string{{"name": "self-hosted"}, {"name": "linux"}, {"name": "wfc-ghp-ephemeral"}},
		})
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	runner, err := client.GetOrgRunner(context.Background(), "GoCodeAlone", 42, "github-token")
	if err != nil {
		t.Fatalf("get organization runner: %v", err)
	}
	if runner.ID != 42 || runner.Name != "wfc-stg-ghp-linux-worker-task" || runner.Status != "online" || runner.Busy || !slices.Equal(runner.Labels, []string{"self-hosted", "linux", "wfc-ghp-ephemeral"}) {
		t.Fatalf("runner: got %+v", runner)
	}
}

func TestT593_GitHubRunnerClientPreflightChecksPaginatedOrgRunnerLabels(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeRunnerWorkflowRefFixture(t, w, r) {
			return
		}
		if r.Method != http.MethodGet {
			t.Fatalf("method: got %q want GET", r.Method)
		}
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
			return
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
			return
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
			return
		case "/orgs/GoCodeAlone/actions/runner-groups":
			if got := r.URL.Query().Get("visible_to_repository"); got != "workflow-compute" {
				t.Fatalf("visible_to_repository = %q, want organization-scoped repository name", got)
			}
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"id": 5, "name": "ephemeral"}}})
			return
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
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerName:   "wfc-stg-ghp-linux-target",
		RunnerGroup:  "ephemeral",
		Labels:       []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target", "wfc-ghp-stg"},
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if result.RunnerCountChecked != 2 || result.RunnerGroupID != 5 {
		t.Fatalf("runner preflight identity: count=%d group_id=%d", result.RunnerCountChecked, result.RunnerGroupID)
	}
	if len(result.ConflictingLabels) != 0 {
		t.Fatalf("conflicting labels: got %+v", result.ConflictingLabels)
	}
	if !result.ActionsEnabled || !result.SelfHostedAllowed {
		t.Fatalf("preflight policy = %+v", result)
	}
}

func TestT916_GitHubRunnerClientPreflightReflectsActionsAndRunnerGroupPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "none"})
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "none"})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"id": 4, "name": "other"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []any{}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if result.ActionsEnabled || result.SelfHostedAllowed {
		t.Fatalf("preflight fabricated policy evidence: %+v", result)
	}
}

func TestT916GitHubRunnerClientPreflightRejectsRepositoryWithActionsDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeRunnerWorkflowRefFixture(t, w, r) {
			return
		}
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": false})
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"name": "ephemeral"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []any{}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if result.ActionsEnabled {
		t.Fatalf("repository-level Actions disablement was ignored: %+v", result)
	}
}

func TestT916GitHubRunnerClientPreflightResolvesActiveWorkflowAndExactRef(t *testing.T) {
	var workflowCalls, branchCalls, tagCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions", "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
		case "/repos/GoCodeAlone/workflow-compute/actions/workflows/dogfood.yml":
			workflowCalls++
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"id": 42, "path": ".github/workflows/dogfood.yml", "state": "active"})
		case "/repos/GoCodeAlone/workflow-compute/git/ref/heads/main":
			branchCalls++
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"ref": "refs/heads/main", "object": map[string]any{"sha": strings.Repeat("a", 40)}})
		case "/repos/GoCodeAlone/workflow-compute/git/ref/tags/main":
			tagCalls++
			writeRunnerProviderJSON(t, w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"name": "ephemeral"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []any{}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if !result.ActionsEnabled || result.ResolvedWorkflowPath != ".github/workflows/dogfood.yml" || workflowCalls != 1 || branchCalls != 1 || tagCalls != 1 {
		t.Fatalf("workflow/ref preflight evidence: result=%+v workflow=%d branch=%d tag=%d", result, workflowCalls, branchCalls, tagCalls)
	}
}

func TestT916GitHubRunnerClientDispatchesNumericWorkflowIDAgainstResolvedPath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/workflows/42/dispatches":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"workflow_run_id": 28449657934,
				"run_url":         "https://api.github.com/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934",
				"html_url":        "https://github.com/GoCodeAlone/workflow-compute/actions/runs/28449657934",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"id": 28449657934, "path": ".github/workflows/dogfood.yml@refs/heads/main", "head_sha": strings.Repeat("a", 40),
			})
		default:
			t.Fatalf("unexpected dispatch request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	_, err := client.DispatchWorkflow(t.Context(), "GoCodeAlone", "workflow-compute", "42", "main", nil, ".github/workflows/dogfood.yml", strings.Repeat("a", 40), "github-token")
	if err != nil {
		t.Fatalf("dispatch numeric workflow ID: %v", err)
	}
}

func TestT916GitHubRunnerClientDoesNotExposeUpstreamErrorBody(t *testing.T) {
	const secret = "github_pat_customer_secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream echoed "+secret, http.StatusBadGateway)
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	_, err := client.doRawAllowed(t.Context(), http.MethodGet, server.URL+"/failure", nil, "github-token", []int{http.StatusOK}, nil)
	if err == nil || !strings.Contains(err.Error(), "502 Bad Gateway") {
		t.Fatalf("upstream error = %v, want status-only failure", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "upstream echoed") {
		t.Fatalf("upstream error exposed response body: %v", err)
	}
}

func TestT916GitHubRunnerClientResolvesAnnotatedTagToCommit(t *testing.T) {
	const tagObjectSHA = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const commitSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/GoCodeAlone/workflow-compute/git/ref/tags/v1.0.0":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"ref": "refs/tags/v1.0.0", "object": map[string]any{"type": "tag", "sha": tagObjectSHA},
			})
		case "/repos/GoCodeAlone/workflow-compute/git/tags/" + tagObjectSHA:
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"sha": tagObjectSHA, "object": map[string]any{"type": "commit", "sha": commitSHA},
			})
		default:
			t.Fatalf("unexpected request: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	resolvedSHA, exists, err := client.repositoryRefExists(t.Context(), "GoCodeAlone", "workflow-compute", "refs/tags/v1.0.0", "github-token")
	if err != nil || !exists || resolvedSHA != commitSHA {
		t.Fatalf("annotated tag resolution: sha=%q exists=%t err=%v", resolvedSHA, exists, err)
	}
}

func TestT916GitHubRunnerClientPreflightDetectsExactRunnerNameConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"name": "ephemeral"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []map[string]any{{
				"name":   "wfc-stg-ghp-linux-target",
				"labels": []map[string]string{{"name": "self-hosted"}, {"name": "linux"}},
			}}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		RunnerName:   "wfc-stg-ghp-linux-target",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if !reflect.DeepEqual(result.ConflictingLabels, []string{"wfc-stg-ghp-linux-target"}) {
		t.Fatalf("runner-name conflict = %v", result.ConflictingLabels)
	}
}

func TestT916GitHubRunnerClientPreflightRejectsCaseInsensitiveRunnerSelectorLabelConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions", "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"name": "ephemeral"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []map[string]any{{
				"name":   "different-runner",
				"labels": []map[string]string{{"name": "self-hosted"}, {"name": "linux"}, {"name": "WFC-STG-GHP-LINUX-TARGET"}, {"name": "wfc-ghp-stg"}, {"name": "wfc-ghp-ephemeral"}},
			}}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		RunnerName:   "wfc-stg-ghp-linux-target",
		RunnerGroup:  "ephemeral",
		Labels:       []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if !reflect.DeepEqual(result.ConflictingLabels, []string{"wfc-stg-ghp-linux-target"}) {
		t.Fatalf("runner selector label conflict = %v", result.ConflictingLabels)
	}
}

func TestT916GitHubRunnerClientPreflightRejectsRepositoryOutsideOrganization(t *testing.T) {
	client := &httpGitHubRunnerClient{baseURL: "https://api.github.invalid", httpClient: http.DefaultClient}
	_, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "OtherOrg/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err == nil || !strings.Contains(err.Error(), "repository owner") {
		t.Fatalf("cross-organization preflight error = %v", err)
	}
}

func TestT916SelectedWorkflowAllowsOneUnqualifiedNamespaceAndRejectsAmbiguity(t *testing.T) {
	tag := "GoCodeAlone/workflow-compute/.github/workflows/dogfood.yml@refs/tags/v1.2.3"
	branch := "GoCodeAlone/workflow-compute/.github/workflows/dogfood.yml@refs/heads/v1.2.3"
	if !selectedWorkflowsAllow([]string{tag}, "gocodealone/WORKFLOW-COMPUTE", "dogfood.yml", "v1.2.3") {
		t.Fatal("one exact unqualified tag restriction must be allowed")
	}
	if selectedWorkflowsAllow([]string{tag, branch}, "GoCodeAlone/workflow-compute", "dogfood.yml", "v1.2.3") {
		t.Fatal("ambiguous unqualified branch/tag restriction must fail closed")
	}
}

func TestT916GitHubRunnerClientPreflightBoundsExistingLabelEvidence(t *testing.T) {
	labels := make([]map[string]string, maxPreflightExistingLabels+10)
	for i := range labels {
		labels[i] = map[string]string{"name": fmt.Sprintf("label-%03d", i)}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeRunnerWorkflowRefFixture(t, w, r) {
			return
		}
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions", "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "all"})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{"name": "ephemeral"}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []map[string]any{{"labels": labels}}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	result, err := client.PreflightOrg(context.Background(), GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerName:   "target-runner",
		RunnerGroup:  "ephemeral",
	}, "github-token")
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	if len(result.ExistingLabels) != maxPreflightExistingLabels || !result.ExistingLabelsTruncated {
		t.Fatalf("bounded label evidence = %+v", result)
	}
	if result.LabelsObserved != maxPreflightExistingLabels+10 {
		t.Fatalf("labels observed = %d", result.LabelsObserved)
	}
}

func TestT916_GitHubRunnerClientPreflightRequiresExactSelectedWorkflowRef(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if writeRunnerWorkflowRefFixture(t, w, r) {
			return
		}
		switch r.URL.Path {
		case "/orgs/GoCodeAlone/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "selected"})
		case "/orgs/GoCodeAlone/actions/permissions/repositories":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"repositories": []map[string]any{{"full_name": "GoCodeAlone/workflow-compute"}}})
		case "/repos/GoCodeAlone/workflow-compute/actions/permissions":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled": true})
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"enabled_repositories": "selected"})
		case "/orgs/GoCodeAlone/actions/permissions/self-hosted-runners/repositories":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"repositories": []map[string]any{{"full_name": "GoCodeAlone/workflow-compute"}}})
		case "/orgs/GoCodeAlone/actions/runner-groups":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runner_groups": []map[string]any{{
				"name":                    "ephemeral",
				"restricted_to_workflows": true,
				"selected_workflows":      []string{"GoCodeAlone/workflow-compute/.github/workflows/dogfood.yml@refs/heads/main"},
			}}})
		case "/orgs/GoCodeAlone/actions/runners":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"runners": []any{}})
		default:
			t.Fatalf("unexpected request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	request := GitHubRunnerProviderPreflightRequest{
		Organization: "GoCodeAlone",
		Repository:   "GoCodeAlone/workflow-compute",
		Workflow:     "dogfood.yml",
		Ref:          "main",
		RunnerName:   "wfc-stg-ghp-linux-target",
		RunnerGroup:  "ephemeral",
	}
	allowed, err := client.PreflightOrg(context.Background(), request, "github-token")
	if err != nil {
		t.Fatalf("matching selected workflow preflight: %v", err)
	}
	if !allowed.ActionsEnabled || !allowed.SelfHostedAllowed {
		t.Fatalf("matching selected workflow rejected: %+v", allowed)
	}

	request.Ref = "release"
	rejected, err := client.PreflightOrg(context.Background(), request, "github-token")
	if err != nil {
		t.Fatalf("mismatched selected workflow preflight: %v", err)
	}
	if !rejected.ActionsEnabled || rejected.SelfHostedAllowed {
		t.Fatalf("mismatched selected workflow ref accepted: %+v", rejected)
	}

	request.Ref = "main"
	request.Workflow = "Dogfood.yml"
	rejected, err = client.PreflightOrg(context.Background(), request, "github-token")
	if err != nil {
		t.Fatalf("case-mismatched selected workflow preflight: %v", err)
	}
	if rejected.ActionsEnabled || rejected.SelfHostedAllowed {
		t.Fatalf("case-mismatched selected workflow path accepted: %+v", rejected)
	}
}

func TestT916ProviderErrorStatusDoesNotMisclassifyUpstreamOrganizationFailure(t *testing.T) {
	err := errors.New("query organization Actions permissions: github API returned status 503")
	if got := providerErrorStatus(err); got != http.StatusBadGateway {
		t.Fatalf("upstream organization failure status = %d, want %d", got, http.StatusBadGateway)
	}
	for _, validationErr := range []error{
		errors.New("organization is required"),
		errors.New("organization contains invalid characters"),
		errors.New("repository must be owner/name"),
		errors.New("repository contains invalid characters"),
		errors.New("runner_id must be an integer"),
		fmt.Errorf("%w: repository owner OtherOrg must match organization GoCodeAlone", errRepositoryOrganizationMismatch),
	} {
		if got := providerErrorStatus(validationErr); got != http.StatusBadRequest {
			t.Fatalf("validation error %q status = %d, want %d", validationErr, got, http.StatusBadRequest)
		}
	}
}

func TestT915_GitHubRunnerClientDispatchesWorkflow(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer github-token" {
			t.Fatalf("authorization header: got %q", r.Header.Get("Authorization"))
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/workflows/dogfood.yml/dispatches":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode body: %v", err)
			}
			if body["ref"] != "main" {
				t.Fatalf("body: %+v", body)
			}
			inputs, ok := body["inputs"].(map[string]any)
			if !ok || inputs["runner_profile"] != "provider" || inputs["allow_github_hosted_fallback"] != "false" {
				t.Fatalf("inputs: got %+v", inputs)
			}
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"workflow_run_id": 28449657934,
				"run_url":         "https://api.github.com/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934",
				"html_url":        "https://github.com/GoCodeAlone/workflow-compute/actions/runs/28449657934",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"id":       28449657934,
				"path":     ".github/workflows/dogfood.yml@refs/heads/main",
				"head_sha": strings.Repeat("a", 40),
			})
		default:
			t.Fatalf("unexpected dispatch request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	dispatch, err := client.DispatchWorkflow(context.Background(), "GoCodeAlone", "workflow-compute", "dogfood.yml", "main", map[string]string{
		"runner_profile":               "provider",
		"allow_github_hosted_fallback": "false",
	}, ".github/workflows/dogfood.yml", strings.Repeat("a", 40), "github-token")
	if err != nil {
		t.Fatalf("dispatch workflow: %v", err)
	}
	if dispatch.WorkflowRunID != 28449657934 || dispatch.RunURL == "" || dispatch.HTMLURL == "" || dispatch.ValidatedHeadSHA != strings.Repeat("a", 40) {
		t.Fatalf("dispatch response = %+v", dispatch)
	}
}

func TestT916GitHubRunnerClientPreservesKnownRunWhenVerificationUnavailable(t *testing.T) {
	oldInterval := workflowDispatchVerificationRetryInterval
	workflowDispatchVerificationRetryInterval = time.Millisecond
	t.Cleanup(func() { workflowDispatchVerificationRetryInterval = oldInterval })
	getCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"workflow_run_id": 28449657934,
				"run_url":         "https://api.github.com/runs/28449657934",
				"html_url":        "https://github.com/runs/28449657934",
			})
		case http.MethodGet:
			getCalls++
			writeRunnerProviderJSON(t, w, http.StatusServiceUnavailable, map[string]any{"message": "temporarily unavailable"})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	dispatch, err := client.DispatchWorkflow(t.Context(), "GoCodeAlone", "workflow-compute", "dogfood.yml", "main", nil, ".github/workflows/dogfood.yml", strings.Repeat("a", 40), "github-token")
	if err == nil || !strings.Contains(err.Error(), "verification is uncertain") {
		t.Fatalf("dispatch verification error = %v", err)
	}
	if dispatch.WorkflowRunID != 28449657934 {
		t.Fatalf("known workflow run was discarded: %+v", dispatch)
	}
	if getCalls < 2 {
		t.Fatalf("workflow run verification attempts = %d, want bounded retry", getCalls)
	}
}

func TestT916GitHubRunnerClientRejectsDispatchedHeadSHAMismatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost:
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"workflow_run_id": 28449657934,
				"run_url":         "https://api.github.com/runs/28449657934",
				"html_url":        "https://github.com/runs/28449657934",
			})
		case r.Method == http.MethodGet && r.URL.Path == "/repos/GoCodeAlone/workflow-compute/actions/runs/28449657934":
			writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{
				"id":       28449657934,
				"path":     ".github/workflows/dogfood.yml@refs/heads/main",
				"head_sha": strings.Repeat("b", 40),
			})
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	_, err := client.DispatchWorkflow(t.Context(), "GoCodeAlone", "workflow-compute", "dogfood.yml", "main", nil, ".github/workflows/dogfood.yml", strings.Repeat("a", 40), "github-token")
	if err == nil || !strings.Contains(err.Error(), "head SHA") {
		t.Fatalf("dispatch SHA mismatch error = %v", err)
	}
}

func TestT916RunnerProviderPreservesUncertainDispatchedRunIdentity(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{
		workflowDispatch: GitHubWorkflowDispatch{
			WorkflowRunID: 28449657934,
			RunURL:        "https://api.github.com/runs/28449657934",
			HTMLURL:       "https://github.com/runs/28449657934",
			Verification:  "uncertain",
		},
		dispatchErr: errWorkflowDispatchVerificationUncertain,
	})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	result, err := module.InvokeMethod("dispatch_workflow", map[string]any{
		"repository":             "GoCodeAlone/workflow-compute",
		"workflow":               "dogfood.yml",
		"ref":                    "main",
		"expected_workflow_path": ".github/workflows/dogfood.yml",
		"expected_head_sha":      strings.Repeat("a", 40),
		"provider_token":         "provider-token",
	})
	if err != nil {
		t.Fatalf("preserve uncertain dispatch: %v", err)
	}
	if result["workflow_run_id"] != int64(28449657934) || result["verification_status"] != "uncertain" {
		t.Fatalf("uncertain dispatch result = %+v", result)
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

func TestT916GitHubRunnerClientRejectsPaginationCycle(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Link", fmt.Sprintf(`<%s%s>; rel="next"`, server.URL, r.URL.RequestURI()))
		writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"jobs": []any{}})
	}))
	defer server.Close()
	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := client.ListWorkflowRunJobs(ctx, "GoCodeAlone", "workflow-compute", 42, "github-token")
	if err == nil || !strings.Contains(err.Error(), "pagination cycle") {
		t.Fatalf("pagination cycle error = %v", err)
	}
}

func TestT916GitHubPaginationGuardEnforcesPageLimit(t *testing.T) {
	var guard githubPaginationGuard
	for page := 0; page < maxGitHubPaginationPages; page++ {
		if err := guard.visit(fmt.Sprintf("https://api.github.test/page/%d", page)); err != nil {
			t.Fatalf("page %d rejected before limit: %v", page, err)
		}
	}
	if err := guard.visit("https://api.github.test/page/overflow"); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("pagination page limit error = %v", err)
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

func TestT915_GitHubRunnerProviderRejectsInsecureExternalAPIBaseURL(t *testing.T) {
	base := func(apiBaseURL string) map[string]any {
		config := map[string]any{
			"token":          "github-token",
			"provider_token": "provider-token",
			"organizations":  []string{"GoCodeAlone"},
			"runner_groups":  []string{"ephemeral"},
			"api_base_url":   apiBaseURL,
		}
		if apiBaseURL == "" {
			delete(config, "api_base_url")
		}
		return config
	}
	if _, err := newGitHubRunnerProviderModule("provider", base("http://github.example.invalid"), &fakeRunnerClient{}); err == nil || !strings.Contains(err.Error(), "api_base_url") {
		t.Fatalf("insecure external API base URL error = %v", err)
	}
	for _, apiBaseURL := range []string{"", "https://api.github.com", "http://127.0.0.1:8080", "http://localhost:8080", "http://[::1]:8080"} {
		if _, err := newGitHubRunnerProviderModule("provider", base(apiBaseURL), &fakeRunnerClient{}); err != nil {
			t.Fatalf("safe API base URL %q rejected: %v", apiBaseURL, err)
		}
	}
	if _, err := newGitHubRunnerProviderModule("provider", base(" https://api.github.com "), &fakeRunnerClient{}); err == nil {
		t.Fatal("raw config validation must reject whitespace-padded API URLs")
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
	if !strings.Contains(err.Error(), "config.repositories") {
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
	t.Cleanup(func() { _ = resp.Body.Close() })
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

func TestT916GitHubRunnerProviderHTTPGetsExactOrgRunner(t *testing.T) {
	const runnerName = "wfc-stg-ghp-linux-worker-task"
	labels := []string{"self-hosted", "linux", runnerName, "wfc-ghp-stg", "wfc-ghp-ephemeral"}
	fake := &fakeRunnerClient{jitRequest: GitHubRunnerJITConfigRequest{RunnerName: runnerName, Labels: labels}}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/42", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readResponseBody(t, resp))
	}
	var got GitHubOrgRunner
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != 42 || got.Name != runnerName || got.Status != "online" || got.Busy || !slices.Equal(got.Labels, labels) {
		t.Fatalf("runner response: got %+v", got)
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
	t.Cleanup(func() { _ = resp.Body.Close() })
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

func TestT916GitHubRunnerProviderReadyzRequiresProviderBearer(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"example/repository"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	unauthorizedCases := []struct {
		name          string
		authorization []string
	}{
		{name: "missing"},
		{name: "bare token", authorization: []string{"provider-token"}},
		{name: "bare bearer", authorization: []string{"Bearer"}},
		{name: "empty bearer", authorization: []string{"Bearer "}},
		{name: "basic", authorization: []string{"Basic provider-token"}},
		{name: "lowercase scheme", authorization: []string{"bearer provider-token"}},
		{name: "extra whitespace", authorization: []string{"Bearer  provider-token"}},
		{name: "wrong token", authorization: []string{"Bearer submitted-wrong-token"}},
		{name: "duplicate", authorization: []string{"Bearer provider-token", "Bearer submitted-wrong-token"}},
	}
	for _, tc := range unauthorizedCases {
		t.Run(tc.name, func(t *testing.T) {
			req, requestErr := http.NewRequest(http.MethodGet, server.URL+"/readyz", nil)
			if requestErr != nil {
				t.Fatalf("request: %v", requestErr)
			}
			for _, authorization := range tc.authorization {
				req.Header.Add("Authorization", authorization)
			}
			resp, requestErr := server.Client().Do(req)
			if requestErr != nil {
				t.Fatalf("readiness request: %v", requestErr)
			}
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				t.Fatalf("read unauthorized response: %v", readErr)
			}
			if resp.StatusCode != http.StatusUnauthorized {
				t.Fatalf("readiness status: got %d want %d body=%s", resp.StatusCode, http.StatusUnauthorized, body)
			}
			if string(body) != "{\"error\":\"provider token is invalid\"}\n" {
				t.Fatalf("unauthorized body: got %q", body)
			}
			for _, token := range []string{"provider-token", "submitted-wrong-token"} {
				if strings.Contains(string(body), token) {
					t.Fatalf("unauthorized response exposed token %q", token)
				}
			}
		})
	}

	directReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	directReq.Header["Authorization"] = []string{"Bearer provider-token "}
	directResp := httptest.NewRecorder()
	module.HTTPHandler().ServeHTTP(directResp, directReq)
	if directResp.Code != http.StatusUnauthorized {
		t.Fatalf("direct trailing-whitespace status: got %d want %d", directResp.Code, http.StatusUnauthorized)
	}

	headReq, err := http.NewRequest(http.MethodHead, server.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("HEAD request: %v", err)
	}
	headReq.Header.Set("Authorization", "Bearer provider-token")
	headResp, err := server.Client().Do(headReq)
	if err != nil {
		t.Fatalf("HEAD readiness request: %v", err)
	}
	_ = headResp.Body.Close()
	if headResp.StatusCode != http.StatusMethodNotAllowed || headResp.Header.Get("Allow") != http.MethodGet {
		t.Fatalf("HEAD readiness response: status=%d allow=%q", headResp.StatusCode, headResp.Header.Get("Allow"))
	}

	req, err := http.NewRequest(http.MethodGet, server.URL+"/readyz", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("authenticated readiness request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("authenticated readiness status: got %d want %d", resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read readiness response: %v", err)
	}
	if string(body) != "{\"status\":\"ok\"}\n" {
		t.Fatalf("readiness response: got %q", body)
	}
}

func TestT916GitHubRunnerProviderHTTPRejectsConcatenatedJSON(t *testing.T) {
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
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/runners/registration-token", strings.NewReader(`{"repository":"GoCodeAlone/workflow-compute"}{"repository":"GoCodeAlone/other"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("concatenated JSON status = %d", resp.StatusCode)
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
	t.Cleanup(func() { _ = resp.Body.Close() })
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

	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/repos/GoCodeAlone/workflow-compute/workflows/dogfood.yml/dispatches", strings.NewReader(`{"ref":"main","inputs":{"runner_profile":"provider","allow_github_hosted_fallback":"false"},"expected_workflow_path":".github/workflows/dogfood.yml","expected_head_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d body=%s", resp.StatusCode, readResponseBody(t, resp))
	}
	var dispatch GitHubWorkflowDispatch
	if err := json.NewDecoder(resp.Body).Decode(&dispatch); err != nil {
		t.Fatalf("decode dispatch response: %v", err)
	}
	if dispatch.WorkflowRunID == 0 {
		t.Fatalf("dispatch response = %+v", dispatch)
	}
	if fake.dispatchedRepository != "GoCodeAlone/workflow-compute" || fake.dispatchedWorkflow != "dogfood.yml" || fake.dispatchedRef != "main" || fake.dispatchedExpectedWorkflowPath != ".github/workflows/dogfood.yml" {
		t.Fatalf("dispatch: repo=%q workflow=%q ref=%q", fake.dispatchedRepository, fake.dispatchedWorkflow, fake.dispatchedRef)
	}
	if got := fake.dispatchedInputs["runner_profile"]; got != "provider" {
		t.Fatalf("runner_profile input: got %q", got)
	}
	if got := fake.dispatchedInputs["allow_github_hosted_fallback"]; got != "false" {
		t.Fatalf("allow_github_hosted_fallback input: got %q", got)
	}
}

func TestT915_GitHubRunnerProviderModuleListsWorkflowRunsFromRFC3339Arg(t *testing.T) {
	createdAfter := "2026-06-30T13:52:30Z"
	fake := &fakeRunnerClient{
		workflowRuns: []GitHubWorkflowRun{{ID: 28449657934, Status: "completed", Conclusion: "success"}},
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("workflow_runs", map[string]any{
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood-provider-target.yml",
		"created_after":  createdAfter,
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("invoke workflow_runs: %v", err)
	}
	runs, ok := result["workflow_runs"].([]GitHubWorkflowRun)
	if !ok || len(runs) != 1 || runs[0].ID != 28449657934 {
		t.Fatalf("workflow_runs output: got %+v", result)
	}
	if fake.listRunsRepository != "GoCodeAlone/workflow-compute" || fake.listRunsWorkflow != "dogfood-provider-target.yml" {
		t.Fatalf("list runs tracking: repo=%q workflow=%q", fake.listRunsRepository, fake.listRunsWorkflow)
	}
	wantCreatedAfter, err := time.Parse(time.RFC3339, createdAfter)
	if err != nil {
		t.Fatalf("parse test timestamp: %v", err)
	}
	if !fake.listRunsCreatedAfter.Equal(wantCreatedAfter) {
		t.Fatalf("created_after: got %s want %s", fake.listRunsCreatedAfter.Format(time.RFC3339), wantCreatedAfter.Format(time.RFC3339))
	}
	if fake.dispatchedRepository != "" || fake.dispatchedWorkflow != "" {
		t.Fatalf("list runs mutated dispatch tracking: repo=%q workflow=%q", fake.dispatchedRepository, fake.dispatchedWorkflow)
	}
}

func TestT915_GitHubRunnerProviderModuleRejectsInvalidCreatedAfterArg(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("workflow_runs", map[string]any{
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood-provider-target.yml",
		"created_after":  123,
		"provider_token": "provider-token",
	})
	if err == nil || !strings.Contains(err.Error(), "created_after must be RFC3339") {
		t.Fatalf("expected created_after validation error, got %v", err)
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

func TestT916GitHubRunnerClientGeneratesExactOrgJITConfig(t *testing.T) {
	var request struct {
		Name          string   `json:"name"`
		RunnerGroupID int64    `json:"runner_group_id"`
		WorkFolder    string   `json:"work_folder"`
		Labels        []string `json:"labels"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/orgs/GoCodeAlone/actions/runners/generate-jitconfig" {
			t.Fatalf("JIT request = %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatalf("decode JIT request: %v", err)
		}
		writeRunnerProviderJSON(t, w, http.StatusCreated, map[string]any{
			"runner":             map[string]any{"id": 42, "name": request.Name},
			"encoded_jit_config": "encoded-jit-config",
		})
	}))
	defer server.Close()
	client := newHTTPGitHubRunnerClient(server.URL).(*httpGitHubRunnerClient)
	client.httpClient = server.Client()
	config, err := client.GenerateOrgJITConfig(t.Context(), GitHubRunnerJITConfigRequest{
		Organization:  "GoCodeAlone",
		RunnerName:    "wfc-stg-ghp-linux-target",
		RunnerGroupID: 5,
		Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"},
	}, "github-token")
	if err != nil {
		t.Fatalf("generate JIT config: %v", err)
	}
	if config.RunnerID != 42 || config.RunnerName != request.Name || config.EncodedJITConfig != "encoded-jit-config" {
		t.Fatalf("JIT config = %+v", config)
	}
	if request.RunnerGroupID != 5 || request.WorkFolder != "_work" || !slices.Equal(request.Labels, []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"}) {
		t.Fatalf("JIT request = %+v", request)
	}
}

func TestT916GitHubRunnerClientReturnsMalformedJITExactIDForDurableCleanup(t *testing.T) {
	var deleteCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/orgs/GoCodeAlone/actions/runners/generate-jitconfig":
			writeRunnerProviderJSON(t, w, http.StatusCreated, map[string]any{
				"runner": map[string]any{"id": 42, "name": "wfc-stg-ghp-linux-target"},
			})
		case r.Method == http.MethodDelete && r.URL.Path == "/orgs/GoCodeAlone/actions/runners/42":
			deleteCalls++
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected malformed JIT cleanup request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()
	client := &httpGitHubRunnerClient{baseURL: server.URL, httpClient: server.Client()}
	config, err := client.GenerateOrgJITConfig(t.Context(), GitHubRunnerJITConfigRequest{
		Organization:  "GoCodeAlone",
		RunnerName:    "wfc-stg-ghp-linux-target",
		RunnerGroupID: 5,
		Labels:        []string{"self-hosted", "linux", "wfc-stg-ghp-linux-target"},
	}, "github-token")
	if err == nil || !strings.Contains(err.Error(), "missing exact runner identity") {
		t.Fatalf("malformed JIT response error = %v", err)
	}
	if config.RunnerID != 42 {
		t.Fatalf("malformed JIT recoverable runner ID = %d", config.RunnerID)
	}
	if deleteCalls != 0 {
		t.Fatalf("client bypassed durable cleanup with %d direct deletes", deleteCalls)
	}
}

func TestT916RunnerProviderJournalsMalformedJITResponseForExactIDCleanup(t *testing.T) {
	removed := make(chan int64, 1)
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		jitConfig:        GitHubRunnerJITConfig{RunnerID: 42},
		jitErr:           errors.New("malformed JIT response"),
		removedRunnerIDs: removed,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	_, err = module.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err == nil || !strings.Contains(err.Error(), "malformed JIT") {
		t.Fatalf("malformed JIT error = %v", err)
	}
	select {
	case runnerID := <-removed:
		if runnerID != 42 {
			t.Fatalf("malformed JIT cleanup runner ID = %d", runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("malformed JIT exact ID was not cleaned through journal")
	}
	if err := module.Stop(t.Context()); err != nil {
		t.Fatalf("stop module after malformed JIT cleanup: %v", err)
	}
}

func TestT916RunnerProviderMalformedJITJournalFailureWaitsForExactIDCleanup(t *testing.T) {
	removeStarted := make(chan struct{}, 1)
	removeRelease := make(chan struct{})
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		jitConfig:     GitHubRunnerJITConfig{RunnerID: 42},
		jitErr:        errors.New("malformed JIT response"),
		removeStarted: removeStarted,
		removeRelease: removeRelease,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	syncCalls := 0
	module.journalDirectorySync = func() error {
		syncCalls++
		if syncCalls == 2 {
			return errors.New("simulated malformed JIT journal sync failure")
		}
		return nil
	}
	done := make(chan error, 1)
	go func() {
		_, invokeErr := module.InvokeMethod("org_jit_config", map[string]any{
			"organization": "GoCodeAlone", "repository": "GoCodeAlone/workflow-compute", "workflow": "dogfood.yml", "ref": "main",
			"runner_name": "wfc-stg-ghp-linux-worker-task", "runner_group": "ephemeral",
			"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
			"provider_token": "provider-token",
		})
		done <- invokeErr
	}()
	select {
	case <-removeStarted:
	case <-time.After(time.Second):
		t.Fatal("malformed JIT journal failure did not begin exact-ID cleanup")
	}
	select {
	case invokeErr := <-done:
		close(removeRelease)
		t.Fatalf("malformed JIT request returned before cleanup completed: %v", invokeErr)
	case <-time.After(25 * time.Millisecond):
	}
	close(removeRelease)
	if invokeErr := <-done; invokeErr == nil || !strings.Contains(invokeErr.Error(), "malformed JIT") {
		t.Fatalf("malformed JIT error = %v", invokeErr)
	}
	if fake.removedRunnerID != 42 {
		t.Fatalf("malformed JIT cleanup runner ID = %d", fake.removedRunnerID)
	}
}

func TestT916RunnerProviderJITHandlerPreflightsAllowlistAndReturnsExactIdentity(t *testing.T) {
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()
	body := `{"repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_name":"wfc-stg-ghp-linux-worker-task","runner_group":"ephemeral","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}`
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/jitconfig", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("JIT handler: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("JIT status = %d body=%s", resp.StatusCode, readResponseBody(t, resp))
	}
	if fake.jitRequest.RunnerGroupID != 5 || fake.jitRequest.RunnerName != "wfc-stg-ghp-linux-worker-task" || fake.preflightOrganization != "GoCodeAlone" {
		t.Fatalf("JIT boundary calls: preflight_org=%q request=%+v", fake.preflightOrganization, fake.jitRequest)
	}
	var result GitHubRunnerJITRegistration
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JIT response: %v", err)
	}
	if result.RunnerID != 42 || result.RunnerName != "wfc-stg-ghp-linux-worker-task" || result.EncodedJITConfig == "" || result.OwnershipToken == "" || result.Preflight == nil || result.Preflight.RunnerGroupID != 5 {
		t.Fatalf("JIT response = %+v", result)
	}
}

func TestT916RunnerProviderJITOwnershipAckTransitionsToOwnedTTL(t *testing.T) {
	removed := make(chan int64, 1)
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removedRunnerIDs: removed,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 25 * time.Millisecond
	module.jitOwnedTTL = 200 * time.Millisecond
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	body := `{"repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_name":"wfc-stg-ghp-linux-worker-task","runner_group":"ephemeral","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}`
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/jitconfig", strings.NewReader(body))
	if err != nil {
		t.Fatalf("JIT request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("JIT request: %v", err)
	}
	var registration GitHubRunnerJITRegistration
	if err := json.NewDecoder(resp.Body).Decode(&registration); err != nil {
		_ = resp.Body.Close()
		t.Fatalf("decode JIT response: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated || registration.OwnershipToken == "" {
		t.Fatalf("JIT response status=%d registration=%+v", resp.StatusCode, registration)
	}

	forgedReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/42/ack", strings.NewReader(`{"ownership_token":"forged"}`))
	if err != nil {
		t.Fatalf("forged ACK request: %v", err)
	}
	forgedReq.Header.Set("Authorization", "Bearer provider-token")
	forgedResp, err := server.Client().Do(forgedReq)
	if err != nil {
		t.Fatalf("forged ACK request: %v", err)
	}
	_ = forgedResp.Body.Close()
	if forgedResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("forged ACK status = %d", forgedResp.StatusCode)
	}

	ackBody := fmt.Sprintf(`{"ownership_token":%q}`, registration.OwnershipToken)
	ackReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/42/ack", strings.NewReader(ackBody))
	if err != nil {
		t.Fatalf("ACK request: %v", err)
	}
	ackReq.Header.Set("Authorization", "Bearer provider-token")
	ackResp, err := server.Client().Do(ackReq)
	if err != nil {
		t.Fatalf("ACK request: %v", err)
	}
	_ = ackResp.Body.Close()
	if ackResp.StatusCode != http.StatusNoContent {
		t.Fatalf("ACK status = %d", ackResp.StatusCode)
	}
	replayReq, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/42/ack", strings.NewReader(ackBody))
	if err != nil {
		t.Fatalf("replayed ACK request: %v", err)
	}
	replayReq.Header.Set("Authorization", "Bearer provider-token")
	replayResp, err := server.Client().Do(replayReq)
	if err != nil {
		t.Fatalf("replayed ACK request: %v", err)
	}
	_ = replayResp.Body.Close()
	if replayResp.StatusCode != http.StatusNotFound {
		t.Fatalf("replayed ACK status = %d", replayResp.StatusCode)
	}

	select {
	case runnerID := <-removed:
		t.Fatalf("acknowledged runner %d was removed by pending ownership TTL", runnerID)
	case <-time.After(4 * module.jitOwnershipTTL):
	}
	select {
	case runnerID := <-removed:
		if runnerID != 42 {
			t.Fatalf("owned TTL removed runner %d, want 42", runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("acknowledged runner was not removed by owned-job TTL")
	}
}

func TestT916RunnerProviderModuleMethodAcknowledgesJITOwnership(t *testing.T) {
	removed := make(chan int64, 1)
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removedRunnerIDs: removed,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 25 * time.Millisecond
	created, err := module.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	acknowledged, err := module.InvokeMethod("ack_org_jit_config", map[string]any{
		"organization":    "GoCodeAlone",
		"runner_id":       created["runner_id"],
		"ownership_token": created["ownership_token"],
		"provider_token":  "provider-token",
	})
	if err != nil {
		t.Fatalf("acknowledge JIT ownership: %v", err)
	}
	if acknowledged["acknowledged"] != true {
		t.Fatalf("ACK output = %#v", acknowledged)
	}
	select {
	case runnerID := <-removed:
		t.Fatalf("module-acknowledged runner %d was removed by ownership TTL", runnerID)
	case <-time.After(4 * module.jitOwnershipTTL):
	}
}

func TestT916RunnerProviderUnacknowledgedJITExpiresByExactID(t *testing.T) {
	removed := make(chan int64, 1)
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removedRunnerIDs: removed,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 25 * time.Millisecond
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	body := `{"repository":"GoCodeAlone/workflow-compute","workflow":"dogfood.yml","ref":"main","runner_name":"wfc-stg-ghp-linux-worker-task","runner_group":"ephemeral","labels":["self-hosted","linux","wfc-stg-ghp-linux-worker-task","wfc-ghp-stg","wfc-ghp-ephemeral"]}`
	req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/jitconfig", strings.NewReader(body))
	if err != nil {
		t.Fatalf("JIT request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("JIT request: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("JIT status = %d", resp.StatusCode)
	}

	select {
	case runnerID := <-removed:
		if runnerID != 42 || fake.removedOrganization != "GoCodeAlone" {
			t.Fatalf("expired JIT cleanup organization=%q runner_id=%d", fake.removedOrganization, runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("unacknowledged JIT runner was not removed by ownership TTL")
	}
}

func TestT916RunnerProviderJITOwnershipJournalSurvivesRestart(t *testing.T) {
	stateDir := t.TempDir()
	config := map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      stateDir,
	}
	preflight := GitHubRunnerProviderPreflight{
		Organization:      "GoCodeAlone",
		RunnerGroup:       "ephemeral",
		RunnerGroupID:     5,
		Ref:               "main",
		ResolvedRefSHA:    strings.Repeat("a", 40),
		ActionsEnabled:    true,
		SelfHostedAllowed: true,
	}
	first, err := newGitHubRunnerProviderModule("provider", config, &fakeRunnerClient{preflight: preflight})
	if err != nil {
		t.Fatalf("first module: %v", err)
	}
	first.jitOwnershipTTL = 25 * time.Millisecond
	created, err := first.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil || created["runner_id"] != int64(42) {
		t.Fatalf("create journaled JIT runner: output=%#v err=%v", created, err)
	}
	if err := first.Stop(t.Context()); err != nil {
		t.Fatalf("stop first module: %v", err)
	}
	time.Sleep(2 * first.jitOwnershipTTL)

	removed := make(chan int64, 1)
	second, err := newGitHubRunnerProviderModule("provider", config, &fakeRunnerClient{removedRunnerIDs: removed})
	if err != nil {
		t.Fatalf("restarted module: %v", err)
	}
	t.Cleanup(func() { _ = second.Stop(context.Background()) })
	select {
	case runnerID := <-removed:
		if runnerID != 42 {
			t.Fatalf("restart cleanup runner ID = %d", runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("restarted provider did not clean expired exact JIT runner ID")
	}
}

func TestT916RunnerProviderRejectsStateDirSymlinkWithoutChangingTarget(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatalf("create state target: %v", err)
	}
	link := filepath.Join(parent, "state-link")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symbolic links unavailable: %v", err)
	}
	_, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      link,
	}, &fakeRunnerClient{})
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("state_dir symlink error = %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat state target: %v", err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("state_dir symlink target mode changed to %o", info.Mode().Perm())
	}
}

func TestT916RunnerProviderClosesStateRootWhenJournalValidationFails(t *testing.T) {
	stateDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(stateDir, jitOwnershipJournalName), []byte(`{"version":1,"unexpected":true}`), 0o600); err != nil {
		t.Fatalf("write malformed journal: %v", err)
	}
	module := &githubRunnerProviderModule{
		config:     githubRunnerProviderConfig{StateDir: stateDir},
		pendingJIT: make(map[pendingJITKey]*pendingJITOwnership),
	}
	err := module.initializeJITOwnershipJournal()
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("malformed journal error = %v", err)
	}
	if module.stateRoot != nil {
		_ = module.stateRoot.Close()
		t.Fatal("failed journal initialization retained the opened state root")
	}
}

func TestT916RunnerProviderJITOwnershipACKSurvivesRestartWithoutRawToken(t *testing.T) {
	stateDir := t.TempDir()
	config := map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      stateDir,
	}
	first, err := newGitHubRunnerProviderModule("provider", config, &fakeRunnerClient{preflight: GitHubRunnerProviderPreflight{
		Organization:      "GoCodeAlone",
		RunnerGroup:       "ephemeral",
		RunnerGroupID:     5,
		Ref:               "main",
		ResolvedRefSHA:    strings.Repeat("a", 40),
		ActionsEnabled:    true,
		SelfHostedAllowed: true,
	}})
	if err != nil {
		t.Fatalf("first module: %v", err)
	}
	created, err := first.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	ownershipToken, _ := created["ownership_token"].(string)
	if ownershipToken == "" {
		t.Fatal("JIT ownership token missing")
	}
	if err := first.Stop(t.Context()); err != nil {
		t.Fatalf("stop first module: %v", err)
	}
	second, err := newGitHubRunnerProviderModule("provider", config, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("restarted module: %v", err)
	}
	if _, err := second.InvokeMethod("ack_org_jit_config", map[string]any{
		"organization":    "GoCodeAlone",
		"runner_id":       int64(42),
		"ownership_token": ownershipToken,
		"provider_token":  "provider-token",
	}); err != nil {
		t.Fatalf("ACK after restart: %v", err)
	}
	if err := second.Stop(t.Context()); err != nil {
		t.Fatalf("stop second module: %v", err)
	}
	journal, err := os.ReadFile(second.jitOwnershipJournalPath())
	if err != nil {
		t.Fatalf("read ownership journal: %v", err)
	}
	if bytes.Contains(journal, []byte(ownershipToken)) || bytes.Contains(journal, []byte(`"token_hash"`)) {
		t.Fatalf("owned journal retained token material:\n%s", journal)
	}
	if !bytes.Contains(journal, []byte(`"state": "owned"`)) {
		t.Fatalf("owned journal state missing:\n%s", journal)
	}
}

func TestT916RunnerProviderJITCleanupFailureRemainsJournaled(t *testing.T) {
	oldRetryInterval := jitOwnershipCleanupRetryInterval
	jitOwnershipCleanupRetryInterval = time.Millisecond
	t.Cleanup(func() { jitOwnershipCleanupRetryInterval = oldRetryInterval })
	removed := make(chan int64, jitOwnershipCleanupAttempts)
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removedRunnerIDs:   removed,
		removeOrgRunnerErr: errors.New("github unavailable"),
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 10 * time.Millisecond
	module.jitRetryTTL = time.Hour
	_, err = module.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	for range jitOwnershipCleanupAttempts {
		select {
		case runnerID := <-removed:
			if runnerID != 42 {
				t.Fatalf("cleanup attempted runner ID %d", runnerID)
			}
		case <-time.After(time.Second):
			t.Fatal("JIT cleanup did not exhaust bounded attempts")
		}
	}
	if err := module.Stop(t.Context()); err != nil {
		t.Fatalf("stop module after failed cleanup: %v", err)
	}
	module.pendingJITMu.Lock()
	pending := module.pendingJIT[pendingJITKey{organization: "gocodealone", runnerID: 42}]
	var lastCleanupStatus string
	var cleanupAttempts int
	if pending != nil {
		lastCleanupStatus = pending.lastCleanupStatus
		cleanupAttempts = pending.cleanupAttempts
	}
	module.pendingJITMu.Unlock()
	if pending == nil || lastCleanupStatus != "remove_failed" || cleanupAttempts != jitOwnershipCleanupAttempts {
		t.Fatalf("retained failed cleanup state: status=%q attempts=%d", lastCleanupStatus, cleanupAttempts)
	}
	journal, err := os.ReadFile(module.jitOwnershipJournalPath())
	if err != nil {
		t.Fatalf("read ownership journal: %v", err)
	}
	for _, want := range []string{`"runner_id": 42`, `"last_cleanup_status": "remove_failed"`, `"cleanup_attempts": 5`} {
		if !bytes.Contains(journal, []byte(want)) {
			t.Fatalf("ownership journal missing %s:\n%s", want, journal)
		}
	}
}

func TestT916RunnerProviderJournalFailureFallsBackToExactIDCleanup(t *testing.T) {
	removed := make(chan int64, 1)
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{removedRunnerIDs: removed})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	if err := module.stateRoot.Close(); err != nil {
		t.Fatalf("close state root to simulate journal failure: %v", err)
	}
	if _, err := module.trackPendingJIT("GoCodeAlone", 42); err == nil {
		t.Fatal("track JIT ownership unexpectedly persisted through closed journal root")
	}
	select {
	case runnerID := <-removed:
		if runnerID != 42 {
			t.Fatalf("journal failure cleanup runner ID = %d", runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("journal failure did not fall back to exact-ID cleanup")
	}
	if err := module.Stop(t.Context()); err == nil {
		t.Fatal("Stop unexpectedly persisted through closed journal root")
	}
}

func TestT916RunnerProviderRetainsCommittedStateAfterDirectorySyncFailure(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	syncCalls := 0
	module.journalDirectorySync = func() error {
		syncCalls++
		if syncCalls == 1 {
			return errors.New("simulated directory sync failure")
		}
		return nil
	}
	key := pendingJITKey{organization: "gocodealone", runnerID: 42}
	module.pendingJITMu.Lock()
	module.pendingJIT[key] = &pendingJITOwnership{
		organization: "GoCodeAlone", tokenHash: sha256.Sum256([]byte("ownership-token")), expiresAt: time.Now().UTC().Add(time.Minute),
	}
	if err := module.persistJITOwnershipJournalLocked(); err == nil || !strings.Contains(err.Error(), "durability is uncertain") {
		module.pendingJITMu.Unlock()
		t.Fatalf("post-rename sync uncertainty error = %v", err)
	}
	if !module.journalDirectorySyncPending {
		module.pendingJITMu.Unlock()
		t.Fatal("post-rename sync uncertainty was not retained")
	}
	if err := module.persistJITOwnershipJournalLocked(); err != nil {
		module.pendingJITMu.Unlock()
		t.Fatalf("retry pending directory sync: %v", err)
	}
	if module.journalDirectorySyncPending {
		module.pendingJITMu.Unlock()
		t.Fatal("successful retry did not clear directory sync uncertainty")
	}
	module.pendingJITMu.Unlock()
	journal, err := os.ReadFile(module.jitOwnershipJournalPath())
	if err != nil || !bytes.Contains(journal, []byte(`"runner_id": 42`)) {
		t.Fatalf("committed journal state: data=%s err=%v", journal, err)
	}
}

func TestT916RunnerProviderDoesNotIssueOwnershipTokenAfterDirectorySyncFailure(t *testing.T) {
	removed := make(chan int64, 1)
	removeStarted := make(chan struct{}, 1)
	removeRelease := make(chan struct{})
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization: "GoCodeAlone", RunnerGroup: "ephemeral", RunnerGroupID: 5,
			ResolvedWorkflowPath: ".github/workflows/dogfood.yml", ResolvedRefSHA: strings.Repeat("a", 40),
			ActionsEnabled: true, SelfHostedAllowed: true,
		},
		removedRunnerIDs: removed,
		removeStarted:    removeStarted,
		removeRelease:    removeRelease,
	})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	syncCalls := 0
	module.journalDirectorySync = func() error {
		syncCalls++
		if syncCalls == 2 {
			return errors.New("simulated post-registration directory sync failure")
		}
		return nil
	}

	type invokeResult struct {
		result map[string]any
		err    error
	}
	done := make(chan invokeResult, 1)
	go func() {
		result, invokeErr := module.InvokeMethod("org_jit_config", map[string]any{
			"organization": "GoCodeAlone", "repository": "GoCodeAlone/workflow-compute", "workflow": "dogfood.yml", "ref": "main",
			"runner_name": "wfc-stg-ghp-linux-worker-task", "runner_group": "ephemeral",
			"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
			"provider_token": "provider-token",
		})
		done <- invokeResult{result: result, err: invokeErr}
	}()
	select {
	case <-removeStarted:
	case <-time.After(time.Second):
		t.Fatal("durability failure did not begin exact-ID cleanup")
	}
	select {
	case got := <-done:
		close(removeRelease)
		t.Fatalf("JIT config returned before exact-ID cleanup completed: result=%+v err=%v", got.result, got.err)
	case <-time.After(25 * time.Millisecond):
	}
	close(removeRelease)
	select {
	case runnerID := <-removed:
		if runnerID != 42 {
			t.Fatalf("durability failure cleanup runner ID = %d", runnerID)
		}
	case <-time.After(time.Second):
		t.Fatal("durability failure did not trigger exact-ID cleanup")
	}
	got := <-done
	if got.err == nil || !strings.Contains(got.err.Error(), "durability is uncertain") {
		t.Fatalf("JIT config durability error = %v", got.err)
	}
	if got.result != nil {
		t.Fatalf("JIT config returned ownership material after durability failure: %+v", got.result)
	}
}

func TestT916RunnerProviderStopReportsDirectorySyncFailure(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.journalDirectorySync = func() error { return errors.New("simulated directory sync failure") }
	if err := module.Stop(t.Context()); err == nil || !strings.Contains(err.Error(), "sync") {
		t.Fatalf("Stop directory sync error = %v", err)
	}
}

func TestT916RunnerProviderJITExpiryAndACKHaveSingleWinner(t *testing.T) {
	cleanupStarted := make(chan struct{}, 1)
	cleanupRelease := make(chan struct{})
	fake := &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removeStarted: cleanupStarted,
		removeRelease: cleanupRelease,
	}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 10 * time.Millisecond
	created, err := module.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("JIT expiry cleanup did not start")
	}
	_, ackErr := module.InvokeMethod("ack_org_jit_config", map[string]any{
		"organization":    "GoCodeAlone",
		"runner_id":       int64(42),
		"ownership_token": created["ownership_token"],
		"provider_token":  "provider-token",
	})
	if !errors.Is(ackErr, errJITOwnershipNotFound) {
		t.Fatalf("ACK raced deleting ownership: %v", ackErr)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- module.Stop(t.Context()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before in-flight cleanup quiesced: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(cleanupRelease)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("stop module after cleanup: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not wait for cleanup completion")
	}
}

func TestT916RunnerProviderStopReportsSyncFailureFromInflightCleanup(t *testing.T) {
	cleanupStarted := make(chan struct{}, 1)
	cleanupRelease := make(chan struct{})
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization: "GoCodeAlone", RunnerGroup: "ephemeral", RunnerGroupID: 5,
			ResolvedRefSHA: strings.Repeat("a", 40), ActionsEnabled: true, SelfHostedAllowed: true,
		},
		removeStarted: cleanupStarted,
		removeRelease: cleanupRelease,
	})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	var failSync atomic.Bool
	module.journalDirectorySync = func() error {
		if failSync.Load() {
			return errors.New("simulated cleanup directory sync failure")
		}
		return nil
	}
	module.jitOwnershipTTL = 10 * time.Millisecond
	if _, err := module.InvokeMethod("org_jit_config", map[string]any{
		"organization": "GoCodeAlone", "repository": "GoCodeAlone/workflow-compute", "workflow": "dogfood.yml", "ref": "main",
		"runner_name": "wfc-stg-ghp-linux-worker-task", "runner_group": "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	}); err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("JIT expiry cleanup did not start")
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- module.Stop(t.Context()) }()
	select {
	case err := <-stopDone:
		t.Fatalf("Stop returned before cleanup release: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	failSync.Store(true)
	close(cleanupRelease)
	select {
	case err := <-stopDone:
		if err == nil || !strings.Contains(err.Error(), "sync") {
			t.Fatalf("Stop in-flight cleanup sync error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not finish after cleanup release")
	}
}

func TestT916RunnerProviderStopHonorsContextDeadline(t *testing.T) {
	cleanupStarted := make(chan struct{}, 1)
	cleanupRelease := make(chan struct{})
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization: "GoCodeAlone", RunnerGroup: "ephemeral", RunnerGroupID: 5,
			ResolvedRefSHA: strings.Repeat("a", 40), ActionsEnabled: true, SelfHostedAllowed: true,
		},
		removeStarted: cleanupStarted,
		removeRelease: cleanupRelease,
	})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	module.jitOwnershipTTL = 10 * time.Millisecond
	if _, err := module.InvokeMethod("org_jit_config", map[string]any{
		"organization": "GoCodeAlone", "repository": "GoCodeAlone/workflow-compute", "workflow": "dogfood.yml", "ref": "main",
		"runner_name": "wfc-stg-ghp-linux-worker-task", "runner_group": "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	}); err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	select {
	case <-cleanupStarted:
	case <-time.After(time.Second):
		t.Fatal("JIT expiry cleanup did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	stopDone := make(chan error, 1)
	go func() { stopDone <- module.Stop(ctx) }()
	select {
	case stopErr := <-stopDone:
		close(cleanupRelease)
		module.cleanupWG.Wait()
		if !errors.Is(stopErr, context.DeadlineExceeded) {
			t.Fatalf("Stop deadline error = %v", stopErr)
		}
	case <-time.After(200 * time.Millisecond):
		close(cleanupRelease)
		stopErr := <-stopDone
		t.Fatalf("Stop ignored context deadline: %v", stopErr)
	}
}

func TestT916RunnerProviderIgnoresStaleExpiryAfterACKReplacesTimer(t *testing.T) {
	removed := make(chan int64, 1)
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"ephemeral"},
		"state_dir":      t.TempDir(),
	}, &fakeRunnerClient{
		preflight: GitHubRunnerProviderPreflight{
			Organization:      "GoCodeAlone",
			RunnerGroup:       "ephemeral",
			RunnerGroupID:     5,
			Ref:               "main",
			ResolvedRefSHA:    strings.Repeat("a", 40),
			ActionsEnabled:    true,
			SelfHostedAllowed: true,
		},
		removedRunnerIDs: removed,
	})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	created, err := module.InvokeMethod("org_jit_config", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	})
	if err != nil {
		t.Fatalf("create JIT config: %v", err)
	}
	key := pendingJITKey{organization: "gocodealone", runnerID: 42}
	module.pendingJITMu.Lock()
	pending := module.pendingJIT[key]
	staleGeneration := pending.timerGeneration
	module.pendingJITMu.Unlock()
	if _, err := module.InvokeMethod("ack_org_jit_config", map[string]any{
		"organization":    "GoCodeAlone",
		"runner_id":       int64(42),
		"ownership_token": created["ownership_token"],
		"provider_token":  "provider-token",
	}); err != nil {
		t.Fatalf("ACK JIT config: %v", err)
	}
	module.expirePendingJIT(key, pending, staleGeneration)
	select {
	case runnerID := <-removed:
		t.Fatalf("stale expiry deleted acknowledged runner %d", runnerID)
	case <-time.After(25 * time.Millisecond):
	}
	if err := module.Stop(t.Context()); err != nil {
		t.Fatalf("stop module: %v", err)
	}
}

func TestT916RunnerProviderJITRejectsIncompleteOrForgedIdentity(t *testing.T) {
	base := map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_name":    "wfc-stg-ghp-linux-worker-task",
		"runner_group":   "ephemeral",
		"labels":         []string{"self-hosted", "linux", "wfc-stg-ghp-linux-worker-task", "wfc-ghp-stg", "wfc-ghp-ephemeral"},
		"provider_token": "provider-token",
	}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing workflow", mutate: func(args map[string]any) { args["workflow"] = "" }},
		{name: "missing ref", mutate: func(args map[string]any) { args["ref"] = "" }},
		{name: "missing labels", mutate: func(args map[string]any) { args["labels"] = []string{} }},
		{name: "forged label", mutate: func(args map[string]any) { args["labels"] = []string{"self-hosted", "linux", "forged"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeRunnerClient{}
			module, err := newGitHubRunnerProviderModule("provider", map[string]any{
				"token":          "github-token",
				"provider_token": "provider-token",
				"organizations":  []any{"GoCodeAlone"},
				"repositories":   []any{"GoCodeAlone/workflow-compute"},
				"runner_groups":  []any{"ephemeral"},
			}, fake)
			if err != nil {
				t.Fatalf("module: %v", err)
			}
			args := make(map[string]any, len(base))
			for key, value := range base {
				args[key] = value
			}
			tc.mutate(args)
			if _, err := module.InvokeMethod("org_jit_config", args); err == nil {
				t.Fatal("invalid JIT identity accepted")
			}
			if fake.preflightOrganization != "" {
				t.Fatalf("invalid JIT identity reached GitHub preflight for %q", fake.preflightOrganization)
			}
		})
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
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"workflow-compute-stg"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	result, err := module.InvokeMethod("preflight", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
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

func TestT916GitHubRunnerProviderModulePreflightRequiresRepositoryAllowlist(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"runner_groups":  []any{"ephemeral"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	_, err = module.InvokeMethod("preflight", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"runner_group":   "ephemeral",
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errRepositoryNotAllowlisted) {
		t.Fatalf("preflight repository allowlist error = %v", err)
	}
}

func TestT593_GitHubRunnerProviderModuleRejectsMissingAllowlistedRunnerGroup(t *testing.T) {
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
		"runner_groups":  []any{"workflow-compute-stg"},
	}, &fakeRunnerClient{})
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("preflight", map[string]any{
		"organization":   "GoCodeAlone",
		"repository":     "GoCodeAlone/workflow-compute",
		"workflow":       "dogfood.yml",
		"ref":            "main",
		"labels":         []any{"wfc-ghp-stg"},
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errRunnerGroupNotAllowlisted) {
		t.Fatalf("preflight err: got %v want %v", err, errRunnerGroupNotAllowlisted)
	}
}

func TestT593_GitHubRunnerProviderModuleRemovesOrgRunner(t *testing.T) {
	fake := &fakeRunnerClient{}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	if _, err := module.trackPendingJIT("GoCodeAlone", 42); err != nil {
		t.Fatalf("track owned JIT runner: %v", err)
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
	if fake.removedRunnerID != 42 {
		t.Fatalf("removed runner ID = %d", fake.removedRunnerID)
	}
}

func TestT916GitHubRunnerProviderModuleRejectsUnownedOrgRunnerRemoval(t *testing.T) {
	fake := &fakeRunnerClient{}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}

	_, err = module.InvokeMethod("remove_org_runner", map[string]any{
		"organization":   "GoCodeAlone",
		"runner_id":      int64(42),
		"provider_token": "provider-token",
	})
	if !errors.Is(err, errJITOwnershipNotFound) {
		t.Fatalf("unowned runner removal error = %v, want ownership rejection", err)
	}
	if fake.removedRunnerID != 0 {
		t.Fatalf("unowned runner %d reached GitHub delete boundary", fake.removedRunnerID)
	}
}

func TestT916GitHubRunnerProviderHTTPRejectsUnownedOrgRunnerRemoval(t *testing.T) {
	fake := &fakeRunnerClient{}
	module, err := newGitHubRunnerProviderModule("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"organizations":  []any{"GoCodeAlone"},
	}, fake)
	if err != nil {
		t.Fatalf("module: %v", err)
	}
	server := httptest.NewServer(module.HTTPHandler())
	defer server.Close()

	req, err := http.NewRequest(http.MethodDelete, server.URL+"/v1/actions/orgs/GoCodeAlone/runners/42", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer provider-token")
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("provider request: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unowned runner removal status = %d body=%s", resp.StatusCode, readResponseBody(t, resp))
	}
	if fake.removedRunnerID != 0 {
		t.Fatalf("unowned runner %d reached GitHub delete boundary", fake.removedRunnerID)
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
		fields[field.Name] = field.Required
	}
	for _, want := range []string{"organizations", "runner_groups"} {
		if _, ok := fields[want]; !ok {
			t.Fatalf("github.runner_provider schema missing config field %q", want)
		}
	}
	if required, ok := fields["state_dir"]; !ok || required {
		t.Fatalf("github.runner_provider state_dir field: present=%t required=%t, want optional for repository-only compatibility", ok, required)
	}
	inputs := map[string]bool{}
	for _, input := range runner.Inputs {
		inputs[input.Name] = true
	}
	for _, want := range []string{"preflight", "org_registration_token", "org_jit_config", "ack_org_jit_config", "org_runner", "remove_org_runner"} {
		if !inputs[want] {
			t.Fatalf("github.runner_provider schema missing input %q", want)
		}
	}
}

func TestT916RunnerProviderHTTPHandlerPreservesRepositoryOnlyCompatibility(t *testing.T) {
	handler, err := NewGitHubRunnerProviderHTTPHandler("provider", map[string]any{
		"token":          "github-token",
		"provider_token": "provider-token",
		"repositories":   []any{"GoCodeAlone/workflow-compute"},
	})
	if err != nil {
		t.Fatalf("repository-only handler: %v", err)
	}
	if handler == nil {
		t.Fatal("repository-only handler is nil")
	}
}

func TestT593_RunnerProviderProtoConfigDeclaresOrgFields(t *testing.T) {
	desc := (&githubv1.RunnerProviderModuleConfig{}).ProtoReflect().Descriptor()
	for _, want := range []protoreflect.Name{"organizations", "runner_groups", "state_dir"} {
		if field := desc.Fields().ByName(want); field == nil {
			t.Fatalf("RunnerProviderModuleConfig missing proto field %s", want)
		}
	}
}

func TestT916StringListArgRejectsMixedTypedValues(t *testing.T) {
	values, err := stringListArg([]any{"linux", 42, "docker"})
	if err == nil || !strings.Contains(err.Error(), "index 1") {
		t.Fatalf("mixed list error = %v", err)
	}
	if values != nil {
		t.Fatalf("mixed list values = %#v, want nil", values)
	}
}

func TestT916StringMapArgRejectsNonStringValues(t *testing.T) {
	values, err := stringMapArg(map[string]any{"valid": "kept", "invalid": 42})
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("mixed map error = %v", err)
	}
	if values != nil {
		t.Fatalf("mixed map values = %#v, want nil", values)
	}
}

type fakeRunnerClient struct {
	token                          GitHubRunnerRegistrationToken
	registrationRepository         string
	orgRegistrationOrganization    string
	removedRepository              string
	removedOrganization            string
	removedRunnerID                int64
	removedRunnerIDs               chan int64
	removeOrgRunnerErr             error
	removeStarted                  chan struct{}
	removeRelease                  chan struct{}
	preflight                      GitHubRunnerProviderPreflight
	preflightOrganization          string
	dispatchedRepository           string
	dispatchedWorkflow             string
	dispatchedRef                  string
	dispatchedInputs               map[string]string
	dispatchedExpectedWorkflowPath string
	workflowDispatch               GitHubWorkflowDispatch
	dispatchErr                    error
	listRunsRepository             string
	listRunsWorkflow               string
	listRunsCreatedAfter           time.Time
	listJobsRepository             string
	listJobsRunID                  int64
	workflowRuns                   []GitHubWorkflowRun
	workflowRun                    GitHubWorkflowRun
	workflowJobs                   []GitHubWorkflowJob
	jitConfig                      GitHubRunnerJITConfig
	jitRequest                     GitHubRunnerJITConfigRequest
	jitErr                         error
}

func (f *fakeRunnerClient) GetWorkflowRun(_ context.Context, owner, repo string, runID int64, _ string) (GitHubWorkflowRun, error) {
	f.listRunsRepository = owner + "/" + repo
	if f.workflowRun.ID == 0 {
		f.workflowRun.ID = runID
	}
	return f.workflowRun, nil
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

func (f *fakeRunnerClient) GenerateOrgJITConfig(_ context.Context, req GitHubRunnerJITConfigRequest, _ string) (GitHubRunnerJITConfig, error) {
	f.jitRequest = req
	if f.jitConfig.RunnerID == 0 {
		f.jitConfig = GitHubRunnerJITConfig{RunnerID: 42, RunnerName: req.RunnerName, EncodedJITConfig: "encoded-jit-config"}
	}
	return f.jitConfig, f.jitErr
}

func (f *fakeRunnerClient) RemoveOrgRunner(_ context.Context, organization string, runnerID int64, _ string) error {
	f.removedOrganization = organization
	f.removedRunnerID = runnerID
	if f.removedRunnerIDs != nil {
		f.removedRunnerIDs <- runnerID
	}
	if f.removeStarted != nil {
		select {
		case f.removeStarted <- struct{}{}:
		default:
		}
	}
	if f.removeRelease != nil {
		<-f.removeRelease
	}
	return f.removeOrgRunnerErr
}

func (f *fakeRunnerClient) GetOrgRunner(_ context.Context, organization string, runnerID int64, _ string) (GitHubOrgRunner, error) {
	return GitHubOrgRunner{ID: runnerID, Name: f.jitRequest.RunnerName, Status: "online", Labels: append([]string(nil), f.jitRequest.Labels...)}, nil
}

func (f *fakeRunnerClient) PreflightOrg(_ context.Context, req GitHubRunnerProviderPreflightRequest, _ string) (GitHubRunnerProviderPreflight, error) {
	f.preflightOrganization = req.Organization
	if f.preflight.ResolvedWorkflowPath == "" {
		f.preflight.ResolvedWorkflowPath = ".github/workflows/dogfood.yml"
	}
	return f.preflight, nil
}

func (f *fakeRunnerClient) DispatchWorkflow(_ context.Context, owner, repo, workflow, ref string, inputs map[string]string, expectedWorkflowPath, expectedHeadSHA, _ string) (GitHubWorkflowDispatch, error) {
	f.dispatchedRepository = owner + "/" + repo
	f.dispatchedWorkflow = workflow
	f.dispatchedRef = ref
	f.dispatchedInputs = inputs
	f.dispatchedExpectedWorkflowPath = expectedWorkflowPath
	if f.workflowDispatch.WorkflowRunID == 0 {
		f.workflowDispatch = GitHubWorkflowDispatch{WorkflowRunID: 28449657934, RunURL: "https://api.github.com/runs/28449657934", HTMLURL: "https://github.com/runs/28449657934", ValidatedHeadSHA: expectedHeadSHA}
	}
	return f.workflowDispatch, f.dispatchErr
}

func (f *fakeRunnerClient) ListWorkflowRuns(_ context.Context, owner, repo, workflow string, createdAfter time.Time, _ string) ([]GitHubWorkflowRun, error) {
	f.listRunsRepository = owner + "/" + repo
	f.listRunsWorkflow = workflow
	f.listRunsCreatedAfter = createdAfter
	return f.workflowRuns, nil
}

func (f *fakeRunnerClient) ListWorkflowRunJobs(_ context.Context, owner, repo string, runID int64, _ string) ([]GitHubWorkflowJob, error) {
	f.listJobsRepository = owner + "/" + repo
	f.listJobsRunID = runID
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

func writeRunnerWorkflowRefFixture(t *testing.T, w http.ResponseWriter, r *http.Request) bool {
	t.Helper()
	switch r.URL.Path {
	case "/repos/GoCodeAlone/workflow-compute/actions/workflows/dogfood.yml", "/repos/GoCodeAlone/workflow-compute/actions/workflows/Dogfood.yml":
		writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"id": 42, "path": ".github/workflows/dogfood.yml", "state": "active"})
		return true
	case "/repos/GoCodeAlone/workflow-compute/git/ref/heads/main", "/repos/GoCodeAlone/workflow-compute/git/ref/heads/release":
		writeRunnerProviderJSON(t, w, http.StatusOK, map[string]any{"ref": strings.TrimPrefix(r.URL.Path, "/repos/GoCodeAlone/workflow-compute/git/ref/"), "object": map[string]any{"sha": strings.Repeat("a", 40)}})
		return true
	case "/repos/GoCodeAlone/workflow-compute/git/ref/tags/main", "/repos/GoCodeAlone/workflow-compute/git/ref/tags/release":
		writeRunnerProviderJSON(t, w, http.StatusNotFound, map[string]any{"message": "Not Found"})
		return true
	default:
		return false
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
