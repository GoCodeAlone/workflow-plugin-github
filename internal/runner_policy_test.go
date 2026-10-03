package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func runnerPolicyFixture() RunnerPolicyDesired {
	return RunnerPolicyDesired{
		SchemaVersion: "github-runner-policy.v1", Organization: "GoCodeAlone", RunnerGroup: "ephemeral",
		Repository: "GoCodeAlone/workflow-compute", Workflow: "GoCodeAlone/workflow-compute/.github/workflows/dogfood-provider-target.yml@refs/heads/main",
		Visibility: "selected", AllowsPublicRepositories: false, RestrictedToWorkflows: true,
		RunnerIDs: []int64{}, ProtectedRunnerIDs: []int64{279, 280, 63},
		Variable: RunnerPolicyVariable{Name: "WORKFLOW_COMPUTE_STAGING_URL", Value: "https://staging.example.test"},
	}
}

type policyAPIFake struct {
	mu              sync.Mutex
	server          *httptest.Server
	group           policyGroup
	repoIDs         []int64
	variable        *RunnerPolicyVariable
	retained        bool
	loseOnce        string
	writes          []string
	crossOrigin     string
	badReadback     bool
	missingList     bool
	missingSafety   bool
	variableFailure bool
	interruptWrite  string
	beforeWrite     bool
	blockReads      bool
}

func newPolicyAPIFake(t *testing.T) *policyAPIFake {
	t.Helper()
	f := &policyAPIFake{
		group:   policyGroup{ID: 2, Name: "ephemeral", Visibility: "selected", SelectedWorkflows: []string{}},
		repoIDs: []int64{17, 18},
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.server.Close)
	return f
}

func (f *policyAPIFake) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer known-private-value" {
		w.WriteHeader(401)
		return
	}
	path := r.URL.Path
	if r.Method == "GET" && f.blockReads {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	page2 := r.URL.Query().Get("page") == "2"
	writeJSON := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
	runner := func(id int64) policyRunner {
		return policyRunner{ID: id, Name: fmt.Sprintf("runner-%d", id), OS: "linux", Labels: []policyLabel{{Name: "self-hosted"}}}
	}
	if r.Method != "GET" {
		f.writes = append(f.writes, r.Method+" "+path)
		if f.interruptWrite == r.Method+" "+path {
			f.blockReads = true
			if f.beforeWrite {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
		switch {
		case r.Method == "PATCH" && path == "/orgs/GoCodeAlone/actions/runner-groups/2":
			var patch struct {
				Name string `json:"name"`
				policyGroupPatch
			}
			if json.NewDecoder(r.Body).Decode(&patch) != nil {
				w.WriteHeader(400)
				return
			}
			if patch.Name == "" || patch.Name != f.group.Name {
				w.WriteHeader(http.StatusUnprocessableEntity)
				return
			}
			f.group.Visibility, f.group.AllowsPublicRepositories, f.group.RestrictedToWorkflows, f.group.SelectedWorkflows = patch.Visibility, patch.AllowsPublicRepositories, patch.RestrictedToWorkflows, patch.SelectedWorkflows
		case r.Method == "PUT" && path == "/orgs/GoCodeAlone/actions/runner-groups/2/repositories":
			var put struct {
				IDs []int64 `json:"selected_repository_ids"`
			}
			if json.NewDecoder(r.Body).Decode(&put) != nil {
				w.WriteHeader(400)
				return
			}
			f.repoIDs = put.IDs
		case (r.Method == "POST" && path == "/repos/GoCodeAlone/workflow-compute/actions/variables") || (r.Method == "PATCH" && path == "/repos/GoCodeAlone/workflow-compute/actions/variables/WORKFLOW_COMPUTE_STAGING_URL"):
			var variable RunnerPolicyVariable
			if json.NewDecoder(r.Body).Decode(&variable) != nil {
				w.WriteHeader(400)
				return
			}
			f.variable = &variable
		case r.Method == "DELETE" && path == "/repos/GoCodeAlone/workflow-compute/actions/variables/WORKFLOW_COMPUTE_STAGING_URL":
			f.variable = nil
		default:
			w.WriteHeader(500)
			_, _ = w.Write([]byte("known-private-value forbidden mutation"))
			return
		}
		if f.loseOnce == r.Method+" "+path {
			f.loseOnce = ""
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if r.Method == "PATCH" && strings.Contains(path, "runner-groups") {
			writeJSON(f.group)
			return
		}
		if r.Method == "POST" {
			w.WriteHeader(201)
		} else {
			w.WriteHeader(204)
		}
		return
	}
	switch path {
	case "/repos/GoCodeAlone/workflow-compute":
		writeJSON(policyRepository{ID: 17, FullName: "GoCodeAlone/workflow-compute", Private: true})
	case "/orgs/GoCodeAlone/actions/runner-groups":
		if !page2 {
			link := f.server.URL + path + "?page=2"
			if f.crossOrigin != "" {
				link = f.crossOrigin
			}
			w.Header().Set("Link", "<"+link+">; rel=\"next\"")
			writeJSON(policyPage{TotalCount: 2, RunnerGroups: []policyGroup{{ID: 1, Name: "Default", Visibility: "all", Default: true, SelectedWorkflows: []string{}}}})
		} else if f.missingSafety {
			data, _ := json.Marshal(f.group)
			data = bytes.Replace(data, []byte(`"inherited":false,`), nil, 1)
			_, _ = fmt.Fprintf(w, `{"total_count":2,"runner_groups":[%s]}`, data)
		} else {
			writeJSON(policyPage{TotalCount: 2, RunnerGroups: []policyGroup{f.group}})
		}
	case "/orgs/GoCodeAlone/actions/runner-groups/2":
		writeJSON(f.group)
	case "/orgs/GoCodeAlone/actions/runner-groups/2/repositories":
		repos := make([]policyRepository, 0, len(f.repoIDs))
		for _, id := range f.repoIDs {
			repos = append(repos, policyRepository{ID: id})
		}
		if f.badReadback && len(f.writes) > 0 {
			repos = append(repos, policyRepository{ID: 999})
		}
		writeJSON(policyPage{TotalCount: len(repos), Repositories: repos})
	case "/orgs/GoCodeAlone/actions/runner-groups/1/runners", "/orgs/GoCodeAlone/actions/runners":
		if !page2 {
			w.Header().Set("Link", "<"+f.server.URL+path+"?page=2>; rel=\"next\"")
			writeJSON(policyPage{TotalCount: 3, Runners: []policyRunner{runner(279), runner(280)}})
		} else {
			writeJSON(policyPage{TotalCount: 3, Runners: []policyRunner{runner(63)}})
		}
	case "/orgs/GoCodeAlone/actions/runner-groups/2/runners":
		if f.missingList {
			_, _ = w.Write([]byte(`{"total_count":0}`))
			return
		}
		if f.retained {
			writeJSON(policyPage{TotalCount: 1, Runners: []policyRunner{runner(63)}})
		} else {
			writeJSON(policyPage{TotalCount: 0, Runners: []policyRunner{}})
		}
	case "/repos/GoCodeAlone/workflow-compute/actions/variables/WORKFLOW_COMPUTE_STAGING_URL":
		if f.variableFailure {
			w.WriteHeader(500)
			_, _ = w.Write([]byte("known-private-value"))
			return
		}
		if f.variable == nil {
			w.WriteHeader(404)
		} else {
			writeJSON(f.variable)
		}
	default:
		w.WriteHeader(404)
	}
}

func policyReconcilerFixture(t *testing.T, f *policyAPIFake, dir string) *runnerPolicyReconciler {
	t.Helper()
	client, err := newRunnerPolicyClient(f.server.URL, "known-private-value")
	if err != nil {
		t.Fatal(err)
	}
	return newRunnerPolicyReconciler(client, dir)
}

func TestRunnerPolicyGroupPatchPreservesRequiredName(t *testing.T) {
	f := newPolicyAPIFake(t)
	f.group.Name = "Ephemeral"
	desired := runnerPolicyFixture()
	desired.RunnerGroup = f.group.Name
	r := policyReconcilerFixture(t, f, t.TempDir())
	if _, err := r.Plan(context.Background(), "required-name", desired); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Apply(context.Background(), "required-name"); err != nil {
		t.Fatalf("PATCH must include the captured unchanged group name: %v", err)
	}
	if _, err := r.Rollback(context.Background(), "required-name"); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.group.Name != "Ephemeral" {
		t.Fatal("policy mutation renamed the runner group")
	}
}

func TestRunnerPolicyDuplicateInitialAuthorityDenied(t *testing.T) {
	f := newPolicyAPIFake(t)
	dir := t.TempDir()
	r := policyReconcilerFixture(t, f, dir)
	desired := runnerPolicyFixture()
	if _, err := r.Plan(context.Background(), "duplicate", desired); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, runnerPolicyAuditFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, data...), 0600); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() (RunnerPolicyResult, error){
		func() (RunnerPolicyResult, error) { return r.Plan(context.Background(), "duplicate", desired) },
		func() (RunnerPolicyResult, error) { return r.Apply(context.Background(), "duplicate") },
		func() (RunnerPolicyResult, error) { return r.Rollback(context.Background(), "duplicate") },
	} {
		if _, err := operation(); err == nil || !strings.Contains(err.Error(), "invalid initial authority") {
			t.Fatalf("duplicate initial authority was not denied: %v", err)
		}
	}
	if len(f.writes) != 0 {
		t.Fatal("duplicate authority denial mutated GitHub")
	}
}

func TestRunnerPolicyStrictSchemaAndBroadeningDenial(t *testing.T) {
	desired := runnerPolicyFixture()
	data, _ := json.Marshal(desired)
	for _, mutate := range []func([]byte) []byte{
		func(d []byte) []byte { return append(d, []byte(` {}`)...) },
		func(d []byte) []byte {
			return bytes.Replace(d, []byte(`"visibility":"selected"`), []byte(`"visibility":"all"`), 1)
		},
		func(d []byte) []byte {
			return bytes.Replace(d, []byte(`"restricted_to_workflows":true`), []byte(`"restricted_to_workflows":false`), 1)
		},
		func(d []byte) []byte {
			return bytes.Replace(d, []byte(`"runner_ids":[]`), []byte(`"runner_ids":[63]`), 1)
		},
		func(d []byte) []byte {
			return bytes.Replace(d, []byte(`"runner_group":"ephemeral"`), []byte(`"runner_group":"Default"`), 1)
		},
		func(d []byte) []byte {
			return bytes.Replace(d, []byte(`"variable":{`), []byte(`"variable":{"token":"known-private-value",`), 1)
		},
		func(d []byte) []byte { return bytes.Replace(d, []byte(`@refs/heads/main`), []byte(`@refs/heads/*`), 1) },
	} {
		if _, err := decodeRunnerPolicy(bytes.NewReader(mutate(bytes.Clone(data)))); err == nil || strings.Contains(err.Error(), "known-private-value") {
			t.Fatalf("unsafe policy accepted/disclosed: %v", err)
		}
	}
	if _, err := decodeRunnerPolicy(bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPolicyPaginationApplyRetryRollbackAndAudit(t *testing.T) {
	for _, lost := range []string{"", "PATCH /orgs/GoCodeAlone/actions/runner-groups/2", "PUT /orgs/GoCodeAlone/actions/runner-groups/2/repositories", "POST /repos/GoCodeAlone/workflow-compute/actions/variables"} {
		t.Run(lost, func(t *testing.T) {
			f := newPolicyAPIFake(t)
			f.loseOnce = lost
			dir := t.TempDir()
			r := policyReconcilerFixture(t, f, dir)
			if _, err := r.Plan(context.Background(), "transaction-1", runnerPolicyFixture()); err != nil {
				t.Fatal(err)
			}
			if len(f.writes) != 0 {
				t.Fatal("plan mutated GitHub")
			}
			result, err := r.Apply(context.Background(), "transaction-1")
			if err != nil {
				t.Fatal(err)
			}
			if result.Phase != "applied" || !f.group.RestrictedToWorkflows || !reflect.DeepEqual(f.repoIDs, []int64{17}) || !reflect.DeepEqual(f.group.SelectedWorkflows, []string{runnerPolicyFixture().Workflow}) || f.variable == nil || f.variable.Value != runnerPolicyFixture().Variable.Value {
				t.Fatalf("wrong exact readback: %+v", result)
			}
			writes := len(f.writes)
			r = policyReconcilerFixture(t, f, dir)
			if _, err := r.Apply(context.Background(), "transaction-1"); err != nil {
				t.Fatal(err)
			}
			if len(f.writes) != writes {
				t.Fatal("identical retry repeated writes")
			}
			before, err := os.ReadFile(filepath.Join(dir, "runner-policy-audit.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			result, err = r.Rollback(context.Background(), "transaction-1")
			if err != nil {
				t.Fatal(err)
			}
			if result.Phase != "rolled-back-restrictive" || f.variable != nil || !f.group.RestrictedToWorkflows || !reflect.DeepEqual(f.repoIDs, []int64{17}) {
				t.Fatal("rollback broadened access or failed variable restore")
			}
			after, err := os.ReadFile(filepath.Join(dir, "runner-policy-audit.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.HasPrefix(after, before) || bytes.Contains(after, []byte("known-private-value")) {
				t.Fatal("audit overwritten or credential disclosed")
			}
			for _, write := range f.writes {
				if strings.Contains(write, "/runners") || strings.Contains(write, "/runner-groups/1") || strings.Contains(write, "registration-token") {
					t.Fatalf("protected state mutated: %s", write)
				}
			}
		})
	}
}

func TestRunnerPolicyRetainedReadbackPaginationAndJournalDenial(t *testing.T) {
	for _, mode := range []string{"retained", "pagination-origin", "readback", "audit-unwritable", "missing-list", "missing-safety", "variable-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := newPolicyAPIFake(t)
			dir := t.TempDir()
			if mode == "retained" {
				f.retained = true
			}
			if mode == "pagination-origin" {
				f.crossOrigin = "https://other.example.test/?known-private-value"
			}
			if mode == "readback" {
				f.badReadback = true
			}
			if mode == "missing-list" {
				f.missingList = true
			}
			if mode == "missing-safety" {
				f.missingSafety = true
			}
			if mode == "variable-failure" {
				f.variableFailure = true
			}
			if mode == "audit-unwritable" {
				if err := os.Mkdir(filepath.Join(dir, "runner-policy-audit.jsonl"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			r := policyReconcilerFixture(t, f, dir)
			_, err := r.Plan(context.Background(), "transaction-1", runnerPolicyFixture())
			if mode == "readback" && err == nil {
				_, err = r.Apply(context.Background(), "transaction-1")
			}
			if err == nil || strings.Contains(err.Error(), "known-private-value") {
				t.Fatalf("unsafe policy operation accepted/disclosed: %v", err)
			}
			if mode != "readback" && len(f.writes) != 0 {
				t.Fatal("denial wrote GitHub state")
			}
		})
	}
}

func TestRunnerPolicyInterruptedIntentResumesWithoutRepeatingConfirmedWrite(t *testing.T) {
	for _, step := range []string{"PATCH /orgs/GoCodeAlone/actions/runner-groups/2", "PUT /orgs/GoCodeAlone/actions/runner-groups/2/repositories", "POST /repos/GoCodeAlone/workflow-compute/actions/variables"} {
		for _, beforeWrite := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/before=%t", step, beforeWrite), func(t *testing.T) {
				f := newPolicyAPIFake(t)
				f.interruptWrite, f.beforeWrite = step, beforeWrite
				dir := t.TempDir()
				r := policyReconcilerFixture(t, f, dir)
				if _, err := r.Plan(context.Background(), "interrupted", runnerPolicyFixture()); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Apply(context.Background(), "interrupted"); err == nil {
					t.Fatal("unconfirmed write incorrectly completed")
				}
				root, err := r.openAudit()
				if err != nil {
					t.Fatal(err)
				}
				_, last, err := r.load(root, "interrupted")
				_ = root.Close()
				if err != nil || last.Phase != "write-intent" {
					t.Fatalf("interrupted transaction lacks durable intent: %+v %v", last, err)
				}
				f.mu.Lock()
				f.blockReads, f.interruptWrite = false, ""
				f.mu.Unlock()
				r = policyReconcilerFixture(t, f, dir)
				result, err := r.Apply(context.Background(), "interrupted")
				if err != nil || result.Phase != "applied" {
					t.Fatalf("new reconciler failed to resume exact intent: %+v %v", result, err)
				}
				f.mu.Lock()
				defer f.mu.Unlock()
				count := 0
				for _, write := range f.writes {
					if write == step {
						count++
					}
				}
				want := 1
				if beforeWrite {
					want = 2
				}
				if count != want || f.variable == nil || f.variable.Value != runnerPolicyFixture().Variable.Value {
					t.Fatalf("resume repeated confirmed mutation or missed desired value: writes=%d want=%d", count, want)
				}
			})
		}
	}
}

func TestRunnerPolicyRollbackRestoresExistingVariableWithoutBroadening(t *testing.T) {
	f := newPolicyAPIFake(t)
	desired := runnerPolicyFixture()
	f.repoIDs = []int64{17}
	f.group.RestrictedToWorkflows = true
	f.group.SelectedWorkflows = []string{desired.Workflow}
	original := RunnerPolicyVariable{Name: desired.Variable.Name, Value: "https://previous.example.test"}
	f.variable = &original
	dir := t.TempDir()
	r := policyReconcilerFixture(t, f, dir)
	if _, err := r.Plan(context.Background(), "restore", desired); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Apply(context.Background(), "restore"); err != nil {
		t.Fatal(err)
	}
	r = policyReconcilerFixture(t, f, dir)
	result, err := r.Rollback(context.Background(), "restore")
	if err != nil || result.Phase != "rolled-back" {
		t.Fatalf("exact safe rollback failed: %+v %v", result, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.variable == nil || *f.variable != original || !reflect.DeepEqual(f.repoIDs, []int64{17}) || !reflect.DeepEqual(f.group.SelectedWorkflows, []string{desired.Workflow}) {
		t.Fatal("rollback failed exact variable or safe ACL readback")
	}
	if len(f.writes) != 2 || f.writes[0] != "PATCH /repos/GoCodeAlone/workflow-compute/actions/variables/WORKFLOW_COMPUTE_STAGING_URL" || f.writes[1] != f.writes[0] {
		t.Fatalf("safe rollback performed unrelated mutations: %v", f.writes)
	}
}
