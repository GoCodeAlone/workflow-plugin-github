package internal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	githubplugin "github.com/GoCodeAlone/workflow-plugin-github"
)

const runnerPolicySchemaVersion = "github-runner-policy.v1"
const runnerPolicyAuditFile = "runner-policy-audit.jsonl"

type RunnerPolicyVariable struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type RunnerPolicyDesired struct {
	SchemaVersion            string               `json:"schema_version"`
	Organization             string               `json:"organization"`
	RunnerGroup              string               `json:"runner_group"`
	Repository               string               `json:"repository"`
	Workflow                 string               `json:"workflow"`
	Visibility               string               `json:"visibility"`
	AllowsPublicRepositories bool                 `json:"allows_public_repositories"`
	RestrictedToWorkflows    bool                 `json:"restricted_to_workflows"`
	RunnerIDs                []int64              `json:"runner_ids"`
	ProtectedRunnerIDs       []int64              `json:"protected_runner_ids"`
	Variable                 RunnerPolicyVariable `json:"variable"`
}

var policyIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$`)
var policyVariableName = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,99}$`)
var policyWorkflowFile = regexp.MustCompile(`^[A-Za-z0-9_.-]+\.ya?ml$`)
var policyWorkflowRef = regexp.MustCompile(`^refs/(heads|tags)/[A-Za-z0-9_./-]+$`)

func decodeRunnerPolicy(reader io.Reader) (RunnerPolicyDesired, error) {
	var desired RunnerPolicyDesired
	data, err := io.ReadAll(io.LimitReader(reader, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return desired, errors.New("policy document exceeds the read limit")
	}
	if err := githubplugin.ValidateRunnerPolicyDocument(data); err != nil {
		return desired, errors.New("policy document does not match the strict schema")
	}
	if err := policyDecodeStrict(bytes.NewReader(data), &desired); err != nil {
		return desired, errors.New("policy document is not strict JSON")
	}
	return desired, desired.validate()
}

func (d RunnerPolicyDesired) validate() error {
	parts := strings.Split(d.Repository, "/")
	if d.SchemaVersion != runnerPolicySchemaVersion || !policyIdentifier.MatchString(d.Organization) || !policyIdentifier.MatchString(d.RunnerGroup) || strings.EqualFold(d.RunnerGroup, "Default") || len(parts) != 2 || !strings.EqualFold(parts[0], d.Organization) || !policyIdentifier.MatchString(parts[1]) {
		return errors.New("policy requires exact organization, repository, and non-Default runner group")
	}
	if d.Visibility != "selected" || d.AllowsPublicRepositories || !d.RestrictedToWorkflows || d.RunnerIDs == nil || len(d.RunnerIDs) != 0 {
		return errors.New("policy must select one repository and workflow with no public repositories or retained runners")
	}
	workflow := strings.TrimPrefix(d.Workflow, d.Repository+"/.github/workflows/")
	selected := strings.Split(workflow, "@")
	if workflow == d.Workflow || len(selected) != 2 || !policyWorkflowFile.MatchString(selected[0]) || selected[0] == ".yml" || selected[0] == ".yaml" || strings.Contains(selected[0], "..") || (!policyWorkflowRef.MatchString(selected[1]) && !isFullGitSHA(selected[1])) || strings.Contains(selected[1], "..") || strings.Contains(selected[1], "//") || strings.HasSuffix(selected[1], "/") {
		return errors.New("workflow must identify an exact repository workflow and full ref without wildcards")
	}
	seen := make(map[int64]bool)
	for _, id := range d.ProtectedRunnerIDs {
		if id <= 0 || seen[id] {
			return errors.New("protected runner IDs must be positive and unique")
		}
		seen[id] = true
	}
	if !policyVariableName.MatchString(d.Variable.Name) || !policyPublicURL(d.Variable.Value) {
		return errors.New("variable must have a public name and HTTPS URL without credentials, query, or fragment")
	}
	return nil
}

func policyPublicURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && !strings.ContainsAny(value, "\x00\r\n\t ")
}

func policyDecodeStrict(reader io.Reader, out any) error {
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

type policyGroup struct {
	ID                           int64    `json:"id"`
	Name                         string   `json:"name"`
	Visibility                   string   `json:"visibility"`
	Default                      bool     `json:"default"`
	Inherited                    bool     `json:"inherited"`
	AllowsPublicRepositories     bool     `json:"allows_public_repositories"`
	RestrictedToWorkflows        bool     `json:"restricted_to_workflows"`
	SelectedWorkflows            []string `json:"selected_workflows"`
	WorkflowRestrictionsReadOnly bool     `json:"workflow_restrictions_read_only"`
}

type policyGroupPatch struct {
	Name                     string   `json:"name"`
	Visibility               string   `json:"visibility"`
	AllowsPublicRepositories bool     `json:"allows_public_repositories"`
	RestrictedToWorkflows    bool     `json:"restricted_to_workflows"`
	SelectedWorkflows        []string `json:"selected_workflows"`
}

type policyRepository struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	Private  bool   `json:"private"`
}

type policyLabel struct {
	Name string `json:"name"`
}
type policyRunner struct {
	ID        int64         `json:"id"`
	Name      string        `json:"name"`
	OS        string        `json:"os"`
	Ephemeral bool          `json:"ephemeral"`
	Labels    []policyLabel `json:"labels"`
}
type policyPage struct {
	TotalCount   int                `json:"total_count"`
	RunnerGroups []policyGroup      `json:"runner_groups"`
	Repositories []policyRepository `json:"repositories"`
	Runners      []policyRunner     `json:"runners"`
	present      map[string]json.RawMessage
}

func (p *policyPage) UnmarshalJSON(data []byte) error {
	type plain policyPage
	if err := json.Unmarshal(data, (*plain)(p)); err != nil {
		return err
	}
	if err := json.Unmarshal(data, &p.present); err != nil {
		return err
	}
	if !p.has("total_count") {
		return errors.New("page lacks a total count")
	}
	var groups []json.RawMessage
	if err := json.Unmarshal(p.present["runner_groups"], &groups); err != nil && p.has("runner_groups") {
		return err
	}
	for _, group := range groups {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(group, &fields); err != nil {
			return err
		}
		for _, name := range []string{"default", "inherited", "allows_public_repositories", "restricted_to_workflows", "workflow_restrictions_read_only"} {
			if len(fields[name]) == 0 || bytes.Equal(fields[name], []byte("null")) {
				return errors.New("runner group lacks explicit safety fields")
			}
		}
	}
	return nil
}

func (p policyPage) has(name string) bool {
	value := p.present[name]
	return len(value) != 0 && !bytes.Equal(value, []byte("null"))
}

type runnerPolicyClient struct {
	http  *httpGitHubRunnerClient
	token string
}

func newRunnerPolicyClient(baseURL, token string) (*runnerPolicyClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && (u.Scheme != "http" || !net.ParseIP(u.Hostname()).IsLoopback())) {
		return nil, errors.New("GitHub API must use HTTPS or literal loopback without credentials, query, or fragment")
	}
	if strings.TrimSpace(token) == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("GITHUB_TOKEN is required")
	}
	return &runnerPolicyClient{http: &httpGitHubRunnerClient{baseURL: strings.TrimRight(baseURL, "/"), httpClient: &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, token: token}, nil
}

func (c *runnerPolicyClient) request(ctx context.Context, method, path string, body, out any, statuses ...int) (int, error) {
	status, _, err := c.send(ctx, method, c.http.baseURL+path, body, out, statuses...)
	return status, err
}

func (c *runnerPolicyClient) send(ctx context.Context, method, endpoint string, body, out any, statuses ...int) (int, http.Header, error) {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, errors.New("GitHub policy request could not be encoded")
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return 0, nil, errors.New("GitHub policy request could not be constructed")
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	req.Header.Set("User-Agent", "workflow-plugin-github")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.httpClient.Do(req)
	if err != nil {
		return 0, nil, errors.New("GitHub policy API transport failed; credential and response omitted")
	}
	defer func() { _ = resp.Body.Close() }()
	if !slices.Contains(statuses, resp.StatusCode) {
		return resp.StatusCode, nil, fmt.Errorf("GitHub policy API returned HTTP %d", resp.StatusCode)
	}
	if out != nil && resp.StatusCode != http.StatusNotFound {
		data, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
		if err != nil || len(data) > 1<<20 || json.Unmarshal(data, out) != nil {
			return resp.StatusCode, nil, errors.New("GitHub policy API response is incomplete, invalid, or exceeds its bound")
		}
	}
	return resp.StatusCode, resp.Header, nil
}

func (c *runnerPolicyClient) pages(ctx context.Context, path, kind string) (policyPage, error) {
	var all policyPage
	guard := githubPaginationGuard{}
	endpoint := c.http.baseURL + path + "?per_page=100"
	for endpoint != "" {
		if err := guard.visit(endpoint); err != nil {
			return all, errors.New("GitHub policy pagination is cyclic or exceeds its bound")
		}
		var page policyPage
		_, header, err := c.send(ctx, http.MethodGet, endpoint, nil, &page, http.StatusOK)
		if err != nil {
			return all, errors.New("GitHub policy page could not be read")
		}
		field := map[string]string{"groups": "runner_groups", "repositories": "repositories", "runners": "runners"}[kind]
		if !page.has(field) {
			return all, errors.New("GitHub policy page lacks an explicit collection")
		}
		if guard.pages == 1 {
			all.TotalCount = page.TotalCount
		}
		if page.TotalCount < 0 || page.TotalCount != all.TotalCount {
			return all, errors.New("GitHub policy pagination count changed")
		}
		all.RunnerGroups = append(all.RunnerGroups, page.RunnerGroups...)
		all.Repositories = append(all.Repositories, page.Repositories...)
		all.Runners = append(all.Runners, page.Runners...)
		endpoint, err = c.http.nextPage(header.Get("Link"))
		if err != nil {
			return all, errors.New("GitHub policy pagination changed origin")
		}
		if endpoint != "" {
			u, err := url.Parse(endpoint)
			original, parseErr := url.Parse(c.http.baseURL + path)
			if err != nil || parseErr != nil || u.Path != original.Path || u.Fragment != "" {
				return all, errors.New("GitHub policy pagination changed collection")
			}
		}
	}
	count := 0
	switch kind {
	case "groups":
		count = len(all.RunnerGroups)
	case "repositories":
		count = len(all.Repositories)
	case "runners":
		count = len(all.Runners)
	}
	if count != all.TotalCount {
		return all, errors.New("GitHub policy pagination did not prove a complete list")
	}
	return all, nil
}

type policyGroupState struct {
	Group         policyGroup `json:"group"`
	RepositoryIDs []int64     `json:"repository_ids"`
	RunnerIDs     []int64     `json:"runner_ids"`
}
type policySnapshot struct {
	RepositoryID  int64                 `json:"repository_id"`
	Target        policyGroupState      `json:"target"`
	ProtectedHash string                `json:"protected_hash"`
	Variable      *RunnerPolicyVariable `json:"variable"`
}

type policyRunnerEvidence struct {
	ID   int64  `json:"id"`
	Hash string `json:"metadata_hash"`
}

func (c *runnerPolicyClient) snapshot(ctx context.Context, d RunnerPolicyDesired, expectedID int64) (policySnapshot, error) {
	var state policySnapshot
	var repo policyRepository
	if _, err := c.request(ctx, http.MethodGet, "/repos/"+d.Repository, nil, &repo, http.StatusOK); err != nil {
		return state, err
	}
	if repo.ID <= 0 || !strings.EqualFold(repo.FullName, d.Repository) || !repo.Private {
		return state, errors.New("selected repository must resolve exactly and be private")
	}
	state.RepositoryID = repo.ID
	orgPath := "/orgs/" + d.Organization + "/actions"
	groups, err := c.pages(ctx, orgPath+"/runner-groups", "groups")
	if err != nil {
		return state, err
	}
	slices.SortFunc(groups.RunnerGroups, func(a, b policyGroup) int { return cmpPolicyID(a.ID, b.ID) })
	var protected []policyGroupState
	seen := make(map[int64]bool)
	found := false
	for _, group := range groups.RunnerGroups {
		if group.ID <= 0 || seen[group.ID] {
			return state, errors.New("runner groups have missing or duplicate IDs")
		}
		seen[group.ID] = true
		group.SelectedWorkflows = append([]string{}, group.SelectedWorkflows...)
		slices.Sort(group.SelectedWorkflows)
		groupPath := fmt.Sprintf("%s/runner-groups/%d", orgPath, group.ID)
		runners, err := c.pages(ctx, groupPath+"/runners", "runners")
		if err != nil {
			return state, err
		}
		gs := policyGroupState{Group: group, RepositoryIDs: []int64{}, RunnerIDs: []int64{}}
		for _, runner := range runners.Runners {
			gs.RunnerIDs = append(gs.RunnerIDs, runner.ID)
		}
		slices.Sort(gs.RunnerIDs)
		if !policyUniqueIDs(gs.RunnerIDs) {
			return state, errors.New("group runner IDs are missing or duplicated")
		}
		if group.Visibility == "selected" {
			repos, err := c.pages(ctx, groupPath+"/repositories", "repositories")
			if err != nil {
				return state, err
			}
			for _, repository := range repos.Repositories {
				gs.RepositoryIDs = append(gs.RepositoryIDs, repository.ID)
			}
			slices.Sort(gs.RepositoryIDs)
			if !policyUniqueIDs(gs.RepositoryIDs) {
				return state, errors.New("selected repository IDs are missing or duplicated")
			}
		}
		if group.Name == d.RunnerGroup {
			if found || group.Default || strings.EqualFold(group.Name, "Default") || group.Inherited || group.WorkflowRestrictionsReadOnly || len(gs.RunnerIDs) != 0 || (expectedID != 0 && group.ID != expectedID) {
				return state, errors.New("target runner group is ambiguous, protected, read-only, changed identity, or contains retained runners")
			}
			state.Target, found = gs, true
		} else {
			protected = append(protected, gs)
		}
	}
	if !found {
		return state, errors.New("exact runner group was not found")
	}
	runners, err := c.pages(ctx, orgPath+"/runners", "runners")
	if err != nil {
		return state, err
	}
	slices.SortFunc(runners.Runners, func(a, b policyRunner) int { return cmpPolicyID(a.ID, b.ID) })
	ids := make([]int64, 0, len(runners.Runners))
	var evidence []policyRunnerEvidence
	for _, runner := range runners.Runners {
		ids = append(ids, runner.ID)
		slices.SortFunc(runner.Labels, func(a, b policyLabel) int { return strings.Compare(a.Name, b.Name) })
		hash, err := policyHash(runner)
		if err != nil {
			return state, err
		}
		evidence = append(evidence, policyRunnerEvidence{runner.ID, hash})
	}
	if !policyUniqueIDs(ids) {
		return state, errors.New("organization runner IDs are missing or duplicated")
	}
	for _, id := range d.ProtectedRunnerIDs {
		if !slices.Contains(ids, id) {
			return state, errors.New("declared protected runner is absent")
		}
	}
	state.ProtectedHash, err = policyHash(struct {
		Groups  []policyGroupState     `json:"groups"`
		Runners []policyRunnerEvidence `json:"runners"`
	}{protected, evidence})
	if err != nil {
		return state, err
	}
	var variable RunnerPolicyVariable
	status, err := c.request(ctx, http.MethodGet, "/repos/"+d.Repository+"/actions/variables/"+d.Variable.Name, nil, &variable, http.StatusOK, http.StatusNotFound)
	if err != nil {
		return state, err
	}
	if status == http.StatusOK {
		if variable.Name != d.Variable.Name || !policyPublicURL(variable.Value) {
			return state, errors.New("existing variable is not exact public URL state; refusing to journal its bytes")
		}
		state.Variable = &variable
	}
	return state, nil
}

func cmpPolicyID(a, b int64) int {
	if a < b {
		return -1
	}
	if a > b {
		return 1
	}
	return 0
}
func policyUniqueIDs(ids []int64) bool {
	for i, id := range ids {
		if id <= 0 || (i > 0 && ids[i-1] == id) {
			return false
		}
	}
	return true
}
func policyHash(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("policy state could not be encoded")
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
func policySame(a, b policySnapshot) bool {
	x, err := policyHash(a)
	if err != nil {
		return false
	}
	y, err := policyHash(b)
	return err == nil && x == y
}

type runnerPolicyEvent struct {
	SchemaVersion string               `json:"schema_version"`
	TransactionID string               `json:"transaction_id"`
	Phase         string               `json:"phase"`
	Step          int                  `json:"step"`
	Time          time.Time            `json:"time"`
	Desired       *RunnerPolicyDesired `json:"desired,omitempty"`
	Before        *policySnapshot      `json:"before,omitempty"`
	After         *policySnapshot      `json:"after,omitempty"`
	BeforeHash    string               `json:"before_hash"`
	AfterHash     string               `json:"after_hash,omitempty"`
}

type RunnerPolicyResult struct {
	TransactionID string `json:"transaction_id"`
	Phase         string `json:"phase"`
	BeforeHash    string `json:"before_hash"`
	AfterHash     string `json:"after_hash,omitempty"`
}

type runnerPolicyReconciler struct {
	client *runnerPolicyClient
	dir    string
	mu     sync.Mutex
}

func newRunnerPolicyReconciler(client *runnerPolicyClient, dir string) *runnerPolicyReconciler {
	return &runnerPolicyReconciler{client: client, dir: dir}
}

func (r *runnerPolicyReconciler) openAudit() (*os.Root, error) {
	if err := os.MkdirAll(r.dir, 0700); err != nil {
		return nil, errors.New("policy state directory could not be created")
	}
	info, err := os.Lstat(r.dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0022 != 0 {
		return nil, errors.New("policy state directory must be an owner-controlled directory")
	}
	root, err := os.OpenRoot(r.dir)
	if err != nil {
		return nil, errors.New("policy state directory could not be opened")
	}
	info, err = root.Lstat(runnerPolicyAuditFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		_ = root.Close()
		return nil, errors.New("policy audit could not be inspected")
	}
	if err == nil && (!info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0) {
		_ = root.Close()
		return nil, errors.New("policy audit must be an owner-only regular file")
	}
	return root, nil
}

func (r *runnerPolicyReconciler) append(root *os.Root, event runnerPolicyEvent) error {
	event.SchemaVersion, event.Time = runnerPolicySchemaVersion, time.Now().UTC()
	data, err := json.Marshal(event)
	if err != nil || bytes.Contains(data, []byte(r.client.token)) {
		return errors.New("policy audit contains invalid or credential-bearing state")
	}
	file, err := root.OpenFile(runnerPolicyAuditFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return errors.New("policy audit could not be opened for append")
	}
	n, writeErr := file.Write(append(data, '\n'))
	syncErr := file.Sync()
	closeErr := file.Close()
	if n != len(data)+1 || errors.Join(writeErr, syncErr, closeErr) != nil {
		return errors.New("policy audit append was not durable")
	}
	if err := syncJITOwnershipJournalDirectoryPlatform(root, r.dir); err != nil {
		return errors.New("policy audit directory sync failed")
	}
	return nil
}

func (r *runnerPolicyReconciler) load(root *os.Root, tx string) (runnerPolicyEvent, runnerPolicyEvent, error) {
	var plan, last runnerPolicyEvent
	file, err := root.Open(runnerPolicyAuditFile)
	if errors.Is(err, os.ErrNotExist) {
		return plan, last, nil
	}
	if err != nil {
		return plan, last, errors.New("policy audit could not be read")
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, (64<<20)+1))
	if err != nil || len(data) > 64<<20 || (len(data) > 0 && data[len(data)-1] != '\n') {
		return plan, last, errors.New("policy audit is incomplete or exceeds its read bound")
	}
	for line := range bytes.SplitSeq(data, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var event runnerPolicyEvent
		if err := policyDecodeStrict(bytes.NewReader(line), &event); err != nil || event.SchemaVersion != runnerPolicySchemaVersion {
			return plan, last, errors.New("policy audit is malformed")
		}
		if event.TransactionID != tx {
			continue
		}
		if event.Phase == "planned" {
			if plan.Desired != nil || event.Desired == nil || event.Before == nil {
				return plan, last, errors.New("policy transaction has invalid initial authority")
			}
			plan = event
		}
		last = event
	}
	return plan, last, nil
}

func (r *runnerPolicyReconciler) Plan(ctx context.Context, tx string, desired RunnerPolicyDesired) (RunnerPolicyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !policyIdentifier.MatchString(tx) {
		return RunnerPolicyResult{}, errors.New("transaction ID must be an exact identifier")
	}
	if err := desired.validate(); err != nil {
		return RunnerPolicyResult{}, err
	}
	root, err := r.openAudit()
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	defer func() { _ = root.Close() }()
	lock, err := r.lockAudit(ctx, root)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	defer func() { _ = lock.Close() }()
	plan, last, err := r.load(root, tx)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	if plan.Desired != nil {
		a, _ := policyHash(desired)
		b, _ := policyHash(*plan.Desired)
		if a != b {
			return RunnerPolicyResult{}, errors.New("transaction ID already binds different desired state")
		}
		return policyEventResult(last), nil
	}
	before, err := r.client.snapshot(ctx, desired, 0)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	if !policyDesiredNarrows(desired, before) {
		return RunnerPolicyResult{}, errors.New("desired policy would broaden current repository or workflow access")
	}
	hash, err := policyHash(before)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	plan = runnerPolicyEvent{TransactionID: tx, Phase: "planned", Desired: &desired, Before: &before, BeforeHash: hash}
	if err := r.append(root, plan); err != nil {
		return RunnerPolicyResult{}, err
	}
	return policyEventResult(plan), nil
}

func policyDesiredNarrows(d RunnerPolicyDesired, before policySnapshot) bool {
	g := before.Target.Group
	if g.Visibility == "selected" && !slices.Contains(before.Target.RepositoryIDs, before.RepositoryID) {
		return false
	}
	if g.Visibility != "selected" && g.Visibility != "all" && g.Visibility != "private" {
		return false
	}
	return !g.RestrictedToWorkflows || slices.Contains(g.SelectedWorkflows, d.Workflow)
}

func policyEventResult(event runnerPolicyEvent) RunnerPolicyResult {
	return RunnerPolicyResult{TransactionID: event.TransactionID, Phase: event.Phase, BeforeHash: event.BeforeHash, AfterHash: event.AfterHash}
}

func policyDesiredState(before policySnapshot, d RunnerPolicyDesired) policySnapshot {
	next := before
	next.Target.Group.Visibility = "selected"
	next.Target.Group.AllowsPublicRepositories = false
	next.Target.Group.RestrictedToWorkflows = true
	next.Target.Group.SelectedWorkflows = []string{d.Workflow}
	next.Target.RepositoryIDs = []int64{before.RepositoryID}
	next.Variable = &d.Variable
	return next
}

func (r *runnerPolicyReconciler) Apply(ctx context.Context, tx string) (RunnerPolicyResult, error) {
	return r.reconcile(ctx, tx, false)
}
func (r *runnerPolicyReconciler) Rollback(ctx context.Context, tx string) (RunnerPolicyResult, error) {
	return r.reconcile(ctx, tx, true)
}

func (r *runnerPolicyReconciler) reconcile(ctx context.Context, tx string, rollback bool) (RunnerPolicyResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !policyIdentifier.MatchString(tx) {
		return RunnerPolicyResult{}, errors.New("transaction ID must be an exact identifier")
	}
	root, err := r.openAudit()
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	defer func() { _ = root.Close() }()
	lock, err := r.lockAudit(ctx, root)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	defer func() { _ = lock.Close() }()
	plan, last, err := r.load(root, tx)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	if plan.Desired == nil || plan.Before == nil {
		return RunnerPolicyResult{}, errors.New("transaction has no durable plan")
	}
	d := *plan.Desired
	if err := d.validate(); err != nil {
		return RunnerPolicyResult{}, errors.New("stored policy is invalid")
	}
	current, err := r.client.snapshot(ctx, d, plan.Before.Target.Group.ID)
	if err != nil {
		return RunnerPolicyResult{}, err
	}
	if current.ProtectedHash != plan.Before.ProtectedHash || current.RepositoryID != plan.Before.RepositoryID {
		return RunnerPolicyResult{}, errors.New("protected groups, runner metadata, or repository identity changed")
	}
	if last.Phase == "applied" && !rollback || strings.HasPrefix(last.Phase, "rolled-back") {
		if last.After == nil || !policySame(current, *last.After) {
			return RunnerPolicyResult{}, errors.New("terminal policy readback changed")
		}
		if strings.HasPrefix(last.Phase, "rolled-back") && !rollback {
			return RunnerPolicyResult{}, errors.New("rolled-back transaction cannot be reapplied")
		}
		return policyEventResult(last), nil
	}
	var expected policySnapshot
	switch last.Phase {
	case "planned":
		expected = *plan.Before
	case "write-intent", "rollback-intent":
		if last.Before == nil || last.After == nil || (!policySame(current, *last.Before) && !policySame(current, *last.After)) {
			return RunnerPolicyResult{}, errors.New("interrupted write has unrecognized readback")
		}
		expected = current
	case "write-ack", "rollback-ack", "applied":
		if last.After == nil {
			return RunnerPolicyResult{}, errors.New("policy acknowledgement is missing readback")
		}
		expected = *last.After
	default:
		return RunnerPolicyResult{}, errors.New("policy journal phase is not resumable")
	}
	if !policySame(current, expected) {
		return RunnerPolicyResult{}, errors.New("policy changed since the recorded plan or acknowledgement")
	}
	if !rollback && strings.HasPrefix(last.Phase, "rollback-") {
		return RunnerPolicyResult{}, errors.New("rollback transaction cannot resume apply")
	}
	wanted := policyDesiredState(*plan.Before, d)
	phase, intent, ack := "applied", "write-intent", "write-ack"
	if rollback {
		wanted = current
		wanted.Variable = plan.Before.Variable
		phase, intent, ack = "rolled-back-restrictive", "rollback-intent", "rollback-ack"
		// Restoring a broad captured ACL would revoke the safety restriction.
		// Keep that ACL restrictive; only restore ACLs proven to be a subset.
		if policyACLSubset(plan.Before.Target, current.Target) {
			wanted.Target = plan.Before.Target
			phase = "rolled-back"
		}
	}
	for step := 1; step <= 3; step++ {
		next := current
		switch step {
		case 1:
			next.Target.Group = wanted.Target.Group
		case 2:
			next.Target.RepositoryIDs = wanted.Target.RepositoryIDs
		case 3:
			next.Variable = wanted.Variable
		}
		beforeHash, _ := policyHash(current)
		afterHash, _ := policyHash(next)
		event := runnerPolicyEvent{TransactionID: tx, Phase: intent, Step: step, Before: &current, After: &next, BeforeHash: beforeHash, AfterHash: afterHash}
		if beforeHash != afterHash {
			if err := r.append(root, event); err != nil {
				return RunnerPolicyResult{}, err
			}
			writeErr := r.write(ctx, d, current, next, step)
			observed, readErr := r.client.snapshot(ctx, d, plan.Before.Target.Group.ID)
			if readErr != nil || !policySame(observed, next) {
				if writeErr != nil {
					return RunnerPolicyResult{}, errors.New("GitHub write was not confirmed; retry the exact transaction")
				}
				return RunnerPolicyResult{}, errors.New("GitHub policy write readback does not equal the exact intended state")
			}
			current = observed
		} else {
			current = next
		}
		event.Phase = ack
		event.After = &current
		if err := r.append(root, event); err != nil {
			return RunnerPolicyResult{}, err
		}
	}
	afterHash, _ := policyHash(current)
	terminal := runnerPolicyEvent{TransactionID: tx, Phase: phase, BeforeHash: plan.BeforeHash, AfterHash: afterHash, After: &current}
	if err := r.append(root, terminal); err != nil {
		return RunnerPolicyResult{}, err
	}
	return policyEventResult(terminal), nil
}

func policyACLSubset(before, current policyGroupState) bool {
	if policyGroupACLHash(before) == policyGroupACLHash(current) {
		return true
	}
	if before.Group.Visibility != "selected" || before.Group.AllowsPublicRepositories || !before.Group.RestrictedToWorkflows || current.Group.Visibility != "selected" || !current.Group.RestrictedToWorkflows {
		return false
	}
	for _, id := range before.RepositoryIDs {
		if !slices.Contains(current.RepositoryIDs, id) {
			return false
		}
	}
	for _, workflow := range before.Group.SelectedWorkflows {
		if !slices.Contains(current.Group.SelectedWorkflows, workflow) {
			return false
		}
	}
	return true
}

func policyGroupACLHash(state policyGroupState) string { hash, _ := policyHash(state); return hash }

func (r *runnerPolicyReconciler) write(ctx context.Context, d RunnerPolicyDesired, before, after policySnapshot, step int) error {
	groupPath := fmt.Sprintf("/orgs/%s/actions/runner-groups/%d", d.Organization, before.Target.Group.ID)
	var err error
	switch step {
	case 1:
		g := after.Target.Group
		_, err = r.client.request(ctx, http.MethodPatch, groupPath, policyGroupPatch{g.Name, g.Visibility, g.AllowsPublicRepositories, g.RestrictedToWorkflows, g.SelectedWorkflows}, nil, http.StatusOK)
	case 2:
		_, err = r.client.request(ctx, http.MethodPut, groupPath+"/repositories", struct {
			IDs []int64 `json:"selected_repository_ids"`
		}{after.Target.RepositoryIDs}, nil, http.StatusNoContent)
	case 3:
		path := "/repos/" + d.Repository + "/actions/variables"
		if after.Variable == nil {
			_, err = r.client.request(ctx, http.MethodDelete, path+"/"+d.Variable.Name, nil, nil, http.StatusNoContent, http.StatusNotFound)
		} else if before.Variable == nil {
			_, err = r.client.request(ctx, http.MethodPost, path, after.Variable, nil, http.StatusCreated)
		} else {
			_, err = r.client.request(ctx, http.MethodPatch, path+"/"+d.Variable.Name, after.Variable, nil, http.StatusNoContent)
		}
	}
	return err
}
