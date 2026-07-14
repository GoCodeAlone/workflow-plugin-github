package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/GoCodeAlone/workflow-plugin-github/internal"
	"github.com/GoCodeAlone/workflow-plugin-github/internal/retainedprovider"
)

const (
	providerProbeProtocolVersion = "github-runner-provider.probe.v1"
	providerProbeMaxBodyBytes    = 1 << 20
	providerProbeHTTPTimeout     = 30 * time.Second
)

type providerProbePreflightRequest struct {
	Repository  string   `json:"repository"`
	Workflow    string   `json:"workflow"`
	Ref         string   `json:"ref"`
	RunnerName  string   `json:"runner_name"`
	RunnerGroup string   `json:"runner_group"`
	Labels      []string `json:"labels"`
}

type providerProbeReadyResponse struct {
	Status string `json:"status"`
}

type providerProbeResult struct {
	ProtocolVersion         string    `json:"protocol_version"`
	Status                  string    `json:"status"`
	Ready                   bool      `json:"ready"`
	Organization            string    `json:"organization"`
	RunnerGroup             string    `json:"runner_group"`
	RunnerGroupID           int64     `json:"runner_group_id"`
	Ref                     string    `json:"ref"`
	ResolvedWorkflowPath    string    `json:"resolved_workflow_path"`
	ResolvedRefSHA          string    `json:"resolved_ref_sha"`
	RunnerCountChecked      int       `json:"runner_count_checked"`
	LabelsObserved          int       `json:"labels_observed"`
	ConflictingLabelCount   int       `json:"conflicting_label_count"`
	ExistingLabelsTruncated bool      `json:"existing_labels_truncated"`
	ActionsEnabled          bool      `json:"actions_enabled"`
	SelfHostedAllowed       bool      `json:"self_hosted_allowed"`
	ObservedAt              time.Time `json:"observed_at"`
}

type providerProbeFlags []string

func (f *providerProbeFlags) String() string {
	return strings.Join(*f, ",")
}

func (f *providerProbeFlags) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("label must not be empty")
	}
	*f = append(*f, value)
	return nil
}

func runProviderProbe(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("github-runner-provider probe", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	rawURL := fs.String("url", "", "provider HTTPS base URL")
	caFile := fs.String("ca-file", "", "provider CA certificate file")
	organization := fs.String("organization", "", "allowed GitHub organization")
	repository := fs.String("repository", "", "target owner/repository")
	workflow := fs.String("workflow", "", "target workflow file")
	ref := fs.String("ref", "", "full target commit SHA")
	runnerName := fs.String("runner-name", "", "unique preflight runner name")
	runnerGroup := fs.String("runner-group", "", "allowed runner group")
	var labels providerProbeFlags
	fs.Var(&labels, "label", "required runner label; repeatable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("probe does not accept positional arguments")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_RUNNER_PROVIDER_TOKEN"))
	if token == "" {
		return errors.New("GITHUB_RUNNER_PROVIDER_TOKEN is required")
	}
	if strings.ContainsAny(token, "\r\n\x00") {
		return errors.New("GITHUB_RUNNER_PROVIDER_TOKEN contains unsupported characters")
	}
	baseURL, err := validateProviderProbeFlags(*rawURL, *caFile, *organization, *repository, *workflow, *ref, *runnerName, *runnerGroup, labels)
	if err != nil {
		return err
	}
	client, err := newProviderProbeHTTPClient(*caFile)
	if err != nil {
		return err
	}

	var ready providerProbeReadyResponse
	if err := providerProbeJSON(ctx, client, http.MethodGet, baseURL.ResolveReference(&url.URL{Path: "/readyz"}), token, nil, &ready); err != nil {
		return fmt.Errorf("provider readiness probe: %w", err)
	}
	if ready.Status != "ok" {
		return fmt.Errorf("provider readiness status is %q", ready.Status)
	}
	request := providerProbePreflightRequest{
		Repository:  strings.TrimSpace(*repository),
		Workflow:    strings.TrimSpace(*workflow),
		Ref:         strings.TrimSpace(*ref),
		RunnerName:  strings.TrimSpace(*runnerName),
		RunnerGroup: strings.TrimSpace(*runnerGroup),
		Labels:      append([]string(nil), labels...),
	}
	preflightURL := baseURL.ResolveReference(&url.URL{Path: "/v1/actions/orgs/" + url.PathEscape(strings.TrimSpace(*organization)) + "/runners/preflight"})
	var preflight internal.GitHubRunnerProviderPreflight
	if err := providerProbeJSON(ctx, client, http.MethodPost, preflightURL, token, request, &preflight); err != nil {
		return fmt.Errorf("provider semantic preflight: %w", err)
	}
	if err := validateProviderProbePreflight(preflight, strings.TrimSpace(*organization), request); err != nil {
		return err
	}
	result := providerProbeResult{
		ProtocolVersion:         providerProbeProtocolVersion,
		Status:                  "passed",
		Ready:                   true,
		Organization:            preflight.Organization,
		RunnerGroup:             preflight.RunnerGroup,
		RunnerGroupID:           preflight.RunnerGroupID,
		Ref:                     preflight.Ref,
		ResolvedWorkflowPath:    preflight.ResolvedWorkflowPath,
		ResolvedRefSHA:          preflight.ResolvedRefSHA,
		RunnerCountChecked:      preflight.RunnerCountChecked,
		LabelsObserved:          preflight.LabelsObserved,
		ConflictingLabelCount:   len(preflight.ConflictingLabels),
		ExistingLabelsTruncated: preflight.ExistingLabelsTruncated,
		ActionsEnabled:          preflight.ActionsEnabled,
		SelfHostedAllowed:       preflight.SelfHostedAllowed,
		ObservedAt:              time.Now().UTC(),
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func validateProviderProbeFlags(rawURL, caFile, organization, repository, workflow, ref, runnerName, runnerGroup string, labels []string) (*url.URL, error) {
	baseURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("parse provider url: %w", err)
	}
	if baseURL.Scheme != "https" || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" || (baseURL.Path != "" && baseURL.Path != "/") {
		return nil, errors.New("provider url must be an HTTPS origin")
	}
	baseURL.Path = ""
	if !retainedprovider.IsCanonicalSafeAbsolutePath(caFile) {
		return nil, errors.New("-ca-file must be an absolute canonical safe path")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "organization", value: organization},
		{name: "repository", value: repository},
		{name: "workflow", value: workflow},
		{name: "ref", value: ref},
		{name: "runner-name", value: runnerName},
		{name: "runner-group", value: runnerGroup},
	} {
		name, value := field.name, field.value
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n\x00") {
			return nil, fmt.Errorf("-%s is required and must be canonical", name)
		}
	}
	owner, repo, ok := strings.Cut(repository, "/")
	if !ok || owner != organization || !safeProviderProbeIdentifier(owner) || !safeProviderProbeIdentifier(repo) || !safeProviderProbeIdentifier(organization) || !safeProviderProbeIdentifier(runnerGroup) {
		return nil, errors.New("provider organization, repository, or runner group is invalid")
	}
	if !safeProviderProbeWorkflow(workflow) {
		return nil, errors.New("provider workflow is invalid")
	}
	if !isProviderProbeFullSHA(ref) {
		return nil, errors.New("provider ref must be a full lowercase commit SHA")
	}
	if len(runnerName) > 100 || !safeProviderProbeIdentifier(runnerName) {
		return nil, errors.New("provider runner name is invalid")
	}
	if len(labels) == 0 || len(labels) > retainedprovider.MaxProviderProbeLabels {
		return nil, fmt.Errorf("between 1 and %d -label values are required", retainedprovider.MaxProviderProbeLabels)
	}
	seen := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		if len(label) > 100 || !safeProviderProbeIdentifier(label) {
			return nil, fmt.Errorf("provider label %q is invalid", label)
		}
		if _, exists := seen[label]; exists {
			return nil, fmt.Errorf("provider label %q is duplicated", label)
		}
		seen[label] = struct{}{}
	}
	return baseURL, nil
}

func newProviderProbeHTTPClient(caFile string) (*http.Client, error) {
	info, err := os.Lstat(caFile)
	if err != nil {
		return nil, fmt.Errorf("inspect provider CA file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > providerProbeMaxBodyBytes {
		return nil, errors.New("provider CA file must be a regular file of at most 1 MiB")
	}
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read provider CA file: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("provider CA file contains no certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	return &http.Client{
		Transport: transport,
		Timeout:   providerProbeHTTPTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("provider probe redirects are forbidden")
		},
	}, nil
}

func providerProbeJSON(ctx context.Context, client *http.Client, method string, endpoint *url.URL, token string, input, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("provider request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, providerProbeMaxBodyBytes))
		return fmt.Errorf("provider returned HTTP status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, providerProbeMaxBodyBytes+1))
	if err != nil {
		return errors.New("read provider response")
	}
	if len(data) > providerProbeMaxBodyBytes {
		return errors.New("provider response exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		return errors.New("provider returned invalid JSON")
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("provider returned multiple JSON values")
	}
	return nil
}

func validateProviderProbePreflight(preflight internal.GitHubRunnerProviderPreflight, organization string, request providerProbePreflightRequest) error {
	expectedWorkflowPath := request.Workflow
	if !strings.Contains(expectedWorkflowPath, "/") {
		expectedWorkflowPath = path.Join(".github/workflows", expectedWorkflowPath)
	}
	if preflight.Organization != organization || preflight.RunnerGroup != request.RunnerGroup || preflight.RunnerGroupID <= 0 {
		return errors.New("provider preflight identity or runner group mismatch")
	}
	if preflight.Ref != request.Ref || preflight.ResolvedRefSHA != request.Ref || preflight.ResolvedWorkflowPath != expectedWorkflowPath {
		return errors.New("provider preflight workflow or ref mismatch")
	}
	if !isProviderProbeFullSHA(preflight.ResolvedRefSHA) || !preflight.ActionsEnabled || !preflight.SelfHostedAllowed {
		return errors.New("provider preflight rejected self-hosted execution")
	}
	if preflight.ExistingLabelsTruncated || len(preflight.ConflictingLabels) != 0 {
		return errors.New("provider preflight found incomplete or conflicting labels")
	}
	return nil
}

func safeProviderProbeIdentifier(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return false
	}
	return true
}

func safeProviderProbeWorkflow(value string) bool {
	if value == "" || strings.TrimSpace(value) != value || strings.HasPrefix(value, "/") || strings.Contains(value, "..") || strings.ContainsAny(value, "\\\r\n\x00") {
		return false
	}
	cleaned := path.Clean(value)
	return cleaned == value && (strings.HasSuffix(value, ".yml") || strings.HasSuffix(value, ".yaml"))
}

func isProviderProbeFullSHA(value string) bool {
	if len(value) != 40 {
		return false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			if r < 'a' || r > 'f' {
				return false
			}
		}
	}
	return true
}
