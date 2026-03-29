package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// graphqlStep implements sdk.StepInstance.
// It executes an arbitrary GraphQL query against the GitHub API.
//
// Config:
//
//	query:     "query { viewer { login } }"
//	variables: {owner: "GoCodeAlone", repo: "workflow"}
//	token:     "${GITHUB_TOKEN}"
type graphqlStep struct {
	name   string
	config graphqlConfig
}

type graphqlConfig struct {
	Query     string         `yaml:"query"`
	Variables map[string]any `yaml:"variables"`
	Token     string         `yaml:"token"`
}

func newGraphQLStep(name string, raw map[string]any) (*graphqlStep, error) {
	var cfg graphqlConfig
	cfg.Query, _ = raw["query"].(string)
	if cfg.Query == "" {
		return nil, fmt.Errorf("step.gh_graphql %q: config.query is required", name)
	}
	cfg.Variables, _ = raw["variables"].(map[string]any)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &graphqlStep{name: name, config: cfg}, nil
}

func (s *graphqlStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	token := s.config.Token
	if token == "" {
		return errorResult("GITHUB_TOKEN is not configured"), nil
	}
	query := resolveField(s.config.Query, triggerData, stepOutputs, current)

	payload := map[string]any{
		"query": query,
	}
	if len(s.config.Variables) > 0 {
		payload["variables"] = s.config.Variables
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return errorResult(fmt.Sprintf("marshal query: %v", err)), nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.github.com/graphql", bytes.NewReader(body))
	if err != nil {
		return errorResult(fmt.Sprintf("create request: %v", err)), nil
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return errorResult(fmt.Sprintf("execute graphql: %v", err)), nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return errorResult(fmt.Sprintf("read response: %v", err)), nil
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return errorResult(fmt.Sprintf("parse response: %v", err)), nil
	}

	if errs, ok := result["errors"]; ok {
		errData, _ := json.Marshal(errs)
		return errorResult(fmt.Sprintf("graphql errors: %s", errData)), nil
	}

	data, _ := result["data"].(map[string]any)
	return &sdk.StepResult{
		Output: map[string]any{
			"data":   data,
			"status": resp.StatusCode,
		},
	}, nil
}
