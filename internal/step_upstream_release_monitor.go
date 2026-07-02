package internal

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

type upstreamReleaseMonitorStep struct {
	name     string
	config   upstreamReleaseMonitorConfig
	ghClient upstreamReleaseClient
}

type upstreamReleaseMonitorConfig struct {
	UpstreamOwner string `yaml:"upstream_owner"`
	UpstreamRepo  string `yaml:"upstream_repo"`
	PinnedTag     string `yaml:"pinned_tag"`
	Token         string `yaml:"token"`
}

type upstreamReleaseInfo struct {
	ID          int64
	TagName     string
	HTMLURL     string
	PublishedAt time.Time
}

type upstreamReleaseClient interface {
	LatestRelease(ctx context.Context, owner, repo, token string) (upstreamReleaseInfo, error)
}

type githubUpstreamReleaseClient struct{}

func newUpstreamReleaseMonitorStep(name string, raw map[string]any, client upstreamReleaseClient) (*upstreamReleaseMonitorStep, error) {
	cfg, err := parseUpstreamReleaseMonitorConfig(raw)
	if err != nil {
		return nil, fmt.Errorf("step.gh_upstream_release_monitor %q: %w", name, err)
	}
	if client == nil {
		client = githubUpstreamReleaseClient{}
	}
	return &upstreamReleaseMonitorStep{name: name, config: cfg, ghClient: client}, nil
}

func parseUpstreamReleaseMonitorConfig(raw map[string]any) (upstreamReleaseMonitorConfig, error) {
	var cfg upstreamReleaseMonitorConfig
	cfg.UpstreamOwner, _ = raw["upstream_owner"].(string)
	if cfg.UpstreamOwner == "" {
		return cfg, fmt.Errorf("config.upstream_owner is required")
	}
	cfg.UpstreamRepo, _ = raw["upstream_repo"].(string)
	if cfg.UpstreamRepo == "" {
		return cfg, fmt.Errorf("config.upstream_repo is required")
	}
	cfg.PinnedTag, _ = raw["pinned_tag"].(string)
	if cfg.PinnedTag == "" {
		return cfg, fmt.Errorf("config.pinned_tag is required")
	}
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return cfg, nil
}

func (s *upstreamReleaseMonitorStep) Execute(
	ctx context.Context,
	triggerData map[string]any,
	stepOutputs map[string]map[string]any,
	current map[string]any,
	_ map[string]any,
	_ map[string]any,
) (*sdk.StepResult, error) {
	owner := resolveField(s.config.UpstreamOwner, triggerData, stepOutputs, current)
	repo := resolveField(s.config.UpstreamRepo, triggerData, stepOutputs, current)
	pinnedTag := resolveField(s.config.PinnedTag, triggerData, stepOutputs, current)

	release, err := s.ghClient.LatestRelease(ctx, owner, repo, s.config.Token)
	if err != nil {
		return errorResult(fmt.Sprintf("get latest upstream release: %v", err)), nil
	}

	publishedAt := ""
	if !release.PublishedAt.IsZero() {
		publishedAt = release.PublishedAt.UTC().Format(time.RFC3339)
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"upstream_owner":   owner,
			"upstream_repo":    repo,
			"pinned_tag":       pinnedTag,
			"latest_tag":       release.TagName,
			"update_available": release.TagName != pinnedTag,
			"release_id":       release.ID,
			"release_url":      release.HTMLURL,
			"published_at":     publishedAt,
		},
	}, nil
}

func (githubUpstreamReleaseClient) LatestRelease(ctx context.Context, owner, repo, token string) (upstreamReleaseInfo, error) {
	client := github.NewClient(nil)
	if token != "" {
		client = client.WithAuthToken(token)
	}

	requestCtx, cancel := githubReleaseLookupContext(ctx)
	defer cancel()

	release, _, err := client.Repositories.GetLatestRelease(requestCtx, owner, repo)
	if err != nil {
		return upstreamReleaseInfo{}, err
	}
	if release == nil {
		return upstreamReleaseInfo{}, fmt.Errorf("empty release response")
	}

	var publishedAt time.Time
	if release.PublishedAt != nil {
		publishedAt = release.PublishedAt.Time
	}
	return upstreamReleaseInfo{
		ID:          release.GetID(),
		TagName:     release.GetTagName(),
		HTMLURL:     release.GetHTMLURL(),
		PublishedAt: publishedAt,
	}, nil
}

func githubReleaseLookupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, 30*time.Second)
}
