package internal

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockUpstreamReleaseClient struct {
	latestReleaseFunc func(ctx context.Context, owner, repo, token string) (upstreamReleaseInfo, error)
}

func (m mockUpstreamReleaseClient) LatestRelease(ctx context.Context, owner, repo, token string) (upstreamReleaseInfo, error) {
	if m.latestReleaseFunc != nil {
		return m.latestReleaseFunc(ctx, owner, repo, token)
	}
	return upstreamReleaseInfo{}, nil
}

func TestUpstreamReleaseMonitorStep_UpdateAvailable(t *testing.T) {
	publishedAt := time.Date(2026, 6, 25, 12, 30, 0, 0, time.UTC)
	var capturedOwner, capturedRepo, capturedToken string
	client := mockUpstreamReleaseClient{
		latestReleaseFunc: func(_ context.Context, owner, repo, token string) (upstreamReleaseInfo, error) {
			capturedOwner = owner
			capturedRepo = repo
			capturedToken = token
			return upstreamReleaseInfo{
				ID:          96,
				TagName:     "v0.96.4",
				HTMLURL:     "https://github.com/signalapp/libsignal/releases/tag/v0.96.4",
				PublishedAt: publishedAt,
			}, nil
		},
	}

	step, err := newUpstreamReleaseMonitorStep("check", map[string]any{
		"upstream_owner": "signalapp",
		"upstream_repo":  "libsignal",
		"pinned_tag":     "v0.96.3",
		"token":          "gh-token",
	}, client)
	if err != nil {
		t.Fatalf("newUpstreamReleaseMonitorStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Fatalf("expected StopPipeline=false, got true with output %#v", result.Output)
	}
	if capturedOwner != "signalapp" || capturedRepo != "libsignal" || capturedToken != "gh-token" {
		t.Fatalf("captured request = %s/%s token %q", capturedOwner, capturedRepo, capturedToken)
	}
	if got := result.Output["update_available"]; got != true {
		t.Fatalf("update_available = %v, want true", got)
	}
	if got := result.Output["latest_tag"]; got != "v0.96.4" {
		t.Fatalf("latest_tag = %v, want v0.96.4", got)
	}
	if got := result.Output["pinned_tag"]; got != "v0.96.3" {
		t.Fatalf("pinned_tag = %v, want v0.96.3", got)
	}
	if got := result.Output["release_id"]; got != int64(96) {
		t.Fatalf("release_id = %v, want 96", got)
	}
	if got := result.Output["published_at"]; got != publishedAt.Format(time.RFC3339) {
		t.Fatalf("published_at = %v, want %s", got, publishedAt.Format(time.RFC3339))
	}
}

func TestUpstreamReleaseMonitorStep_NoUpdate(t *testing.T) {
	step, err := newUpstreamReleaseMonitorStep("check", map[string]any{
		"upstream_owner": "signalapp",
		"upstream_repo":  "libsignal",
		"pinned_tag":     "v0.96.4",
	}, mockUpstreamReleaseClient{
		latestReleaseFunc: func(context.Context, string, string, string) (upstreamReleaseInfo, error) {
			return upstreamReleaseInfo{TagName: "v0.96.4"}, nil
		},
	})
	if err != nil {
		t.Fatalf("newUpstreamReleaseMonitorStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.StopPipeline {
		t.Fatalf("expected StopPipeline=false, got true")
	}
	if got := result.Output["update_available"]; got != false {
		t.Fatalf("update_available = %v, want false", got)
	}
}

func TestUpstreamReleaseMonitorStep_ResolvesRuntimeFields(t *testing.T) {
	var capturedOwner, capturedRepo string
	step, err := newUpstreamReleaseMonitorStep("check", map[string]any{
		"upstream_owner": "{{.owner}}",
		"upstream_repo":  "{{.repo}}",
		"pinned_tag":     "{{.current.current_pin}}",
	}, mockUpstreamReleaseClient{
		latestReleaseFunc: func(_ context.Context, owner, repo, _ string) (upstreamReleaseInfo, error) {
			capturedOwner = owner
			capturedRepo = repo
			return upstreamReleaseInfo{TagName: "v2"}, nil
		},
	})
	if err != nil {
		t.Fatalf("newUpstreamReleaseMonitorStep: %v", err)
	}

	result, err := step.Execute(
		context.Background(),
		map[string]any{"owner": "upstream", "repo": "project"},
		nil,
		map[string]any{"current_pin": "v1"},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if capturedOwner != "upstream" || capturedRepo != "project" {
		t.Fatalf("captured request = %s/%s, want upstream/project", capturedOwner, capturedRepo)
	}
	if got := result.Output["pinned_tag"]; got != "v1" {
		t.Fatalf("pinned_tag = %v, want v1", got)
	}
	if got := result.Output["update_available"]; got != true {
		t.Fatalf("update_available = %v, want true", got)
	}
}

func TestUpstreamReleaseMonitorStep_APIErrorStopsPipeline(t *testing.T) {
	step, err := newUpstreamReleaseMonitorStep("check", map[string]any{
		"upstream_owner": "signalapp",
		"upstream_repo":  "libsignal",
		"pinned_tag":     "v0.96.3",
	}, mockUpstreamReleaseClient{
		latestReleaseFunc: func(context.Context, string, string, string) (upstreamReleaseInfo, error) {
			return upstreamReleaseInfo{}, errors.New("rate limited")
		},
	})
	if err != nil {
		t.Fatalf("newUpstreamReleaseMonitorStep: %v", err)
	}

	result, err := step.Execute(context.Background(), nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !result.StopPipeline {
		t.Fatalf("expected StopPipeline=true on API error")
	}
}

func TestParseUpstreamReleaseMonitorConfig_RequiredFields(t *testing.T) {
	tests := []struct {
		name string
		raw  map[string]any
	}{
		{name: "missing owner", raw: map[string]any{"upstream_repo": "libsignal", "pinned_tag": "v1"}},
		{name: "missing repo", raw: map[string]any{"upstream_owner": "signalapp", "pinned_tag": "v1"}},
		{name: "missing pinned tag", raw: map[string]any{"upstream_owner": "signalapp", "upstream_repo": "libsignal"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseUpstreamReleaseMonitorConfig(tt.raw); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestGitHubPluginCreateStep_UpstreamReleaseMonitor(t *testing.T) {
	p := &githubPlugin{}
	step, err := p.CreateStep("step.gh_upstream_release_monitor", "check", map[string]any{
		"upstream_owner": "signalapp",
		"upstream_repo":  "libsignal",
		"pinned_tag":     "v0.96.4",
	})
	if err != nil {
		t.Fatalf("CreateStep: %v", err)
	}
	if _, ok := step.(*upstreamReleaseMonitorStep); !ok {
		t.Fatalf("CreateStep returned %T, want *upstreamReleaseMonitorStep", step)
	}
}
