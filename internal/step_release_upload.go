package internal

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/go-github/v69/github"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// releaseUploadStep implements sdk.StepInstance.
// It uploads a file asset to a GitHub release.
//
// Config:
//
//	owner:      "GoCodeAlone"
//	repo:       "workflow"
//	release_id: 12345678    # from gh_release_create output
//	file:       "bin/server-linux-amd64"
//	name:       "server-linux-amd64"   # asset display name
//	token:      "${GITHUB_TOKEN}"
type releaseUploadStep struct {
	name   string
	config releaseUploadConfig
}

type releaseUploadConfig struct {
	Owner        string `yaml:"owner"`
	Repo         string `yaml:"repo"`
	ReleaseID    int64  `yaml:"release_id"`
	ReleaseIDRaw string `yaml:"-"` // raw string for dynamic {{.field}} resolution
	File         string `yaml:"file"`
	Name         string `yaml:"name"`
	Token        string `yaml:"token"`
}

func newReleaseUploadStep(name string, raw map[string]any) (*releaseUploadStep, error) {
	var cfg releaseUploadConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_release_upload %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	if cfg.Repo == "" {
		return nil, fmt.Errorf("step.gh_release_upload %q: config.repo is required", name)
	}
	switch v := raw["release_id"].(type) {
	case int:
		cfg.ReleaseID = int64(v)
	case int64:
		cfg.ReleaseID = v
	case float64:
		cfg.ReleaseID = int64(v)
	case string:
		if v != "" {
			if strings.Contains(v, "{{") && strings.Contains(v, "}}") {
				cfg.ReleaseIDRaw = v
			} else {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("step.gh_release_upload %q: config.release_id is not a valid integer: %w", name, err)
				}
				cfg.ReleaseID = n
			}
		}
	}
	if cfg.ReleaseID == 0 && cfg.ReleaseIDRaw == "" {
		return nil, fmt.Errorf("step.gh_release_upload %q: config.release_id is required", name)
	}
	cfg.File, _ = raw["file"].(string)
	if cfg.File == "" {
		return nil, fmt.Errorf("step.gh_release_upload %q: config.file is required", name)
	}
	cfg.Name, _ = raw["name"].(string)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &releaseUploadStep{name: name, config: cfg}, nil
}

func (s *releaseUploadStep) Execute(
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
	owner := resolveField(s.config.Owner, triggerData, stepOutputs, current)
	repo := resolveField(s.config.Repo, triggerData, stepOutputs, current)
	filePath := resolveField(s.config.File, triggerData, stepOutputs, current)
	assetName := resolveField(s.config.Name, triggerData, stepOutputs, current)
	if assetName == "" {
		assetName = filePath
	}

	// Resolve release_id — may be a static int or a dynamic template reference.
	releaseID := s.config.ReleaseID
	if s.config.ReleaseIDRaw != "" {
		resolved := resolveField(s.config.ReleaseIDRaw, triggerData, stepOutputs, current)
		n, err := strconv.ParseInt(resolved, 10, 64)
		if err != nil {
			return errorResult(fmt.Sprintf("release_id resolved to non-integer value %q: %v", resolved, err)), nil
		}
		releaseID = n
	}
	if releaseID == 0 {
		return errorResult("release_id resolved to zero — check pipeline context"), nil
	}

	f, err := os.Open(filePath) //nolint:gosec // G304: path from step config, trusted
	if err != nil {
		return errorResult(fmt.Sprintf("open file %q: %v", filePath, err)), nil
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return errorResult(fmt.Sprintf("stat file %q: %v", filePath, err)), nil
	}

	client := NewSDKClient(token)
	asset, _, err := client.GH.Repositories.UploadReleaseAsset(ctx, owner, repo, releaseID,
		&github.UploadOptions{Name: assetName},
		f)
	if err != nil {
		return errorResult(fmt.Sprintf("upload asset: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"asset_id": asset.GetID(),
			"url":      asset.GetBrowserDownloadURL(),
			"name":     asset.GetName(),
			"size":     fi.Size(),
		},
	}, nil
}
