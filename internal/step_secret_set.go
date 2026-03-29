package internal

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/google/go-github/v69/github"
	"golang.org/x/crypto/nacl/box"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// secretSetStep implements sdk.StepInstance.
// It sets a repository or organization secret, encrypting the value with
// the repo's public key before upload (as required by GitHub's API).
//
// Config:
//
//	owner:  "GoCodeAlone"
//	repo:   "workflow"          # omit for org-level secrets
//	name:   "DATABASE_URL"
//	value:  "${DATABASE_URL}"   # env var reference or literal
//	token:  "${GITHUB_TOKEN}"
type secretSetStep struct {
	name   string
	config secretSetConfig
}

type secretSetConfig struct {
	Owner string `yaml:"owner"`
	Repo  string `yaml:"repo"`
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
	Token string `yaml:"token"`
}

func newSecretSetStep(name string, raw map[string]any) (*secretSetStep, error) {
	var cfg secretSetConfig
	cfg.Owner, _ = raw["owner"].(string)
	if cfg.Owner == "" {
		return nil, fmt.Errorf("step.gh_secret_set %q: config.owner is required", name)
	}
	cfg.Repo, _ = raw["repo"].(string)
	cfg.Name, _ = raw["name"].(string)
	if cfg.Name == "" {
		return nil, fmt.Errorf("step.gh_secret_set %q: config.name is required", name)
	}
	cfg.Value, _ = raw["value"].(string)
	cfg.Token, _ = raw["token"].(string)
	cfg.Token = os.ExpandEnv(cfg.Token)
	return &secretSetStep{name: name, config: cfg}, nil
}

func (s *secretSetStep) Execute(
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
	secretName := resolveField(s.config.Name, triggerData, stepOutputs, current)
	secretValue := os.ExpandEnv(resolveField(s.config.Value, triggerData, stepOutputs, current))

	client := NewSDKClient(token)

	// Fetch the repo's public key for secret encryption.
	pubKey, _, err := client.GH.Actions.GetRepoPublicKey(ctx, owner, repo)
	if err != nil {
		return errorResult(fmt.Sprintf("get repo public key: %v", err)), nil
	}

	// Decode the base64-encoded public key.
	keyBytes, err := base64.StdEncoding.DecodeString(pubKey.GetKey())
	if err != nil {
		return errorResult(fmt.Sprintf("decode public key: %v", err)), nil
	}

	// Encrypt the secret value using libsodium-compatible NaCl box sealing.
	var recipientKey [32]byte
	copy(recipientKey[:], keyBytes)
	encrypted, err := sealWithPublicKey(recipientKey, []byte(secretValue))
	if err != nil {
		return errorResult(fmt.Sprintf("encrypt secret: %v", err)), nil
	}

	encryptedBase64 := base64.StdEncoding.EncodeToString(encrypted)
	_, err = client.GH.Actions.CreateOrUpdateRepoSecret(ctx, owner, repo, &github.EncryptedSecret{
		Name:           secretName,
		KeyID:          pubKey.GetKeyID(),
		EncryptedValue: encryptedBase64,
	})
	if err != nil {
		return errorResult(fmt.Sprintf("set secret: %v", err)), nil
	}

	return &sdk.StepResult{
		Output: map[string]any{
			"name":  secretName,
			"owner": owner,
			"repo":  repo,
			"set":   true,
		},
	}, nil
}

// sealWithPublicKey encrypts plaintext using NaCl box anonymous sealing.
func sealWithPublicKey(recipientPubKey [32]byte, plaintext []byte) ([]byte, error) {
	ephemeralPub, ephemeralPriv, err := box.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate ephemeral key: %w", err)
	}
	var nonce [24]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	encrypted := box.Seal(ephemeralPub[:], plaintext, &nonce, &recipientPubKey, ephemeralPriv)
	return encrypted, nil
}
