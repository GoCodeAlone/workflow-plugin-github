package internal

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// githubAppModule implements sdk.ModuleInstance.
// It manages GitHub App authentication, generating installation access tokens
// from an App's private key and installation ID.
//
// Module config:
//
//	app_id:          12345
//	installation_id: 67890
//	private_key:     "${GITHUB_APP_PRIVATE_KEY}"  # PEM-encoded RSA key
type githubAppModule struct {
	name   string
	config githubAppConfig

	// cached token and expiry for reuse within the valid window
	cachedToken  string
	tokenExpiry  time.Time
}

type githubAppConfig struct {
	AppID          int64  `yaml:"app_id"`
	InstallationID int64  `yaml:"installation_id"`
	PrivateKey     string `yaml:"private_key"`
}

// newGitHubAppModule parses the config map and returns a githubAppModule.
func newGitHubAppModule(name string, config map[string]any) (*githubAppModule, error) {
	var cfg githubAppConfig

	switch v := config["app_id"].(type) {
	case int:
		cfg.AppID = int64(v)
	case int64:
		cfg.AppID = v
	case float64:
		cfg.AppID = int64(v)
	}
	if cfg.AppID == 0 {
		return nil, fmt.Errorf("github.app %q: config.app_id is required", name)
	}

	switch v := config["installation_id"].(type) {
	case int:
		cfg.InstallationID = int64(v)
	case int64:
		cfg.InstallationID = v
	case float64:
		cfg.InstallationID = int64(v)
	}
	if cfg.InstallationID == 0 {
		return nil, fmt.Errorf("github.app %q: config.installation_id is required", name)
	}

	cfg.PrivateKey, _ = config["private_key"].(string)
	cfg.PrivateKey = os.ExpandEnv(cfg.PrivateKey)
	if cfg.PrivateKey == "" {
		return nil, fmt.Errorf("github.app %q: config.private_key is required", name)
	}

	return &githubAppModule{name: name, config: cfg}, nil
}

// Init is a no-op; the module is ready after construction.
func (m *githubAppModule) Init() error { return nil }

// Start is a no-op.
func (m *githubAppModule) Start(_ context.Context) error { return nil }

// Stop is a no-op.
func (m *githubAppModule) Stop(_ context.Context) error { return nil }

// Name returns the module name.
func (m *githubAppModule) Name() string { return m.name }

// GetInstallationToken returns a valid GitHub App installation access token,
// using a cached value if it is still valid (expires in >5 minutes).
func (m *githubAppModule) GetInstallationToken(ctx context.Context) (string, error) {
	if m.cachedToken != "" && time.Until(m.tokenExpiry) > 5*time.Minute {
		return m.cachedToken, nil
	}

	jwtToken, err := m.generateJWT()
	if err != nil {
		return "", fmt.Errorf("generate app JWT: %w", err)
	}

	client := NewSDKClient(jwtToken)
	token, _, err := client.GH.Apps.CreateInstallationToken(ctx, m.config.InstallationID, nil)
	if err != nil {
		return "", fmt.Errorf("create installation token: %w", err)
	}

	m.cachedToken = token.GetToken()
	m.tokenExpiry = token.GetExpiresAt().Time
	return m.cachedToken, nil
}

// generateJWT creates a short-lived JWT for GitHub App authentication.
func (m *githubAppModule) generateJWT() (string, error) {
	key, err := parseRSAPrivateKey(m.config.PrivateKey)
	if err != nil {
		return "", err
	}

	now := time.Now()
	claims := jwt.MapClaims{
		"iat": now.Add(-60 * time.Second).Unix(), // issued 60s ago to handle clock skew
		"exp": now.Add(10 * time.Minute).Unix(),
		"iss": m.config.AppID,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	return token.SignedString(key)
}

// parseRSAPrivateKey decodes a PEM-encoded RSA private key.
func parseRSAPrivateKey(pem_encoded string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pem_encoded))
	if block == nil {
		return nil, fmt.Errorf("failed to decode PEM block for RSA private key")
	}
	key, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		// Try PKCS8 format as well.
		iface, err2 := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err2 != nil {
			return nil, fmt.Errorf("parse RSA private key (PKCS1: %v, PKCS8: %v)", err, err2)
		}
		rsakey, ok := iface.(*rsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("private key is not RSA")
		}
		return rsakey, nil
	}
	return key, nil
}

// AppTransport implements http.RoundTripper for GitHub App authentication,
// automatically refreshing the installation token as needed.
type AppTransport struct {
	module    *githubAppModule
	base      http.RoundTripper
}

// NewAppTransport creates an http.RoundTripper that uses App installation tokens.
func NewAppTransport(mod *githubAppModule) *AppTransport {
	return &AppTransport{module: mod, base: http.DefaultTransport}
}

// RoundTrip injects the installation token into each request.
func (t *AppTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	token, err := t.module.GetInstallationToken(req.Context())
	if err != nil {
		return nil, fmt.Errorf("get installation token: %w", err)
	}
	reqCopy := req.Clone(req.Context())
	reqCopy.Header.Set("Authorization", "Bearer "+token)
	return t.base.RoundTrip(reqCopy)
}

// GetSDKClient returns an SDK client authenticated with this App's installation token.
func (m *githubAppModule) GetSDKClient() *SDKClient {
	return NewSDKClientFromTransport(NewAppTransport(m))
}

// Ensure githubAppModule satisfies sdk.ModuleInstance at compile time.
var _ sdk.ModuleInstance = (*githubAppModule)(nil)
