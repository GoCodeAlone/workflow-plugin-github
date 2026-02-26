package internal

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	sdk "github.com/GoCodeAlone/workflow/plugin/external/sdk"
)

// GitEvent is the normalized event schema published to the message broker.
type GitEvent struct {
	Provider   string          `json:"provider"`    // "github"
	EventType  string          `json:"event_type"`  // "push", "pull_request", etc.
	Repository string          `json:"repository"`  // "owner/repo"
	Branch     string          `json:"branch"`      // "main", "feature/xyz"
	Commit     string          `json:"commit"`      // SHA
	Author     string          `json:"author"`      // username
	Message    string          `json:"message"`     // commit message or PR title
	URL        string          `json:"url"`         // link to commit/PR
	RawPayload json.RawMessage `json:"raw_payload"` // original payload
	Timestamp  time.Time       `json:"timestamp"`
}

// webhookModule implements sdk.ModuleInstance and sdk.MessageAwareModule.
// It registers an HTTP handler at /webhooks/github that validates GitHub
// webhook signatures and publishes normalized GitEvent messages to a topic.
type webhookModule struct {
	name   string
	config webhookConfig

	publisher sdk.MessagePublisher
}

// webhookConfig holds the parsed configuration for a git.webhook module.
type webhookConfig struct {
	Provider string   `yaml:"provider"`
	Secret   string   `yaml:"secret"`
	Events   []string `yaml:"events"`
	Topic    string   `yaml:"topic"`
}

// newWebhookModule parses the config map and returns a webhookModule.
func newWebhookModule(name string, config map[string]any) (*webhookModule, error) {
	cfg, err := parseWebhookConfig(config)
	if err != nil {
		return nil, fmt.Errorf("git.webhook %q: %w", name, err)
	}
	return &webhookModule{
		name:   name,
		config: cfg,
	}, nil
}

// parseWebhookConfig converts a raw config map to webhookConfig.
func parseWebhookConfig(raw map[string]any) (webhookConfig, error) {
	var cfg webhookConfig

	provider, _ := raw["provider"].(string)
	if provider == "" {
		provider = "github"
	}
	cfg.Provider = provider

	cfg.Secret, _ = raw["secret"].(string)

	if events, ok := raw["events"].([]any); ok {
		for _, e := range events {
			if s, ok := e.(string); ok {
				cfg.Events = append(cfg.Events, s)
			}
		}
	}

	topic, _ := raw["topic"].(string)
	if topic == "" {
		topic = "git.events"
	}
	cfg.Topic = topic

	return cfg, nil
}

// SetMessagePublisher is called by the engine to inject the message publisher.
func (m *webhookModule) SetMessagePublisher(pub sdk.MessagePublisher) {
	m.publisher = pub
}

// SetMessageSubscriber is a no-op; this module only publishes.
func (m *webhookModule) SetMessageSubscriber(_ sdk.MessageSubscriber) {}

// Init is a no-op; the module is ready after construction.
func (m *webhookModule) Init() error { return nil }

// Start is a no-op; the webhook route is declared via ConfigFragment so the
// engine's HTTP server registers it through the normal config pipeline.
func (m *webhookModule) Start(_ context.Context) error { return nil }

// Stop is a no-op.
func (m *webhookModule) Stop(_ context.Context) error { return nil }

// Name returns the module name.
func (m *webhookModule) Name() string { return m.name }

// handleWebhook is the HTTP handler for incoming GitHub webhook events.
func (m *webhookModule) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := readLimitedBody(r, 25*1024*1024) // 25 MB limit
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Validate HMAC-SHA256 signature when a secret is configured.
	if m.config.Secret != "" {
		sig := r.Header.Get("X-Hub-Signature-256")
		if sig == "" {
			http.Error(w, "missing X-Hub-Signature-256 header", http.StatusUnauthorized)
			return
		}
		if !validateSignature(body, m.config.Secret, sig) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	eventType := r.Header.Get("X-GitHub-Event")
	if eventType == "" {
		http.Error(w, "missing X-GitHub-Event header", http.StatusBadRequest)
		return
	}

	// Filter to configured event types if specified.
	if len(m.config.Events) > 0 && !containsString(m.config.Events, eventType) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ignored"}`))
		return
	}

	event, err := normalizeGitHubEvent(eventType, body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to normalize event: %v", err), http.StatusBadRequest)
		return
	}

	if m.publisher != nil {
		payload, err := json.Marshal(event)
		if err != nil {
			http.Error(w, "failed to marshal event", http.StatusInternalServerError)
			return
		}
		_, err = m.publisher.Publish(m.config.Topic, payload, map[string]string{
			"event_type": event.EventType,
			"provider":   event.Provider,
			"repository": event.Repository,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to publish event: %v", err), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"accepted"}`))
}

// validateSignature verifies a GitHub webhook HMAC-SHA256 signature.
// sig is expected in the format "sha256=<hex>".
func validateSignature(body []byte, secret, sig string) bool {
	const prefix = "sha256="
	if !strings.HasPrefix(sig, prefix) {
		return false
	}
	expected := computeSignature(body, secret)
	return hmac.Equal([]byte(sig[len(prefix):]), []byte(expected))
}

// computeSignature returns the HMAC-SHA256 hex digest of body signed with secret.
func computeSignature(body []byte, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// normalizeGitHubEvent converts a raw GitHub webhook payload into a GitEvent.
func normalizeGitHubEvent(eventType string, body []byte) (*GitEvent, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	event := &GitEvent{
		Provider:   "github",
		EventType:  eventType,
		RawPayload: json.RawMessage(body),
		Timestamp:  time.Now().UTC(),
	}

	// Extract repository full_name.
	if repo, ok := payload["repository"].(map[string]any); ok {
		event.Repository, _ = repo["full_name"].(string)
	}

	switch eventType {
	case "push":
		normalizePushEvent(event, payload)
	case "pull_request":
		normalizePREvent(event, payload)
	case "release":
		normalizeReleaseEvent(event, payload)
	case "create", "delete":
		normalizeRefEvent(event, payload)
	default:
		// Best-effort extraction for unknown event types.
		normalizeGenericEvent(event, payload)
	}

	return event, nil
}

// normalizePushEvent extracts fields from a push event payload.
func normalizePushEvent(event *GitEvent, payload map[string]any) {
	ref, _ := payload["ref"].(string)
	// Convert "refs/heads/main" → "main"
	event.Branch = strings.TrimPrefix(ref, "refs/heads/")

	if headCommit, ok := payload["head_commit"].(map[string]any); ok {
		event.Commit, _ = headCommit["id"].(string)
		event.Message, _ = headCommit["message"].(string)
		event.URL, _ = headCommit["url"].(string)
		if author, ok := headCommit["author"].(map[string]any); ok {
			if name, _ := author["username"].(string); name != "" {
				event.Author = name
			} else {
				event.Author, _ = author["name"].(string)
			}
		}
	} else {
		event.Commit, _ = payload["after"].(string)
	}

	if pusher, ok := payload["pusher"].(map[string]any); ok && event.Author == "" {
		event.Author, _ = pusher["name"].(string)
	}

	if sender, ok := payload["sender"].(map[string]any); ok && event.Author == "" {
		event.Author, _ = sender["login"].(string)
	}
}

// normalizePREvent extracts fields from a pull_request event payload.
func normalizePREvent(event *GitEvent, payload map[string]any) {
	if pr, ok := payload["pull_request"].(map[string]any); ok {
		event.Message, _ = pr["title"].(string)
		event.URL, _ = pr["html_url"].(string)

		if head, ok := pr["head"].(map[string]any); ok {
			event.Branch, _ = head["ref"].(string)
			event.Commit, _ = head["sha"].(string)
		}
		if user, ok := pr["user"].(map[string]any); ok {
			event.Author, _ = user["login"].(string)
		}
	}
}

// normalizeReleaseEvent extracts fields from a release event payload.
func normalizeReleaseEvent(event *GitEvent, payload map[string]any) {
	if release, ok := payload["release"].(map[string]any); ok {
		event.Message, _ = release["tag_name"].(string)
		event.URL, _ = release["html_url"].(string)
		event.Branch, _ = release["tag_name"].(string)
		if author, ok := release["author"].(map[string]any); ok {
			event.Author, _ = author["login"].(string)
		}
	}
}

// normalizeRefEvent extracts fields from a create/delete event payload.
func normalizeRefEvent(event *GitEvent, payload map[string]any) {
	event.Branch, _ = payload["ref"].(string)
	if sender, ok := payload["sender"].(map[string]any); ok {
		event.Author, _ = sender["login"].(string)
	}
}

// normalizeGenericEvent does best-effort extraction from an unknown event.
func normalizeGenericEvent(event *GitEvent, payload map[string]any) {
	if sender, ok := payload["sender"].(map[string]any); ok {
		event.Author, _ = sender["login"].(string)
	}
}

// readLimitedBody reads up to maxBytes from the request body.
// It uses io.LimitReader to cap reads safely without requiring a ResponseWriter.
// If the body is exactly maxBytes, an extra byte is attempted to detect overflow.
func readLimitedBody(r *http.Request, maxBytes int64) ([]byte, error) {
	lr := io.LimitReader(r.Body, maxBytes+1)
	buf, err := io.ReadAll(lr)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}
	if int64(len(buf)) > maxBytes {
		return nil, fmt.Errorf("request body exceeds %d bytes", maxBytes)
	}
	return buf, nil
}

// containsString reports whether slice contains s.
func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
