package internal

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// --- helpers ---

func signBody(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

func newTestWebhookModule(t *testing.T, config map[string]any) *webhookModule {
	t.Helper()
	m, err := newWebhookModule("test-webhooks", config)
	if err != nil {
		t.Fatalf("newWebhookModule: %v", err)
	}
	return m
}

func doRequest(t *testing.T, m *webhookModule, method, eventType string, body []byte, extraHeaders map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/webhooks/github", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if eventType != "" {
		req.Header.Set("X-GitHub-Event", eventType)
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	m.handleWebhook(rr, req)
	return rr
}

// --- webhook config tests ---

func TestParseWebhookConfig_Defaults(t *testing.T) {
	cfg, err := parseWebhookConfig(map[string]any{})
	if err != nil {
		t.Fatalf("parseWebhookConfig: %v", err)
	}
	if cfg.Provider != "github" {
		t.Errorf("expected default provider=github, got %q", cfg.Provider)
	}
	if cfg.Topic != "git.events" {
		t.Errorf("expected default topic=git.events, got %q", cfg.Topic)
	}
}

func TestParseWebhookConfig_CustomValues(t *testing.T) {
	cfg, err := parseWebhookConfig(map[string]any{
		"provider": "github",
		"secret":   "mysecret",
		"events":   []any{"push", "pull_request"},
		"topic":    "custom.topic",
	})
	if err != nil {
		t.Fatalf("parseWebhookConfig: %v", err)
	}
	if cfg.Secret != "mysecret" {
		t.Errorf("expected secret=mysecret, got %q", cfg.Secret)
	}
	if len(cfg.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(cfg.Events))
	}
	if cfg.Topic != "custom.topic" {
		t.Errorf("expected topic=custom.topic, got %q", cfg.Topic)
	}
}

// --- signature validation tests ---

func TestValidateSignature_Valid(t *testing.T) {
	body := []byte(`{"key":"value"}`)
	secret := "test-secret"
	sig := signBody(secret, body)

	if !validateSignature(body, secret, sig) {
		t.Error("expected valid signature to pass")
	}
}

func TestValidateSignature_Invalid(t *testing.T) {
	body := []byte(`{"key":"value"}`)
	if validateSignature(body, "secret", "sha256=badhash") {
		t.Error("expected invalid signature to fail")
	}
}

func TestValidateSignature_MissingPrefix(t *testing.T) {
	body := []byte(`{}`)
	sig := computeSignature(body, "secret") // hex only, no prefix
	if validateSignature(body, "secret", sig) {
		t.Error("expected signature without sha256= prefix to fail")
	}
}

func TestValidateSignature_WrongSecret(t *testing.T) {
	body := []byte(`{}`)
	sig := signBody("correct-secret", body)
	if validateSignature(body, "wrong-secret", sig) {
		t.Error("expected signature with wrong secret to fail")
	}
}

// --- HTTP handler tests ---

func TestHandleWebhook_MethodNotAllowed(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})
	rr := doRequest(t, m, http.MethodGet, "push", nil, nil)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rr.Code)
	}
}

func TestHandleWebhook_MissingEventHeader(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})
	body := []byte(`{}`)
	rr := doRequest(t, m, http.MethodPost, "", body, nil)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rr.Code)
	}
}

func TestHandleWebhook_MissingSignatureWhenSecretSet(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{"secret": "my-secret"})
	body := []byte(`{}`)
	rr := doRequest(t, m, http.MethodPost, "push", body, nil)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestHandleWebhook_InvalidSignature(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{"secret": "my-secret"})
	body := []byte(`{}`)
	rr := doRequest(t, m, http.MethodPost, "push", body, map[string]string{
		"X-Hub-Signature-256": "sha256=badhash",
	})
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rr.Code)
	}
}

func TestHandleWebhook_ValidSignature(t *testing.T) {
	secret := "my-secret"
	m := newTestWebhookModule(t, map[string]any{"secret": secret})

	body := []byte(`{"ref":"refs/heads/main","head_commit":{"id":"abc123","message":"test","url":"https://github.com","author":{"name":"alice"}}}`)
	sig := signBody(secret, body)

	rr := doRequest(t, m, http.MethodPost, "push", body, map[string]string{
		"X-Hub-Signature-256": sig,
	})
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestHandleWebhook_NoSecret(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})

	body := []byte(`{"ref":"refs/heads/main","head_commit":{"id":"abc123","message":"test","url":"https://github.com","author":{"name":"alice"}}}`)
	rr := doRequest(t, m, http.MethodPost, "push", body, nil)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
}

func TestHandleWebhook_FilteredEvent(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{
		"events": []any{"push"}, // only accept push events
	})

	body := []byte(`{}`)
	// Send a pull_request event which is not in the filter list.
	rr := doRequest(t, m, http.MethodPost, "pull_request", body, nil)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 (ignored), got %d", rr.Code)
	}
	var resp map[string]any
	json.NewDecoder(rr.Body).Decode(&resp) //nolint:errcheck
	if resp["status"] != "ignored" {
		t.Errorf("expected status=ignored, got %v", resp["status"])
	}
}

func TestHandleWebhook_PublishesEvent(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})
	pub := &fakePublisher{}
	m.SetMessagePublisher(pub)

	body := []byte(`{"ref":"refs/heads/main","head_commit":{"id":"abc123","message":"fix bug","url":"https://github.com","author":{"username":"alice"}},"repository":{"full_name":"owner/repo"}}`)
	rr := doRequest(t, m, http.MethodPost, "push", body, nil)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	if len(pub.messages) != 1 {
		t.Fatalf("expected 1 published message, got %d", len(pub.messages))
	}

	var event GitEvent
	if err := json.Unmarshal(pub.messages[0].payload, &event); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if event.Provider != "github" {
		t.Errorf("expected provider=github, got %q", event.Provider)
	}
	if event.EventType != "push" {
		t.Errorf("expected event_type=push, got %q", event.EventType)
	}
	if event.Repository != "owner/repo" {
		t.Errorf("expected repository=owner/repo, got %q", event.Repository)
	}
	if event.Branch != "main" {
		t.Errorf("expected branch=main, got %q", event.Branch)
	}
	if event.Commit != "abc123" {
		t.Errorf("expected commit=abc123, got %q", event.Commit)
	}
	if event.Author != "alice" {
		t.Errorf("expected author=alice, got %q", event.Author)
	}
	if event.Message != "fix bug" {
		t.Errorf("expected message='fix bug', got %q", event.Message)
	}
}

// --- normalization tests ---

func TestNormalizePushEvent(t *testing.T) {
	body := []byte(`{
		"ref": "refs/heads/feature/xyz",
		"after": "deadbeef",
		"head_commit": {
			"id": "deadbeef",
			"message": "add feature",
			"url": "https://github.com/owner/repo/commit/deadbeef",
			"author": {"username": "bob", "name": "Bob Smith"}
		},
		"repository": {"full_name": "owner/repo"}
	}`)

	event, err := normalizeGitHubEvent("push", body)
	if err != nil {
		t.Fatalf("normalizeGitHubEvent: %v", err)
	}
	if event.Branch != "feature/xyz" {
		t.Errorf("expected branch=feature/xyz, got %q", event.Branch)
	}
	if event.Commit != "deadbeef" {
		t.Errorf("expected commit=deadbeef, got %q", event.Commit)
	}
	if event.Author != "bob" {
		t.Errorf("expected author=bob, got %q", event.Author)
	}
}

func TestNormalizePREvent(t *testing.T) {
	body := []byte(`{
		"pull_request": {
			"title": "Add feature",
			"html_url": "https://github.com/owner/repo/pull/1",
			"head": {"ref": "feature/pr", "sha": "aabbcc"},
			"user": {"login": "carol"}
		},
		"repository": {"full_name": "owner/repo"}
	}`)

	event, err := normalizeGitHubEvent("pull_request", body)
	if err != nil {
		t.Fatalf("normalizeGitHubEvent: %v", err)
	}
	if event.EventType != "pull_request" {
		t.Errorf("expected event_type=pull_request, got %q", event.EventType)
	}
	if event.Branch != "feature/pr" {
		t.Errorf("expected branch=feature/pr, got %q", event.Branch)
	}
	if event.Commit != "aabbcc" {
		t.Errorf("expected commit=aabbcc, got %q", event.Commit)
	}
	if event.Author != "carol" {
		t.Errorf("expected author=carol, got %q", event.Author)
	}
	if event.Message != "Add feature" {
		t.Errorf("expected message='Add feature', got %q", event.Message)
	}
}

func TestNormalizeReleaseEvent(t *testing.T) {
	body := []byte(`{
		"release": {
			"tag_name": "v1.0.0",
			"html_url": "https://github.com/owner/repo/releases/v1.0.0",
			"author": {"login": "dave"}
		},
		"repository": {"full_name": "owner/repo"}
	}`)

	event, err := normalizeGitHubEvent("release", body)
	if err != nil {
		t.Fatalf("normalizeGitHubEvent: %v", err)
	}
	if event.Branch != "v1.0.0" {
		t.Errorf("expected branch=v1.0.0, got %q", event.Branch)
	}
	if event.Author != "dave" {
		t.Errorf("expected author=dave, got %q", event.Author)
	}
}

func TestNormalizeRefEvent(t *testing.T) {
	body := []byte(`{
		"ref": "v2.0.0",
		"sender": {"login": "eve"},
		"repository": {"full_name": "owner/repo"}
	}`)

	event, err := normalizeGitHubEvent("create", body)
	if err != nil {
		t.Fatalf("normalizeGitHubEvent: %v", err)
	}
	if event.Branch != "v2.0.0" {
		t.Errorf("expected branch=v2.0.0, got %q", event.Branch)
	}
	if event.Author != "eve" {
		t.Errorf("expected author=eve, got %q", event.Author)
	}
}

func TestNormalizeUnknownEvent(t *testing.T) {
	body := []byte(`{"sender":{"login":"frank"},"repository":{"full_name":"owner/repo"}}`)

	event, err := normalizeGitHubEvent("star", body)
	if err != nil {
		t.Fatalf("normalizeGitHubEvent: %v", err)
	}
	if event.EventType != "star" {
		t.Errorf("expected event_type=star, got %q", event.EventType)
	}
	if event.Author != "frank" {
		t.Errorf("expected author=frank, got %q", event.Author)
	}
}

func TestNormalizeGitHubEvent_InvalidJSON(t *testing.T) {
	_, err := normalizeGitHubEvent("push", []byte("not json"))
	if err == nil {
		t.Error("expected error for invalid JSON payload")
	}
}

// --- module lifecycle ---

func TestWebhookModule_Lifecycle(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})
	if err := m.Init(); err != nil {
		t.Fatalf("Init: %v", err)
	}
	// Start registers the HTTP handler on the default mux; just verify it does not error.
	// (We can't easily un-register it, but it won't conflict in tests.)
}

func TestWebhookModule_Name(t *testing.T) {
	m := newTestWebhookModule(t, map[string]any{})
	if m.Name() != "test-webhooks" {
		t.Errorf("expected name=test-webhooks, got %q", m.Name())
	}
}

// --- fakePublisher ---

type publishedMessage struct {
	topic    string
	payload  []byte
	metadata map[string]string
}

type fakePublisher struct {
	messages []publishedMessage
}

func (p *fakePublisher) Publish(topic string, payload []byte, metadata map[string]string) (string, error) {
	p.messages = append(p.messages, publishedMessage{topic: topic, payload: payload, metadata: metadata})
	return "msg-id", nil
}
