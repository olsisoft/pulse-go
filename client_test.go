package pulse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newTestServer spins up an httptest.Server, registers the provided handler,
// and returns the server URL + cleanup func. Closest equivalent of msw / respx
// for Go — uses the real net/http stack, so the test exercises the full
// transport including Bearer header injection, URL encoding, and Retry-After
// parsing.
func newTestServer(t *testing.T, handler http.HandlerFunc) (string, func()) {
	t.Helper()
	srv := httptest.NewServer(handler)
	return srv.URL, srv.Close
}

func newClient(t *testing.T, baseURL string, opts ...Option) *Client {
	t.Helper()
	all := append([]Option{WithBaseURL(baseURL)}, opts...)
	c, err := NewClient(all...)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func writeJSON(t *testing.T, w http.ResponseWriter, status int, body any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body != nil {
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Fatalf("encode response: %v", err)
		}
	}
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

func TestLifecycle_TokenMutable(t *testing.T) {
	c := newClient(t, "http://example.test")
	if c.Token() != "" {
		t.Fatalf("expected empty token, got %q", c.Token())
	}
	c.SetToken("abc")
	if c.Token() != "abc" {
		t.Fatalf("expected abc, got %q", c.Token())
	}
	c.SetToken("")
	if c.Token() != "" {
		t.Fatalf("expected empty after clear")
	}
}

func TestLifecycle_BaseURLTrailingSlashStripped(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]string{"version": "2.6.0"})
	})
	defer stop()
	c := newClient(t, url+"//")
	got, err := c.Version(context.Background())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if got["version"] != "2.6.0" {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestLifecycle_MissingBaseURLFails(t *testing.T) {
	_, err := NewClient()
	if err == nil {
		t.Fatal("expected error for missing baseURL")
	}
}

func TestLifecycle_NilHTTPClientFails(t *testing.T) {
	_, err := NewClient(WithBaseURL("http://x"), WithHTTPClient(nil))
	if err == nil {
		t.Fatal("expected error for nil http.Client")
	}
}

// ---------------------------------------------------------------------------
// Version
// ---------------------------------------------------------------------------

func TestVersion_PublicNoTokenRequired(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pulse/version" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]string{"version": "2.6.0", "edition": "desktop"})
	})
	defer stop()
	c := newClient(t, url)
	if c.Token() != "" {
		t.Fatal("token should be empty")
	}
	got, err := c.Version(context.Background())
	if err != nil {
		t.Fatalf("Version: %v", err)
	}
	if got["edition"] != "desktop" {
		t.Fatalf("unexpected: %v", got)
	}
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

func TestAuth_LoginCachesToken(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"alice"`) {
			t.Fatalf("body missing username: %s", body)
		}
		writeJSON(t, w, 200, map[string]any{
			"accessToken":        "new.jwt.token",
			"refreshToken": "refresh.token",
			"activeOrg":    map[string]string{"id": "org1", "name": "Acme"},
		})
	})
	defer stop()
	c := newClient(t, url)
	got, err := c.Auth.Login(context.Background(), "alice", "secret")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if c.Token() != "new.jwt.token" {
		t.Fatalf("expected token cached: %q", c.Token())
	}
	if got["refreshToken"] != "refresh.token" {
		t.Fatalf("missing refreshToken: %v", got)
	}
}

func TestAuth_LoginFailureRaisesAuthError(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 401, map[string]string{"error": "Invalid credentials"})
	})
	defer stop()
	c := newClient(t, url)
	_, err := c.Auth.Login(context.Background(), "alice", "wrong")
	if err == nil {
		t.Fatal("expected error")
	}
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "Invalid credentials") {
		t.Fatalf("expected error to mention body: %v", err)
	}
	if c.Token() != "" {
		t.Fatalf("token should NOT be cached on failure: %q", c.Token())
	}
}

func TestAuth_RefreshCachesNewToken(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]string{"token": "refreshed.jwt"})
	})
	defer stop()
	c := newClient(t, url)
	if _, err := c.Auth.Refresh(context.Background(), "rtok"); err != nil {
		t.Fatal(err)
	}
	if c.Token() != "refreshed.jwt" {
		t.Fatalf("token not cached: %q", c.Token())
	}
}

func TestAuth_OrganizationsUnwrapsEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"organizations": []map[string]string{{"id": "o1", "name": "Acme"}},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	orgs, err := c.Auth.Organizations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orgs) != 1 || orgs[0]["id"] != "o1" {
		t.Fatalf("unexpected: %v", orgs)
	}
}

func TestAuth_SwitchOrgCachesNewToken(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]string{"token": "switched.jwt"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	if _, err := c.Auth.SwitchOrg(context.Background(), "org2"); err != nil {
		t.Fatal(err)
	}
	if c.Token() != "switched.jwt" {
		t.Fatalf("token not cached: %q", c.Token())
	}
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

func TestPipelines_ListUnwrapsEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"pipelines": []map[string]any{
				{"id": "p1", "name": "demo"},
				{"id": "p2", "name": "fraud"},
			},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	pipelines, err := c.Pipelines.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(pipelines) != 2 || pipelines[0]["id"] != "p1" {
		t.Fatalf("unexpected: %v", pipelines)
	}
}

func TestPipelines_ListEmptyOnMissingEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	pipelines, err := c.Pipelines.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if pipelines == nil {
		t.Fatal("expected non-nil empty slice (so range-over works)")
	}
	if len(pipelines) != 0 {
		t.Fatalf("expected empty: %v", pipelines)
	}
}

func TestPipelines_GetReturnsOnePipeline(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pulse/pipelines/p1" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]string{"id": "p1", "name": "demo"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	got, err := c.Pipelines.Get(context.Background(), "p1")
	if err != nil {
		t.Fatal(err)
	}
	if got["id"] != "p1" {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestPipelines_GetMissingRaisesNotFound(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]string{"error": "not found"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.Pipelines.Get(context.Background(), "nope")
	var notFound *NotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestPipelines_CreateReturnsCreated(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"name":"new"`) {
			t.Fatalf("body missing name: %s", body)
		}
		writeJSON(t, w, 201, map[string]any{"id": "p3", "name": "new"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	got, err := c.Pipelines.Create(context.Background(), map[string]any{
		"name":  "new",
		"nodes": []map[string]any{{"id": "n1", "type": "source"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got["id"] != "p3" {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestPipelines_CreateValidationRaisesValidationError(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 400, map[string]string{"error": "Missing required field: nodes"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.Pipelines.Create(context.Background(), map[string]any{"name": "bad"})
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestPipelines_Delete204Returns(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/pulse/pipelines/p1" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(204)
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	if err := c.Pipelines.Delete(context.Background(), "p1"); err != nil {
		t.Fatal(err)
	}
}

func TestPipelines_PathParamsURLEncoded(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// foo/bar should be encoded to foo%2Fbar in the URL path
		if !strings.Contains(r.URL.EscapedPath(), "foo%2Fbar") {
			t.Fatalf("expected encoded path, got: %s", r.URL.EscapedPath())
		}
		writeJSON(t, w, 200, map[string]string{"id": "foo/bar"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	got, err := c.Pipelines.Get(context.Background(), "foo/bar")
	if err != nil {
		t.Fatal(err)
	}
	if got["id"] != "foo/bar" {
		t.Fatalf("unexpected: %v", got)
	}
}

// ---------------------------------------------------------------------------
// Agents + Templates
// ---------------------------------------------------------------------------

func TestAgents_ListUnwrapsEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"agents": []map[string]any{
				{"id": "a1", "name": "fraud-detector", "engineType": "streaming"},
			},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	agents, err := c.Agents.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agents[0]["engineType"] != "streaming" {
		t.Fatalf("unexpected: %v", agents)
	}
}

func TestAgents_GetReturnsOne(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]string{"id": "a1", "name": "fraud-detector"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	got, err := c.Agents.Get(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if got["id"] != "a1" {
		t.Fatalf("unexpected: %v", got)
	}
}

func TestAgents_UpdatePutsFullConfigAndReturnsFreshSnapshot(t *testing.T) {
	var receivedBody []byte
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/api/pulse/agents/a1" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		receivedBody = readAll(t, r)
		writeJSON(t, w, 200, map[string]any{
			"id":         "a1",
			"name":       "fraud-detector-v2",
			"engineType": "rule-based",
			"status":     "running",
		})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	newConfig := map[string]any{
		"name":       "fraud-detector-v2",
		"engineType": "rule-based",
		"config": map[string]any{
			"rules": []map[string]string{{"if": "amount > 5000", "then": "block"}},
		},
	}
	result, err := c.Agents.Update(context.Background(), "a1", newConfig)
	if err != nil {
		t.Fatal(err)
	}
	if result["name"] != "fraud-detector-v2" {
		t.Fatalf("name: %v", result["name"])
	}
	var body map[string]any
	if err := json.Unmarshal(receivedBody, &body); err != nil {
		t.Fatalf("body not JSON: %s", receivedBody)
	}
	if body["engineType"] != "rule-based" {
		t.Fatalf("unexpected wire body: %v", body)
	}
}

func TestAgents_UpdateRaisesValidationOnSelfLoop400(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 400, map[string]any{
			"error":        "Agent would self-loop: outputTopic == inputTopic",
			"unsafeFields": []string{"outputTopic"},
		})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	badConfig := map[string]any{"name": "x", "inputTopic": "t", "outputTopic": "t"}
	_, err := c.Agents.Update(context.Background(), "a1", badConfig)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestAgents_UpdateRaisesNotFoundOnMissingAgent(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{"error": "Agent not found: missing"})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.Agents.Update(context.Background(), "missing", map[string]any{"name": "x"})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestAgents_DeleteReturnsNilOn204(t *testing.T) {
	hit := false
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/pulse/agents/a1" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		hit = true
		w.WriteHeader(204)
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	if err := c.Agents.Delete(context.Background(), "a1"); err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("server was not called")
	}
}

func TestAgents_DeleteRaisesNotFound(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{"error": "Agent not found"})
	})
	defer stop()

	c := newClient(t, endpoint, WithToken("fake.jwt"))
	err := c.Agents.Delete(context.Background(), "missing")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestAgents_UpdateWithoutTokenRaisesAuthBeforeAnyHttpCall(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) { hits++ })
	defer stop()

	c := newClient(t, endpoint) // no WithToken
	_, err := c.Agents.Update(context.Background(), "a1", map[string]any{"name": "x"})
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}

func TestAgents_DeleteWithoutTokenRaisesAuthBeforeAnyHttpCall(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) { hits++ })
	defer stop()

	c := newClient(t, endpoint) // no WithToken
	err := c.Agents.Delete(context.Background(), "a1")
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}

func TestTemplates_ListUnwrapsEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"templates": []map[string]string{{"id": "fraud-detection", "name": "Fraud Detection"}},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	templates, err := c.Templates.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if templates[0]["id"] != "fraud-detection" {
		t.Fatalf("unexpected: %v", templates)
	}
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

func TestErrors_NoTokenSetRaisesAuthErrorWithoutCallingServer(t *testing.T) {
	hits := 0
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
	})
	defer stop()
	c := newClient(t, url) // no WithToken
	_, err := c.Pipelines.List(context.Background())
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}

func TestErrors_RateLimitParsesRetryAfterFromBody(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 429, map[string]any{
			"error":             "Rate limit exceeded",
			"errorCode":         "RATE_LIMITED",
			"retryAfterSeconds": 60,
			"limit":             120,
			"remaining":         0,
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.Pipelines.List(context.Background())
	var rl *RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("expected *RateLimitError, got %T: %v", err, err)
	}
	if rl.RetryAfterSeconds != 60 {
		t.Fatalf("expected retryAfterSeconds=60, got %d", rl.RetryAfterSeconds)
	}
}

func TestErrors_RateLimitFallsBackToRetryAfterHeader(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
		_, _ = fmt.Fprintln(w, "Too Many Requests")
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.Pipelines.List(context.Background())
	var rl *RateLimitError
	if !errors.As(err, &rl) {
		t.Fatalf("expected *RateLimitError, got %T: %v", err, err)
	}
	if rl.RetryAfterSeconds != 30 {
		t.Fatalf("expected retryAfterSeconds=30, got %d", rl.RetryAfterSeconds)
	}
}

func TestErrors_Unknown5xxRaisesGenericAPIError(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 500, map[string]string{"error": "Internal", "errorClass": "NPE"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.Pipelines.List(context.Background())
	// Should be *APIError but NOT one of the specialised types
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	var (
		authErr       *AuthError
		notFound      *NotFoundError
		validationErr *ValidationError
		rateLimitErr  *RateLimitError
	)
	if errors.As(err, &authErr) || errors.As(err, &notFound) || errors.As(err, &validationErr) || errors.As(err, &rateLimitErr) {
		t.Fatalf("expected base *APIError only, got specialised: %T", err)
	}
	if apiErr.StatusCode != 500 {
		t.Fatalf("expected 500, got %d", apiErr.StatusCode)
	}
}

func TestErrors_BearerTokenAttachedToOutboundRequest(t *testing.T) {
	var observedAuth string
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedAuth = r.Header.Get("Authorization")
		writeJSON(t, w, 200, map[string]any{"pipelines": []any{}})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt.token"))
	if _, err := c.Pipelines.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observedAuth != "Bearer fake.jwt.token" {
		t.Fatalf("expected Bearer header, got %q", observedAuth)
	}
}

func TestErrors_UserAgentHeaderIsSet(t *testing.T) {
	var observedUA string
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedUA = r.Header.Get("User-Agent")
		writeJSON(t, w, 200, map[string]any{"pipelines": []any{}})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	if _, err := c.Pipelines.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(observedUA, "pulse-client-go") {
		t.Fatalf("expected pulse-client-go in User-Agent, got %q", observedUA)
	}
}

// ---------------------------------------------------------------------------
// Events SSE — B-098 Phase 7
// ---------------------------------------------------------------------------

func TestEvents_StreamYieldsParsedEvents(t *testing.T) {
	sseBody := "data: {\"type\":\"fraud_signal\",\"payload\":{\"customerId\":\"c1\"}}\n\n" +
		"data: {\"type\":\"heartbeat\"}\n\n"
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("expected text/event-stream Accept, got %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(sseBody))
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	events, errCh := c.Events.Stream(context.Background())

	var collected []map[string]any
	for ev := range events {
		collected = append(collected, ev)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(collected) != 2 {
		t.Fatalf("expected 2 events, got %d: %v", len(collected), collected)
	}
	if collected[0]["type"] != "fraud_signal" {
		t.Fatalf("unexpected first event: %v", collected[0])
	}
}

func TestEvents_StreamSkipsCommentsAndHeartbeats(t *testing.T) {
	sseBody := ": keep-alive\n\n" +
		"data: {\"type\":\"a\"}\n\n" +
		": another keep-alive\n\n" +
		"data: {\"type\":\"b\"}\n\n"
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(sseBody))
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	events, errCh := c.Events.Stream(context.Background())
	var types []string
	for ev := range events {
		types = append(types, fmt.Sprintf("%v", ev["type"]))
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(types) != 2 || types[0] != "a" || types[1] != "b" {
		t.Fatalf("unexpected: %v", types)
	}
}

func TestEvents_StreamFallbackForNonJSONPayload(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: not-json-here\n\n"))
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	events, errCh := c.Events.Stream(context.Background())
	var got map[string]any
	for ev := range events {
		got = ev
	}
	if err := <-errCh; err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got["data"] != "not-json-here" {
		t.Fatalf("expected raw envelope, got: %v", got)
	}
}

func TestEvents_StreamRaisesAuthErrorWhenNoToken(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("server should not have been called")
	})
	defer stop()
	c := newClient(t, url) // no token
	events, errCh := c.Events.Stream(context.Background())
	// events channel closes immediately, errCh has the AuthError
	for range events {
		// drain
	}
	err := <-errCh
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
}

func TestEvents_StreamRaisesOn401(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(401)
		_, _ = w.Write([]byte(`{"error":"expired"}`))
	})
	defer stop()
	c := newClient(t, url, WithToken("expired.jwt"))
	events, errCh := c.Events.Stream(context.Background())
	for range events {
	}
	err := <-errCh
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected *AuthError on 401, got %T: %v", err, err)
	}
}

func TestEvents_StreamRespectsContextCancellation(t *testing.T) {
	// Server emits one event then blocks; cancellation should terminate.
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("ResponseWriter doesn't support flushing")
		}
		_, _ = w.Write([]byte("data: {\"type\":\"first\"}\n\n"))
		flusher.Flush()
		<-r.Context().Done()
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	ctx, cancel := context.WithCancel(context.Background())
	events, _ := c.Events.Stream(ctx)
	got := <-events
	if got["type"] != "first" {
		t.Fatalf("expected first event, got: %v", got)
	}
	cancel()
	// Drain — channel should close shortly after cancel
	for range events {
	}
}

func TestErrors_ContextCancellationCancelsRequest(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Never respond — the test cancels the context to interrupt
		<-r.Context().Done()
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before call
	_, err := c.Pipelines.List(ctx)
	if err == nil {
		t.Fatal("expected context-cancelled error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Wasm — B-110 sandboxed WASM module registry (client.Wasm)
// ---------------------------------------------------------------------------

// parseWasmUpload reads a multipart/form-data POST body, returning the "module"
// file part bytes plus the text fields. Mirrors the server-side contract the
// upload endpoint enforces (file field "module" + fields "name"/"description").
func parseWasmUpload(t *testing.T, r *http.Request) (fileBytes []byte, fields map[string]string) {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		t.Fatalf("expected multipart/form-data, got %q (%v)", mediaType, err)
	}
	mr := multipart.NewReader(r.Body, params["boundary"])
	fields = map[string]string{}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read multipart part: %v", err)
		}
		data, err := io.ReadAll(part)
		if err != nil {
			t.Fatalf("read part body: %v", err)
		}
		if part.FormName() == "module" {
			fileBytes = data
		} else {
			fields[part.FormName()] = string(data)
		}
	}
	return fileBytes, fields
}

func TestWasm_UploadFromBytesSendsMultipart(t *testing.T) {
	var gotFile []byte
	var gotFields map[string]string
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/pulse/wasm-modules" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		gotFile, gotFields = parseWasmUpload(t, r)
		writeJSON(t, w, http.StatusCreated, map[string]any{"name": "redactor", "version": 1, "sizeBytes": 9})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))

	wasmBytes := validWasmModuleBytes()
	meta, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{
		Name: "redactor", Data: wasmBytes, Description: "pii",
	})
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if meta["name"] != "redactor" {
		t.Fatalf("expected name redactor, got %v", meta["name"])
	}
	if string(gotFile) != string(wasmBytes) {
		t.Fatalf("file part mismatch: got %x", gotFile)
	}
	if gotFields["name"] != "redactor" {
		t.Fatalf("expected name field redactor, got %q", gotFields["name"])
	}
	if gotFields["description"] != "pii" {
		t.Fatalf("expected description pii, got %q", gotFields["description"])
	}
}

func TestWasm_UploadFromPathReadsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mod.wasm")
	wasmBytes := validWasmModuleBytes()
	if err := os.WriteFile(path, wasmBytes, 0o600); err != nil {
		t.Fatalf("write temp wasm: %v", err)
	}
	var gotFile []byte
	var gotFields map[string]string
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotFile, gotFields = parseWasmUpload(t, r)
		writeJSON(t, w, http.StatusCreated, map[string]any{"name": "fromfile"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))

	if _, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "fromfile", Path: path}); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if string(gotFile) != string(wasmBytes) {
		t.Fatalf("file part mismatch: got %x", gotFile)
	}
	if _, ok := gotFields["description"]; ok {
		t.Fatalf("description must be omitted when unset, got %q", gotFields["description"])
	}
}

func TestWasm_UploadRejectsBlankName(t *testing.T) {
	c := newClient(t, "http://unused", WithToken("fake.jwt"))
	if _, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "  ", Data: []byte{0x01}}); err == nil ||
		!strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("expected non-empty-name error, got %v", err)
	}
}

func TestWasm_UploadRequiresExactlyOneSource(t *testing.T) {
	c := newClient(t, "http://unused", WithToken("fake.jwt"))
	if _, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "m"}); err == nil ||
		!strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected exactly-one error (neither), got %v", err)
	}
	if _, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "m", Path: "x", Data: []byte{0x01}}); err == nil ||
		!strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("expected exactly-one error (both), got %v", err)
	}
}

func TestWasm_UploadRejectsEmptyBytes(t *testing.T) {
	// A non-nil but zero-length Data passes the exactly-one-of guard (Data !=
	// nil), so the dedicated empty-bytes guard must fire.
	c := newClient(t, "http://unused", WithToken("fake.jwt"))
	if _, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "m", Data: []byte{}}); err == nil ||
		!strings.Contains(err.Error(), "empty") {
		t.Fatalf("expected empty-bytes error, got %v", err)
	}
}

func TestWasm_ListUnwrapsEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/pulse/wasm-modules" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		writeJSON(t, w, http.StatusOK, map[string]any{"modules": []any{map[string]any{"name": "redactor"}}})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	mods, err := c.Wasm.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(mods) != 1 || mods[0]["name"] != "redactor" {
		t.Fatalf("expected one module redactor, got %v", mods)
	}
}

func TestWasm_ListEmptyOnMissingEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	mods, err := c.Wasm.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if mods == nil || len(mods) != 0 {
		t.Fatalf("expected empty non-nil slice, got %v", mods)
	}
}

func TestWasm_GetReturnsOneModule(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/pulse/wasm-modules/redactor" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		writeJSON(t, w, http.StatusOK, map[string]any{"name": "redactor", "version": 2})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	mod, err := c.Wasm.Get(context.Background(), "redactor")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v, _ := mod["version"].(float64); v != 2 {
		t.Fatalf("expected version 2, got %v", mod["version"])
	}
}

func TestWasm_GetRejectsBlankName(t *testing.T) {
	c := newClient(t, "http://unused", WithToken("fake.jwt"))
	if _, err := c.Wasm.Get(context.Background(), "  "); err == nil ||
		!strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("expected non-empty-name error, got %v", err)
	}
}

func TestWasm_DeleteReturnsNil(t *testing.T) {
	var hit bool
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/api/pulse/wasm-modules/redactor" {
			t.Fatalf("unexpected %s %s", r.Method, r.URL.Path)
		}
		hit = true
		writeJSON(t, w, http.StatusOK, map[string]any{"deleted": "redactor"})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	if err := c.Wasm.Delete(context.Background(), "redactor"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !hit {
		t.Fatal("expected DELETE to reach server")
	}
}

func TestWasm_DeleteRejectsBlankName(t *testing.T) {
	c := newClient(t, "http://unused", WithToken("fake.jwt"))
	if err := c.Wasm.Delete(context.Background(), ""); err == nil ||
		!strings.Contains(err.Error(), "non-empty") {
		t.Fatalf("expected non-empty-name error, got %v", err)
	}
}

func TestWasm_UploadWithoutTokenRaisesAuthBeforeHTTP(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("server must not be reached without a token")
	})
	defer stop()
	c := newClient(t, url) // no token
	_, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "m", Data: validWasmModuleBytes()})
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		t.Fatalf("expected AuthError, got %v", err)
	}
}

// hexToBytes decodes a space-separated hex string into bytes for WASM fixtures.
func hexToBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatalf("decode hex fixture: %v", err)
	}
	return b
}

// validWasmModuleBytes returns a minimal module that passes validateWasmModule:
// magic+version, an export section listing alloc, process and memory, no imports.
// Export section: id 0x07, size 0x1c (28), count 0x03, then three entries
// (alloc/process/memory), each name(uleb len+bytes) + kind byte + uleb index.
func validWasmModuleBytes() []byte {
	return []byte{
		0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
		0x07, 0x1c, 0x03,
		0x05, 0x61, 0x6c, 0x6c, 0x6f, 0x63, 0x00, 0x00,
		0x07, 0x70, 0x72, 0x6f, 0x63, 0x65, 0x73, 0x73, 0x00, 0x00,
		0x06, 0x6d, 0x65, 0x6d, 0x6f, 0x72, 0x79, 0x02, 0x00,
	}
}

func TestValidateWasmModule_AcceptsValid(t *testing.T) {
	if err := validateWasmModule(validWasmModuleBytes()); err != nil {
		t.Fatalf("expected valid module to pass, got %v", err)
	}
}

func TestValidateWasmModule_RejectsImports(t *testing.T) {
	mod := hexToBytes(t, "00 61 73 6d 01 00 00 00 02 09 01 03 65 6e 76 01 66 00 00 "+
		"07 1c 01 05 61 6c 6c 6f 63 00 00 07 70 72 6f 63 65 73 73 00 00 06 6d 65 6d 6f 72 79 02 00")
	err := validateWasmModule(mod)
	if err == nil || !strings.Contains(err.Error(), "imports host functions") {
		t.Fatalf("expected imports-host-functions error, got %v", err)
	}
}

func TestValidateWasmModule_RejectsEmpty(t *testing.T) {
	if err := validateWasmModule(nil); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected too-short error, got %v", err)
	}
	if err := validateWasmModule([]byte{0x00, 0x61, 0x73}); err == nil || !strings.Contains(err.Error(), "too short") {
		t.Fatalf("expected too-short error, got %v", err)
	}
}

func TestValidateWasmModule_RejectsBadMagic(t *testing.T) {
	bad := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x00, 0x00, 0x00}
	if err := validateWasmModule(bad); err == nil || !strings.Contains(err.Error(), "bad magic/version") {
		t.Fatalf("expected bad-magic error, got %v", err)
	}
	// correct magic, wrong version
	badVer := []byte{0x00, 0x61, 0x73, 0x6d, 0x02, 0x00, 0x00, 0x00}
	if err := validateWasmModule(badVer); err == nil || !strings.Contains(err.Error(), "bad magic/version") {
		t.Fatalf("expected bad-version error, got %v", err)
	}
}

func TestValidateWasmModule_RejectsMissingExport(t *testing.T) {
	// Export section lists only alloc — process and memory absent.
	mod := hexToBytes(t, "00 61 73 6d 01 00 00 00 07 09 01 05 61 6c 6c 6f 63 00 00")
	err := validateWasmModule(mod)
	if err == nil || !strings.Contains(err.Error(), "must export alloc, process and memory") {
		t.Fatalf("expected missing-export error, got %v", err)
	}
}

func TestWasm_UploadRejectsInvalidModuleWithoutHTTP(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("server must not be reached for an invalid module")
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	// bad magic — fails validation before the HTTP request
	_, err := c.Wasm.Upload(context.Background(), UploadWasmOptions{Name: "m", Data: []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}})
	if err == nil || !strings.Contains(err.Error(), "bad magic/version") {
		t.Fatalf("expected bad-magic error, got %v", err)
	}
}
