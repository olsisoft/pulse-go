package pulse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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
			"token":        "new.jwt.token",
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
