package pulse

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
)

// B-113 Events.Replay tests. Mirrors pulse-py events.replay: sends
// from/to/limit, hits the IQ state/replay route, unwraps the changes slice.

func TestEvents_ReplayUnwrapsChanges(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		if r.URL.Path != "/api/pulse/iq/agents/user-sessions/state/replay/u42" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId": "user-sessions",
			"key":     "u42",
			"count":   2,
			"changes": []map[string]any{
				{"timestamp": 1716541200000, "changeType": "PUT", "value": map[string]any{"cart_value": 0}, "eventId": "e1"},
				{"timestamp": 1716544800000, "changeType": "PUT", "value": map[string]any{"cart_value": 70}, "eventId": "e2"},
			},
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	changes, err := c.Events.Replay(context.Background(), "user-sessions", "u42", EventsReplayOptions{
		From: "2026-05-24T10:00:00Z", To: "2026-05-24T11:00:00Z", Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("from") != "2026-05-24T10:00:00Z" || observedQuery.Get("to") != "2026-05-24T11:00:00Z" {
		t.Fatalf("unexpected from/to: %v", observedQuery)
	}
	if observedQuery.Get("limit") != "50" {
		t.Fatalf("expected limit=50, got: %q", observedQuery.Get("limit"))
	}
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d", len(changes))
	}
	if changes[0]["changeType"] != "PUT" {
		t.Fatalf("unexpected changeType: %v", changes[0])
	}
	if int(changes[1]["value"].(map[string]any)["cart_value"].(float64)) != 70 {
		t.Fatalf("unexpected value: %v", changes[1])
	}
}

func TestEvents_ReplayEmptyOptionsSendsDefaults(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1", "key": "k1", "count": 0, "changes": []any{},
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	changes, err := c.Events.Replay(context.Background(), "a1", "k1", EventsReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("from") != "-1h" || observedQuery.Get("to") != "now" {
		t.Fatalf("expected default from=-1h&to=now, got: %v", observedQuery)
	}
	if observedQuery.Get("limit") != "100" {
		t.Fatalf("expected default limit=100, got: %q", observedQuery.Get("limit"))
	}
	if len(changes) != 0 {
		t.Fatalf("expected empty changes, got %d", len(changes))
	}
}

func TestEvents_ReplayMissingChangesReturnsEmpty(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// server returns no "changes" field
		writeJSON(t, w, 200, map[string]any{"agentId": "a1", "key": "k1", "count": 0})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	changes, err := c.Events.Replay(context.Background(), "a1", "k1", EventsReplayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if changes == nil || len(changes) != 0 {
		t.Fatalf("expected non-nil empty slice, got: %v", changes)
	}
}

func TestEvents_Replay404AgentNotQueryableRaises(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error": "Agent has no queryable state", "agentId": "a1",
			"reason": "non-streaming or stopped",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.Events.Replay(context.Background(), "a1", "k1", EventsReplayOptions{})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestEvents_ReplayWithoutTokenRaisesAuthError(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
	})
	defer stop()
	c := newClient(t, endpoint) // no token
	_, err := c.Events.Replay(context.Background(), "a1", "k1", EventsReplayOptions{})
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}
