package pulse

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"
)

// B-113 time-travel IQ tests. Mirrors the pulse-py B-113 coverage:
// GetAsOf sends as_of + returns the historical value, Diff sends from/to
// and returns the changes map.

func TestIQ_GetAsOfSendsParamAndReturnsValue(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		if r.URL.Path != "/api/pulse/iq/agents/user-sessions/state/value/u42" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId": "user-sessions",
			"key":     "u42",
			"value":   map[string]any{"cart_value": 0, "items": 1},
			"asOf":    1716544800000,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, err := c.IQ.GetAsOf(context.Background(), "user-sessions", "u42", "-1h")
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("as_of") != "-1h" {
		t.Fatalf("expected as_of=-1h, got: %q", observedQuery.Get("as_of"))
	}
	if int(result["asOf"].(float64)) != 1716544800000 {
		t.Fatalf("expected resolved asOf in response, got: %v", result["asOf"])
	}
	value := result["value"].(map[string]any)
	if int(value["cart_value"].(float64)) != 0 {
		t.Fatalf("unexpected historical value: %v", value)
	}
}

func TestIQ_GetAsOfEmptySpecSendsNoParam(t *testing.T) {
	var observedQuery url.Values
	var rawQuery string
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		rawQuery = r.URL.RawQuery
		writeJSON(t, w, 200, map[string]any{"agentId": "a1", "key": "k1", "value": 5})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.GetAsOf(context.Background(), "a1", "k1", "")
	if err != nil {
		t.Fatal(err)
	}
	if rawQuery != "" {
		t.Fatalf("expected no query string for empty as_of, got: %q", rawQuery)
	}
	if observedQuery.Has("as_of") {
		t.Fatalf("expected no as_of param, got: %v", observedQuery)
	}
}

func TestIQ_GetAsOf404KeyNotFoundRaises(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error": "Key not found", "agentId": "a1", "key": "gone",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.GetAsOf(context.Background(), "a1", "gone", "-1h")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestIQ_DiffSendsFromToAndReturnsChanges(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		if r.URL.Path != "/api/pulse/iq/agents/user-sessions/state/diff/u42" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId": "user-sessions",
			"key":     "u42",
			"fromTs":  1716541200000,
			"toTs":    1716544800000,
			"changes": map[string]any{
				"cart_value": map[string]any{"delta": 70.0, "from": 0, "to": 70},
				"coupon":     map[string]any{"added": "SAVE10"},
				"abandoned":  map[string]any{"removed": true},
			},
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, err := c.IQ.Diff(context.Background(), "user-sessions", "u42", IQDiffOptions{From: "-1h", To: "now"})
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("from") != "-1h" || observedQuery.Get("to") != "now" {
		t.Fatalf("expected from=-1h&to=now, got: %v", observedQuery)
	}
	changes := result["changes"].(map[string]any)
	cart := changes["cart_value"].(map[string]any)
	if int(cart["delta"].(float64)) != 70 {
		t.Fatalf("unexpected cart delta: %v", cart)
	}
	if changes["coupon"].(map[string]any)["added"] != "SAVE10" {
		t.Fatalf("unexpected added: %v", changes["coupon"])
	}
}

func TestIQ_DiffEmptyOptionsSendsServerDefaults(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1", "key": "k1",
			"fromTs": 0, "toTs": 1, "changes": map[string]any{},
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.Diff(context.Background(), "a1", "k1", IQDiffOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("from") != "-1h" || observedQuery.Get("to") != "now" {
		t.Fatalf("expected default from=-1h&to=now, got: %v", observedQuery)
	}
}

func TestIQ_Diff404AgentNotQueryableRaises(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error": "Agent has no queryable state", "agentId": "a1",
			"reason": "non-streaming or stopped",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.Diff(context.Background(), "a1", "k1", IQDiffOptions{})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestIQ_DiffWithoutTokenRaisesAuthErrorBeforeHttp(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
	})
	defer stop()
	c := newClient(t, endpoint) // no token
	_, err := c.IQ.Diff(context.Background(), "a1", "k1", IQDiffOptions{})
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}
