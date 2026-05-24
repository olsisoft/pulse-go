package pulse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// B-106 Interactive Queries tests. Mirrors pulse-py / pulse-js / pulse-java
// coverage: happy path + url encoding + null value + 404 key-not-found vs
// agent-not-queryable + 400 invalid filter + auth gating.

func TestIQ_SummaryReturnsStateMetadata(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pulse/iq/agents/fraud-detector/state" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId":          "fraud-detector",
			"queryable":        true,
			"backend":          "rocksdb",
			"hotSize":          1500,
			"hotBytes":         32768,
			"coldSize":         50000,
			"coldBytes":        4194304,
			"lastCheckpointId": 42,
			"totalSize":        51500,
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	summary, err := c.IQ.Summary(context.Background(), "fraud-detector")
	if err != nil {
		t.Fatal(err)
	}
	if summary["queryable"] != true || summary["backend"] != "rocksdb" {
		t.Fatalf("unexpected summary: %v", summary)
	}
	if int(summary["totalSize"].(float64)) != 51500 {
		t.Fatalf("totalSize: %v", summary["totalSize"])
	}
}

func TestIQ_SummaryHandlesNonQueryableAgent(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"agentId": "rule-agent", "queryable": false, "backend": "none",
			"hotSize": 0, "hotBytes": 0, "coldSize": 0, "coldBytes": 0,
			"lastCheckpointId": -1, "totalSize": 0,
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	summary, _ := c.IQ.Summary(context.Background(), "rule-agent")
	if summary["queryable"] != false {
		t.Fatalf("expected queryable=false: %v", summary["queryable"])
	}
	if int(summary["lastCheckpointId"].(float64)) != -1 {
		t.Fatalf("lastCheckpointId: %v", summary["lastCheckpointId"])
	}
}

func TestIQ_SummaryUrlEncodesAgentIdWithSlash(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		// net/url unescapes the path before delivering to handler. We assert
		// on the RAW path to verify our encoding produced the right wire bytes.
		if !strings.Contains(r.URL.RawPath, "tenant%2Fagent") {
			t.Fatalf("expected encoded path, raw=%q escaped=%q", r.URL.RawPath, r.URL.EscapedPath())
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId": "tenant/agent", "queryable": true, "backend": "rocksdb",
			"hotSize": 0, "hotBytes": 0, "coldSize": 0, "coldBytes": 0,
			"lastCheckpointId": 0, "totalSize": 0,
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	result, _ := c.IQ.Summary(context.Background(), "tenant/agent")
	if result["agentId"] != "tenant/agent" {
		t.Fatalf("unexpected: %v", result)
	}
}

func TestIQ_GetReturnsValueAtKey(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"agentId": "fraud-detector",
			"key":     "customer-42",
			"value":   map[string]any{"tx_count_60s": 7, "total_amount_60s": 12500},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	result, err := c.IQ.Get(context.Background(), "fraud-detector", "customer-42")
	if err != nil {
		t.Fatal(err)
	}
	if result["key"] != "customer-42" {
		t.Fatalf("unexpected key: %v", result["key"])
	}
	value := result["value"].(map[string]any)
	if int(value["tx_count_60s"].(float64)) != 7 {
		t.Fatalf("unexpected tx_count_60s: %v", value)
	}
}

func TestIQ_GetUrlEncodesKeyWithSlash(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawPath, "user%3A123%2Forders") {
			t.Fatalf("expected encoded key, raw=%q", r.URL.RawPath)
		}
		writeJSON(t, w, 200, map[string]any{
			"agentId": "sessions",
			"key":     "user:123/orders",
			"value":   []string{"o1", "o2", "o3"},
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	result, _ := c.IQ.Get(context.Background(), "sessions", "user:123/orders")
	values := result["value"].([]any)
	if len(values) != 3 || values[0] != "o1" {
		t.Fatalf("unexpected: %v", values)
	}
}

func TestIQ_GetReturnsNullValueWhenPresentWithNull(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{"agentId": "a1", "key": "k1", "value": nil})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	result, _ := c.IQ.Get(context.Background(), "a1", "k1")
	if _, ok := result["value"]; !ok {
		t.Fatal("expected 'value' key to be present (with nil value)")
	}
	if result["value"] != nil {
		t.Fatalf("expected nil value, got: %v", result["value"])
	}
}

func TestIQ_Get404KeyNotFoundRaisesWithKeyBody(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error": "Key not found", "agentId": "a1", "key": "missing-key",
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.IQ.Get(context.Background(), "a1", "missing-key")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
	if nf.Body["error"] != "Key not found" {
		t.Fatalf("expected 'Key not found' error body, got: %v", nf.Body)
	}
	if nf.Body["key"] != "missing-key" {
		t.Fatalf("expected key in body, got: %v", nf.Body)
	}
}

func TestIQ_Get404AgentNotQueryableRaisesWithReason(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error":   "Agent has no queryable state",
			"agentId": "a1",
			"reason":  "non-streaming or stopped",
		})
	})
	defer stop()
	c := newClient(t, url, WithToken("fake.jwt"))
	_, err := c.IQ.Get(context.Background(), "a1", "k1")
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
	if nf.Body["reason"] != "non-streaming or stopped" {
		t.Fatalf("expected reason in body, got: %v", nf.Body)
	}
}

func TestIQ_ScanReturnsEntriesWithDefaultLimit(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1",
			"entries": []map[string]any{
				{"key": "k1", "value": 1}, {"key": "k2", "value": 2},
			},
			"count": 2, "truncated": false, "limitApplied": 100,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, err := c.IQ.Scan(context.Background(), "a1", IQScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries := result["entries"].([]any)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	// Default Limit=0 → server gets limit=100
	if observedQuery.Get("limit") != "100" {
		t.Fatalf("expected limit=100 default, got: %q", observedQuery.Get("limit"))
	}
	if observedQuery.Get("start") != "" || observedQuery.Get("end") != "" {
		t.Fatalf("expected no start/end, got: %v", observedQuery)
	}
}

func TestIQ_ScanPassesThroughRangeParams(t *testing.T) {
	var observedQuery url.Values
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		observedQuery = r.URL.Query()
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1", "entries": []any{}, "count": 0,
			"truncated": false, "limitApplied": 50, "start": "alice", "end": "bob",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.Scan(context.Background(), "a1", IQScanOptions{
		Start: "alice", End: "bob", Limit: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	if observedQuery.Get("limit") != "50" {
		t.Fatalf("limit: %q", observedQuery.Get("limit"))
	}
	if observedQuery.Get("start") != "alice" || observedQuery.Get("end") != "bob" {
		t.Fatalf("start/end: %v", observedQuery)
	}
}

func TestIQ_Scan404AgentNotQueryableRaises(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 404, map[string]any{
			"error": "Agent has no queryable state",
			"agentId": "a1", "reason": "non-streaming or stopped",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.Scan(context.Background(), "a1", IQScanOptions{})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("expected *NotFoundError, got %T: %v", err, err)
	}
}

func TestIQ_ListKeysReturnsKeysArray(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1",
			"keys":    []string{"alpha", "beta", "gamma"},
			"count":   3, "truncated": false, "limitApplied": 100,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, _ := c.IQ.ListKeys(context.Background(), "a1", IQScanOptions{})
	keys := result["keys"].([]any)
	if len(keys) != 3 || keys[0] != "alpha" {
		t.Fatalf("unexpected: %v", keys)
	}
}

func TestIQ_QueryFlatWithFilter(t *testing.T) {
	var sentBody []byte
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		sentBody = readAll(t, r)
		writeJSON(t, w, 200, map[string]any{
			"agentId": "fraud-detector",
			"entries": []map[string]any{
				{"key": "c1", "value": map[string]any{"tx_count_60s": 8}},
			},
			"count": 1, "totalScanned": 1500, "matchedCount": 1,
			"truncated": false, "limitApplied": 100,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, err := c.IQ.Query(context.Background(), "fraud-detector", IQQueryOptions{
		Filter: IQLeaf("tx_count_60s", "gt", 5),
	})
	if err != nil {
		t.Fatal(err)
	}
	if int(result["count"].(float64)) != 1 {
		t.Fatalf("count: %v", result["count"])
	}
	// Verify wire body shape
	var body map[string]any
	if err := json.Unmarshal(sentBody, &body); err != nil {
		t.Fatalf("body not JSON: %s", sentBody)
	}
	filter := body["filter"].(map[string]any)
	if filter["field"] != "tx_count_60s" || filter["op"] != "gt" {
		t.Fatalf("unexpected filter: %v", filter)
	}
}

func TestIQ_QueryGroupedReturnsGroups(t *testing.T) {
	var sentBody []byte
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		sentBody = readAll(t, r)
		writeJSON(t, w, 200, map[string]any{
			"agentId": "users",
			"groups": []map[string]any{
				{"groupKey": "free", "count": 8420},
				{"groupKey": "pro", "count": 312},
			},
			"groupCount": 2, "totalScanned": 8732, "matchedCount": 8732,
			"truncated": false, "limitApplied": 100,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	result, _ := c.IQ.Query(context.Background(), "users", IQQueryOptions{GroupBy: "plan"})
	if int(result["groupCount"].(float64)) != 2 {
		t.Fatalf("groupCount: %v", result["groupCount"])
	}
	var body map[string]any
	_ = json.Unmarshal(sentBody, &body)
	if body["groupBy"] != "plan" {
		t.Fatalf("groupBy not in body: %v", body)
	}
}

func TestIQ_QueryEmptyOptionsSendsNoBody(t *testing.T) {
	var bodyLen int
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		bodyLen = int(r.ContentLength)
		writeJSON(t, w, 200, map[string]any{
			"agentId": "a1", "entries": []any{}, "count": 0,
			"totalScanned": 0, "matchedCount": 0,
			"truncated": false, "limitApplied": 100,
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	_, err := c.IQ.Query(context.Background(), "a1", IQQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if bodyLen > 0 {
		t.Fatalf("expected no body for empty options, got Content-Length=%d", bodyLen)
	}
}

func TestIQ_Query400InvalidFilterRaisesValidation(t *testing.T) {
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, 400, map[string]any{
			"error": "filter cannot mix discriminators (field/and/or/not) at the same level",
		})
	})
	defer stop()
	c := newClient(t, endpoint, WithToken("fake.jwt"))
	// Build a knowingly-bad filter (mixes field + and at same level)
	badFilter := map[string]any{
		"field": "a",
		"and":   []map[string]any{IQLeaf("b", "eq", 1)},
	}
	_, err := c.IQ.Query(context.Background(), "a1", IQQueryOptions{Filter: badFilter})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if !strings.Contains(ve.Body["error"].(string), "discriminator") {
		t.Fatalf("expected 'discriminator' in body, got: %v", ve.Body)
	}
}

func TestIQ_SummaryWithoutTokenRaisesAuthErrorBeforeAnyHttpCall(t *testing.T) {
	hits := 0
	endpoint, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		hits++
	})
	defer stop()
	c := newClient(t, endpoint) // no WithToken
	_, err := c.IQ.Summary(context.Background(), "a1")
	var ae *AuthError
	if !errors.As(err, &ae) {
		t.Fatalf("expected *AuthError, got %T: %v", err, err)
	}
	if hits != 0 {
		t.Fatalf("server should not have been called, was hit %d time(s)", hits)
	}
}

// ---- Helpers ----

// readAll reads the full request body for inspection. Trims to nil if empty.
func readAll(t *testing.T, r *http.Request) []byte {
	t.Helper()
	if r.Body == nil {
		return nil
	}
	defer r.Body.Close()
	buf := make([]byte, r.ContentLength)
	if r.ContentLength > 0 {
		_, _ = r.Body.Read(buf)
	}
	return buf
}
