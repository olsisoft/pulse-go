package pulse

import (
	"context"
	"net/http"
	"testing"
)

// B-093 follow-up — client.Connectors catalogue parity.
func TestConnectors_ListReturnsSinksAndSources(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pulse/connectors" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		writeJSON(t, w, http.StatusOK, map[string]any{
			"sinks":   []map[string]any{{"subType": "segment", "displayName": "Segment"}},
			"sources": []map[string]any{{"subType": "posthog-source", "displayName": "PostHog Source (poll)"}},
		})
	})
	defer stop()

	c := newClient(t, url, WithToken("fake.jwt"))
	catalog, err := c.Connectors.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	sinks, _ := catalog["sinks"].([]any)
	if len(sinks) != 1 {
		t.Fatalf("expected 1 sink, got %v", catalog["sinks"])
	}
}

func TestConnectors_SinksAndSourcesHelpers(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, http.StatusOK, map[string]any{
			"sinks": []map[string]any{{"subType": "amplitude"}},
		})
	})
	defer stop()

	c := newClient(t, url, WithToken("fake.jwt"))
	sinks, err := c.Connectors.Sinks(context.Background())
	if err != nil {
		t.Fatalf("Sinks: %v", err)
	}
	if len(sinks) != 1 || sinks[0]["subType"] != "amplitude" {
		t.Fatalf("expected amplitude sink, got %v", sinks)
	}
	// missing "sources" key degrades to empty
	sources, err := c.Connectors.Sources(context.Background())
	if err != nil {
		t.Fatalf("Sources: %v", err)
	}
	if len(sources) != 0 {
		t.Fatalf("expected no sources, got %v", sources)
	}
}
