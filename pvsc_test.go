package pulse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
)

// PVSC and eval suites over the wire.
//
// The five Pulse SDKs had no PVSC surface at all — `grep -r pvsc` across
// pulse-js / pulse-py / pulse-rs / pulse-go / pulse-java returned nothing.
// Anything an operator could do to a topic contract, an arbitration policy or
// an eval suite was reachable only from the browser.

// decodeBody reads a request body as a generic JSON object.
func decodeBody(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal body %q: %v", raw, err)
	}
	return out
}

func TestPvscSchemasUnwrapsTheEnvelope(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"schemas": []any{map[string]any{"topic": "quotes"}}, "count": 1,
		})
	})
	defer stop()

	schemas, err := newClient(t, url, WithToken("jwt")).Pvsc.Schemas(context.Background())
	if err != nil {
		t.Fatalf("Schemas: %v", err)
	}
	if len(schemas) != 1 || schemas[0]["topic"] != "quotes" {
		t.Fatalf("unexpected schemas: %#v", schemas)
	}
}

func TestPvscSaveSchemaCarriesTheGroundingPolicy(t *testing.T) {
	// A grounding policy could be written from Java and from nowhere else
	// until recently. An SDK that dropped it would be the next place it was
	// unreachable from.
	var sent map[string]any
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		sent = decodeBody(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "saved"})
	})
	defer stop()

	_, err := newClient(t, url, WithToken("jwt")).Pvsc.SaveSchema(context.Background(), map[string]any{
		"topic": "quotes",
		"optionalFields": map[string]any{
			"price": map[string]any{"type": "number", "grounding": "required"},
		},
	})
	if err != nil {
		t.Fatalf("SaveSchema: %v", err)
	}

	fields, _ := sent["optionalFields"].(map[string]any)
	price, _ := fields["price"].(map[string]any)
	if price["grounding"] != "required" {
		t.Fatalf("grounding did not reach the wire: %#v", sent)
	}
}

func TestPvscSetStancesWritesThroughConfig(t *testing.T) {
	var sent map[string]any
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/pulse/pvsc/config" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		sent = decodeBody(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "updated"})
	})
	defer stop()

	_, err := newClient(t, url, WithToken("jwt")).Pvsc.SetStances(context.Background(), []map[string]any{
		{"domain": "legal", "precedence": 1, "veto": true},
	})
	if err != nil {
		t.Fatalf("SetStances: %v", err)
	}

	stances, ok := sent["arbitrationStances"].([]any)
	if !ok || len(stances) != 1 {
		t.Fatalf("stances did not reach the wire: %#v", sent)
	}
}

func TestPvscSetStancesSendsAnEmptyListNotNull(t *testing.T) {
	// Clearing the stances disables arbitration. A nil slice would marshal to
	// null, which the server reads as "no such key" — the operator's clear
	// would silently do nothing.
	var raw []byte
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		raw, _ = io.ReadAll(r.Body)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "updated"})
	})
	defer stop()

	if _, err := newClient(t, url, WithToken("jwt")).Pvsc.SetStances(context.Background(), nil); err != nil {
		t.Fatalf("SetStances: %v", err)
	}
	if got := string(raw); got != `{"arbitrationStances":[]}` {
		t.Fatalf("expected an empty list on the wire, got %s", got)
	}
}

func TestPvscMetricsSurfacesTheQuorumYield(t *testing.T) {
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"pvscModeCount":                300,
			"quorumInformationYield":       0.3333,
			"quorumRedundantGuardianCalls": 400,
		})
	})
	defer stop()

	metrics, err := newClient(t, url, WithToken("jwt")).Pvsc.Metrics(context.Background())
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if metrics["quorumRedundantGuardianCalls"].(float64) != 400 {
		t.Fatalf("unexpected metrics: %#v", metrics)
	}
}

func TestPvscDlqListAndReinject(t *testing.T) {
	var sent map[string]any
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pulse/pvsc/dlq" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"entries": []any{map[string]any{
					"eventId": "e1", "rejectedBy": "pvsc-agent-gate"}},
				"total": 1,
			})
			return
		}
		sent = decodeBody(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"reinjected": true})
	})
	defer stop()

	client := newClient(t, url, WithToken("jwt"))
	entries, err := client.Pvsc.Dlq(context.Background())
	if err != nil {
		t.Fatalf("Dlq: %v", err)
	}
	if entries[0]["rejectedBy"] != "pvsc-agent-gate" {
		t.Fatalf("unexpected entries: %#v", entries)
	}

	if _, err := client.Pvsc.Reinject(context.Background(), "e1"); err != nil {
		t.Fatalf("Reinject: %v", err)
	}
	if sent["eventId"] != "e1" {
		t.Fatalf("unexpected reinject body: %#v", sent)
	}
}

func TestEvalsSuitesAndCases(t *testing.T) {
	var observedSuite string
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/pulse/evals" {
			_ = json.NewEncoder(w).Encode(map[string]any{"suites": []any{"pricing"}, "count": 1})
			return
		}
		observedSuite = r.URL.Query().Get("suite")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cases": []any{map[string]any{"caseId": "c1"}}, "count": 1,
		})
	})
	defer stop()

	client := newClient(t, url, WithToken("jwt"))
	suites, err := client.Evals.Suites(context.Background())
	if err != nil {
		t.Fatalf("Suites: %v", err)
	}
	if len(suites) != 1 || suites[0] != "pricing" {
		t.Fatalf("unexpected suites: %#v", suites)
	}

	cases, err := client.Evals.Cases(context.Background(), "pricing")
	if err != nil {
		t.Fatalf("Cases: %v", err)
	}
	if len(cases) != 1 || observedSuite != "pricing" {
		t.Fatalf("unexpected cases %#v / suite %q", cases, observedSuite)
	}
}

func TestEvalsRegressionIsAVerdictNotAnError(t *testing.T) {
	// The server answers 200 with blocksRelease=true because the run
	// succeeded. A caller in CI branches on blocksRelease; if this returned a
	// non-nil error, "the suite regressed" and "the call broke" would be the
	// same event.
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"suiteId": "pricing", "total": 10, "passing": 7, "failing": 3,
			"baseline": 9, "gate": "REGRESSION", "blocksRelease": true,
			"summary": "REGRESSION", "cases": []any{},
		})
	})
	defer stop()

	report, err := newClient(t, url, WithToken("jwt")).Evals.Run(context.Background(), "pricing")
	if err != nil {
		t.Fatalf("Run must not error on a regression: %v", err)
	}
	if report["gate"] != "REGRESSION" || report["blocksRelease"] != true {
		t.Fatalf("unexpected report: %#v", report)
	}
}

func TestEvalsRecordBaseline(t *testing.T) {
	var sent map[string]any
	url, stop := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		sent = decodeBody(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{"suiteId": "pricing", "baseline": 7})
	})
	defer stop()

	if _, err := newClient(t, url, WithToken("jwt")).Evals.RecordBaseline(context.Background(), "pricing"); err != nil {
		t.Fatalf("RecordBaseline: %v", err)
	}
	if sent["suiteId"] != "pricing" {
		t.Fatalf("unexpected body: %#v", sent)
	}
}
