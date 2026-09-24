package pulse

import (
	"context"
	"net/http"
	"net/url"
)

// ---------------------------------------------------------------------------
// PvscService — client.Pvsc
// ---------------------------------------------------------------------------

// PvscService groups the governance surface: topic contracts, the arbitration
// policy, the guardian pool and the firewall dead-letter queue.
type PvscService struct {
	client *Client
}

// Schemas returns every registered topic contract
// (GET /api/pulse/pvsc/schemas).
func (s *PvscService) Schemas(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/pvsc/schemas", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["schemas"]), nil
}

// SaveSchema registers or replaces a topic's contract
// (PUT /api/pulse/pvsc/schemas).
//
// A field rule may carry "grounding": "required" blocks a value the agent
// could not have derived from what it was given, "warn" reports it, and the
// default "ignore" does not look. That is the check that catches a figure
// which is well-typed, in range, confidently asserted and invented — every
// other rule in the schema passes such a value.
//
// A field rule may also carry "derivation": "deny" (the default — the figure
// must appear in the input) or "allow" (the agent may compute it in one step).
// Arithmetic provenance is opt-in because "derivable" is not "derived": with an
// input of 42, the value 84 is reachable as 42 + 42 without anything having
// performed that addition.
//
// The write REPLACES the schema rather than merging into it: a field you omit
// is gone, grounding policy included. Read the current schema first if you are
// changing one field of several.
func (s *PvscService) SaveSchema(ctx context.Context, schema map[string]any) (map[string]any, error) {
	return s.client.request(ctx, http.MethodPut, "/api/pulse/pvsc/schemas", schema, true)
}

// DeleteSchema drops a topic's contract (DELETE /api/pulse/pvsc/schemas).
func (s *PvscService) DeleteSchema(ctx context.Context, topic string) (map[string]any, error) {
	body := map[string]any{"topic": topic}
	return s.client.request(ctx, http.MethodDelete, "/api/pulse/pvsc/schemas", body, true)
}

// Config returns the consensus, degradation and arbitration settings
// (GET /api/pulse/pvsc/config).
func (s *PvscService) Config(ctx context.Context) (map[string]any, error) {
	return s.client.request(ctx, http.MethodGet, "/api/pulse/pvsc/config", nil, true)
}

// UpdateConfig patches the settings named in patch
// (PUT /api/pulse/pvsc/config).
func (s *PvscService) UpdateConfig(ctx context.Context, patch map[string]any) (map[string]any, error) {
	return s.client.request(ctx, http.MethodPut, "/api/pulse/pvsc/config", patch, true)
}

// SetStances replaces the arbitration stances.
//
// A stance is attached to the guardian that votes, never read out of what the
// vote says. Lower precedence wins — rank 1 outranks rank 2 — and a veto
// stance blocks by construction rather than by count. An empty slice disables
// arbitration, so the majority result stands; it is sent as [] rather than
// omitted, because clearing the stances is a real instruction.
func (s *PvscService) SetStances(ctx context.Context, stances []map[string]any) (map[string]any, error) {
	if stances == nil {
		stances = []map[string]any{}
	}
	return s.UpdateConfig(ctx, map[string]any{"arbitrationStances": stances})
}

// Metrics returns the PVSC counters plus the quorum information yield
// (GET /api/pulse/pvsc/metrics).
//
// quorumInformationYield / quorumRedundantGuardianCalls /
// quorumInterpretation answer whether consulting the quorum changed any
// decision the first guardian would have made alone.
func (s *PvscService) Metrics(ctx context.Context) (map[string]any, error) {
	return s.client.request(ctx, http.MethodGet, "/api/pulse/pvsc/metrics", nil, true)
}

// Guardians returns the registered guardian pool
// (GET /api/pulse/pvsc/guardians).
func (s *PvscService) Guardians(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/pvsc/guardians", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["guardians"]), nil
}

// Dlq returns the events the firewall turned away (GET /api/pulse/pvsc/dlq).
func (s *PvscService) Dlq(ctx context.Context) ([]map[string]any, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/pvsc/dlq", nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["entries"]), nil
}

// Reinject replays one blocked event (POST /api/pulse/pvsc/dlq/reinject).
func (s *PvscService) Reinject(ctx context.Context, eventID string) (map[string]any, error) {
	body := map[string]any{"eventId": eventID}
	return s.client.request(ctx, http.MethodPost, "/api/pulse/pvsc/dlq/reinject", body, true)
}

// Discard drops one blocked event for good
// (POST /api/pulse/pvsc/dlq/discard).
func (s *PvscService) Discard(ctx context.Context, eventID string) (map[string]any, error) {
	body := map[string]any{"eventId": eventID}
	return s.client.request(ctx, http.MethodPost, "/api/pulse/pvsc/dlq/discard", body, true)
}

// ---------------------------------------------------------------------------
// EvalsService — client.Evals
// ---------------------------------------------------------------------------

// EvalsService groups the eval-suite endpoints: golden cases replayed against
// live agents, with a ratcheting non-regression gate.
//
// The gate counts PASSES against a recorded floor rather than counting
// failures, so deleting an assertion cannot satisfy it. Cases run
// node-isolated: nothing is persisted, published to a downstream topic, or
// acted on, which is what makes running a suite against production agents
// safe.
type EvalsService struct {
	client *Client
}

// Suites returns the suite ids that have at least one case
// (GET /api/pulse/evals).
func (s *EvalsService) Suites(ctx context.Context) ([]string, error) {
	result, err := s.client.request(ctx, http.MethodGet, "/api/pulse/evals", nil, true)
	if err != nil {
		return nil, err
	}
	raw, ok := result["suites"].([]any)
	if !ok {
		return []string{}, nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if str, ok := item.(string); ok {
			out = append(out, str)
		}
	}
	return out, nil
}

// Cases returns the cases in one suite
// (GET /api/pulse/evals/cases?suite=).
func (s *EvalsService) Cases(ctx context.Context, suiteID string) ([]map[string]any, error) {
	q := url.Values{}
	q.Set("suite", suiteID)
	path := "/api/pulse/evals/cases?" + q.Encode()
	result, err := s.client.request(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	return unwrapList(result["cases"]), nil
}

// SaveCase adds or replaces one case (POST /api/pulse/evals/cases).
func (s *EvalsService) SaveCase(ctx context.Context, evalCase map[string]any) (map[string]any, error) {
	return s.client.request(ctx, http.MethodPost, "/api/pulse/evals/cases", evalCase, true)
}

// Run replays every case in the suite (POST /api/pulse/evals/run).
//
// A REGRESSION comes back as a normal response with blocksRelease=true, not as
// an error: the run succeeded and the gate's verdict is data. Branch on
// blocksRelease, not on whether err is nil.
func (s *EvalsService) Run(ctx context.Context, suiteID string) (map[string]any, error) {
	body := map[string]any{"suiteId": suiteID}
	return s.client.request(ctx, http.MethodPost, "/api/pulse/evals/run", body, true)
}

// RecordBaseline records the current passing count as the floor future runs
// are held to (POST /api/pulse/evals/baseline).
//
// Call it after a run you are happy with; calling it after a bad one ratchets
// the floor DOWN.
func (s *EvalsService) RecordBaseline(ctx context.Context, suiteID string) (map[string]any, error) {
	body := map[string]any{"suiteId": suiteID}
	return s.client.request(ctx, http.MethodPost, "/api/pulse/evals/baseline", body, true)
}
