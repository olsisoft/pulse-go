package pulse

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// IQService — client.IQ. B-106 Interactive Queries.
//
// Query the live state of streaming agents like a database from any Go
// microservice. The killer use case is a synchronous decision service
// (fraud, rate-limit, pricing) calling Get on every request and reading
// agent state from RAM with zero ingest-to-decision lag:
//
//	state, err := client.IQ.Get(ctx, "fraud-detector", "customer-42")
//	if err != nil {
//	    var nf *NotFoundError
//	    if errors.As(err, &nf) {
//	        // key absent OR agent not queryable — inspect nf.Body
//	    }
//	    return err
//	}
//	value := state["value"].(map[string]any)
//	if int(value["tx_count_60s"].(float64)) > 5 {
//	    return ErrDeny
//	}
//
// All methods require AGENT_READ permission (Owner, Platform Admin,
// Developer, Auditor personas by default — see B-105).
//
// Responses are returned as map[string]any so callers can paginate,
// inspect truncated/limitApplied/totalScanned metadata, and read fields
// without going through a wrapper layer. Strongly-typed structs can be
// layered on top in user code if desired; the SDK stays close to the wire.
type IQService struct {
	client *Client
}

// IQScanOptions — optional range bounds + page size for Scan / ListKeys.
//
// Zero value is valid: no range, Limit=0 means "use server default 100".
// Limit > 1000 is clamped server-side (response header
// X-Pulse-Pagination-Clamped: true is set when clamped, not surfaced
// in the body — read via the underlying response if needed).
type IQScanOptions struct {
	// Start — inclusive lower bound on key range. Empty = beginning.
	Start string
	// End — exclusive upper bound on key range. Empty = end.
	End string
	// Limit — page size. 0 (zero value) sends limit=100 to the server.
	Limit int
}

// IQQueryOptions — optional inputs for Query.
//
// The Filter is a recursive map shaped per the IQFilterExpression schema:
// each node MUST carry exactly ONE of "field" (leaf), "and", "or", "not".
// Mixing in a single node returns HTTP 400.
type IQQueryOptions struct {
	Start      string
	End        string
	Limit      int // 0 → server default
	Filter     map[string]any
	Projection []string
	GroupBy    string // empty → flat response; non-empty → grouped response
}

// IQDiffOptions — the time window for Diff. B-113.
//
// Both fields accept the same specs as GetAsOf: "now", a relative offset
// ("-1h", "-30m", "-7d"), an ISO-8601 instant, or epoch millis. Empty
// strings fall back to the server defaults (From="-1h", To="now").
type IQDiffOptions struct {
	// From — start of the window. Empty → server default "-1h".
	From string
	// To — end of the window. Empty → server default "now".
	To string
}

// Summary — GET /api/pulse/iq/agents/{id}/state — headline state summary.
//
// Returned map always carries the 9 fields: agentId, queryable, backend,
// hotSize, hotBytes, coldSize, coldBytes, lastCheckpointId, totalSize.
// queryable=false when the agent has no live streaming backend (other
// numerics are 0, backend="none", lastCheckpointId=-1).
func (s *IQService) Summary(ctx context.Context, agentID string) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) + "/state"
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// Get — GET /api/pulse/iq/agents/{id}/state/value/{key} — point lookup.
//
// Returns the IQValue map (agentId, key, value). The value field is the
// JSON-decoded payload; nil is a legal value (the server distinguishes
// "key present with null" from "key absent" — the latter returns 404
// which surfaces as *NotFoundError).
//
// Caller can branch on the not-found cause via the body:
//
//	state, err := client.IQ.Get(ctx, "fraud", "key1")
//	var nf *NotFoundError
//	if errors.As(err, &nf) {
//	    if nf.Body["error"] == "Key not found" {
//	        // key absent
//	    } else if reason, ok := nf.Body["reason"]; ok {
//	        // agent not queryable; reason explains why
//	    }
//	}
func (s *IQService) Get(ctx context.Context, agentID, key string) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) +
		"/state/value/" + encodeIQSegment(key)
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// GetAsOf — GET /api/pulse/iq/agents/{id}/state/value/{key}?as_of=<spec> —
// B-113 time-travel point lookup.
//
// Reads the value of key as it was at a past instant instead of the live
// value. asOf accepts the same time specs as the rest of B-113: "now", a
// relative offset ("-1h", "-30m", "-7d"), an ISO-8601 instant, or epoch
// millis — the string is passed through to the server unchanged.
//
// Returns the IQValue map (agentId, key, value) which additionally carries
// asOf (the resolved epoch ms). An empty asOf sends no query parameter and
// is equivalent to Get (the live value).
//
// Like Get, raises *NotFoundError when the key is absent or the agent is
// not queryable — branch on the body to distinguish.
func (s *IQService) GetAsOf(ctx context.Context, agentID, key, asOf string) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) +
		"/state/value/" + encodeIQSegment(key)
	if asOf != "" {
		q := url.Values{}
		q.Set("as_of", asOf)
		path += "?" + q.Encode()
	}
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// Diff — GET /api/pulse/iq/agents/{id}/state/diff/{key}?from=&to= — B-113
// field-level state diff between two instants.
//
// Returns the raw response map: agentId, key, fromTs, toTs (resolved epoch
// ms), and changes — a map from each changed field name to one of:
//
//   - {delta, from, to} — value changed (delta present for numeric fields)
//   - {added}           — field present at "to" but not at "from"
//   - {removed}         — field present at "from" but not at "to"
//
// The from/to specs default to "-1h" / "now" when opts leaves them empty.
//
//	d, _ := client.IQ.Diff(ctx, "user-sessions", "u42", IQDiffOptions{From: "-1h", To: "now"})
//	changes := d["changes"].(map[string]any)
//	cart := changes["cart_value"].(map[string]any) // {"delta": 70, "from": 0, "to": 70}
//
// Like the other state endpoints, raises *NotFoundError when the agent is
// not queryable.
func (s *IQService) Diff(ctx context.Context, agentID, key string, opts IQDiffOptions) (map[string]any, error) {
	from := opts.From
	if from == "" {
		from = "-1h"
	}
	to := opts.To
	if to == "" {
		to = "now"
	}
	q := url.Values{}
	q.Set("from", from)
	q.Set("to", to)
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) +
		"/state/diff/" + encodeIQSegment(key) + "?" + q.Encode()
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// Scan — GET /api/pulse/iq/agents/{id}/state/scan — paginated range scan.
//
// Returns the raw response map. Inspect "truncated" to decide if more
// data exists; paginate by setting opts.Start on the next call to the
// last returned key plus a sentinel suffix.
func (s *IQService) Scan(ctx context.Context, agentID string, opts IQScanOptions) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) +
		"/state/scan" + scanQuery(opts)
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// ListKeys — GET /api/pulse/iq/agents/{id}/state/keys — keys-only range scan.
//
// Same shape as Scan minus the values. Returns the IQKeysResponse map;
// the "keys" field is a []any (JSON array of strings).
func (s *IQService) ListKeys(ctx context.Context, agentID string, opts IQScanOptions) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) +
		"/state/keys" + scanQuery(opts)
	return s.client.request(ctx, http.MethodGet, path, nil, true)
}

// Query — POST /api/pulse/iq/agents/{id}/state/query — filtered / projected /
// grouped query.
//
// When opts.GroupBy is non-empty the response shape is
// {groups: [{groupKey, count}], groupCount, ...} instead of
// {entries: [...], count, ...}.
//
// Returns *ValidationError (HTTP 400) on invalid filter syntax (mixed
// discriminators, missing field, etc.) and *NotFoundError (HTTP 404)
// when the agent is not queryable.
func (s *IQService) Query(ctx context.Context, agentID string, opts IQQueryOptions) (map[string]any, error) {
	path := "/api/pulse/iq/agents/" + encodeIQSegment(agentID) + "/state/query"
	body := opts.toBody()
	// Empty body → nil so we send no Content-Length and the server
	// falls through to default scan (matches the in-tree handler's
	// behaviour on missing body).
	var payload any
	if len(body) > 0 {
		payload = body
	}
	return s.client.request(ctx, http.MethodPost, path, payload, true)
}

// scanQuery builds the ?limit=N&start=...&end=... suffix for the GET endpoints.
//
// limit is always sent (defaulting to 100 when opts.Limit is 0) so the
// server gets a deterministic value. Start/End are omitted when empty so
// the URL stays clean — the server defaults them to "beginning" / "end".
func scanQuery(opts IQScanOptions) string {
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if opts.Start != "" {
		q.Set("start", opts.Start)
	}
	if opts.End != "" {
		q.Set("end", opts.End)
	}
	return "?" + q.Encode()
}

// toBody flattens IQQueryOptions to the JSON body shape the server expects.
// Only includes keys the caller actually set so the wire payload is stable
// and the server doesn't see defaults it didn't ask for.
func (o IQQueryOptions) toBody() map[string]any {
	body := make(map[string]any)
	if o.Start != "" {
		body["start"] = o.Start
	}
	if o.End != "" {
		body["end"] = o.End
	}
	if o.Limit > 0 {
		body["limit"] = o.Limit
	}
	if o.Filter != nil {
		body["filter"] = o.Filter
	}
	if o.Projection != nil {
		body["projection"] = o.Projection
	}
	if o.GroupBy != "" {
		body["groupBy"] = o.GroupBy
	}
	return body
}

// IQFilter helpers — construct filter expressions ergonomically without
// having to write nested map literals.

// IQLeaf builds a leaf filter node: {"field": ..., "op": ..., "value": ...}.
// Pass an empty op to test field presence only via {"field": "name", "op": "exists"}.
func IQLeaf(field, op string, value any) map[string]any {
	m := map[string]any{"field": field}
	if op != "" {
		m["op"] = op
	}
	m["value"] = value
	return m
}

// IQAnd builds an AND filter combining all children.
func IQAnd(children ...map[string]any) map[string]any {
	return map[string]any{"and": children}
}

// IQOr builds an OR filter combining all children.
func IQOr(children ...map[string]any) map[string]any {
	return map[string]any{"or": children}
}

// IQNot negates a child filter.
func IQNot(child map[string]any) map[string]any {
	return map[string]any{"not": child}
}

// encodeIQSegment percent-encodes a path segment aggressively — same
// semantics as Python's urllib.quote(safe=”), Java's URLEncoder.encode
// followed by '+'→'%20', and JavaScript's encodeURIComponent. This keeps
// the wire format identical across all 5 Pulse SDKs so a key like
// "user:123/orders" produces the same URL bytes regardless of caller
// language, and the server's URLDecoder round-trips cleanly.
//
// We deliberately don't use url.PathEscape — that one leaves ':', '@',
// and other sub-delims unencoded per RFC 3986 Path semantics, which
// would diverge from the sibling SDKs and might confuse downstream
// observability tooling (proxies, audit logs) that pattern-match URLs.
func encodeIQSegment(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}
