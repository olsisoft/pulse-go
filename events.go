package pulse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// EventsReplayOptions — the time window + cap for Events.Replay. B-113.
//
// From / To accept the same specs as IQ.GetAsOf ("now", "-1h", an ISO-8601
// instant, or epoch millis); empty strings fall back to the server defaults
// (From="-1h", To="now"). Limit caps the number of changes returned; 0 (the
// zero value) sends the server default of 100.
type EventsReplayOptions struct {
	From  string
	To    string
	Limit int
}

// EventsService — client.Events. Live SSE stream of events flowing through
// the Pulse engine.
//
// Usage:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	events, errCh := client.Events.Stream(ctx)
//	for {
//	    select {
//	    case event, ok := <-events:
//	        if !ok {
//	            return  // stream closed
//	        }
//	        fmt.Println(event["type"])
//	    case err := <-errCh:
//	        return err
//	    case <-ctx.Done():
//	        return ctx.Err()
//	    }
//	}
//
// The channel API matches Go's idiomatic pattern for producer/consumer
// streams. Cancel the context to terminate.
type EventsService struct {
	client *Client
}

// Stream subscribes to GET /api/pulse/events/stream and returns two
// channels:
//
//   - events: parsed events as they arrive. Closed when the server ends
//     the stream, the context is cancelled, or a fatal error fires on errCh.
//   - errCh:  carries a single fatal error (auth failure, transport
//     failure, parse failure) then closes.
//
// Both channels are buffered (capacity 16 for events, 1 for errCh) so a
// slow consumer doesn't immediately back-pressure the network goroutine.
// If the buffer fills the goroutine blocks on send — drain events
// promptly to avoid stalling the stream.
func (s *EventsService) Stream(ctx context.Context) (<-chan map[string]any, <-chan error) {
	events := make(chan map[string]any, 16)
	errCh := make(chan error, 1)

	go func() {
		defer close(events)
		defer close(errCh)

		token := s.client.Token()
		if token == "" {
			errCh <- &AuthError{APIError{
				StatusCode: 401,
				Path:       "/api/pulse/events/stream",
				Body:       map[string]any{"error": "no token set for SSE stream"},
			}}
			return
		}

		// Idle-read watchdog context: the server writes a keep-alive comment every
		// ~15s, so a healthy stream is NEVER silent for long. Without a read bound,
		// a half-open TCP (Pulse restarted, container network churn) blocks the
		// scanner FOREVER with no error — the consumer looks connected while
		// receiving nothing, and the reconnect loop never fires (2026-08-03: the
		// insights read-model starved exactly this way; the Java native consumer
		// had the same hang class and got SO_TIMEOUT for it). 60s of total silence
		// cancels the request, which surfaces as a read error → the caller's
		// reconnect loop takes over.
		streamCtx, cancelStream := context.WithCancel(ctx)
		defer cancelStream()

		req, err := http.NewRequestWithContext(
			streamCtx,
			http.MethodGet,
			s.client.baseURL+"/api/pulse/events/stream",
			nil,
		)
		if err != nil {
			errCh <- fmt.Errorf("pulse: failed to build SSE request: %w", err)
			return
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("User-Agent", userAgent)

		// SSE is a long-lived stream. The shared client's unary Timeout
		// (http.Client.Timeout bounds the WHOLE request, including reading
		// the body) would abort the stream after Timeout seconds — surfacing
		// as context-deadline-exceeded every Timeout seconds and dropping any
		// events published during the reconnect gap. Use a client with no
		// overall Timeout for the stream; its lifecycle is bounded by ctx
		// instead. Preserve the configured Transport so connection pooling
		// and any custom RoundTripper (WithHTTPClient) still apply.
		streamClient := s.client.http
		if streamClient.Timeout != 0 {
			streamClient = &http.Client{
				Transport:     s.client.http.Transport,
				CheckRedirect: s.client.http.CheckRedirect,
				Jar:           s.client.http.Jar,
			}
		}
		resp, err := streamClient.Do(req)
		if err != nil {
			// Context-cancellation surfaces as context.Canceled / context.DeadlineExceeded
			errCh <- err
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			errCh <- translateError(resp, "/api/pulse/events/stream", bodyBytes)
			return
		}

		// SSE parser — bufio.Scanner walks line-by-line; we accumulate
		// data: lines and dispatch on blank line. See
		// https://html.spec.whatwg.org/multipage/server-sent-events.html
		scanner := bufio.NewScanner(resp.Body)
		// Default Scanner max token size is 64KB. Bump to 1MB so large event
		// payloads (with embedded LLM completions, image URLs, etc.) don't
		// silently truncate.
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

		// Arm the idle watchdog: every received line (data, comment keep-alive,
		// blank) counts as activity. 60s with none = dead connection → cancel.
		// B-017 wave 1 (audit C#F6): the watchdog must measure READ starvation
		// only. This goroutine also performs the (blocking) channel send to the
		// consumer — a slow handler stops the Scan loop without the socket being
		// dead, and cancelling then would kill a healthy connection AND lose the
		// events published during the reconnect gap (SSE has no offsets). While
		// blocked on the send, inSend suppresses the watchdog.
		var lastActivity atomic.Int64
		var inSend atomic.Bool
		lastActivity.Store(time.Now().UnixMilli())
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-streamCtx.Done():
					return
				case <-ticker.C:
					if !inSend.Load() && time.Now().UnixMilli()-lastActivity.Load() > 60_000 {
						cancelStream()
						return
					}
				}
			}
		}()

		var dataLines []string
		for scanner.Scan() {
			line := scanner.Text()
			lastActivity.Store(time.Now().UnixMilli())

			if line == "" {
				// Event boundary — assemble + dispatch
				if len(dataLines) > 0 {
					payload := strings.Join(dataLines, "\n")
					dataLines = dataLines[:0]
					var event map[string]any
					if err := json.Unmarshal([]byte(payload), &event); err != nil {
						// Non-JSON payload — surface as {data: ...}
						event = map[string]any{"data": payload}
					}
					inSend.Store(true)
					select {
					case events <- event:
					case <-ctx.Done():
						return
					}
					inSend.Store(false)
					lastActivity.Store(time.Now().UnixMilli())
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue // SSE comment / keep-alive
			}
			if strings.HasPrefix(line, "data:") {
				dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			}
			// Other SSE fields (event:/id:/retry:) consumed but not surfaced.
		}
		if err := scanner.Err(); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("pulse: SSE stream read error: %w", err)
		}
	}()

	return events, errCh
}

// Replay — GET /api/pulse/iq/agents/{affectingState}/state/replay/{key}?from=&to=&limit=
// — B-113. The ordered changes that touched a state key between two instants.
//
// affectingState is the agent whose state store to inspect; key is the state
// key. The endpoint lives under the IQ state surface (it reads the same
// change log the time-travel Get/Diff read), so it is exposed here on the
// Events service to mirror the sibling SDKs' client.events.replay placement.
//
// Returns the unwrapped changes slice — each entry carries timestamp,
// changeType ("PUT" / "DELETE"), the resulting value, and eventId when
// known. The from/to/limit specs default to "-1h" / "now" / 100 when opts
// leaves them empty / zero.
//
//	changes, _ := client.Events.Replay(ctx, "user-sessions", "u42", EventsReplayOptions{
//	    From: "2026-05-24T10:00:00Z", To: "2026-05-24T11:00:00Z",
//	})
//	for _, ch := range changes {
//	    fmt.Println(ch["timestamp"], ch["changeType"], ch["value"])
//	}
//
// Raises *NotFoundError when the agent is not queryable.
func (s *EventsService) Replay(ctx context.Context, affectingState, key string, opts EventsReplayOptions) ([]map[string]any, error) {
	from := opts.From
	if from == "" {
		from = "-1h"
	}
	to := opts.To
	if to == "" {
		to = "now"
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = 100
	}
	q := url.Values{}
	q.Set("from", from)
	q.Set("to", to)
	q.Set("limit", strconv.Itoa(limit))
	path := "/api/pulse/iq/agents/" + encodeIQSegment(affectingState) +
		"/state/replay/" + encodeIQSegment(key) + "?" + q.Encode()

	result, err := s.client.request(ctx, http.MethodGet, path, nil, true)
	if err != nil {
		return nil, err
	}
	raw, ok := result["changes"].([]any)
	if !ok {
		return []map[string]any{}, nil
	}
	changes := make([]map[string]any, 0, len(raw))
	for _, item := range raw {
		if m, ok := item.(map[string]any); ok {
			changes = append(changes, m)
		}
	}
	return changes, nil
}
