package pulse

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

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

		req, err := http.NewRequestWithContext(
			ctx,
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

		resp, err := s.client.http.Do(req)
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

		var dataLines []string
		for scanner.Scan() {
			line := scanner.Text()

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
					select {
					case events <- event:
					case <-ctx.Done():
						return
					}
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
