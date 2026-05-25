package pulse

// B-114 — bidirectional duplex channel for synchronous decision agents.
//
// Opens ONE WebSocket to /api/pulse/agents/{id}/duplex: events are streamed IN
// and the agent's correlated outputs come back OUT on the same connection,
// matched by a correlation id. Eliminates the 2-connection publish-then-poll
// pattern for decision microservices (fraud, pricing, A/B assignment).
//
// The duplex endpoint runs on the Pulse WebSocket port (REST port + 1 by
// convention); DeriveWSURL derives it from the client's base URL.
//
// Quick start:
//
//	ch, err := client.Duplex(ctx, "fraud-detector")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer ch.Close()
//
//	cid, err := ch.Send(ctx, map[string]any{"amount": 5000}, "tx-1")
//	out, err := ch.Recv(ctx)   // the agent's output for THIS input
//	if out["correlationId"] == cid {
//	    // out["payload"] is the decision
//	}

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/coder/websocket"
)

// DeriveWSURL builds the duplex WebSocket URL from the client's REST base URL.
//
// http→ws / https→wss, host unchanged, port → REST port + 1 (the Pulse
// WebSocket server convention). The JWT, when set, rides as a "token" query
// param (the server reads it from the upgrade request line).
func DeriveWSURL(baseURL, agentID, token string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("pulse: failed to parse base URL %q: %w", baseURL, err)
	}
	scheme := "ws"
	if parsed.Scheme == "https" || parsed.Scheme == "wss" {
		scheme = "wss"
	}
	host := parsed.Hostname()
	if host == "" {
		host = "localhost"
	}
	netloc := host
	if portStr := parsed.Port(); portStr != "" {
		port, convErr := strconv.Atoi(portStr)
		if convErr != nil {
			return "", fmt.Errorf("pulse: invalid port %q in base URL: %w", portStr, convErr)
		}
		netloc = host + ":" + strconv.Itoa(port+1)
	}
	out := &url.URL{
		Scheme: scheme,
		Host:   netloc,
		Path:   "/api/pulse/agents/" + agentID + "/duplex",
	}
	if token != "" {
		q := url.Values{}
		q.Set("token", token)
		out.RawQuery = q.Encode()
	}
	return out.String(), nil
}

// Duplex opens a bidirectional duplex channel to an agent (B-114).
//
// It connects to the Pulse WebSocket port (REST port + 1) and consumes the
// server's first frame: "connected" → ready, "error" → the channel is closed
// and the error returned. Pass the agent id (e.g. "fraud-detector").
//
// The returned *DuplexChannel is NOT safe for concurrent Send + Recv from
// multiple goroutines without external synchronisation, mirroring a single
// request/response decision loop; serialise calls or use one channel per
// goroutine. Always Close() it when done.
func (c *Client) Duplex(ctx context.Context, agentID string) (*DuplexChannel, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, errors.New("pulse: Duplex — agentID must be a non-empty string")
	}
	wsURL, err := DeriveWSURL(c.baseURL, agentID, c.Token())
	if err != nil {
		return nil, err
	}
	return c.dialDuplex(ctx, wsURL)
}

// dialDuplex performs the WebSocket handshake + reads the opening frame. Split
// out so tests can target an explicit ws:// URL via DeriveWSURL.
func (c *Client) dialDuplex(ctx context.Context, wsURL string) (*DuplexChannel, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return nil, fmt.Errorf("pulse: duplex WebSocket dial failed for %s: %w", wsURL, err)
	}

	// The server sends a 'connected' frame first (or 'error' + close for an
	// unknown agent / disabled duplex). Surface the error eagerly.
	_, data, err := conn.Read(ctx)
	if err != nil {
		conn.CloseNow()
		return nil, fmt.Errorf("pulse: duplex handshake read failed for %s: %w", wsURL, err)
	}
	var first map[string]any
	if err := json.Unmarshal(data, &first); err != nil {
		conn.CloseNow()
		return nil, fmt.Errorf("pulse: duplex handshake frame was not JSON for %s: %w", wsURL, err)
	}
	if first["type"] == "error" {
		conn.CloseNow()
		return nil, &ValidationError{APIError{StatusCode: 400, Path: wsURL, Body: first}}
	}

	return &DuplexChannel{url: wsURL, conn: conn}, nil
}

// DuplexChannel is an open duplex session. Send publishes an event to the
// agent's input topic and returns its correlation id; Recv returns the next
// output event the agent produced (each carries a correlationId matching the
// input that caused it). Acknowledgement / keep-alive frames are consumed
// transparently. Close it when done.
type DuplexChannel struct {
	url string

	mu     sync.Mutex
	conn   *websocket.Conn
	closed bool
}

// Send publishes payload to the agent's input topic. Returns the correlation
// id (generated as a UUIDv4 when correlationID is "") that the matching output
// will carry. Respects ctx cancellation.
func (ch *DuplexChannel) Send(ctx context.Context, payload map[string]any, correlationID string) (string, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed || ch.conn == nil {
		return "", errors.New("pulse: duplex channel is closed")
	}
	cid := correlationID
	if cid == "" {
		generated, err := newUUIDv4()
		if err != nil {
			return "", fmt.Errorf("pulse: duplex failed to generate correlation id: %w", err)
		}
		cid = generated
	}
	frame := map[string]any{"type": "send", "correlationId": cid, "payload": payload}
	encoded, err := json.Marshal(frame)
	if err != nil {
		return "", fmt.Errorf("pulse: duplex failed to encode send frame: %w", err)
	}
	if err := ch.conn.Write(ctx, websocket.MessageText, encoded); err != nil {
		return "", fmt.Errorf("pulse: duplex send failed: %w", err)
	}
	return cid, nil
}

// Recv returns the next agent output event (skips ack / pong / connected
// frames). The returned map is the agent's output event ("id" / "topic" /
// "type" / "key" / "payload") plus a "correlationId" field identifying the
// input that produced it. Respects ctx cancellation.
func (ch *DuplexChannel) Recv(ctx context.Context) (map[string]any, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed || ch.conn == nil {
		return nil, errors.New("pulse: duplex channel is closed")
	}
	for {
		_, data, err := ch.conn.Read(ctx)
		if err != nil {
			return nil, fmt.Errorf("pulse: duplex recv failed: %w", err)
		}
		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, fmt.Errorf("pulse: duplex received non-JSON frame: %w", err)
		}
		switch msg["type"] {
		case "output":
			event := map[string]any{}
			if raw, ok := msg["event"].(map[string]any); ok {
				for k, v := range raw {
					event[k] = v
				}
			} else {
				event["value"] = msg["event"]
			}
			event["correlationId"] = msg["correlationId"]
			return event, nil
		case "error":
			return nil, &ValidationError{APIError{StatusCode: 400, Path: ch.url, Body: msg}}
		default:
			// ack / pong / connected → transparently skipped
		}
	}
}

// Close terminates the duplex session. Idempotent.
func (ch *DuplexChannel) Close() error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed || ch.conn == nil {
		ch.closed = true
		return nil
	}
	ch.closed = true
	conn := ch.conn
	ch.conn = nil
	return conn.Close(websocket.StatusNormalClosure, "")
}

// newUUIDv4 generates a random RFC-4122 v4 UUID without a third-party
// dependency, matching the Python SDK's uuid.uuid4() correlation-id behaviour.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
