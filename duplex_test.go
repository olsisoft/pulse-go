package pulse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// DeriveWSURL — URL derivation (mirrors the Python derive_ws_url tests).
// ---------------------------------------------------------------------------

func TestDeriveWSURL(t *testing.T) {
	cases := []struct {
		name    string
		baseURL string
		agentID string
		token   string
		want    string
	}{
		{
			name:    "http with port → ws, port+1",
			baseURL: "http://localhost:9090",
			agentID: "fraud",
			token:   "",
			want:    "ws://localhost:9091/api/pulse/agents/fraud/duplex",
		},
		{
			name:    "https with port → wss, port+1",
			baseURL: "https://pulse.example.com:8443",
			agentID: "pricing",
			token:   "",
			want:    "wss://pulse.example.com:8444/api/pulse/agents/pricing/duplex",
		},
		{
			name:    "token rides as query param",
			baseURL: "http://localhost:9090",
			agentID: "fraud",
			token:   "ey.tok",
			want:    "ws://localhost:9091/api/pulse/agents/fraud/duplex?token=ey.tok",
		},
		{
			name:    "no port → host unchanged",
			baseURL: "http://localhost",
			agentID: "ab",
			token:   "",
			want:    "ws://localhost/api/pulse/agents/ab/duplex",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DeriveWSURL(tc.baseURL, tc.agentID, tc.token)
			if err != nil {
				t.Fatalf("DeriveWSURL: %v", err)
			}
			if got != tc.want {
				t.Fatalf("DeriveWSURL = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Round-trip — connect, send, skip ack, recv output, close.
// ---------------------------------------------------------------------------

// echoDuplexServer accepts a WS connection, sends a 'connected' frame, then
// for each 'send' frame replies with an 'ack' (which the client must skip)
// followed by an 'output' frame echoing the payload under the same id.
func echoDuplexServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx := r.Context()

		connected, _ := json.Marshal(map[string]any{"type": "connected", "agentId": "fraud"})
		if err := conn.Write(ctx, websocket.MessageText, connected); err != nil {
			return
		}
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			var in map[string]any
			if err := json.Unmarshal(data, &in); err != nil {
				return
			}
			if in["type"] != "send" {
				continue
			}
			cid := in["correlationId"]
			ack, _ := json.Marshal(map[string]any{"type": "ack", "correlationId": cid})
			_ = conn.Write(ctx, websocket.MessageText, ack)
			out, _ := json.Marshal(map[string]any{
				"type":          "output",
				"correlationId": cid,
				"event": map[string]any{
					"topic":   "fraud-decisions",
					"payload": map[string]any{"decision": "DENY", "echo": in["payload"]},
				},
			})
			_ = conn.Write(ctx, websocket.MessageText, out)
		}
	}))
}

func TestDuplex_RoundTrip(t *testing.T) {
	srv := echoDuplexServer(t)
	defer srv.Close()

	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "/api/pulse/agents/fraud/duplex"

	c, err := NewClient(WithBaseURL("http://example.test"))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.dialDuplex(ctx, wsURL)
	if err != nil {
		t.Fatalf("dialDuplex: %v", err)
	}
	defer ch.Close()

	// Send with an explicit correlation id.
	cid, err := ch.Send(ctx, map[string]any{"amount": 5000}, "tx-1")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if cid != "tx-1" {
		t.Fatalf("Send returned cid %q, want tx-1", cid)
	}

	out, err := ch.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if out["correlationId"] != "tx-1" {
		t.Fatalf("output correlationId = %v, want tx-1", out["correlationId"])
	}
	payload, ok := out["payload"].(map[string]any)
	if !ok {
		t.Fatalf("output payload missing/typed wrong: %v", out["payload"])
	}
	if payload["decision"] != "DENY" {
		t.Fatalf("decision = %v, want DENY", payload["decision"])
	}
}

func TestDuplex_SendGeneratesUUID(t *testing.T) {
	srv := echoDuplexServer(t)
	defer srv.Close()
	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "/api/pulse/agents/fraud/duplex"

	c, _ := NewClient(WithBaseURL("http://example.test"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.dialDuplex(ctx, wsURL)
	if err != nil {
		t.Fatalf("dialDuplex: %v", err)
	}
	defer ch.Close()

	cid, err := ch.Send(ctx, map[string]any{"x": 1}, "")
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	// UUIDv4: 36 chars, 8-4-4-4-12, version nibble == '4'.
	if len(cid) != 36 || strings.Count(cid, "-") != 4 || cid[14] != '4' {
		t.Fatalf("Send did not generate a UUIDv4: %q", cid)
	}
	out, err := ch.Recv(ctx)
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if out["correlationId"] != cid {
		t.Fatalf("output correlationId = %v, want %q", out["correlationId"], cid)
	}
}

// errorDuplexServer sends an 'error' frame on connect and closes.
func TestDuplex_ConnectError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		frame, _ := json.Marshal(map[string]any{"type": "error", "error": "unknown agent"})
		_ = conn.Write(r.Context(), websocket.MessageText, frame)
	}))
	defer srv.Close()
	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "/api/pulse/agents/nope/duplex"

	c, _ := NewClient(WithBaseURL("http://example.test"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.dialDuplex(ctx, wsURL)
	if err == nil {
		t.Fatalf("expected an error from a server that sends an 'error' frame on connect")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
}

func TestDuplex_EmptyAgentID(t *testing.T) {
	c, _ := NewClient(WithBaseURL("http://example.test"))
	_, err := c.Duplex(context.Background(), "   ")
	if err == nil {
		t.Fatalf("expected error for blank agentID")
	}
}

func TestDuplex_ClosedChannel(t *testing.T) {
	srv := echoDuplexServer(t)
	defer srv.Close()
	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) + "/api/pulse/agents/fraud/duplex"

	c, _ := NewClient(WithBaseURL("http://example.test"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := c.dialDuplex(ctx, wsURL)
	if err != nil {
		t.Fatalf("dialDuplex: %v", err)
	}
	if err := ch.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Close is idempotent.
	if err := ch.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := ch.Send(ctx, map[string]any{"x": 1}, "c"); err == nil {
		t.Fatalf("Send on a closed channel must error")
	}
	if _, err := ch.Recv(ctx); err == nil {
		t.Fatalf("Recv on a closed channel must error")
	}
}
