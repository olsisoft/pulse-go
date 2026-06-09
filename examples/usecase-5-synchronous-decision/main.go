// Use-case 5 — synchronous ALLOW/DENY decision over a duplex channel (B-114).
//
// What it shows: open ONE bidirectional WebSocket to the fraud-decider agent,
// Send a couple of charges, Recv the correlated decision on the same
// connection, and print ALLOW/DENY with the correlation id — the synchronous
// decision path with no publish-then-poll round trip.
//
// Prerequisites:
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - A "fraud-decider" decision agent deployed and duplex-enabled.
//   - Auth: set PULSE_TOKEN (the duplex upgrade carries the JWT).
//
// Run:  go run ./examples/usecase-5-synchronous-decision
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pulse "github.com/olsisoft/pulse-go/v2"
)

const agentID = "fraud-decider"

func main() {
	client, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	ch, err := client.Duplex(ctx, agentID)
	if err != nil {
		log.Fatalf("duplex: %v", err)
	}
	defer ch.Close()

	// One hot card (likely DENY — already over its velocity budget) and one
	// fresh card (likely ALLOW). The agent decides; we just relay + print.
	charges := []struct {
		correlationID string
		charge        map[string]any
	}{
		{"charge-hot-1", map[string]any{"cardId": "card-007", "amount": 4200}},
		{"charge-fresh-1", map[string]any{"cardId": "card-999", "amount": 35}},
	}

	for _, c := range charges {
		cid, err := ch.Send(ctx, c.charge, c.correlationID)
		if err != nil {
			log.Fatalf("send %s: %v", c.correlationID, err)
		}

		recvCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		out, err := ch.Recv(recvCtx)
		cancel()
		if err != nil {
			log.Fatalf("recv %s: %v", cid, err)
		}

		fmt.Printf("card %v amount %v → %s (correlationId=%v)\n",
			c.charge["cardId"], c.charge["amount"], decision(out), out["correlationId"])
	}
}

// decision pulls the ALLOW/DENY verdict out of the agent's output payload,
// falling back to the raw output if the agent uses a different field.
func decision(out map[string]any) string {
	if payload, ok := out["payload"].(map[string]any); ok {
		if d, ok := payload["decision"].(string); ok && d != "" {
			return d
		}
		if d, ok := payload["verdict"].(string); ok && d != "" {
			return d
		}
		return fmt.Sprintf("%v", payload)
	}
	return fmt.Sprintf("%v", out["payload"])
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
