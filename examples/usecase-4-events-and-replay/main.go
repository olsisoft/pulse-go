// Use-case 4 — live fraud-alert events + replay one card's history.
//
// What it shows: tail the live SSE event stream and print a handful of
// fraud-alert events (stops after a few or a short timeout), then replay the
// committed state-change history (time-travel) for one card.
//
// Prerequisites:
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - The card-velocity-60s pipeline deployed + producing (see usecase-2).
//   - Auth: set PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD (stream + replay are authenticated).
//
// Run:  go run ./examples/usecase-4-events-and-replay
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pulse "github.com/olsisoft/pulse-go/v2"
)

const agentID = "card-velocity-60s"

func main() {
	client, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	// Authenticate: a pre-minted PULSE_TOKEN already authenticates; otherwise log
	// in with PULSE_USER + PULSE_PASSWORD (the SSE stream + replay require a JWT).
	if client.Token() == "" {
		if u, p := os.Getenv("PULSE_USER"), os.Getenv("PULSE_PASSWORD"); u != "" && p != "" {
			if _, err := client.Auth.Login(ctx, u, p); err != nil {
				log.Fatalf("login: %v", err)
			}
		}
	}

	// Tail the live event stream — stop after a handful of fraud-alert events or
	// a short timeout, whichever comes first.
	streamCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	events, errCh := client.Events.Stream(streamCtx)
	fmt.Println("Tailing live events for fraud-alerts (up to 5, 15s budget)…")
	seen := 0
loop:
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				break loop
			}
			if evt["topic"] == "fraud-alerts" || evt["type"] == "fraud-alert" {
				fmt.Println("  fraud-alert:", evt)
				if seen++; seen >= 5 {
					cancel()
					break loop
				}
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				log.Printf("stream ended: %v", err)
			}
			break loop
		case <-streamCtx.Done():
			fmt.Println("  (timeout reached)")
			break loop
		}
	}

	// Replay the committed state-change history (time-travel) for one card.
	changes, err := client.Events.Replay(ctx, agentID, "card-007",
		pulse.EventsReplayOptions{From: "-1h", To: "now", Limit: 50})
	if err != nil {
		log.Fatalf("replay: %v", err)
	}
	fmt.Printf("Replayed %d state change(s) for card-007:\n", len(changes))
	for _, ch := range changes {
		fmt.Printf("  %v  %v  %v\n", ch["timestamp"], ch["changeType"], ch["value"])
	}
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
