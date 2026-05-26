// Use case 2 — consume live mesh events and replay state history.
//
//   - tail the live event stream — bounded here to the first 10 events;
//   - replay the committed state-change history for one key (time-travel).
//
// Run:  PULSE_URL=http://localhost:9090 go run ./examples/events_live_and_replay
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pulse "github.com/olsisoft/pulse-go/v2"
)

func main() {
	client, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	// Replay the last hour of committed state changes for one account key.
	changes, err := client.Events.Replay(ctx, "balance", "acct-42",
		pulse.EventsReplayOptions{From: "-1h", To: "now", Limit: 50})
	if err != nil {
		log.Fatalf("replay: %v", err)
	}
	fmt.Printf("Replayed %d state change(s) for acct-42\n", len(changes))

	// Tail the live event stream — stop after the first 10 events.
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events, errCh := client.Events.Stream(streamCtx)
	fmt.Println("Tailing live events (first 10)…")
	seen := 0
	for {
		select {
		case evt, ok := <-events:
			if !ok {
				return
			}
			fmt.Println("  event:", evt)
			if seen++; seen >= 10 {
				cancel()
				return
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				log.Fatalf("stream: %v", err)
			}
			return
		}
	}
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
