// Use-case 3 — interactive query the live fraud state.
//
// What it shows: read the card-velocity-60s agent's materialized state like a
// database — a Summary, a filtered Query (cards with txCount > 5 via the IQ
// filter builder), and a point Get for one card. Wraps the query in a small
// caller-side retry that honours RetryAfterSeconds — the SDK never auto-retries.
//
// Prerequisites:
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - The card-velocity-60s pipeline deployed (see usecase-2).
//   - Auth: set PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD (IQ requires AGENT_READ).
//
// Run:  go run ./examples/usecase-3-interactive-query
package main

import (
	"context"
	"errors"
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
	// in with PULSE_USER + PULSE_PASSWORD (IQ requires the AGENT_READ permission).
	if client.Token() == "" {
		if u, p := os.Getenv("PULSE_USER"), os.Getenv("PULSE_PASSWORD"); u != "" && p != "" {
			if _, err := client.Auth.Login(ctx, u, p); err != nil {
				log.Fatalf("login: %v", err)
			}
		}
	}

	// Headline state summary — backend, hot/cold sizes, whether it's queryable.
	summary, err := client.IQ.Summary(ctx, agentID)
	if err != nil {
		log.Fatalf("summary: %v", err)
	}
	fmt.Println("Summary:", summary)

	// Filtered query: cards over the velocity threshold. The IQ filter builder
	// produces the leaf {field, op, value} the server expects.
	result, err := queryWithRetry(ctx, client, pulse.IQQueryOptions{
		Filter: pulse.IQLeaf("txCount", "gt", 5),
		Limit:  20,
	})
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Println("Cards over velocity threshold:", result)

	// Point lookup for one card's current window state.
	value, err := client.IQ.Get(ctx, agentID, "card-007")
	if err != nil {
		var nf *pulse.NotFoundError
		if errors.As(err, &nf) {
			fmt.Println("card-007: no live state (key absent or agent not queryable)")
		} else {
			log.Fatalf("get: %v", err)
		}
	} else {
		fmt.Println("card-007:", value)
	}
}

// queryWithRetry runs an IQ Query, retrying only on a RateLimitError and only
// for the wait the server advised. This demonstrates the SDK's no-auto-retry
// contract: the caller decides the backoff policy.
func queryWithRetry(ctx context.Context, client *pulse.Client, opts pulse.IQQueryOptions) (map[string]any, error) {
	const maxAttempts = 3
	for attempt := 1; ; attempt++ {
		result, err := client.IQ.Query(ctx, agentID, opts)
		if err == nil {
			return result, nil
		}
		var rl *pulse.RateLimitError
		if !errors.As(err, &rl) || attempt >= maxAttempts {
			return nil, err
		}
		wait := time.Duration(rl.RetryAfterSeconds) * time.Second
		if wait <= 0 {
			wait = time.Second // server gave no hint — use a small default
		}
		fmt.Printf("rate limited; retrying in %s (attempt %d/%d)\n", wait, attempt, maxAttempts)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
