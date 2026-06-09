// Use case 3 — Interactive Query over mesh-materialized agent state.
//
// Reads the live, queryable state an agent maintains on the mesh: a summary, a
// point lookup by key, a bounded scan, and a filtered/grouped query.
//
// Run:  PULSE_URL=http://localhost:9090 go run ./examples/interactive_query
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pulse "github.com/olsisoft/pulse-go/v2"
)

const agentID = "merchant-rollups-1m"

func main() {
	client, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	summary, err := client.IQ.Summary(ctx, agentID)
	if err != nil {
		log.Fatalf("summary: %v", err)
	}
	fmt.Println("Summary:", summary)

	// Point lookup for one merchant's current rollup.
	value, err := client.IQ.Get(ctx, agentID, "merchant-7")
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Println("merchant-7:", value)

	// Bounded scan of the keyspace.
	scan, err := client.IQ.Scan(ctx, agentID, pulse.IQScanOptions{Limit: 10})
	if err != nil {
		log.Fatalf("scan: %v", err)
	}
	fmt.Println("Scan (first 10):", scan)

	// Filtered + grouped query.
	result, err := client.IQ.Query(ctx, agentID, pulse.IQQueryOptions{
		Filter:  map[string]any{"field": "total_amount", "op": "gt", "value": 1000},
		GroupBy: "region",
		Limit:   20,
	})
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	fmt.Println("High-volume merchants by region:", result)
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
