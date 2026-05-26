// Use case 5 — sink a mesh stream to an external connector.
//
// Discovers the available sink connectors, then declares a stream that delivers
// the per-merchant rollups to a ClickHouse warehouse via a connector sink.
//
// Run:  PULSE_URL=http://localhost:9090 go run ./examples/stream_to_connector
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

	sinks, err := client.Connectors.Sinks(ctx)
	if err != nil {
		log.Fatalf("sinks: %v", err)
	}
	fmt.Printf("%d sink connector(s) available\n", len(sinks))

	builder := pulse.NewStreamBuilder("rollups-to-warehouse").
		FromTopic("merchant-rollups-1m").
		Filter("total_amount > 0").
		ToConnector("clickhouse", map[string]any{
			"url":   "http://clickhouse:8123",
			"table": "merchant_rollups",
		})

	deployed, err := client.Streams.Deploy(ctx, builder)
	if err != nil {
		log.Fatalf("deploy: %v", err)
	}
	fmt.Println("Deployed:", deployed)
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
