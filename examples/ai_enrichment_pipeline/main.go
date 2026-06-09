// Use case 4 — agentic enrichment pipeline (LLM + extract + MCP) on the mesh.
//
// Enriches support tickets streaming through the mesh: classify sentiment with
// an LLM, pull structured fields out of free text, then call an MCP tool to
// look the customer up — a declarative stream that runs on the cluster.
//
// Run:  PULSE_URL=http://localhost:9090 go run ./examples/ai_enrichment_pipeline
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pulse "github.com/olsisoft/pulse-go/v2"
)

func main() {
	builder := pulse.NewStreamBuilder("ticket-enrichment").
		FromTopic("support-tickets").
		Filter("priority != 'spam'").
		MapLlm("Classify the ticket sentiment as positive, neutral, or negative.",
			pulse.MapLlmOptions{OutputField: "sentiment"}).
		Extract(pulse.ExtractOptions{
			Instruction: "Extract the product name and the customer's requested action.",
			Schema:      map[string]string{"product": "string", "requested_action": "string"},
		}).
		McpCall("crm.lookup_customer", pulse.McpCallOptions{
			Args:        map[string]any{"email": "${customer_email}"},
			OutputField: "customer",
		}).
		ToTopic("tickets-enriched")

	spec, err := builder.Build("")
	if err != nil {
		log.Fatalf("build: %v", err)
	}
	fmt.Println("Pipeline spec:", spec)

	client, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	deployed, err := client.Streams.Deploy(context.Background(), builder)
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
