// Use-case 2 — deploy the card-velocity fraud pipeline.
//
// What it shows: build a streaming pipeline with the DSL — tumbling 60s windows
// keyed by card, count/sum/max aggregations, a velocity filter (>5 auths/min) —
// compile it offline to inspect the JSON, then deploy it to the mesh.
//
// Prerequisites:
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - Auth: set PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD (deploying a pipeline
//     is an authenticated call).
//
// Run:  go run ./examples/usecase-2-deploy-velocity-pipeline
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pulse "github.com/olsisoft/pulse-go/v2"
)

func main() {
	// Fraud rule: more than 5 authorizations on one card within a 60s tumbling
	// window. KeyBy(cardId) → window → filter txCount > 5 → fraud-alerts topic,
	// with a "dashboard" sink channel for the live ops view.
	builder := pulse.NewStreamBuilder("card-velocity-60s").
		DescribedAs("Flag cards with more than 5 authorizations in any 60s window.").
		FromTopic("card-authorizations").
		Filter("amount > 0").
		KeyBy("cardId").
		Window(pulse.WindowsTumbling("60s"), pulse.WindowOptions{
			Aggregations: map[string]string{
				"txCount":     pulse.AggsCount(),
				"totalAmount": pulse.AggsSum("amount"),
				"maxAmount":   pulse.AggsMax("amount"),
			},
		}).
		Filter("txCount > 5").
		ToTopic("fraud-alerts", pulse.ToTopicOptions{SinkChannel: "dashboard"})

	c := client()

	// Compile offline first — the Compile call makes no server request — to
	// inspect the pipeline JSON before pushing it.
	spec, err := c.Streams.Compile(builder)
	if err != nil {
		log.Fatalf("compile: %v", err)
	}
	fmt.Println("Compiled pipeline:", spec)

	// Deploy to the mesh.
	deployed, err := c.Streams.Deploy(context.Background(), builder)
	if err != nil {
		log.Fatalf("deploy: %v", err)
	}
	fmt.Println("Deployed:", deployed)
}

// client builds the SDK client and authenticates: a pre-minted PULSE_TOKEN
// authenticates directly; otherwise it logs in with PULSE_USER + PULSE_PASSWORD
// (deploying a pipeline is an authenticated call).
func client() *pulse.Client {
	c, err := pulse.NewClient(pulse.WithBaseURL(baseURL()), pulse.WithToken(os.Getenv("PULSE_TOKEN")))
	if err != nil {
		log.Fatalf("client: %v", err)
	}
	if c.Token() == "" {
		if u, p := os.Getenv("PULSE_USER"), os.Getenv("PULSE_PASSWORD"); u != "" && p != "" {
			if _, err := c.Auth.Login(context.Background(), u, p); err != nil {
				log.Fatalf("login: %v", err)
			}
		}
	}
	return c
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
