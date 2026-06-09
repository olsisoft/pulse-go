// Use-case 1 — connect & list (card-payments fraud monitoring, hello-world rung).
//
// What it shows: construct the client, call Version (public), optionally log in,
// then list pipelines and connectors — the connectivity smoke test for the rest
// of the ladder.
//
// Prerequisites:
//   - A reachable Pulse at PULSE_URL (default http://localhost:9090).
//   - Auth is optional: set PULSE_TOKEN, or PULSE_USER + PULSE_PASSWORD. Without
//     them the example still prints Version and degrades gracefully on the
//     authenticated calls.
//
// Run:  go run ./examples/usecase-1-connect-and-list
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

	// Version is public — no JWT required. Good first connectivity check.
	version, err := client.Version(ctx)
	if err != nil {
		log.Fatalf("version: %v", err)
	}
	fmt.Println("Connected to Pulse:", version)

	// Authenticate if credentials are provided. A pre-minted PULSE_TOKEN (set
	// above via WithToken) already authenticates; username/password is an
	// alternative that caches the JWT on the client.
	if client.Token() == "" {
		user, pass := os.Getenv("PULSE_USER"), os.Getenv("PULSE_PASSWORD")
		if user != "" && pass != "" {
			if _, err := client.Auth.Login(ctx, user, pass); err != nil {
				log.Printf("login failed (continuing unauthenticated): %v", err)
			} else {
				fmt.Println("Logged in as", user)
			}
		} else {
			fmt.Println("No PULSE_TOKEN / PULSE_USER+PULSE_PASSWORD set — skipping authenticated calls.")
		}
	}

	if client.Token() == "" {
		fmt.Println("Done (unauthenticated). Set credentials to list pipelines and connectors.")
		return
	}

	// List the deployed pipelines in this org.
	pipelines, err := client.Pipelines.List(ctx)
	if err != nil {
		log.Fatalf("pipelines: %v", err)
	}
	fmt.Printf("%d pipeline(s):\n", len(pipelines))
	for _, p := range pipelines {
		fmt.Printf("  - %v\n", p["name"])
	}

	// List the connector catalogue (sinks + sources) for the fraud pipeline to
	// route alerts into downstream systems.
	sinks, err := client.Connectors.Sinks(ctx)
	if err != nil {
		log.Fatalf("sink connectors: %v", err)
	}
	sources, err := client.Connectors.Sources(ctx)
	if err != nil {
		log.Fatalf("source connectors: %v", err)
	}
	fmt.Printf("%d sink + %d source connector(s):\n", len(sinks), len(sources))
	for _, c := range sinks {
		fmt.Printf("  sink:   %v (%v)\n", c["subType"], c["displayName"])
	}
	for _, c := range sources {
		fmt.Printf("  source: %v (%v)\n", c["subType"], c["displayName"])
	}
}

func baseURL() string {
	if u := os.Getenv("PULSE_URL"); u != "" {
		return u
	}
	return "http://localhost:9090"
}
