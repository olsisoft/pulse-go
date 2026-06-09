// Use case 1 — real-time windowed aggregation on the event mesh.
//
// Declares a streaming pipeline that rolls up payment transactions per merchant
// in 1-minute tumbling windows and writes the rollups back to a mesh topic.
// Deployed to a Pulse attached to a StreamFlow cluster, this runs on the mesh.
//
// Run:  PULSE_URL=http://localhost:9090 go run ./examples/realtime_windowed_aggregation
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	pulse "github.com/olsisoft/pulse-go/v2"
)

func main() {
	builder := pulse.NewStreamBuilder("merchant-rollups-1m").
		FromTopic("transactions").
		Filter("amount > 0").
		KeyBy("merchant_id").
		Window(pulse.WindowsTumbling("1m"), pulse.WindowOptions{
			Aggregations: map[string]string{
				"txnCount":    pulse.AggsCount(),
				"totalAmount": pulse.AggsSum("amount"),
				"avgAmount":   pulse.AggsAvg("amount"),
				"maxAmount":   pulse.AggsMax("amount"),
			},
		}).
		ToTopic("merchant-rollups-1m")

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
