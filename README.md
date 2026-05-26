# pulse-go — Go SDK for StreamFlow Pulse

Official Go client for [Pulse](https://github.com/olsisoft/pulse-go) — the AI Agent Platform. Targets **Go 1.22+**, **zero external dependencies** (stdlib only: `net/http`, `encoding/json`).

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/olsisoft/pulse-go/v2"
)

func main() {
    ctx := context.Background()

    client, err := pulse.NewClient(pulse.WithBaseURL("http://localhost:9090"))
    if err != nil {
        log.Fatal(err)
    }

    if _, err := client.Auth.Login(ctx, "alice", "secret"); err != nil {
        log.Fatal(err)
    }

    pipelines, err := client.Pipelines.List(ctx)
    if err != nil {
        log.Fatal(err)
    }
    for _, p := range pipelines {
        fmt.Println(p["name"])
    }
}
```

## Install

```bash
go get github.com/olsisoft/pulse-go/v2
```

Requires **Go 1.22+**.

## Why pulse-go

- **Zero external dependencies** — stdlib `net/http` + `encoding/json`, nothing else. The compiled binary stays tiny.
- **Idiomatic Go** — `ctx context.Context` on every call, `errors.As` for typed errors, options-pattern constructor, public field accessors (`client.Pipelines.List(ctx)` matches the AWS SDK v2 / Google Cloud Go SDK convention).
- **Safe for concurrent use** — a single `*pulse.Client` can be shared across an entire app; the embedded `http.Client` pools connections.
- **Sibling parity** — same surface + naming as the Python (`pulse-py`), JavaScript (`@olsisoft/pulse-client`), and Java (`com.streamflow:pulse-client`) SDKs.
- **Spec-aligned** — every method corresponds 1:1 to an endpoint in the [Pulse OpenAPI 3.1 spec](../streamflow-pulse/src/main/resources/openapi/openapi.yaml). Drift caught at PR time by the in-tree spec invariant tests (B-103).

## Quick start

```go
import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/olsisoft/pulse-go/v2"
)

ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

client, err := pulse.NewClient(
    pulse.WithBaseURL(os.Getenv("PULSE_URL")),
)
if err != nil {
    log.Fatal(err)
}

// Login — token is cached on the client automatically
if _, err := client.Auth.Login(ctx, os.Getenv("PULSE_USER"), os.Getenv("PULSE_PASSWORD")); err != nil {
    var authErr *pulse.AuthError
    if errors.As(err, &authErr) {
        log.Fatal("bad credentials")
    }
    log.Fatal(err)
}

// List + inspect
pipelines, _ := client.Pipelines.List(ctx)
for _, p := range pipelines {
    log.Printf("%s — %s", p["name"], p["status"])
}

// Create from a template
newPipeline, _ := client.Pipelines.Create(ctx, map[string]any{
    "name":       "my-fraud-detector",
    "templateId": "fintech-fraud-detection-realtime",
    "nodes": []map[string]any{
        {"id": "src", "type": "source", "subType": "kafka-source"},
        {"id": "agt", "type": "agent", "subType": "streaming"},
        {"id": "snk", "type": "sink", "subType": "telegram"},
    },
})
log.Println("created:", newPipeline["id"])
```

## Supported surfaces (v2.6.0)

| Resource | Methods | Notes |
|---|---|---|
| `client.Auth` | `Login(ctx, user, pass)`, `Refresh(ctx, refreshToken)`, `Organizations(ctx)`, `SwitchOrg(ctx, orgID)` | Auto-caches JWT after Login / Refresh / SwitchOrg. |
| `client.Pipelines` | `List(ctx)`, `Get(ctx, id)`, `Create(ctx, definition)`, `Delete(ctx, id)` | `definition` follows the CreatePipelineRequest schema. |
| `client.Agents` | `List(ctx)`, `Get(ctx, id)` | Read-only — agents are owned by pipelines. |
| `client.Templates` | `List(ctx)` | The 223+ first-party templates. |
| `client.Users` | `List(ctx)` | Requires USERS_LIST permission (Owner / Platform Admin personas). |
| `client.Version(ctx)` | top-level | Public — no JWT required. |

Full ~112-endpoint surface documented in Swagger UI at `<pulse-server>/api-docs`. Less-used methods land opportunistically as user-facing demand surfaces.

## Embedded ML inference & duplex

Score events with an uploaded ONNX model in-process (B-112), and open a
bidirectional duplex channel for synchronous decisions (B-114). Full guide:
[ML inference & duplex](https://github.com/olsisoft/pulse-go/blob/dev/docs/SDK-ML-INFERENCE-AND-DUPLEX.md).

```go
// Upload + score with an ONNX model (no model-server hop)
client.Models.Upload(ctx, pulse.UploadModelOptions{
    Name: "fraud", Path: "./fraud.onnx",
    InputSchema: map[string]string{"amount": "float", "country": "float"}})
builder.FromTopic("transactions").
    MlPredict(pulse.MlPredictOptions{
        Model: "fraud", InputFields: []string{"amount", "country"}, OutputField: "prediction"}).
    Filter("prediction.fraud_score > 0.8").ToTopic("flagged")

// Duplex: one connection, send in / receive the correlated output
ch, _ := client.Duplex(ctx, "fraud-detector")
defer ch.Close()
cid, _ := ch.Send(ctx, map[string]any{"amount": 5000}, "tx-1")
out, _ := ch.Recv(ctx) // out["correlationId"] == "tx-1"
```

## Authentication

Three patterns:

```go
// 1. Username + password (interactive / CLI tools)
client, _ := pulse.NewClient(pulse.WithBaseURL("http://localhost:9090"))
client.Auth.Login(ctx, "alice", "secret")

// 2. Pre-minted JWT (CI / service accounts)
client, _ := pulse.NewClient(
    pulse.WithBaseURL("http://localhost:9090"),
    pulse.WithToken(os.Getenv("PULSE_JWT")),
)

// 3. Hot token rotation (long-running daemons)
client.SetToken(freshlyMintedToken)
```

For long-running processes, persist `refreshToken` from `Login()` and call `client.Auth.Refresh(ctx, refreshToken)` before the JWT expires (default 1 h TTL).

## Error handling

Every server error becomes a typed exception you can match precisely with `errors.As`:

```go
import (
    "errors"
    "time"

    "github.com/olsisoft/pulse-go/v2"
)

_, err := client.Pipelines.Get(ctx, "nope")
switch {
case err == nil:
    // happy path

case errors.Is(err, context.Canceled):
    log.Println("cancelled")

case func() bool { var e *pulse.NotFoundError; return errors.As(err, &e) }():
    log.Println("doesn't exist — fine")

case func() bool { var e *pulse.RateLimitError; return errors.As(err, &e) }():
    var rl *pulse.RateLimitError
    errors.As(err, &rl)
    time.Sleep(time.Duration(rl.RetryAfterSeconds) * time.Second)
    // retry

default:
    var apiErr *pulse.APIError
    if errors.As(err, &apiErr) {
        log.Printf("HTTP %d from %s: %v", apiErr.StatusCode, apiErr.Path, apiErr.Body)
    }
    log.Fatal(err)
}
```

Every `APIError` carries `StatusCode`, `Path`, and `Body` so log lines + bug reports are actionable.

## Custom *http.Client (proxies, mTLS, shared pools, tracing)

```go
shared := &http.Client{
    Timeout: 5 * time.Second,
    Transport: &http.Transport{
        // your TLS / proxy / tracing wiring
    },
}

client, _ := pulse.NewClient(
    pulse.WithBaseURL("http://pulse.acme.com"),
    pulse.WithHTTPClient(shared),
)
```

## Development

```bash
git clone https://github.com/olsisoft/pulse-go.git
cd pulse-go

go build ./...
go vet ./...
go test -v ./...
go test -race ./...    # ensure concurrent token rotation is safe
```

CI runs the same on every push touching `pulse-go/` — see `.github/workflows/pulse-go.yaml`.

## Roadmap

- **v2.5.x** — current sync API, 5 core resources, `Version()`.
- **v2.6.x** — expanded resource coverage: backups, schedules, credentials, settings, approvals, chat.
- **v3.0** — event-stream consumer over channels: `events := client.Events.Stream(ctx); for ev := range events { ... }` consuming `/api/pulse/events/stream` (SSE).
- **B-098 satellite** — once `olsisoft/pulse-go` exists as its own repo, this in-tree code lifts out wholesale. `go get` will switch to the satellite; in-tree continues to mirror for one release cycle.

Track progress in [`docs/STREAMFLOW-BACKLOG.md`](../docs/STREAMFLOW-BACKLOG.md) under item **B-098**.

## License

Apache 2.0 — same as the parent Pulse repository.
