# Pulse Go SDK — Examples

Five runnable examples showing how an application drives the **StreamFlow event
mesh** through Pulse. The SDK *declares* the work; Pulse runs it on the cluster
(sharded, replicated) — `app → SDK → Pulse API → bridge → mesh`.

## Use cases

| # | Package | What it shows |
|---|---------|---------------|
| 1 | [`realtime_windowed_aggregation`](realtime_windowed_aggregation/main.go) | Per-merchant 1-minute tumbling-window rollup (`count`/`sum`/`avg`/`max`) → topic |
| 2 | [`events_live_and_replay`](events_live_and_replay/main.go) | Tail the live event stream (channels + `context`) **and** replay a key's state history |
| 3 | [`interactive_query`](interactive_query/main.go) | Interactive Query — `Summary` / point `Get` / bounded `Scan` / filtered + grouped `Query` |
| 4 | [`ai_enrichment_pipeline`](ai_enrichment_pipeline/main.go) | Agentic stream — LLM sentiment → `Extract` structured fields → MCP CRM lookup |
| 5 | [`stream_to_connector`](stream_to_connector/main.go) | Discover sink connectors, then `Filter` → sink a stream to a ClickHouse connector |

## Prerequisites

- **Go 1.22+** and the SDK: `go get github.com/olsisoft/pulse-go/v2`.
- A reachable **Pulse** instance — embedded mesh, or attached to a StreamFlow
  cluster (Settings → Data Plane → REMOTE).

## Run

```bash
export PULSE_URL=http://localhost:9090      # your Pulse base URL
export PULSE_TOKEN=...                       # only if your Pulse requires auth

go run ./examples/realtime_windowed_aggregation
go run ./examples/events_live_and_replay
go run ./examples/interactive_query
go run ./examples/ai_enrichment_pipeline
go run ./examples/stream_to_connector
```

Build/vet them all with `go build ./...` and `go vet ./examples/...`.
