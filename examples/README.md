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

## Use-case ladder (simplest → most complex)

A graduated 5-rung ladder over **one** domain — card-payments fraud monitoring
(topic `card-authorizations`, events `{cardId, merchantId, amount, ts}`; fraud
rule = more than 5 authorizations on one card in a 60s tumbling window). Each
rung builds on the previous one.

| # | Directory | What it shows | Run |
|---|-----------|---------------|-----|
| 1 | [`usecase-1-connect-and-list`](usecase-1-connect-and-list/main.go) | Connect, `Version`, optional login, list pipelines + connectors (hello-world) | `go run ./examples/usecase-1-connect-and-list` |
| 2 | [`usecase-2-deploy-velocity-pipeline`](usecase-2-deploy-velocity-pipeline/main.go) | DSL build of `card-velocity-60s` (KeyBy → 60s tumbling window → velocity filter), `Compile` offline then `Deploy` | `go run ./examples/usecase-2-deploy-velocity-pipeline` |
| 3 | [`usecase-3-interactive-query`](usecase-3-interactive-query/main.go) | IQ `Summary` / filtered `Query` (`txCount > 5`) / point `Get`, with caller-side rate-limit retry (no auto-retry) | `go run ./examples/usecase-3-interactive-query` |
| 4 | [`usecase-4-events-and-replay`](usecase-4-events-and-replay/main.go) | Tail the live `fraud-alert` event stream, then `Replay` one card's state history | `go run ./examples/usecase-4-events-and-replay` |
| 5 | [`usecase-5-synchronous-decision`](usecase-5-synchronous-decision/main.go) | Duplex channel to `fraud-decider`: `Send` charges, `Recv` ALLOW/DENY + correlation id (B-114) | `go run ./examples/usecase-5-synchronous-decision` |

All five talk to a live Pulse at `PULSE_URL` (default `http://localhost:9090`);
set `PULSE_TOKEN` (or `PULSE_USER` + `PULSE_PASSWORD` for rung 1) for the
authenticated calls.
