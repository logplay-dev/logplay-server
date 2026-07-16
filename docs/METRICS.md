# LogPlay Server — Metrics Catalog

The server emits OpenTelemetry metrics for the high-signal control-plane operations. Metrics are
**off by default**; enable them via app config and standard OpenTelemetry environment variables.

## Enabling

```
metrics.enabled = true                # app config (Vert.x) — the on/off switch
metrics.service.name = logplay-server # optional; default "logplay-server"
metrics.otlp.endpoint = http://otel-collector:4317   # optional; else OTEL_EXPORTER_OTLP_ENDPOINT
```

When enabled, the server installs the OpenTelemetry SDK via the standard autoconfigure contract, so
any `OTEL_*` env var / `otel.*` system property applies (endpoint, headers, protocol, resource
attributes, export interval). Defaults set here: metrics export over OTLP; traces and logs off.

## Semantic metrics (domain)

Recorded by the domain use cases through the framework-free `Metrics` port; the OpenTelemetry
adapter (`OtelMetrics`) maps them to the instruments below. Instrumentation scope:
`org.zeplinko.logplay.server`.

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `logplay.jobs.created` | counter | 1 | — | Jobs created and committed |
| `logplay.jobs.duplicate_rejected` | counter | 1 | — | Creates rejected on duplicate `(groupId, idempotencyKey)` |
| `logplay.acquire.calls` | counter | 1 | `outcome`=`hit`\|`empty` | Acquire calls; `empty` = the poll found no work (idle-poll waste) |
| `logplay.jobs.acquired` | counter | 1 | — | Jobs actually claimed by acquire |
| `logplay.checkpoints.saved` | counter | 1 | — | Checkpoints saved |
| `logplay.checkpoints.data_bytes` | histogram | By | — | Checkpoint payload size distribution |
| `logplay.jobs.completed` | counter | 1 | — | Jobs finished |
| `logplay.jobs.released` | counter | 1 | `scheduled`=`true`\|`false` | Jobs released; `scheduled` = future `availableAt` (durable sleep) |
| `logplay.errors.reported` | counter | 1 | `retry`=`true`\|`false` | Execution errors; `retry=false` = went terminal FAILED |
| `logplay.jobs.failed` | counter | 1 | — | Jobs that reached terminal FAILED |
| `logplay.jobs.aborted` | counter | 1 | — | Jobs aborted |

**Deriving the Phase-0 signals**
- **Empty-acquire ratio** = `logplay.acquire.calls{outcome=empty}` / `logplay.acquire.calls` — the idle-poll-waste signal.
- **Checkpoint write rate** = rate of `logplay.checkpoints.saved` — the heaviest write path.
- **Retry pressure** = `logplay.errors.reported{retry=true}` rate.

## HTTP metrics (transport)

Recorded by `HttpMetrics` (a router handler). Route cardinality is bounded to the registered
patterns.

| Metric | Type | Unit | Attributes | Meaning |
|---|---|---|---|---|
| `logplay.http.server.duration` | histogram | ms | `http.request.method`, `http.route`, `http.response.status_code` | Per-request server latency |
| `logplay.http.server.requests` | counter | 1 | `http.request.method`, `http.route`, `http.response.status_code` | Request count |

## Not yet emitted (follow-up)

- **JVM runtime** (GC, heap, threads) and **pg connection-pool** (active/idle/pending) metrics —
  planned; runtime metrics via `opentelemetry-runtime-telemetry`, pool metrics via the Vert.x
  metrics SPI. The Postgres engine signals (`pg_stat_wal` fsync, `xact_commit`, dead tuples, locks)
  are collected by the OpenTelemetry Collector's `postgresql` receiver, not by the server.
