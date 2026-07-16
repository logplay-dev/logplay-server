# LogPlay Server — Configuration

Configuration is environment-variable based. Each backend module (`logplay-server-postgres`,
`logplay-server-h2`) builds a `Config { app: AppConfig, db: <backend>DbConfig }` at startup via
`Config.load()`, passes `AppConfig` to `MainVerticle`'s constructor, and uses the `DbConfig` to set
up the connection pool and run migrations.

Values are parsed **fail-fast** — a malformed value aborts startup with a clear message rather than
silently falling back to a default. Blank values are treated as unset. Parsing is done by the small
injectable `Env` reader (`logplay-server-app`), which is also what makes the config unit-testable.

## App settings (both backends) — `AppConfig`

| Env var | Default | Meaning |
|---|---|---|
| `HTTP_HOST` | `0.0.0.0` | Bind address |
| `HTTP_PORT` | `8080` | HTTP port (`0` = pick a free port) |
| `MAX_BODY_BYTES` | `-1` | Max request body size in bytes (`-1` = unlimited) |
| `CLEANUP_INTERVAL_MS` | `30000` | Dead-worker cleanup sweep interval |
| `METRICS_ENABLED` | `false` | Enable OpenTelemetry metrics |
| `METRICS_SERVICE_NAME` | `logplay-server` | OTel `service.name` |
| `METRICS_OTLP_ENDPOINT` | (SDK default) | OTLP endpoint metrics are exported to |

When metrics are enabled, standard OpenTelemetry SDK autoconfigure env vars still apply for
transport details the app doesn't own (e.g. `OTEL_EXPORTER_OTLP_PROTOCOL`,
`OTEL_METRIC_EXPORT_INTERVAL`). See `docs/METRICS.md` for the metric catalog.

## Postgres backend — `PostgresDbConfig`

| Env var | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `5432` |
| `DB_NAME` | `logplay` |
| `DB_USER` | `logplay` |
| `DB_PASSWORD` | (empty) |
| `DB_POOL_MAX_SIZE` | `32` |
| `DB_CACHE_PREPARED_STATEMENTS` | `true` |
| `DB_PREPARED_STATEMENT_CACHE_MAX_SIZE` | `256` |
| `DB_PIPELINING_LIMIT` | `256` |

## H2 backend — `H2DbConfig`

| Env var | Default |
|---|---|
| `H2_URL` | `jdbc:h2:file:./logplay-data;AUTO_SERVER=TRUE` |
| `H2_USER` | `sa` |
| `H2_PASSWORD` | (empty) |
| `H2_POOL_MAX_SIZE` | `H2UnitOfWork.RECOMMENDED_JDBC_POOL_SIZE` |
