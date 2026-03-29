# LogPlay Server

LogPlay is a **durable execution platform**. It lets you write long-running, fault-tolerant workflows in plain code — the platform ensures they run to completion even across process restarts, network failures, or arbitrary delays.

---

## How it works

LogPlay separates *orchestration* (the server) from *execution* (the client SDK):

```
┌──────────────────────────────────────────────────────────────┐
│                        LogPlay Server                        │
│                                                              │
│   Job Registry   ──►   Job Queue   ──►   Checkpoints        │
└───────┬──────────────────────┬──────────────────┬───────────┘
        │ GET /jobs/pending    │ POST             │ POST
        │                      │ /jobs/{id}/      │ /jobs/{id}/
        │                      │ checkpoints      │ complete
        │                      │                  │
┌───────▼──────────────────────┴──────────────────┴───────────┐
│                        Client SDK                            │
│                                                              │
│   1. Poll for pending jobs                                   │
│   2. Execute the job's business logic                        │
│   3. Save checkpoints as execution progresses                │
│   4. Notify the server when the job is done                  │
└──────────────────────────────────────────────────────────────┘
```

### Server responsibilities

- Stores and manages jobs and their checkpoints
- Exposes an HTTP API that the client SDK calls
- Owns the job status state machine

### Client SDK responsibilities

- Polls the server for `PENDING` jobs
- Runs the business logic associated with each job type
- Saves checkpoints as execution progresses — each checkpoint carries a serialized **byte array payload** and a **class type string** so the SDK can deserialize it back into the correct type when resuming
- Calls the server to mark the job complete when execution finishes

---

## Job lifecycle

```
PENDING ──► FINISHED
       └──► FAILED
       └──► ABORTED
```

| Status     | Meaning |
|------------|---------|
| `PENDING`  | Job is registered and awaiting completion — whether a client has started on it is tracked via checkpoints, not status |
| `FINISHED` | Client explicitly marked the job as complete |
| `FAILED`   | Execution failed after exhausting retries |
| `ABORTED`  | Execution was cancelled before completion |

---

## Checkpoints

A checkpoint is a durable snapshot of execution progress saved by the client SDK. Each checkpoint stores:

| Field | Required | Description |
|-------|----------|-------------|
| `jobId` | yes | The job this checkpoint belongs to |
| `classType` | yes | Fully-qualified class name of the payload — used by the client SDK to deserialize `data` back into the correct type on resume |
| `data` | yes | Serialized payload (`ByteArray`) |
| `description` | no | Optional human-readable step name (e.g. `"payment-processed"`) |
| `createdAt` | yes | When the checkpoint was saved |

If a client process dies mid-execution, the next client to pick up the job resumes from the last saved checkpoint by deserializing `data` using `classType`.

---

## Module structure

```
logplay-server/
├── logplay-server-domain/   ← pure domain logic, zero framework dependencies
│   └── src/main/kotlin/dev/logplay/server/core/job/
│       ├── Enums.kt         ← JobStatus
│       ├── Models.kt        ← Job, Checkpoint, command records
│       ├── Ports.kt         ← JobGateway interface
│       ├── UseCase.kt       ← use case interfaces
│       ├── Exception.kt     ← domain exceptions
│       └── impl/            ← use case implementations
├── logplay-server-app/      ← Vert.x HTTP server, in-memory gateway, wiring
└── logplay-server-*-db/     ← (planned) one module per database backend
```

The domain module has **zero framework dependencies** — no Vert.x, no database drivers. This is enforced at build time: framework imports in the domain are a compile error, not a code review comment.

Persistence is pluggable: `logplay-server-app` ships with an in-memory `JobGateway` implementation. Production database backends will live in their own modules (`logplay-server-postgres-db`, etc.) and can be swapped in without touching the domain or app layers.

---

## Tech stack

| Concern | Technology |
|---------|------------|
| HTTP server | [Vert.x 5](https://vertx.io) with Kotlin coroutines |
| Persistence | In-memory (default) — pluggable by module; separate persistence modules will be added per database backend |
| Build | Gradle with Kotlin DSL |
| Code style | [ktfmt](https://github.com/facebook/ktfmt) via Spotless |
| Tests | JUnit Jupiter 5 + AssertJ |

---

## Development

```bash
# Run tests
./gradlew clean test

# Build fat JAR
./gradlew clean assemble

# Run the server (port 8888)
./gradlew clean run

# Apply code formatting
./gradlew spotlessApply
```