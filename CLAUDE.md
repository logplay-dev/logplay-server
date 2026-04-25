# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
# Run tests
./gradlew clean test

# Build fat JARs (per backend)
./gradlew :logplay-server-h2:shadowJar       # -> logplay-server-h2/build/libs/logplay-server-h2-0.0.1-fat.jar
./gradlew :logplay-server-postgres:shadowJar  # -> logplay-server-postgres/build/libs/logplay-server-postgres-0.0.1-fat.jar

# Run the application (pick one backend)
./gradlew :logplay-server-h2:run
./gradlew :logplay-server-postgres:run

# Check/apply code formatting (Spotless with ktfmt)
./gradlew spotlessCheck
./gradlew spotlessApply
```

## Architecture

This is a **Vert.x 5 + Kotlin** durable execution server using **clean architecture** across four Gradle modules:

- **`logplay-server-domain`** — Pure domain logic (no framework dependencies). Contains domain models, use case interfaces and implementations, gateway interfaces, and exceptions.
- **`logplay-server-app`** — Infrastructure and HTTP layer. Contains Vert.x verticles, HTTP controllers, DTOs, and use case wiring. Depends on `logplay-server-domain`.
- **`logplay-server-h2`** — H2 database backend (dev/testing). Implements `JobGateway` and `WorkerGateway`.
- **`logplay-server-postgres`** — PostgreSQL database backend (production). Implements `JobGateway` and `WorkerGateway`.

### Domain Model

The domain is organized by feature (screaming architecture) under `logplay-server-domain/src/main/kotlin/org/zeplinko/logplay/server/core/`:

**`job/`** — Job and checkpoint lifecycle. Jobs follow: `PENDING → ACQUIRED → FINISHED | FAILED | ABORTED`
- `Models.kt` — `Job`, `Checkpoint`, and job command data classes
- `Enums.kt` — `JobStatus` enum
- `Ports.kt` — `JobGateway` interface
- `UseCase.kt` — Job use case interfaces
- `Exception.kt` — `JobException` base + job-specific exceptions
- `impl/` — Job use case implementations

**`worker/`** — Worker registration, heartbeat, and lifecycle
- `Models.kt` — `Worker` and worker command data classes
- `Ports.kt` — `WorkerGateway` interface
- `UseCase.kt` — Worker use case interfaces
- `Exception.kt` — `WorkerException` base + worker-specific exceptions
- `impl/` — Worker use case implementations

### App Layer

Located in `logplay-server-app/src/main/kotlin/org/zeplinko/logplay/server/`:
- `MainVerticle.kt` — Entry point, HTTP server on configurable port (default 8080)
- `job/web/JobController.kt` — Job HTTP endpoint handlers
- `job/web/Dtos.kt` — Job request/response DTOs
- `job/use/case/JobUseCaseLookUp.kt` — Job use case dependency factory (manual DI)
- `worker/web/WorkerController.kt` — Worker HTTP endpoint handlers
- `worker/web/Dtos.kt` — Worker request/response DTOs
- `worker/use/case/WorkerUseCaseLookUp.kt` — Worker use case dependency factory

### Key Tech

- **Vert.x 5.0.6** with Kotlin coroutines (`vertx-lang-kotlin-coroutines`) — all async ops use `suspend` functions
- **H2** (dev/test) and **PostgreSQL** via `vertx-pg-client` (production, async, non-blocking) — pluggable via `JobGateway`/`WorkerGateway`
- **Shadow JAR** (`com.gradleup.shadow`) for fat JAR packaging
- **Spotless + ktfmt** enforces `kotlinlangStyle()` formatting — run `spotlessApply` before committing
- **JUnit Jupiter 5** for tests; unit tests in `logplay-server-domain`, integration tests in `logplay-server-h2` and `logplay-server-postgres`