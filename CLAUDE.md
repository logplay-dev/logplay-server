# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Development Commands

```bash
# Run tests
./gradlew clean test

# Build fat JAR (output: logplay-server-app/build/libs/logplay-server-app-0.0.1-fat.jar)
./gradlew clean assemble

# Run the application
./gradlew clean run

# Check/apply code formatting (Spotless with ktfmt)
./gradlew spotlessCheck
./gradlew spotlessApply
```

## Architecture

This is a **Vert.x 5 + Kotlin** job management server using **clean architecture** across two Gradle modules:

- **`logplay-server-core`** — Pure domain logic (no framework dependencies). Contains domain models, use case interfaces and implementations, the `JobManager` interface, and exceptions.
- **`logplay-server-app`** — Infrastructure and HTTP layer. Contains Vert.x verticles, HTTP controllers, the `JobManager` implementation, and the persistence repository. Depends on `logplay-server-core`.

### Domain Model

Jobs follow this status flow: `QUEUED → RUNNING → FINISHED | FAILED | ABORTED`

Key entities in `logplay-server-core/src/main/kotlin/dev/logplay/server/core/job/`:
- `Domain.kt` — `Job` and `Checkpoint` data classes
- `Manager.kt` — `JobManager` interface (insert, update, getPending)
- `UseCase.kt` / `UseCaseDtos.kt` — Use case interfaces and command DTOs
- `Exception.kt` — Domain exceptions (`BlankJobTypeException`, `JobAlreadyExistsException`)
- `impl/` — Use case implementations

### App Layer

Located in `logplay-server-app/src/main/kotlin/dev/logplay/server/`:
- `Main.kt` + `MainVerticle.kt` — Entry point, HTTP server on port 8888
- `job/web/JobController.kt` — HTTP endpoint handlers
- `job/managers/JobManagerImpl.kt` — `JobManager` implementation (wires to repository)
- `job/persistence/JobRepository.kt` — Data persistence
- `job/use/case/JobUseCaseLookUp.kt` — Use case dependency factory (manual DI)

### Key Tech

- **Vert.x 5.0.6** with Kotlin coroutines (`vertx-lang-kotlin-coroutines`) — all async ops use `suspend` functions
- **PostgreSQL** via `vertx-pg-client` (async, non-blocking)
- **Shadow JAR** (`com.gradleup.shadow`) for fat JAR packaging
- **Spotless + ktfmt** enforces `kotlinlangStyle()` formatting — run `spotlessApply` before committing
- **JUnit Jupiter 5** for tests; module tests are in `src/test/kotlin/`