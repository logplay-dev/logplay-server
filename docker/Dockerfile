# Runtime image for the LogPlay Postgres-backed server.
#
# Uses the application-plugin install layout (an unshaded classpath — each dependency jar separate),
# NOT a shaded fat JAR. Flyway discovers its plugins (including the SQL-migration resolver) via
# ServiceLoader files that a normal classpath exposes but shading strips — so the fat JAR can't
# migrate, while this layout migrates on startup exactly like host mode. Build from the repo root:
#   ./gradlew :logplay-server-postgres:installDist
#   docker build -f docker/Dockerfile -t logplay-server:local .
#
# Configuration is via environment variables (see logplay-server-postgres Main.kt):
#   DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD
#   METRICS_ENABLED=true  OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
FROM eclipse-temurin:25-jre

WORKDIR /app
COPY logplay-server-postgres/build/install/logplay-server-postgres/ /app/

EXPOSE 8080
ENTRYPOINT ["/app/bin/logplay-server-postgres"]
