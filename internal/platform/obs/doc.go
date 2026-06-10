// Package obs is the observability bootstrap shared by every Boltrope service and
// projectord. It wires the three pillars the architecture mandates (architecture
// §9, FR-OBS-01..05):
//
//   - Structured logging — a [log/slog] JSON logger ([NewLogger]) whose handler
//     redacts [github.com/boltrope/boltrope/internal/platform/secret.Secret]
//     values (via slog's LogValuer) and injects the active OTel span's trace_id
//     and span_id into every record so logs correlate to traces (FR-OBS-03,
//     NFR-OBS-02).
//   - Tracing — an OpenTelemetry [go.opentelemetry.io/otel/sdk/trace.TracerProvider]
//     exporting OTLP/gRPC to a configurable collector ([SetupTracing]); spans
//     follow the OTel GenAI conventions emitted by the services (FR-OBS-01,
//     NFR-OBS-01).
//   - Metrics — an OpenTelemetry [go.opentelemetry.io/otel/sdk/metric.MeterProvider]
//     fed into a Prometheus [github.com/prometheus/client_golang/prometheus.Registry]
//     ([SetupMetrics]), plus the RED/USE instrument set ([Metrics]) and a
//     promhttp handler for /metrics ([MetricsHandler]) (FR-OBS-02).
//
// # Configuration boundary
//
// This package takes plain parameters (a [slog.Level], a [TracingConfig], a
// *prometheus.Registry); it deliberately does NOT import the config package, so
// it has no opinion on precedence or sources and stays usable from tests and from
// any service's wiring. Callers resolve configuration elsewhere and pass values
// in.
//
// # Determinism
//
// obs is platform wiring, not domain/app logic, so it is outside the determinism
// rule's forbidigo scope and uses the standard library time/uuid transitively
// through the OTel SDK. Domain/app code never imports obs for time; it injects
// clock.Clock (NFR-TEST-01).
package obs
