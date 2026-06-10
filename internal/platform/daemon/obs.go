// Package daemon is the shared process-lifecycle harness every Boltrope server
// binary (cmd/boltrope-orchestratord, -modelgwd, -toolruntimed, -projectord)
// composes so each cmd main stays thin (T-CMD-01, T-CMD-02; architecture §5.1
// infra/server, §10). It wires the cross-cutting concerns identically across
// services:
//
//   - observability bootstrap (structured slog JSON + OTel tracer/meter) with a
//     single combined [Telemetry.Shutdown] that flushes on exit (FR-OBS-01/03);
//   - gRPC transport credentials selection — SPIFFE mTLS in production, the
//     fail-closed BOLTROPE_DEV_INSECURE static-cert fallback otherwise
//     (NFR-SEC-01; architecture §8.1);
//   - the gRPC server (with the platform interceptor chain + a grpc.health.v1
//     service) and an HTTP server exposing /livez, /readyz, and /metrics
//     (FR-OBS-02, FR-OBS-05);
//   - readiness that gates on dependency reachability + SVID presence and flips
//     the gRPC health SERVING status only once ready (FR-OBS-05);
//   - graceful shutdown on SIGINT/SIGTERM that drains in-flight RPCs, runs
//     caller-registered closers (pools, sources), and flushes telemetry
//     (NFR-OPS-04; architecture §10.1).
//
// It deliberately holds no service-specific knowledge: each daemon's wiring file
// constructs its adapters + inbound gRPC server and hands them to [Run] via a
// [Service]. The package depends only on the platform packages
// ([github.com/boltrope/boltrope/internal/platform/obs],
// [github.com/boltrope/boltrope/internal/platform/grpcx],
// [github.com/boltrope/boltrope/internal/platform/config]) and gRPC, never on a
// service's app/domain, so it cannot violate the service-isolation boundary
// (DOD-08; architecture §12.4).
package daemon

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/boltrope/boltrope/internal/platform/config"
	"github.com/boltrope/boltrope/internal/platform/obs"
)

// Telemetry bundles the bootstrapped observability primitives a daemon shares: a
// trace-correlating JSON [*slog.Logger], the RED/USE [*obs.Metrics] instrument
// set, the Prometheus registry both they and the OTel→Prometheus bridge register
// on, and a single [Shutdown] that flushes and releases the tracer/meter
// providers. It is constructed by [SetupTelemetry] during wiring.
type Telemetry struct {
	// Logger is the process logger (JSON to stderr, trace_id/span_id injected
	// from the active span). It is also installed as the slog default.
	Logger *slog.Logger
	// Metrics is the RED/USE instrument set, registered on Registry.
	Metrics *obs.Metrics
	// Registry is the Prometheus registry exposed at /metrics; both Metrics and
	// the OTel→Prometheus bridge register on it.
	Registry *prometheus.Registry
	// Shutdown flushes and releases the tracer + meter providers. It is bounded
	// by its context, safe to call once (further calls are no-ops), and is run by
	// [Run] on shutdown after the servers drain.
	Shutdown obs.ShutdownFunc
}

// SetupTelemetry bootstraps observability for a service named service: it parses
// the configured log level (fail-fast on an invalid name), builds the
// trace-correlating JSON logger over w and installs it as the slog default,
// creates a fresh Prometheus registry, wires the OTel→Prometheus meter bridge and
// the OTLP tracer (export disabled when cfg.OTLP.Endpoint is empty), and
// registers the RED/USE metrics. The returned [Telemetry.Shutdown] flushes both
// providers; the caller defers it (see [Run]).
//
// w is the log sink (production passes os.Stderr); it is a parameter so a test
// can capture output. version is the optional service version stamped on the
// OTel resource.
func SetupTelemetry(ctx context.Context, service, version string, cfg *config.Config, w io.Writer) (*Telemetry, error) {
	level, err := obs.ParseLevel(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("daemon: %w", err)
	}
	logger := obs.NewLogger(w, level)
	slog.SetDefault(logger)

	reg := prometheus.NewRegistry()

	_, metricShutdown, err := obs.SetupMetrics(reg)
	if err != nil {
		return nil, fmt.Errorf("daemon: setup metrics: %w", err)
	}

	_, traceShutdown, err := obs.SetupTracing(ctx, obs.TracingConfig{
		ServiceName:    service,
		ServiceVersion: version,
		OTLPEndpoint:   cfg.OTLP.Endpoint,
		Insecure:       cfg.OTLP.Insecure,
	})
	if err != nil {
		// Release the already-built meter provider before failing so a partial
		// bootstrap never leaks a background reader.
		_ = metricShutdown(ctx)
		return nil, fmt.Errorf("daemon: setup tracing: %w", err)
	}

	metrics := obs.NewMetrics(reg, service)

	return &Telemetry{
		Logger:   logger,
		Metrics:  metrics,
		Registry: reg,
		Shutdown: combineShutdown(traceShutdown, metricShutdown),
	}, nil
}

// combineShutdown returns a [obs.ShutdownFunc] that runs each provided shutdown
// in order, joining their errors, and runs at most once. Tracing is flushed
// before metrics so a final span export is recorded before the meter reader is
// torn down.
func combineShutdown(fns ...obs.ShutdownFunc) obs.ShutdownFunc {
	var done bool
	return func(ctx context.Context) error {
		if done {
			return nil
		}
		done = true
		var firstErr error
		for _, fn := range fns {
			if fn == nil {
				continue
			}
			if err := fn(ctx); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
}
