package obs

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ShutdownFunc releases the resources held by a bootstrapped provider, flushing
// any buffered telemetry. It is bounded by the passed context and is safe to call
// more than once (subsequent calls are no-ops). Callers should defer it and pass
// a short, separate shutdown context.
type ShutdownFunc func(context.Context) error

// TracingConfig parameterizes [SetupTracing]. It is a plain value passed by the
// caller's wiring; obs does not read configuration itself (see the package doc).
type TracingConfig struct {
	// ServiceName populates the OTel service.name resource attribute on every
	// span (NFR-OBS-01). Required when an endpoint is set.
	ServiceName string
	// ServiceVersion populates service.version when non-empty. Optional.
	ServiceVersion string
	// OTLPEndpoint is the OTLP/gRPC collector endpoint (host:port). When empty,
	// tracing export is disabled: a resource-only TracerProvider is returned so
	// spans are still created (and propagated) but never exported, and callers
	// need not special-case the no-collector deployment.
	OTLPEndpoint string
	// Insecure disables transport credentials for the OTLP/gRPC connection (for a
	// local/in-cluster collector reached over plaintext). Production wiring leaves
	// this false and supplies mTLS via the collector's network path.
	Insecure bool
}

// SetupMetrics wires an OpenTelemetry [sdkmetric.MeterProvider] whose metrics are
// exported into the Prometheus registry reg via the OTel→Prometheus bridge, sets
// it as the global meter provider, and returns it with a [ShutdownFunc].
//
// Application RED/USE metrics ([NewMetrics]) register on the same reg, so a single
// [MetricsHandler] over reg exposes both the OTel-instrumented metrics (e.g. the
// gateway's gen_ai usage) and the hand-rolled RED/USE set in one /metrics scrape
// (FR-OBS-02).
func SetupMetrics(reg prometheus.Registerer) (*sdkmetric.MeterProvider, ShutdownFunc, error) {
	exporter, err := promexporter.New(promexporter.WithRegisterer(reg))
	if err != nil {
		return nil, nil, fmt.Errorf("obs: new prometheus exporter: %w", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	otel.SetMeterProvider(mp)
	return mp, once(mp.Shutdown), nil
}

// SetupTracing wires an OpenTelemetry [sdktrace.TracerProvider] exporting spans
// over OTLP/gRPC to cfg.OTLPEndpoint, sets it as the global tracer provider, sets
// the global propagator to W3C TraceContext+Baggage (so trace context rides gRPC
// metadata, FR-OBS-01), and returns the provider with a [ShutdownFunc].
//
// When cfg.OTLPEndpoint is empty, export is disabled and a resource-only provider
// is returned (spans are created and propagated but not exported). The returned
// shutdown flushes the batch processor and is safe to call repeatedly.
func SetupTracing(ctx context.Context, cfg TracingConfig) (*sdktrace.TracerProvider, ShutdownFunc, error) {
	res, err := buildResource(ctx, cfg)
	if err != nil {
		return nil, nil, err
	}

	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if cfg.OTLPEndpoint != "" {
		grpcOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.OTLPEndpoint)}
		if cfg.Insecure {
			grpcOpts = append(grpcOpts, otlptracegrpc.WithInsecure())
		}
		exporter, expErr := otlptracegrpc.New(ctx, grpcOpts...)
		if expErr != nil {
			return nil, nil, fmt.Errorf("obs: new otlp trace exporter: %w", expErr)
		}
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp, once(tp.Shutdown), nil
}

// buildResource assembles the OTel resource (service.name/version) merged onto
// the SDK defaults.
func buildResource(ctx context.Context, cfg TracingConfig) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{}
	if cfg.ServiceName != "" {
		attrs = append(attrs, semconv.ServiceName(cfg.ServiceName))
	}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}
	res, err := resource.New(ctx, resource.WithAttributes(attrs...))
	if err != nil {
		return nil, fmt.Errorf("obs: build resource: %w", err)
	}
	merged, err := resource.Merge(resource.Default(), res)
	if err != nil {
		return nil, fmt.Errorf("obs: merge resource: %w", err)
	}
	return merged, nil
}

// once wraps a shutdown so it runs at most once; later calls return the first
// call's error. This makes ShutdownFunc safe to defer and also call explicitly.
func once(fn func(context.Context) error) ShutdownFunc {
	var (
		done bool
		err  error
	)
	return func(ctx context.Context) error {
		if done {
			return err
		}
		done = true
		err = fn(ctx)
		return err
	}
}
