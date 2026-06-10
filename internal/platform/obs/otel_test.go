package obs_test

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"

	"github.com/boltrope/boltrope/internal/platform/obs"
)

func TestSetupMetrics_FeedsPrometheusRegistry(t *testing.T) {
	reg := prometheus.NewRegistry()
	mp, shutdown, err := obs.SetupMetrics(reg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(context.Background()) })
	require.NotNil(t, mp)
	// The returned provider must satisfy the global MeterProvider interface so
	// it can be installed via otel.SetMeterProvider.
	var _ metric.MeterProvider = mp

	// Record through an OTel instrument and assert it surfaces via the
	// prometheus exporter registered on reg.
	ctr, err := mp.Meter("test").Int64Counter("widgets_made_total")
	require.NoError(t, err)
	ctr.Add(context.Background(), 7)

	rec := scrapeMetrics(t, reg)
	assert.Contains(t, rec, "widgets_made_total")
	assert.Contains(t, rec, "7")
}

func TestSetupTracing_ReturnsProviderAndShutdown(t *testing.T) {
	tp, shutdown, err := obs.SetupTracing(context.Background(), obs.TracingConfig{
		ServiceName:  "orchestrator",
		OTLPEndpoint: "localhost:4317",
		Insecure:     true,
	})
	require.NoError(t, err)
	require.NotNil(t, tp)

	// A tracer can be obtained without error. We deliberately do NOT export a
	// span here: with WithBatcher pointed at an absent collector, flushing on
	// Shutdown would block until the export timeout, slowing the unit suite.
	// Span creation/propagation without a collector is covered by
	// TestSetupTracing_EmptyEndpointDisabled.
	require.NotNil(t, tp.Tracer("test"))

	// Shutdown must be callable, bounded by ctx, and idempotent. With no spans
	// buffered there is nothing to flush, so it returns promptly.
	require.NoError(t, shutdown(context.Background()))
	require.NoError(t, shutdown(context.Background()))
}

// SetupTracing with an empty endpoint disables export but still returns a
// no-op-ish provider and a usable shutdown, so callers need not special-case it.
func TestSetupTracing_EmptyEndpointDisabled(t *testing.T) {
	tp, shutdown, err := obs.SetupTracing(context.Background(), obs.TracingConfig{
		ServiceName:  "orchestrator",
		OTLPEndpoint: "",
	})
	require.NoError(t, err)
	require.NotNil(t, tp)

	// Spans are still created (and would propagate) with no exporter configured;
	// shutdown returns promptly because there is no batcher to flush.
	_, span := tp.Tracer("test").Start(context.Background(), "unit")
	span.End()
	require.NoError(t, shutdown(context.Background()))
}
