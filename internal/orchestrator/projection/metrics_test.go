package projection

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/xd1lab/harness-ai/internal/platform/obs"
)

// swapMeterProvider installs mp as the global OTel meter provider for the test
// and restores the previous one on cleanup. NewOTelMetrics reads the global
// (that is its wiring contract), so the tests must own it for their duration;
// none of this package's tests run in parallel.
func swapMeterProvider(t *testing.T, mp metric.MeterProvider) {
	t.Helper()
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })
}

// costSumsByTenant collects gen_ai.cost.usd from the manual reader and returns
// the per-tenant_id sums, asserting the counter is monotonic on the way.
func costSumsByTenant(t *testing.T, reader *sdkmetric.ManualReader) map[string]float64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		if sm.Scope.Name != meterName {
			continue
		}
		for _, m := range sm.Metrics {
			if m.Name != "gen_ai.cost.usd" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[float64])
			if !ok {
				t.Fatalf("gen_ai.cost.usd data is %T, want Sum[float64]", m.Data)
			}
			if !sum.IsMonotonic {
				t.Fatal("gen_ai.cost.usd is not monotonic; the runner feeds deltas to a monotonic counter")
			}
			for _, dp := range sum.DataPoints {
				tenant, ok := dp.Attributes.Value(attribute.Key("tenant_id"))
				if !ok {
					t.Fatalf("cost data point missing the tenant_id attribute: %v", dp.Attributes.ToSlice())
				}
				out[tenant.AsString()] += dp.Value
			}
		}
	}
	return out
}

// TestNewOTelMetrics_PublishesCostAndLag drives the production sink against an
// in-memory OTel reader and a fresh Prometheus registry: per-tenant cost deltas
// accumulate on the monotonic counter under the tenant_id attribute, a zero
// delta is dropped, and the lag lands on the shared projection_lag_events
// gauge (FR-OBS-01/FR-OBS-02).
func TestNewOTelMetrics_PublishesCostAndLag(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	swapMeterProvider(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	reg := prometheus.NewRegistry()
	red := obs.NewMetrics(reg, "projection-test")
	sink, err := NewOTelMetrics(red)
	if err != nil {
		t.Fatalf("NewOTelMetrics: %v", err)
	}

	ctx := context.Background()
	sink.SetProjectionLag(42)
	sink.AddCost(ctx, "tenant-a", 0.25)
	sink.AddCost(ctx, "tenant-a", 0.50)
	sink.AddCost(ctx, "tenant-b", 1.00)
	sink.AddCost(ctx, "tenant-zero", 0) // zero delta: dropped, never a data point

	sums := costSumsByTenant(t, reader)
	if !floatEq(sums["tenant-a"], 0.75) || !floatEq(sums["tenant-b"], 1.00) {
		t.Fatalf("cost sums = %v, want tenant-a 0.75 / tenant-b 1.00", sums)
	}
	if len(sums) != 2 {
		t.Fatalf("got %d tenant series %v, want 2 (the zero delta must not create a series)", len(sums), sums)
	}

	// The lag gauge is exposed on the shared registry as projection_lag_events.
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	found := false
	for _, mf := range families {
		if mf.GetName() != "projection_lag_events" {
			continue
		}
		found = true
		if got := mf.GetMetric()[0].GetGauge().GetValue(); got != 42 {
			t.Fatalf("projection_lag_events = %v, want 42", got)
		}
	}
	if !found {
		t.Fatal("projection_lag_events not found in the registry")
	}
}

// TestOTelMetrics_NilGuards pins the sink's tolerance contract: a nil RED set
// makes SetProjectionLag a no-op, and a nil counter makes AddCost a no-op —
// a minimal deployment without metrics must not panic the worker.
func TestOTelMetrics_NilGuards(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	swapMeterProvider(t, sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))

	sink, err := NewOTelMetrics(nil)
	if err != nil {
		t.Fatalf("NewOTelMetrics(nil): %v", err)
	}
	sink.SetProjectionLag(7) // must not panic without a RED set

	var bare otelMetrics
	bare.AddCost(context.Background(), "t", 1.0) // must not panic without a counter
	bare.SetProjectionLag(1)                     // must not panic either
}

// erroringMeterProvider returns a meter whose Float64Counter constructor fails,
// to reach NewOTelMetrics' only error branch.
type erroringMeterProvider struct{ noop.MeterProvider }

func (erroringMeterProvider) Meter(string, ...metric.MeterOption) metric.Meter {
	return erroringMeter{}
}

type erroringMeter struct{ noop.Meter }

func (erroringMeter) Float64Counter(string, ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	return nil, errors.New("counter construction blew up")
}

// TestNewOTelMetrics_CounterError asserts a failed instrument construction is
// wrapped and surfaced (the wiring edge must fail loudly, not return a sink
// that silently drops cost).
func TestNewOTelMetrics_CounterError(t *testing.T) {
	swapMeterProvider(t, erroringMeterProvider{})
	if _, err := NewOTelMetrics(nil); err == nil || !strings.Contains(err.Error(), "creating cost counter") {
		t.Fatalf("NewOTelMetrics = %v, want wrapped creating-cost-counter error", err)
	}
}
