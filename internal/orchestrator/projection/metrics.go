package projection

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/xd1lab/harness-ai/internal/platform/obs"
)

// meterName is the OTel instrumentation scope for projectord's metrics. It names
// this package so the cost instrument is attributable to the projection worker in
// the exported scope.
const meterName = "github.com/xd1lab/harness-ai/internal/orchestrator/projection"

// otelMetrics is the production [MetricSink]: it sets the projection-lag
// USE gauge on the shared [obs.Metrics] (so /metrics exposes projection_lag_events
// alongside the rest of the RED/USE set) and adds folded cost to an OTel
// Float64Counter (bridged into the same Prometheus registry by
// [obs.SetupMetrics]). This is the wiring edge where the OTel/obs dependency
// lives; the [Runner] depends only on the [MetricSink] interface.
type otelMetrics struct {
	red  *obs.Metrics
	cost metric.Float64Counter
}

// NewOTelMetrics builds the production metric sink. It reads the global OTel meter
// provider (set by [obs.SetupMetrics] during wiring) for the cost counter and
// uses red for the projection-lag gauge. It returns an error only if the cost
// instrument cannot be created.
//
// The cost counter is named gen_ai.cost.usd and carries a tenant attribute so the
// running cost is attributable per tenant; it is monotonic, fed the per-batch
// delta by the runner (architecture §11.6, FR-OBS-01).
func NewOTelMetrics(red *obs.Metrics) (MetricSink, error) {
	meter := otel.GetMeterProvider().Meter(meterName)
	cost, err := meter.Float64Counter(
		"gen_ai.cost.usd",
		metric.WithDescription("Cumulative model cost in USD folded from TurnFinished/TurnAborted events, attributed by tenant (FR-OBS-01)."),
		metric.WithUnit("{USD}"),
	)
	if err != nil {
		return nil, fmt.Errorf("projection: creating cost counter: %w", err)
	}
	return &otelMetrics{red: red, cost: cost}, nil
}

// SetProjectionLag publishes the lag to the shared RED/USE gauge.
func (m *otelMetrics) SetProjectionLag(events int64) {
	if m.red != nil {
		m.red.SetProjectionLag(events)
	}
}

// AddCost adds a per-tenant cost delta (USD) to the monotonic cost counter.
func (m *otelMetrics) AddCost(ctx context.Context, tenantID string, deltaUSD float64) {
	if m.cost == nil || deltaUSD == 0 {
		return
	}
	m.cost.Add(ctx, deltaUSD, metric.WithAttributes(attribute.String("tenant_id", tenantID)))
}

// Compile-time assertion that otelMetrics satisfies MetricSink.
var _ MetricSink = (*otelMetrics)(nil)
