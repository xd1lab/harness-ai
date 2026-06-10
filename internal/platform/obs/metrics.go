package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the RED + USE instrument set every service exposes (FR-OBS-02). It
// owns Prometheus collectors registered on a caller-provided
// [prometheus.Registerer] so the same registry can also receive the OTel→
// Prometheus bridge from [SetupMetrics] and be served by [MetricsHandler].
//
//   - RED (per RPC): a request counter, an error counter broken down by typed
//     termination subtype, and a duration histogram.
//   - Operational: a doom-loop detection counter labeled by tool (FR-OBS-04).
//   - USE (saturation gauges): errgroup worker-pool occupancy, live sandbox
//     count, PostgreSQL connection-pool utilization, and blob-store usage in
//     bytes (FR-OBS-02; gauges relevant to a given service are set, the rest stay
//     zero).
//
// Metric methods are safe for concurrent use (Prometheus collectors are). Construct
// one with [NewMetrics] during wiring.
type Metrics struct {
	requests      *prometheus.CounterVec
	errors        *prometheus.CounterVec
	duration      *prometheus.HistogramVec
	doomLoops     *prometheus.CounterVec
	liveSandbox   prometheus.Gauge
	workerPool    prometheus.Gauge
	dbPoolInUse   prometheus.Gauge
	blobBytes     prometheus.Gauge
	projectionLag prometheus.Gauge
}

// NewMetrics constructs the RED/USE instrument set and registers it on reg.
//
// Each Boltrope service runs in its own process with its own registry and
// /metrics endpoint, so the service identity is the Prometheus scrape target, not
// a metric label; metric names therefore stay label-clean (e.g.
// run_errors_total{rpc,subtype}) exactly as the FR-OBS-02 acceptance criteria
// assert. The service argument is recorded only in the metrics' help text for
// operator clarity. NewMetrics panics if registration fails (a duplicate
// registration is a programming error caught at startup, never in steady state).
func NewMetrics(reg prometheus.Registerer, service string) *Metrics {
	help := func(s string) string { return s + " [service=" + service + "]" }
	factory := promauto(reg)

	m := &Metrics{
		requests: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "run_requests_total",
			Help: help("Total RPC requests handled, labeled by RPC method (RED: rate)."),
		}, []string{"rpc"}),
		errors: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "run_errors_total",
			Help: help("Total RPC errors, labeled by RPC method and typed termination subtype (RED: errors)."),
		}, []string{"rpc", "subtype"}),
		duration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "run_request_duration_seconds",
			Help:    help("RPC handling duration in seconds, labeled by RPC method (RED: duration)."),
			Buckets: prometheus.DefBuckets,
		}, []string{"rpc"}),
		doomLoops: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "doom_loop_detected_total",
			Help: help("Stuck-loop detections (repeated identical tool calls), labeled by tool (FR-OBS-04)."),
		}, []string{"tool"}),
		liveSandbox: factory.NewGauge(prometheus.GaugeOpts{
			Name: "live_sandboxes",
			Help: help("Number of live sandboxes on this node (USE: saturation)."),
		}),
		workerPool: factory.NewGauge(prometheus.GaugeOpts{
			Name: "worker_pool_occupancy",
			Help: help("In-flight tasks occupying the errgroup worker pool (USE: saturation)."),
		}),
		dbPoolInUse: factory.NewGauge(prometheus.GaugeOpts{
			Name: "db_pool_connections_in_use",
			Help: help("PostgreSQL connection-pool connections currently in use (USE: saturation)."),
		}),
		blobBytes: factory.NewGauge(prometheus.GaugeOpts{
			Name: "blob_store_bytes",
			Help: help("Total bytes stored in the blob store (USE: utilization)."),
		}),
		projectionLag: factory.NewGauge(prometheus.GaugeOpts{
			Name: "projection_lag_events",
			Help: help("projectord projection lag in unprocessed events (USE: saturation)."),
		}),
	}
	return m
}

// RecordRequest increments the request counter for the given RPC method.
func (m *Metrics) RecordRequest(rpc string) {
	m.requests.WithLabelValues(rpc).Inc()
}

// RecordError increments the error counter for the given RPC method and typed
// termination subtype (e.g. "error_max_turns", "error_max_budget_usd",
// "error_during_execution"). The subtype is the label FR-OBS-02 AC-2 asserts.
func (m *Metrics) RecordError(rpc, subtype string) {
	m.errors.WithLabelValues(rpc, subtype).Inc()
}

// ObserveRequestDuration records a handled-request duration (in seconds) for the
// given RPC method into the duration histogram.
func (m *Metrics) ObserveRequestDuration(rpc string, seconds float64) {
	m.duration.WithLabelValues(rpc).Observe(seconds)
}

// RecordDoomLoop increments the stuck-loop detection counter for the given tool
// name (FR-OBS-04).
func (m *Metrics) RecordDoomLoop(tool string) {
	m.doomLoops.WithLabelValues(tool).Inc()
}

// SetLiveSandboxes sets the live-sandbox saturation gauge.
func (m *Metrics) SetLiveSandboxes(n int) { m.liveSandbox.Set(float64(n)) }

// SetWorkerPoolOccupancy sets the worker-pool occupancy saturation gauge.
func (m *Metrics) SetWorkerPoolOccupancy(n int) { m.workerPool.Set(float64(n)) }

// SetDBPoolInUse sets the PostgreSQL connection-pool utilization gauge.
func (m *Metrics) SetDBPoolInUse(n int) { m.dbPoolInUse.Set(float64(n)) }

// SetBlobStoreBytes sets the blob-store utilization gauge (bytes).
func (m *Metrics) SetBlobStoreBytes(bytes int64) { m.blobBytes.Set(float64(bytes)) }

// SetProjectionLag sets the projectord projection-lag saturation gauge (events).
func (m *Metrics) SetProjectionLag(events int64) { m.projectionLag.Set(float64(events)) }

// MetricsHandler returns an [http.Handler] that serves g in the Prometheus text
// exposition format, suitable for mounting at /metrics. Pass the same registry
// given to [NewMetrics] and [SetupMetrics] so application RED/USE metrics and the
// OTel-bridged metrics are exposed together.
func MetricsHandler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// promauto returns a tiny factory that registers each constructed collector on
// reg, panicking on registration error. It avoids pulling the promauto package's
// global default-registry behavior while keeping construction terse.
func promauto(reg prometheus.Registerer) metricFactory { return metricFactory{reg: reg} }

// metricFactory constructs and registers Prometheus collectors on a fixed
// registerer.
type metricFactory struct{ reg prometheus.Registerer }

func (f metricFactory) NewCounterVec(opts prometheus.CounterOpts, labels []string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(opts, labels)
	f.reg.MustRegister(c)
	return c
}

func (f metricFactory) NewHistogramVec(opts prometheus.HistogramOpts, labels []string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(opts, labels)
	f.reg.MustRegister(h)
	return h
}

func (f metricFactory) NewGauge(opts prometheus.GaugeOpts) prometheus.Gauge {
	g := prometheus.NewGauge(opts)
	f.reg.MustRegister(g)
	return g
}
