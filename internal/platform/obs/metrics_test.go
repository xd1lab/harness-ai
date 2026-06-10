package obs_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/obs"
)

func TestMetrics_REDCounterIncrementsAndRenders(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg, "orchestrator")

	// One request, one error of subtype error_max_turns.
	m.RecordRequest("Run")
	m.RecordError("Run", "error_max_turns")

	body := scrapeMetrics(t, reg)

	// FR-OBS-02 AC-2: run_errors_total broken down by termination subtype.
	assert.Contains(t, body, `run_errors_total{rpc="Run",subtype="error_max_turns"} 1`)
	// FR-OBS-02 AC-1 shape: a request counter is exposed.
	assert.Contains(t, body, `run_requests_total{rpc="Run"} 1`)
}

func TestMetrics_DoomLoopCounter(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg, "orchestrator")

	m.RecordDoomLoop("bash")
	m.RecordDoomLoop("bash")

	body := scrapeMetrics(t, reg)
	// FR-OBS-04: doom_loop_detected_total labeled by tool.
	assert.Contains(t, body, `doom_loop_detected_total{tool="bash"} 2`)
}

func TestMetrics_DurationHistogramRenders(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg, "orchestrator")

	m.ObserveRequestDuration("Run", 0.42)

	body := scrapeMetrics(t, reg)
	assert.Contains(t, body, "run_request_duration_seconds")
	assert.Contains(t, body, `run_request_duration_seconds_count{rpc="Run"} 1`)
}

func TestMetrics_USEGaugesRender(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg, "toolruntime")

	m.SetLiveSandboxes(3)
	m.SetWorkerPoolOccupancy(2)

	body := scrapeMetrics(t, reg)
	assert.Contains(t, body, "live_sandboxes 3")
	assert.Contains(t, body, "worker_pool_occupancy 2")
}

// The /metrics handler returns the same registry rendered in Prometheus text
// exposition format.
func TestMetricsHandler_ServesText(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := obs.NewMetrics(reg, "orchestrator")
	m.RecordRequest("Run")

	srv := httptest.NewServer(obs.MetricsHandler(reg))
	defer srv.Close()

	resp, err := http.Get(srv.URL) //nolint:noctx // test server, no context needed
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body := readAll(t, resp)
	assert.Contains(t, body, "run_requests_total")
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
}

// scrapeMetrics renders the registry through the promhttp handler and returns
// the Prometheus text body — exercising the real exposition path.
func scrapeMetrics(t *testing.T, g prometheus.Gatherer) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	obs.MetricsHandler(g).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	return rec.Body.String()
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	return sb.String()
}
