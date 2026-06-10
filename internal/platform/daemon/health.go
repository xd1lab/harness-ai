package daemon

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/xd1lab/harness-ai/internal/platform/obs"
)

// ReadinessCheck reports whether one dependency is currently reachable/usable.
// It returns nil when ready and a non-nil error (surfaced in the /readyz body)
// when not. Each daemon supplies the checks for its dependencies — a PostgreSQL
// ping, a downstream gRPC health probe, the container-runtime availability — so
// /readyz gates on real dependency reachability (FR-OBS-05). A check should
// respect ctx (the probe carries a short deadline) and must not block
// indefinitely.
type ReadinessCheck struct {
	// Name labels the check in the /readyz response body for operator clarity.
	Name string
	// Probe performs the reachability test.
	Probe func(ctx context.Context) error
}

// readinessProbeTimeout bounds a single /readyz evaluation so a hung dependency
// probe cannot wedge the readiness endpoint (FR-OBS-05: readiness must answer
// promptly with 503, not hang).
const readinessProbeTimeout = 3 * time.Second

// healthHandler builds the HTTP mux serving the three operational endpoints every
// daemon exposes (FR-OBS-02, FR-OBS-05):
//
//   - GET /livez  — liveness: always 200 once the process is up (the process is
//     running; it does not gate on dependencies, so a dependency outage does not
//     trigger a restart loop).
//   - GET /readyz — readiness: 200 only when a server identity is present AND
//     every dependency check passes; otherwise 503 with a plaintext reason. This
//     is the gate Kubernetes/compose uses to route traffic (architecture §10.1).
//   - GET /metrics — the Prometheus exposition of reg (RED/USE + OTel bridge).
//
// hasIdentity reports SVID presence (see [HasServerIdentity]); checks are the
// dependency probes.
func healthHandler(reg prometheus.Gatherer, hasIdentity func() bool, checks []ReadinessCheck) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessProbeTimeout)
		defer cancel()

		if hasIdentity != nil && !hasIdentity() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: no server identity (SVID) present\n"))
			return
		}

		for _, c := range checks {
			if c.Probe == nil {
				continue
			}
			if err := c.Probe(ctx); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready: " + c.Name + ": " + err.Error() + "\n"))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	mux.Handle("/metrics", obs.MetricsHandler(reg))
	return mux
}

// GRPCHealthReadiness builds a [ReadinessCheck] that performs a grpc.health.v1
// Check against a downstream service over conn — the SAME mutually-authenticated
// client connection the daemon uses to call that service. Because the probe runs
// over the real inter-service mTLS channel, /readyz only reports ready once that
// channel actually handshakes (and the peer reports SERVING), so a broken shared
// trust anchor — e.g. a dev-CA mismatch that would make every inter-service RPC
// fail — keeps the daemon out of rotation at `up --wait` instead of surfacing only
// on the first real request (FR-OBS-05; architecture §10.1).
//
// The empty service name ("") probes the server's overall serving status, which
// the harness flips to SERVING once it is up (see [Run]). A non-SERVING status,
// an Unimplemented health service, an RBAC denial, or a transport/handshake
// failure all yield a non-nil error surfaced in the /readyz body. conn is dialed
// lazily by the caller, so this never blocks startup; the probe carries the
// /readyz deadline.
func GRPCHealthReadiness(name string, conn grpc.ClientConnInterface) ReadinessCheck {
	return ReadinessCheck{
		Name: name,
		Probe: func(ctx context.Context) error {
			resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			if err != nil {
				return err
			}
			if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
				return fmt.Errorf("downstream health status is %s (want SERVING)", resp.GetStatus())
			}
			return nil
		},
	}
}
