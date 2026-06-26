package daemon

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
	"github.com/xd1lab/harness-ai/internal/platform/grpcx/grpcxtest"
)

// minimalConfig returns a Config that passes validation, with no OTLP endpoint
// (so tracing export is disabled and SetupTelemetry needs no collector).
func minimalConfig() *config.Config {
	return &config.Config{
		Server:       config.ServerConfig{GRPCAddr: "127.0.0.1:0", HTTPAddr: "127.0.0.1:0"},
		Postgres:     config.PostgresConfig{DSN: "postgres://localhost/x", Version: 14},
		OTLP:         config.OTLPConfig{Endpoint: ""},
		ModelGateway: config.ModelGatewayConfig{Endpoint: "localhost:9001"},
		Blob:         config.BlobConfig{Dir: "/tmp/blob"},
		LogLevel:     "info",
		DevInsecure:  true,
	}
}

func TestSetupTelemetry(t *testing.T) {
	t.Run("constructs logger+metrics and a shutdown that does not error", func(t *testing.T) {
		var buf strings.Builder
		tel, err := SetupTelemetry(context.Background(), "test-svc", "0.0.1", minimalConfig(), &buf)
		require.NoError(t, err)
		require.NotNil(t, tel)
		require.NotNil(t, tel.Logger)
		require.NotNil(t, tel.Metrics)
		require.NotNil(t, tel.Registry)
		require.NotNil(t, tel.Shutdown)

		// The metrics set is registered on the registry: a RecordRequest then a
		// gather must surface run_requests_total.
		tel.Metrics.RecordRequest("/x/Y")
		mfs, gerr := tel.Registry.Gather()
		require.NoError(t, gerr)
		var found bool
		for _, mf := range mfs {
			if mf.GetName() == "run_requests_total" {
				found = true
			}
		}
		assert.True(t, found, "RED metrics must be registered on the telemetry registry")

		// Shutdown is idempotent and non-erroring with export disabled.
		require.NoError(t, tel.Shutdown(context.Background()))
		require.NoError(t, tel.Shutdown(context.Background()))
	})

	t.Run("rejects an invalid log level (fail-fast)", func(t *testing.T) {
		cfg := minimalConfig()
		cfg.LogLevel = "loud"
		_, err := SetupTelemetry(context.Background(), "svc", "", cfg, io.Discard)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "log level")
	})
}

func devCreds(t *testing.T) CredsConfig {
	t.Helper()
	td := spiffeid.RequireTrustDomainFromString("example.org")
	return CredsConfig{
		TrustDomain: td,
		ServerID:    spiffeid.RequireFromString("spiffe://example.org/test"),
		DevInsecure: true,
	}
}

func TestServerCredentials(t *testing.T) {
	t.Run("dev-insecure with env set yields credentials", func(t *testing.T) {
		t.Setenv("BOLTROPE_DEV_INSECURE", "1")
		creds, err := ServerCredentials(devCreds(t))
		require.NoError(t, err)
		require.NotNil(t, creds)
	})

	t.Run("dev-insecure without env fails closed", func(t *testing.T) {
		// Ensure the env var is unset for this subtest.
		t.Setenv("BOLTROPE_DEV_INSECURE", "")
		_, err := ServerCredentials(devCreds(t))
		require.Error(t, err)
		assert.True(t, errors.Is(err, grpcx.ErrDevInsecureNotEnabled))
	})

	t.Run("non-dev with no SPIFFE source is rejected (SPIFFE-or-exit)", func(t *testing.T) {
		cfg := devCreds(t)
		cfg.DevInsecure = false
		_, err := ServerCredentials(cfg)
		require.Error(t, err)
		assert.True(t, errors.Is(err, ErrNoServerIdentity))
	})
}

func TestHasServerIdentity(t *testing.T) {
	assert.True(t, HasServerIdentity(CredsConfig{DevInsecure: true}))
	assert.False(t, HasServerIdentity(CredsConfig{DevInsecure: false}))
}

func TestHealthHandler(t *testing.T) {
	reg := mustRegistry(t).Registry

	t.Run("livez is always 200", func(t *testing.T) {
		h := healthHandler(reg, func() bool { return false }, nil, nil)
		assert.Equal(t, http.StatusOK, doGet(t, h, "/livez"))
	})

	t.Run("readyz 503 when no identity", func(t *testing.T) {
		h := healthHandler(reg, func() bool { return false }, nil, nil)
		assert.Equal(t, http.StatusServiceUnavailable, doGet(t, h, "/readyz"))
	})

	t.Run("readyz 503 when a dependency check fails", func(t *testing.T) {
		h := healthHandler(reg, func() bool { return true }, []ReadinessCheck{
			{Name: "db", Probe: func(context.Context) error { return errors.New("down") }},
		}, nil)
		assert.Equal(t, http.StatusServiceUnavailable, doGet(t, h, "/readyz"))
	})

	t.Run("readyz 200 when identity present and all checks pass", func(t *testing.T) {
		h := healthHandler(reg, func() bool { return true }, []ReadinessCheck{
			{Name: "db", Probe: func(context.Context) error { return nil }},
		}, nil)
		assert.Equal(t, http.StatusOK, doGet(t, h, "/readyz"))
	})

	t.Run("metrics endpoint serves prometheus text", func(t *testing.T) {
		h := healthHandler(reg, func() bool { return true }, nil, nil)
		assert.Equal(t, http.StatusOK, doGet(t, h, "/metrics"))
	})
}

// TestRun_LifecycleAndGracefulShutdown is the core smoke: Run brings up both
// listeners, flips gRPC health to SERVING, serves /livez and /readyz, then drains
// and runs the registered closer when the context is cancelled.
func TestRun_LifecycleAndGracefulShutdown(t *testing.T) {
	var buf strings.Builder
	tel, err := SetupTelemetry(context.Background(), "test-svc", "", minimalConfig(), &buf)
	require.NoError(t, err)

	// A single shared CA mints both the server and client SVIDs so the mTLS
	// handshake of the gRPC health probe succeeds (the ephemeral dev fallback
	// mints a fresh CA per call and cannot verify a separately-minted client).
	td := spiffeid.RequireTrustDomainFromString("example.org")
	serverID := spiffeid.RequireFromString("spiffe://example.org/test-svc")
	clientID := spiffeid.RequireFromString("spiffe://example.org/health-probe")
	ca := grpcxtest.NewCA(t, td)
	creds := credentials.NewTLS(ca.ServerTLSConfig(t, serverID))

	// The grpc.health.v1 service runs behind the deny-by-default RBAC interceptor,
	// so the policy must explicitly allow the probe's client to call Check/Watch.
	policy := grpcx.RBACPolicy{
		"/grpc.health.v1.Health/Check": {clientID},
		"/grpc.health.v1.Health/Watch": {clientID},
	}

	// Grab two free localhost ports.
	grpcAddr := freeAddr(t)
	httpAddr := freeAddr(t)

	var closerRan atomic.Bool
	var registered atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RunInput{
			GRPCAddr:    grpcAddr,
			HTTPAddr:    httpAddr,
			Creds:       creds,
			Policy:      policy,
			Telemetry:   tel,
			HasIdentity: func() bool { return true },
			Service: Service{
				Register: func(srv *grpc.Server) {
					// Registering the health service is done by NewServer; here we
					// just confirm Register is invoked with a real server.
					_ = srv
					registered.Store(true)
				},
				ReadinessChecks: []ReadinessCheck{
					{Name: "always", Probe: func(context.Context) error { return nil }},
				},
				Closers: []func() error{
					func() error { closerRan.Store(true); return nil },
				},
			},
		})
	}()

	// Wait for the HTTP server to accept and /livez to answer 200.
	requireEventually(t, 3*time.Second, func() bool {
		return httpStatus(t, "http://"+httpAddr+"/livez") == http.StatusOK
	}, "livez never became 200")

	assert.True(t, registered.Load(), "Service.Register must be invoked")
	assert.Equal(t, http.StatusOK, httpStatus(t, "http://"+httpAddr+"/readyz"), "readyz should be 200 when ready")

	// The gRPC health service must report SERVING over real mTLS.
	requireGRPCServing(t, grpcAddr, ca, clientID, serverID)

	// Trigger graceful shutdown and assert a clean (nil) return + closer ran.
	cancel()
	select {
	case rerr := <-done:
		require.NoError(t, rerr, "clean shutdown returns nil")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancel")
	}
	assert.True(t, closerRan.Load(), "registered closer must run on shutdown")
}

// TestRun_BackgroundWorkerRunsAndStops asserts the optional background worker is
// launched on a context cancelled at shutdown, so a daemon (e.g. projectord) can
// serve health while running a projection loop.
func TestRun_BackgroundWorkerRunsAndStops(t *testing.T) {
	tel, err := SetupTelemetry(context.Background(), "svc-bg", "", minimalConfig(), io.Discard)
	require.NoError(t, err)

	td := spiffeid.RequireTrustDomainFromString("example.org")
	serverID := spiffeid.RequireFromString("spiffe://example.org/svc-bg")
	ca := grpcxtest.NewCA(t, td)
	creds := credentials.NewTLS(ca.ServerTLSConfig(t, serverID))

	httpAddr := freeAddr(t)
	var started, stopped atomic.Bool

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, RunInput{
			GRPCAddr:    freeAddr(t),
			HTTPAddr:    httpAddr,
			Creds:       creds,
			Telemetry:   tel,
			HasIdentity: func() bool { return true },
			Service: Service{
				Register: func(*grpc.Server) {},
				Background: func(bctx context.Context) error {
					started.Store(true)
					<-bctx.Done() // run until shutdown cancels the worker context
					stopped.Store(true)
					return bctx.Err()
				},
			},
		})
	}()

	requireEventually(t, 3*time.Second, func() bool {
		return httpStatus(t, "http://"+httpAddr+"/livez") == http.StatusOK && started.Load()
	}, "background worker never started")

	cancel()
	select {
	case rerr := <-done:
		require.NoError(t, rerr, "a worker that returns Canceled on shutdown is a clean exit")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	// The worker sets stopped after <-bctx.Done(); under a CPU-starved runner that
	// store can lag Run's return, so poll rather than assert once (avoids a flake).
	requireEventually(t, 2*time.Second, func() bool {
		return stopped.Load()
	}, "background worker must observe the shutdown cancellation")
}

// TestRun_ListenBindFailure asserts a bind failure on the gRPC address is
// returned (fail-fast), not swallowed.
func TestRun_ListenBindFailure(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	tel, err := SetupTelemetry(context.Background(), "svc", "", minimalConfig(), io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	creds, err := ServerCredentials(devCreds(t))
	require.NoError(t, err)

	err = Run(context.Background(), RunInput{
		GRPCAddr:    "256.256.256.256:1", // invalid host → bind/listen fails
		HTTPAddr:    freeAddr(t),
		Creds:       creds,
		Telemetry:   tel,
		HasIdentity: func() bool { return true },
		Service:     Service{Register: func(*grpc.Server) {}},
	})
	require.Error(t, err)
}

// TestGRPCHealthReadiness exercises the downstream gRPC-health readiness helper
// over a real mTLS connection (the property that makes /readyz catch an
// inter-service trust break at `up --wait`): a SERVING peer passes, a NOT_SERVING
// peer fails, and an RBAC denial (the caller's SPIFFE id not allowed to call
// Check) fails.
func TestGRPCHealthReadiness(t *testing.T) {
	td := spiffeid.RequireTrustDomainFromString("example.org")
	serverID := spiffeid.RequireFromString("spiffe://example.org/downstream")
	callerID := spiffeid.RequireFromString("spiffe://example.org/caller")
	ca := grpcxtest.NewCA(t, td)
	srvCreds := credentials.NewTLS(ca.ServerTLSConfig(t, serverID))

	// startDownstream stands up a health-serving mTLS server under policy and
	// returns a client conn dialed over the caller's mTLS creds.
	startDownstream := func(t *testing.T, policy grpcx.RBACPolicy, status healthpb.HealthCheckResponse_ServingStatus) grpc.ClientConnInterface {
		t.Helper()
		srv, hs := grpcx.NewServer(grpcx.ServerConfig{Creds: srvCreds, Policy: policy})
		hs.SetServingStatus("", status)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.GracefulStop)

		conn, err := grpcx.Dial(grpcx.DialConfig{
			Target: lis.Addr().String(),
			Creds:  credentials.NewTLS(ca.ClientTLSConfig(t, callerID, serverID)),
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return conn
	}

	allowCheck := grpcx.RBACPolicy{"/grpc.health.v1.Health/Check": {callerID}}

	t.Run("serving peer is ready", func(t *testing.T) {
		conn := startDownstream(t, allowCheck, healthpb.HealthCheckResponse_SERVING)
		check := GRPCHealthReadiness("downstream", conn)
		assert.Equal(t, "downstream", check.Name)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		assert.NoError(t, check.Probe(ctx))
	})

	t.Run("not-serving peer is not ready", func(t *testing.T) {
		conn := startDownstream(t, allowCheck, healthpb.HealthCheckResponse_NOT_SERVING)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		err := GRPCHealthReadiness("downstream", conn).Probe(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NOT_SERVING")
	})

	t.Run("rbac denial (mTLS up but Check not allowed) is not ready", func(t *testing.T) {
		// Deny-all policy: the handshake still succeeds, but the verb gate rejects
		// Check — the failure mode of a misconfigured downstream RBAC.
		conn := startDownstream(t, grpcx.RBACPolicy{}, healthpb.HealthCheckResponse_SERVING)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		assert.Error(t, GRPCHealthReadiness("downstream", conn).Probe(ctx))
	})
}

// ---- helpers ----

func mustRegistry(t *testing.T) *Telemetry {
	t.Helper()
	tel, err := SetupTelemetry(context.Background(), "svc-health", "", minimalConfig(), io.Discard)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	return tel
}

func doGet(t *testing.T, h http.Handler, path string) int {
	t.Helper()
	srv := &http.Server{Handler: h, ReadHeaderTimeout: time.Second}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { _ = srv.Close() })
	return httpStatus(t, "http://"+lis.Addr().String()+path)
}

func httpStatus(t *testing.T, url string) int {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url) //nolint:noctx // short test probe
	if err != nil {
		return -1
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())
	return addr
}

func requireEventually(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

func requireGRPCServing(t *testing.T, addr string, ca *grpcxtest.CA, clientID, serverID spiffeid.ID) {
	t.Helper()
	clientCreds := credentials.NewTLS(ca.ClientTLSConfig(t, clientID, serverID))

	conn, err := grpcx.Dial(grpcx.DialConfig{Target: addr, Creds: clientCreds})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	hc := healthpb.NewHealthClient(conn)
	requireEventually(t, 3*time.Second, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		resp, herr := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		return herr == nil && resp.GetStatus() == healthpb.HealthCheckResponse_SERVING
	}, "gRPC health never reported SERVING")
}
