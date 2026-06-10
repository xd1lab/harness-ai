package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// shutdownTimeout bounds the graceful-drain + closer + telemetry-flush phase so a
// stuck connection cannot block process exit forever; on expiry the gRPC server
// is force-stopped (architecture §10.1).
const shutdownTimeout = 20 * time.Second

// Service is the per-daemon contribution to [Run]: how to register the service's
// inbound gRPC server onto the shared [*grpc.Server], the dependency readiness
// checks for /readyz, and the closers to run on shutdown (pools, SPIFFE sources).
// A daemon's wiring file builds its adapters + inbound server and packages them
// here; everything cross-cutting (creds, interceptors, health, signals, drain,
// telemetry flush) is the harness's job.
type Service struct {
	// Register attaches the service's gRPC server(s) to srv. It is called after
	// the server is constructed with credentials + interceptors and before it
	// begins serving. Required.
	Register func(srv *grpc.Server)
	// ReadinessChecks gate /readyz on real dependency reachability (FR-OBS-05).
	// They are evaluated on every probe; an empty slice means readiness depends
	// only on SVID presence.
	ReadinessChecks []ReadinessCheck
	// Closers are run (in reverse registration order) during graceful shutdown,
	// after RPCs drain — e.g. pool.Close(), spiffeSource.Close(). Each is
	// best-effort; an error is logged, not fatal.
	Closers []func() error
	// Background, when non-nil, is a long-running worker [Run] launches on a
	// context cancelled at shutdown start (e.g. projectord's projection loop). A
	// non-nil return other than the worker's own clean context-cancellation is
	// treated as fatal and triggers shutdown; a nil/Canceled return is clean. It
	// runs alongside the gRPC/HTTP servers, so a daemon can be both a server and a
	// worker (architecture §10.4: projectord serves health while it projects).
	Background func(ctx context.Context) error
}

// RunInput parameterizes [Run]: the listen addresses, the resolved server
// credentials, the per-RPC RBAC policy, the bootstrapped telemetry, the
// SVID-presence reporter for readiness, and the per-daemon [Service].
type RunInput struct {
	// GRPCAddr is the gRPC listen address (host:port). Required.
	GRPCAddr string
	// HTTPAddr is the health/metrics listen address (host:port). Required.
	HTTPAddr string
	// Creds are the resolved server transport credentials (see
	// [ServerCredentials]). Required: there is no plaintext path.
	Creds credentials.TransportCredentials
	// Policy is the deny-by-default per-RPC SPIFFE-ID verb gate enforced by the
	// platform RBAC interceptor (architecture §8.1). A nil/empty policy denies
	// every inter-service RPC; daemons populate it with their caller→RPC matrix.
	Policy grpcx.RBACPolicy
	// Telemetry is the bootstrapped observability bundle; its logger backs the
	// interceptors and its registry backs /metrics. Required.
	Telemetry *Telemetry
	// HasIdentity reports SVID presence for /readyz (bind [HasServerIdentity] to
	// the daemon's CredsConfig). A nil func is treated as "identity present".
	HasIdentity func() bool
	// ExtraServerOptions are appended to the gRPC server options after the
	// platform interceptor chain — used to install a service-specific interceptor
	// such as the orchestrator's client-edge JWT auth (which runs after the
	// platform logging/recovery/RBAC chain; architecture §8.7). Optional.
	ExtraServerOptions []grpc.ServerOption
	// Service is the per-daemon registration + readiness checks + closers.
	Service Service
}

// Run is the daemon main loop. It builds the gRPC server (platform interceptor
// chain + grpc.health.v1) with the supplied credentials and RBAC policy,
// registers the service, starts the gRPC and HTTP (/livez,/readyz,/metrics)
// listeners, flips the gRPC health status to SERVING, and blocks until ctx is
// done or a SIGINT/SIGTERM arrives. On shutdown it gracefully drains in-flight
// RPCs (bounded by [shutdownTimeout], then force-stops), stops the HTTP server,
// runs the service's closers in reverse, and flushes telemetry — the ordering of
// architecture §10.1. It returns the first fatal error (a listener bind failure,
// or a serve error other than the expected close), or nil on a clean shutdown.
//
// The context ctx is the parent lifecycle; cancelling it triggers the same
// graceful shutdown as a signal. All transport, identity, readiness, and
// lifecycle parameters are supplied via [RunInput].
func Run(ctx context.Context, in RunInput) error {
	log := in.Telemetry.Logger
	if log == nil {
		log = slog.Default()
	}

	// Listen first so a bind failure is reported before any background work.
	grpcLis, err := net.Listen("tcp", in.GRPCAddr)
	if err != nil {
		return fmt.Errorf("daemon: listen gRPC on %s: %w", in.GRPCAddr, err)
	}
	httpLis, err := net.Listen("tcp", in.HTTPAddr)
	if err != nil {
		_ = grpcLis.Close()
		return fmt.Errorf("daemon: listen HTTP on %s: %w", in.HTTPAddr, err)
	}

	grpcSrv, healthSrv := grpcx.NewServer(grpcx.ServerConfig{
		Creds:  in.Creds,
		Policy: in.Policy,
		Logger: log,
		Extra:  in.ExtraServerOptions,
	})
	in.Service.Register(grpcSrv)

	httpSrv := &http.Server{
		Handler:           healthHandler(in.Telemetry.Registry, in.HasIdentity, in.Service.ReadinessChecks),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Signal-aware run context: a SIGINT/SIGTERM cancels it, triggering drain.
	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 2)
	go func() {
		if err := grpcSrv.Serve(grpcLis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serveErr <- fmt.Errorf("daemon: gRPC serve: %w", err)
		}
	}()
	go func() {
		if err := httpSrv.Serve(httpLis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("daemon: HTTP serve: %w", err)
		}
	}()
	if in.Service.Background != nil {
		go func() {
			// The worker runs on runCtx, so shutdown cancels it. A clean
			// context-cancellation return is not a fatal error; anything else is.
			if err := in.Service.Background(runCtx); err != nil && !errors.Is(err, context.Canceled) {
				serveErr <- fmt.Errorf("daemon: background worker: %w", err)
			}
		}()
	}

	// Everything is up: announce readiness to the gRPC health protocol. The
	// dependency-gated readiness is on /readyz; the gRPC health SERVING status is
	// the inter-service liveness signal (architecture §10.1).
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	log.Info("daemon started",
		slog.String("grpc_addr", in.GRPCAddr),
		slog.String("http_addr", in.HTTPAddr))

	var runErr error
	select {
	case <-runCtx.Done():
		log.Info("daemon shutting down", slog.String("cause", context.Cause(runCtx).Error()))
	case runErr = <-serveErr:
		log.Error("daemon serve error, shutting down", slog.Any("error", runErr))
	}

	gracefulShutdown(grpcSrv, healthSrv, httpSrv, in.Service.Closers, in.Telemetry.Shutdown, log)
	return runErr
}

// gracefulShutdown performs the drain → stop-http → run-closers → flush-telemetry
// sequence, all bounded by [shutdownTimeout]. The gRPC health status is flipped
// to NOT_SERVING first so a load balancer stops routing during the drain.
func gracefulShutdown(
	grpcSrv *grpc.Server,
	healthSrv *health.Server,
	httpSrv *http.Server,
	closers []func() error,
	flushTelemetry func(context.Context) error,
	log *slog.Logger,
) {
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Drain in-flight gRPC RPCs, force-stopping if the deadline passes.
	stopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-shutdownCtx.Done():
		log.Warn("daemon: graceful gRPC drain timed out; forcing stop")
		grpcSrv.Stop()
	}

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Warn("daemon: HTTP server shutdown error", slog.Any("error", err))
	}

	// Run closers in reverse registration order (LIFO), so a resource is torn
	// down before the resource it depends on.
	for i := len(closers) - 1; i >= 0; i-- {
		if closers[i] == nil {
			continue
		}
		if err := closers[i](); err != nil {
			log.Warn("daemon: closer error", slog.Any("error", err))
		}
	}

	if flushTelemetry != nil {
		if err := flushTelemetry(shutdownCtx); err != nil {
			log.Warn("daemon: telemetry flush error", slog.Any("error", err))
		}
	}
	log.Info("daemon stopped")
}
