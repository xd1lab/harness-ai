package grpcx

import (
	"log/slog"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// ServerConfig parameterizes [NewServer]: the transport credentials, the
// authorization policy, the logger, and any extra gRPC server options.
type ServerConfig struct {
	// Creds are the transport credentials for the listener — typically built by
	// [SPIFFEServerCredentials] (production) or [StaticDevCredentials] (the
	// fail-closed dev fallback). Required: NewServer does not silently start a
	// plaintext server.
	Creds credentials.TransportCredentials
	// Policy is the deny-by-default per-RPC SPIFFE-ID allowlist enforced by the
	// RBAC interceptor (architecture §8.1). A nil/empty policy denies every RPC,
	// which is the safe default; callers populate it with their service's
	// caller→RPC matrix.
	Policy RBACPolicy
	// Logger backs the logging and recovery interceptors. When nil,
	// [slog.Default] is used.
	Logger *slog.Logger
	// OTelOptions are passed to the OTel gRPC stats handler (e.g. a non-global
	// MeterProvider/TracerProvider in tests). Optional.
	OTelOptions []otelgrpc.Option
	// Extra are additional server options appended after the ones NewServer
	// installs (credentials, stats handler, interceptor chains). Use for
	// keepalive, message-size limits, etc. Optional.
	Extra []grpc.ServerOption
}

// NewServer constructs a [*grpc.Server] wired with the full Boltrope server
// stack, in the documented order:
//
//  1. OTel gRPC stats handler — creates the server span and extracts incoming
//     W3C trace context from gRPC metadata so it is active for the rest of the
//     chain (FR-OBS-01 propagation).
//  2. logging interceptor — structured slog access log with trace correlation.
//  3. recovery interceptor — converts a handler panic into codes.Internal so one
//     bad RPC never crashes the process.
//  4. RBAC interceptor — deny-by-default per-RPC peer-SPIFFE-ID verb gate
//     (architecture §8.1).
//
// The same order is applied to both unary and stream RPCs. A
// grpc.health.v1.Health service is registered and its [*health.Server] is
// returned so the caller can flip serving status as dependencies become ready
// (FR-OBS-05). Credentials are mandatory; there is no plaintext path.
func NewServer(cfg ServerConfig) (*grpc.Server, *health.Server) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	opts := []grpc.ServerOption{
		grpc.Creds(cfg.Creds),
		grpc.StatsHandler(ServerStatsHandler(cfg.OTelOptions...)),
		grpc.ChainUnaryInterceptor(
			UnaryLoggingInterceptor(logger),
			UnaryRecoveryInterceptor(logger),
			UnaryRBACInterceptor(cfg.Policy),
		),
		grpc.ChainStreamInterceptor(
			StreamLoggingInterceptor(logger),
			StreamRecoveryInterceptor(logger),
			StreamRBACInterceptor(cfg.Policy),
		),
	}
	opts = append(opts, cfg.Extra...)

	srv := grpc.NewServer(opts...)

	hs := health.NewServer()
	healthpb.RegisterHealthServer(srv, hs)
	return srv, hs
}
