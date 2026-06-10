// Package grpcx is the gRPC transport bootstrap shared by every Boltrope service.
// It centralizes the two cross-cutting concerns the architecture mandates for all
// service-to-service traffic (architecture §8.1, NFR-SEC-01, FR-OBS-01):
//
//   - Identity & transport security — mutual TLS built from SPIFFE/SPIRE
//     workload identity. [SPIFFEServerCredentials]/[SPIFFEClientCredentials] wire
//     a [SPIFFESource] (a SPIRE X509Source) into gRPC
//     [google.golang.org/grpc/credentials.TransportCredentials] via the tlsconfig
//     MTLS helpers. A development-only static-cert fallback
//     ([StaticDevCredentials]) lives behind the same constructor surface and
//     FAILS CLOSED: it refuses to start unless BOLTROPE_DEV_INSECURE=1 is set,
//     logs a loud warning, mints ephemeral certs, and is never the silent
//     production default.
//
//   - The server interceptor chain — [ServerStatsHandler] (OTel gRPC stats
//     handler for spans + W3C trace-context propagation across gRPC metadata),
//     a structured slog logging interceptor with trace correlation
//     ([UnaryLoggingInterceptor]/[StreamLoggingInterceptor]), a panic-recovery
//     interceptor ([UnaryRecoveryInterceptor]/[StreamRecoveryInterceptor]), and a
//     peer-SPIFFE-ID RBAC interceptor ([UnaryRBACInterceptor]/[StreamRBACInterceptor])
//     enforcing a deny-by-default, per-RPC service allowlist (architecture §8.1).
//     [NewServer] assembles them in the documented order; [Dial] provides the
//     matching client side.
//
// # The verb/row split (architecture §8.1)
//
// The RBAC interceptor gates the VERB: it checks the peer's SPIFFE ID against a
// per-RPC allowlist (only orchestrator may call tool-runtime.ExecuteTool, etc.).
// It is deliberately coarse and is NOT the only line of defense — every data
// access is additionally constrained by the propagated tenant token (§8.2) and by
// PostgreSQL row-level security (§8.3). Service identity gates the verb; the
// tenant token + RLS gate the row; neither alone is sufficient. This package owns
// only the verb gate.
//
// # Configuration boundary
//
// Like the obs package, grpcx takes plain parameters (an [*slog.Logger], a SPIFFE
// source, an [RBACPolicy]); it does not import the config package and has no
// opinion on precedence or sources. Callers resolve configuration elsewhere and
// pass values in.
//
// # Determinism
//
// grpcx is platform transport wiring, not domain/app logic, so it is outside the
// determinism rule's forbidigo scope and uses the standard library time/crypto
// transitively. Domain/app code never imports grpcx.
package grpcx
