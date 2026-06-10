package daemon

import (
	"errors"
	"fmt"
	"log/slog"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc/credentials"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// ErrNoServerIdentity is returned by [ServerCredentials] when a non-dev process
// has no SPIFFE source to present an SVID from. It is the startup guarantee of
// NFR-SEC-01 / DOD-05: a production service exits rather than starting without a
// verifiable identity, and never silently downgrades to the static-cert dev
// fallback (which is itself gated on BOLTROPE_DEV_INSECURE). Recover it with
// [errors.Is].
var ErrNoServerIdentity = errors.New(
	"daemon: no SPIFFE source for server identity and dev-insecure mode is off (set BOLTROPE_DEV_INSECURE=1 for the local static-cert fallback, or provide a SPIFFE source)")

// CredsConfig parameterizes [ServerCredentials]: the SPIFFE trust domain and the
// workload's own SPIFFE ID, the resolved dev-insecure decision, and the optional
// live SPIFFE source.
type CredsConfig struct {
	// TrustDomain is the SPIFFE trust domain peers are authorized within
	// (architecture §8.1). Required.
	TrustDomain spiffeid.TrustDomain
	// ServerID is this workload's SPIFFE ID — presented in the dev fallback's
	// ephemeral SVID and used as the spiffe:// identity. Required.
	ServerID spiffeid.ID
	// DevInsecure is the resolved BOLTROPE_DEV_INSECURE decision (from config).
	// When true and no Source is present, the fail-closed static-cert fallback is
	// used; the fallback itself re-checks the env var so a stray DevInsecure=true
	// without the env still refuses (defense in depth).
	DevInsecure bool
	// Source is the live SPIFFE source (a *grpcx.X509Source from the SPIRE
	// Workload API, built under the `spire` build tag by the wiring). When non-nil
	// it is always preferred over the dev fallback. When nil and DevInsecure is
	// false, [ServerCredentials] fails closed with [ErrNoServerIdentity].
	Source grpcx.SPIFFESource
	// Logger receives the loud warning the dev fallback emits. When nil
	// [slog.Default] is used.
	Logger *slog.Logger
}

// ServerCredentials selects the gRPC server transport credentials for a daemon,
// preferring SPIFFE mTLS and falling back — fail-closed — to the ephemeral
// static-cert dev path only when BOLTROPE_DEV_INSECURE is set (NFR-SEC-01;
// architecture §8.1). The selection is made once, at this single call site, so
// the SPIFFE-or-dev-fallback choice is auditable and a production deployment
// cannot silently downgrade.
//
//   - cfg.Source != nil → SPIFFE mTLS authorizing any peer in the trust domain
//     (the coarse per-RPC verb gate is the RBAC interceptor's job, not here).
//   - cfg.Source == nil && cfg.DevInsecure → the static-cert fallback, which
//     itself returns [grpcx.ErrDevInsecureNotEnabled] unless the env var is "1".
//   - otherwise → [ErrNoServerIdentity] (the process must not start).
func ServerCredentials(cfg CredsConfig) (credentials.TransportCredentials, error) {
	if cfg.Source != nil {
		return grpcx.SPIFFEServerCredentials(cfg.Source, cfg.TrustDomain), nil
	}
	if !cfg.DevInsecure {
		return nil, ErrNoServerIdentity
	}
	creds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
		TrustDomain: cfg.TrustDomain,
		ServerID:    cfg.ServerID,
		Logger:      cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("daemon: dev-insecure server credentials: %w", err)
	}
	return creds, nil
}

// HasServerIdentity reports whether a verifiable server identity is available
// under cfg — a live SPIFFE source, or the explicitly-enabled dev fallback. The
// readiness probe consults it so /readyz reflects the SVID-present requirement of
// FR-OBS-05 (a daemon with no identity is never ready). It performs no I/O.
func HasServerIdentity(cfg CredsConfig) bool {
	return cfg.Source != nil || cfg.DevInsecure
}
