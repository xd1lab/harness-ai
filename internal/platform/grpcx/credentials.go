package grpcx

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/grpc/credentials"
)

// devInsecureEnv is the environment variable that must equal "1" to unlock the
// static-cert development fallback. Its name is intentionally alarming and is
// referenced verbatim in the spec (NFR-SEC-01) and architecture (§8.1).
const devInsecureEnv = "BOLTROPE_DEV_INSECURE"

// ErrDevInsecureNotEnabled is returned by [StaticDevCredentials] when the
// static-cert development fallback is requested without BOLTROPE_DEV_INSECURE=1.
// It is a sentinel so wiring can branch with [errors.Is] and so the fail-closed
// behavior is testable (NFR-TEST-05(g)). Production code never sets the variable,
// so this error is the guarantee that a deployment cannot silently downgrade to
// ephemeral static certs in place of SPIFFE mTLS (architecture §8.1).
var ErrDevInsecureNotEnabled = errors.New(
	"grpcx: static-cert dev fallback refused: set " + devInsecureEnv + "=1 to enable insecure local mode")

// SPIFFESource is the SPIFFE identity material the mTLS credential constructors
// need: the workload's own X509-SVID and the X.509 trust bundle used to verify
// peers. It is the intersection of go-spiffe's [x509svid.Source] and
// [x509bundle.Source], and is satisfied by a *workloadapi.X509Source (the live,
// auto-rotating SPIRE Workload API source) as well as by in-memory test sources.
// Depending on the two narrow interfaces rather than the concrete Workload API
// type keeps this file free of the Workload API client and makes the
// constructors unit-testable with synthetic SVIDs.
type SPIFFESource interface {
	x509svid.Source
	x509bundle.Source
}

// SPIFFEServerCredentials builds gRPC server transport credentials that present
// the workload's X509-SVID from src and require, verify, and authorize the
// client's X509-SVID against the trust bundle (mutual TLS). The authorizer
// admits any SVID in trustDomain; the coarser per-RPC verb gate is enforced
// separately by [UnaryRBACInterceptor]/[StreamRBACInterceptor] (architecture
// §8.1). src is typically a *workloadapi.X509Source obtained from the SPIRE
// Workload API (see [NewSPIFFESource]).
func SPIFFEServerCredentials(src SPIFFESource, trustDomain spiffeid.TrustDomain) credentials.TransportCredentials {
	cfg := tlsconfig.MTLSServerConfig(src, src, tlsconfig.AuthorizeMemberOf(trustDomain))
	return credentials.NewTLS(cfg)
}

// SPIFFEClientCredentials builds gRPC client transport credentials that present
// the workload's X509-SVID from src and verify + authorize the server's
// X509-SVID to be exactly serverID. Pinning the callee's identity prevents a
// confused-deputy call to the wrong service even within the trust domain.
func SPIFFEClientCredentials(src SPIFFESource, serverID spiffeid.ID) credentials.TransportCredentials {
	cfg := tlsconfig.MTLSClientConfig(src, src, tlsconfig.AuthorizeID(serverID))
	return credentials.NewTLS(cfg)
}

// SPIFFEClientCredentialsForTrustDomain is like [SPIFFEClientCredentials] but
// authorizes any server SVID in trustDomain rather than a single pinned ID. Use
// it for clients that legitimately fan out to multiple services in the same
// trust domain; prefer the pinned form when the callee is known.
func SPIFFEClientCredentialsForTrustDomain(src SPIFFESource, trustDomain spiffeid.TrustDomain) credentials.TransportCredentials {
	cfg := tlsconfig.MTLSClientConfig(src, src, tlsconfig.AuthorizeMemberOf(trustDomain))
	return credentials.NewTLS(cfg)
}

// StaticDevConfig parameterizes the static-cert development fallback
// ([StaticDevCredentials]).
type StaticDevConfig struct {
	// TrustDomain is the SPIFFE trust domain the ephemeral CA and SVIDs are
	// minted under. Required.
	TrustDomain spiffeid.TrustDomain
	// ServerID is the SPIFFE ID embedded in the ephemeral server/peer SVID this
	// process presents. Required.
	ServerID spiffeid.ID
	// Logger receives the mandatory loud warning when the fallback engages. When
	// nil, [slog.Default] is used so the warning is never silently dropped.
	Logger *slog.Logger
	// LookupEnv reads an environment variable, returning its value and whether it
	// was set. It is injected so the fail-closed gate is testable without mutating
	// the process environment; when nil, [os.LookupEnv] is used so production
	// wiring reads the real environment (NFR-TEST-01-style seam, though grpcx is
	// outside the determinism scope).
	LookupEnv func(key string) (string, bool)
}

// StaticDevCredentials builds gRPC mutual-TLS server transport credentials from
// a SHARED, deterministically-derived in-process SPIFFE certificate authority —
// the development-only fallback for local `docker compose`/CI where no SPIRE
// agent is present (architecture §8.1, NFR-SEC-01).
//
// It FAILS CLOSED. Unless the BOLTROPE_DEV_INSECURE environment variable equals
// exactly "1", it returns [ErrDevInsecureNotEnabled] and no credentials, so a
// production deployment can never silently substitute static certs for SPIFFE
// mTLS. When enabled it logs a prominent WARN-level warning (naming the variable)
// then derives the CA deterministically from a shared seed read from
// BOLTROPE_DEV_CA_SEED (a fixed, well-known default with its own warning when
// unset) and mints a fresh per-identity leaf SVID under that CA at startup —
// nothing is read from or written to disk and no certs are committed. Because the
// CA is a pure function of the seed, every process sharing the seed produces the
// SAME CA cert and trust bundle, so a dev server here and a dev client in another
// process/container complete mutual TLS against the same trust anchor — the
// property that was missing when each process minted its own ephemeral CA. The
// credentials still require and authorize a client SVID in the trust domain, so
// the dev path exercises the same mTLS and RBAC code as production.
//
// This constructor is intended to be the *only* place a static-cert path is
// reachable, behind the same call site as the SPIFFE path, so callers choose
// "SPIFFE or dev-fallback" once and the downgrade is auditable.
func StaticDevCredentials(cfg StaticDevConfig) (credentials.TransportCredentials, error) {
	lookup := cfg.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if v, ok := lookup(devInsecureEnv); !ok || v != "1" {
		return nil, ErrDevInsecureNotEnabled
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn(
		"INSECURE DEV MODE: "+devInsecureEnv+"=1 — using a SHARED-SEED static mTLS CA instead of SPIFFE/SPIRE; "+
			"this MUST NOT be used in production and is compiled out of release images",
		slog.String("trust_domain", cfg.TrustDomain.String()),
		slog.String("server_id", cfg.ServerID.String()),
	)

	svid, bundle, err := mintSharedDevSVID(cfg.TrustDomain, cfg.ServerID, lookup, logger)
	if err != nil {
		return nil, fmt.Errorf("grpcx: mint shared dev SVID: %w", err)
	}
	tlsCfg := tlsconfig.MTLSServerConfig(svid, bundle, tlsconfig.AuthorizeMemberOf(cfg.TrustDomain))
	return credentials.NewTLS(tlsCfg), nil
}

// StaticDevClientCredentials builds the client side of the static-cert dev
// fallback, mirroring [StaticDevCredentials]: it fails closed unless
// BOLTROPE_DEV_INSECURE=1, derives the SAME shared-seed CA (so it trusts a dev
// server built from the same BOLTROPE_DEV_CA_SEED), mints a leaf SVID for
// cfg.ServerID (used here as the client identity), and authorizes the server SVID
// to be serverID. Provided so a local `boltrope-ctl`/service client can dial a
// dev server over the same mTLS path without SPIRE.
func StaticDevClientCredentials(cfg StaticDevConfig, serverID spiffeid.ID) (credentials.TransportCredentials, error) {
	lookup := cfg.LookupEnv
	if lookup == nil {
		lookup = os.LookupEnv
	}
	if v, ok := lookup(devInsecureEnv); !ok || v != "1" {
		return nil, ErrDevInsecureNotEnabled
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn(
		"INSECURE DEV MODE: "+devInsecureEnv+"=1 — using a SHARED-SEED static mTLS client cert instead of SPIFFE/SPIRE",
		slog.String("trust_domain", cfg.TrustDomain.String()),
		slog.String("client_id", cfg.ServerID.String()),
	)

	svid, bundle, err := mintSharedDevSVID(cfg.TrustDomain, cfg.ServerID, lookup, logger)
	if err != nil {
		return nil, fmt.Errorf("grpcx: mint shared dev client SVID: %w", err)
	}
	tlsCfg := tlsconfig.MTLSClientConfig(svid, bundle, tlsconfig.AuthorizeID(serverID))
	return credentials.NewTLS(tlsCfg), nil
}

// mintSharedDevSVID derives the shared, seed-determined dev CA (see [newDevCA])
// and mints a leaf X509-SVID for id under it, returning the SVID and a trust
// bundle containing the shared CA. The CA is a pure function of the seed read via
// lookup (BOLTROPE_DEV_CA_SEED, or a fixed default), so all processes sharing the
// seed obtain the byte-identical CA and trust bundle and can therefore complete
// mutual TLS with each other. The leaf key is per-process random (crypto/rand) —
// each process presents its own freshly minted leaf signed by the shared CA. The
// material lives only in memory and is regenerated each call; the SPIFFE path
// uses real SPIRE-issued SVIDs instead.
func mintSharedDevSVID(td spiffeid.TrustDomain, id spiffeid.ID, lookup func(string) (string, bool), logger *slog.Logger) (*x509svid.SVID, *x509bundle.Bundle, error) {
	seed, _ := resolveDevCASeed(lookup, logger)
	ca, err := newDevCA(td, seed)
	if err != nil {
		return nil, nil, fmt.Errorf("derive shared dev CA: %w", err)
	}
	return ca.issueSVID(id, rand.Reader)
}
