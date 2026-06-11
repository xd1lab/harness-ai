package grpc

import (
	"context"
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// Principal is the authenticated client identity derived from the edge bearer
// token (or the dev principal in dev mode). It is placed on the request context
// by the auth interceptor and read back by the handlers to enforce
// session-ownership (architecture §8.7). TenantID is the RLS scoping tenant;
// Subject is the principal/subject claim used for audit.
type Principal struct {
	// TenantID is the owning tenant the caller is authorized for. Every
	// Run/Control/Fork target session must belong to this tenant.
	TenantID string
	// Subject is the principal/subject identity (the JWT `sub`), for audit and
	// the operator identity on bypass-mode activation.
	Subject string
}

// principalCtxKey is the unexported context key carrying the verified
// [Principal]. A distinct type prevents collision with other packages' keys.
type principalCtxKey struct{}

// withPrincipal returns a child context carrying p as the verified principal AND
// scopes the tenant for the event-store RLS GUC acquire-hook via
// [db.WithTenant], so a single placement keeps the public-edge tenant and the
// database tenant consistent (architecture §8.2, §8.7).
func withPrincipal(ctx context.Context, p Principal) context.Context {
	ctx = context.WithValue(ctx, principalCtxKey{}, p)
	ctx = db.WithTenant(ctx, p.TenantID)
	return ctx
}

// ContextWithPrincipal is the exported form of the principal placement used by
// sibling inbound transports (the REST/SSE facade): it carries p as the
// verified principal AND scopes the RLS tenant, identically to what the gRPC
// interceptors do — so every transport feeds the same ownership and
// row-level-security path. p MUST come from [Authenticator.VerifyBearer] (or
// the dev path); placing an unverified principal here bypasses edge auth.
func ContextWithPrincipal(ctx context.Context, p Principal) context.Context {
	return withPrincipal(ctx, p)
}

// PrincipalFromContext returns the verified [Principal] placed on ctx by the
// auth interceptor, or false when none is present (the request was not
// authenticated). Handlers use it to enforce tenant ownership.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalCtxKey{}).(Principal)
	return p, ok && p.TenantID != ""
}

// AuthConfig parameterizes [NewAuthInterceptor]. Exactly one of the production
// path (a non-empty Algorithms + a Keyfunc + Issuer/Audience) or the dev path
// (DevInsecure=true) must be configured; otherwise the interceptor fails closed
// and rejects every call (NFR-SEC-01: no silent open edge).
type AuthConfig struct {
	// Issuer is the required `iss` claim. Empty means the issuer is not checked
	// (still validated for presence only when non-empty).
	Issuer string
	// Audience is the required `aud` claim; a token whose audience set does not
	// contain it is rejected. Empty means the audience is not checked.
	Audience string
	// Algorithms is the PINNED set of accepted JWT signing algorithms (e.g.
	// {"RS256"} or {"HS256"}). It MUST be non-empty in production: the parser is
	// constrained to exactly these, so a token presenting any other alg —
	// notably "none" — is rejected before signature verification (FR-API-03
	// AC-2; architecture §8.7). "none" is rejected even if explicitly listed.
	Algorithms []string
	// Keyfunc resolves the verification key for a token (e.g. a JWKS-backed
	// function with rotation). Required in production. It is invoked only after
	// the alg has been validated against Algorithms.
	Keyfunc jwt.Keyfunc
	// TenantClaim is the custom claim name carrying the tenant id. Defaults to
	// "tenant_id" when empty.
	TenantClaim string

	// DevInsecure enables the dev path: the interceptor accepts requests without
	// a verifiable token and injects DevPrincipal. It is gated on
	// BOLTROPE_DEV_INSECURE by the caller (infra wiring); this struct only
	// records the resolved decision. When false and no Keyfunc is set, the
	// interceptor fails closed.
	DevInsecure bool
	// DevPrincipal is the principal injected in dev mode. When DevInsecure is
	// true and this is the zero value, a default {TenantID: DevTenantID,
	// Subject: "dev"} is used — TenantID is a VALID UUID ([DevTenantID]) because
	// the event-store tenant_id column is a UUID; a non-UUID dev tenant breaks the
	// first CreateSession.
	DevPrincipal Principal
}

const defaultTenantClaim = "tenant_id"

// DevTenantID is the fixed tenant id assigned to the dev principal under
// dev-insecure mode. It MUST be a valid UUID because the event-store schema
// types tenant_id as a UUID column (and RLS scopes on
// current_setting('app.current_tenant')::uuid), so a non-UUID dev tenant (the
// historical literal "dev") makes the very first CreateSession fail with
// SQLSTATE 22P02 (invalid input syntax for type uuid). This is a well-known,
// non-secret constant scoped to dev: the compose `boltrope-grant` one-shot seeds
// a tenants row with this id so the FK from sessions/events is satisfied. The
// value is a syntactically valid v4 UUID (the leading nibbles spell "0de0c0de"
// as a mnemonic) and round-trips through uuid.Parse unchanged.
const DevTenantID = "0de0c0de-0000-4000-8000-000000000000"

// authError is the typed UNAUTHENTICATED status returned for any edge-auth
// failure. The message is intentionally coarse so it does not leak why a token
// was rejected to an unauthenticated caller.
func authError(format string, args ...any) error {
	return status.Errorf(codes.Unauthenticated, "edge auth: "+format, args...)
}

// Authenticator carries the resolved auth policy and performs the per-call
// validation shared by the unary and stream gRPC interceptors AND the REST
// facade (one policy, every transport). Construct it with [NewAuthenticator].
type Authenticator struct {
	cfg         AuthConfig
	tenantClaim string
	parserOpts  []jwt.ParserOption
}

// NewAuthenticator validates the config and builds the shared validator. It
// returns an error when the config is neither a valid production policy nor dev
// mode (fail-closed): a non-dev policy MUST pin at least one algorithm and
// supply a Keyfunc.
func NewAuthenticator(cfg AuthConfig) (*Authenticator, error) {
	tenantClaim := cfg.TenantClaim
	if tenantClaim == "" {
		tenantClaim = defaultTenantClaim
	}

	if !cfg.DevInsecure {
		if cfg.Keyfunc == nil {
			return nil, errors.New("grpc: edge auth requires a Keyfunc unless dev mode (BOLTROPE_DEV_INSECURE) is set (fail-closed)")
		}
		if len(pinnedAlgs(cfg.Algorithms)) == 0 {
			return nil, errors.New("grpc: edge auth requires at least one pinned non-none signing algorithm (alg=none is rejected; architecture §8.7)")
		}
	}

	// Pin the accepted algorithms so the parser rejects any token whose alg is
	// outside the set — including "none" — before the key is even consulted.
	opts := []jwt.ParserOption{
		jwt.WithValidMethods(pinnedAlgs(cfg.Algorithms)),
		jwt.WithExpirationRequired(),
	}
	if cfg.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(cfg.Issuer))
	}
	if cfg.Audience != "" {
		opts = append(opts, jwt.WithAudience(cfg.Audience))
	}

	return &Authenticator{cfg: cfg, tenantClaim: tenantClaim, parserOpts: opts}, nil
}

// pinnedAlgs returns the configured algorithms with any "none" variant removed
// (case-insensitive). The unsigned-token alg is NEVER accepted, even if a
// misconfiguration lists it (defense in depth on top of the parser's own
// none-rejection).
func pinnedAlgs(algs []string) []string {
	out := make([]string, 0, len(algs))
	for _, a := range algs {
		if strings.EqualFold(strings.TrimSpace(a), "none") {
			continue
		}
		if a != "" {
			out = append(out, a)
		}
	}
	return out
}

// authenticate validates the request and returns the derived [Principal]. In dev
// mode it returns the dev principal without inspecting the token. Otherwise it
// extracts the bearer token from the gRPC `authorization` metadata and verifies
// it via [Authenticator.VerifyBearer].
func (a *Authenticator) authenticate(ctx context.Context) (Principal, error) {
	if a.cfg.DevInsecure {
		return a.devPrincipal(), nil
	}
	raw, err := bearerToken(ctx)
	if err != nil {
		return Principal{}, err
	}
	return a.VerifyBearer(raw)
}

// devPrincipal returns the configured dev principal (or the default).
func (a *Authenticator) devPrincipal() Principal {
	p := a.cfg.DevPrincipal
	if p.TenantID == "" {
		p = Principal{TenantID: DevTenantID, Subject: "dev"}
	}
	return p
}

// VerifyBearer parses and verifies a raw bearer token under the pinned policy
// and derives the tenant + subject. It is the single token-verification path
// shared by the gRPC interceptors and the REST facade, so the two transports
// can never drift (FR-API-03's "identical auth"). In dev mode the dev
// principal is returned regardless of raw (the dev edge requires no token).
func (a *Authenticator) VerifyBearer(raw string) (Principal, error) {
	if a.cfg.DevInsecure {
		return a.devPrincipal(), nil
	}

	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, a.cfg.Keyfunc, a.parserOpts...)
	if err != nil {
		return Principal{}, authError("invalid token: %v", err)
	}
	if !token.Valid {
		return Principal{}, authError("token is not valid")
	}
	// Belt-and-suspenders: reject alg=none even if the parser was somehow
	// misconfigured to allow it. A constant-time compare avoids leaking the
	// header via timing (the value is not secret, but this keeps the check
	// uniform with other rejections).
	if alg, _ := token.Header["alg"].(string); subtle.ConstantTimeCompare([]byte(strings.ToLower(alg)), []byte("none")) == 1 {
		return Principal{}, authError("unsigned tokens (alg=none) are rejected")
	}

	tenant, _ := claims[a.tenantClaim].(string)
	if tenant == "" {
		return Principal{}, authError("token carries no %q claim", a.tenantClaim)
	}
	sub, _ := claims["sub"].(string)
	return Principal{TenantID: tenant, Subject: sub}, nil
}

// bearerToken extracts the bearer token from the incoming gRPC `authorization`
// metadata header, returning UNAUTHENTICATED when absent or malformed.
func bearerToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", authError("missing request metadata")
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return "", authError("missing authorization header")
	}
	const prefix = "bearer "
	v := vals[0]
	if len(v) <= len(prefix) || !strings.EqualFold(v[:len(prefix)], prefix) {
		return "", authError("authorization header is not a bearer token")
	}
	tok := strings.TrimSpace(v[len(prefix):])
	if tok == "" {
		return "", authError("empty bearer token")
	}
	return tok, nil
}

// NewAuthInterceptor returns the unary edge-auth server interceptor: it
// validates the bearer JWT (or accepts the dev principal in dev mode) and places
// the derived [Principal] on the handler context. It is wired AFTER the platform
// logging/recovery/RBAC interceptors in the chain (this is the client-edge
// token gate, distinct from the inter-service SPIFFE RBAC gate). A config that
// is neither a valid production policy nor dev mode causes construction to fail
// closed via the returned error.
func NewAuthInterceptor(cfg AuthConfig) (grpc.UnaryServerInterceptor, error) {
	a, err := NewAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		p, err := a.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(withPrincipal(ctx, p), req)
	}, nil
}

// NewStreamAuthInterceptor is the streaming counterpart of [NewAuthInterceptor]:
// it authenticates the stream's metadata and wraps the [grpc.ServerStream] so the
// handler's stream context carries the verified [Principal].
func NewStreamAuthInterceptor(cfg AuthConfig) (grpc.StreamServerInterceptor, error) {
	a, err := NewAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		p, err := a.authenticate(ss.Context())
		if err != nil {
			return err
		}
		return handler(srv, &authServerStream{ServerStream: ss, ctx: withPrincipal(ss.Context(), p)})
	}, nil
}

// authServerStream overrides Context so the authenticated principal propagates
// to the streaming handler (the standard grpc-middleware pattern: the embedded
// ServerStream's Context is immutable, so we wrap it).
type authServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

// Context returns the principal-bearing context.
func (s *authServerStream) Context() context.Context { return s.ctx }
