package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// testSigningKey is the symmetric key used by the HS256 edge-auth tests. The
// production wiring uses a JWKS-backed asymmetric Keyfunc; HS256 keeps the unit
// test self-contained while still exercising signature verification, alg
// pinning, and claim checks.
var testSigningKey = []byte("test-edge-signing-key-0123456789")

// hs256Keyfunc returns the test symmetric key for HS256 tokens and refuses any
// other method, mirroring how a production Keyfunc must assert the method.
func hs256Keyfunc(t *jwt.Token) (any, error) {
	if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, jwt.ErrTokenSignatureInvalid
	}
	return testSigningKey, nil
}

// signToken signs claims with HS256 under the test key.
func signToken(t *testing.T, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString(testSigningKey)
	require.NoError(t, err)
	return s
}

// validClaims returns a set of claims that pass the prod policy (iss/aud/exp +
// tenant).
func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss":       "https://issuer.test",
		"aud":       "boltrope-orchestrator",
		"sub":       "user-1",
		"tenant_id": "tenant-A",
		"exp":       time.Now().Add(time.Hour).Unix(),
	}
}

// ctxWithBearer returns a context carrying the bearer token as gRPC incoming
// metadata.
func ctxWithBearer(token string) context.Context {
	md := metadata.New(map[string]string{"authorization": "Bearer " + token})
	return metadata.NewIncomingContext(context.Background(), md)
}

// prodAuthConfig is the pinned, HS256-verifying production policy used by tests.
func prodAuthConfig() AuthConfig {
	return AuthConfig{
		Issuer:     "https://issuer.test",
		Audience:   "boltrope-orchestrator",
		Algorithms: []string{"HS256"},
		Keyfunc:    hs256Keyfunc,
	}
}

// runAuth drives the unary auth interceptor for ctx and returns the principal the
// handler observed (or the rejection error). A passing handler records the
// principal from its context.
func runAuth(ctx context.Context, t *testing.T, cfg AuthConfig) (Principal, error) {
	t.Helper()
	interceptor, err := NewAuthInterceptor(cfg)
	require.NoError(t, err)
	var got Principal
	var seen bool
	_, err = interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/M"},
		func(hctx context.Context, _ any) (any, error) {
			got, seen = PrincipalFromContext(hctx)
			return nil, nil
		})
	if err != nil {
		return Principal{}, err
	}
	require.True(t, seen, "handler should observe a principal on success")
	return got, nil
}

func TestAuth_AcceptsValidToken(t *testing.T) {
	p, err := runAuth(ctxWithBearer(signToken(t, validClaims())), t, prodAuthConfig())
	require.NoError(t, err)
	assert.Equal(t, "tenant-A", p.TenantID)
	assert.Equal(t, "user-1", p.Subject)
}

func TestAuth_RejectsAlgNone(t *testing.T) {
	// Build an unsigned (alg=none) token manually. golang-jwt's SignedString with
	// the special UnsafeAllowNoneSignatureType produces a real alg=none token.
	claims := validClaims()
	tok := jwt.NewWithClaims(jwt.SigningMethodNone, claims)
	raw, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	_, err = runAuth(ctxWithBearer(raw), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "alg=none must be rejected (FR-API-03 AC-2)")
}

func TestAuth_RejectsBadAudience(t *testing.T) {
	claims := validClaims()
	claims["aud"] = "some-other-service"
	_, err := runAuth(ctxWithBearer(signToken(t, claims)), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_RejectsExpired(t *testing.T) {
	claims := validClaims()
	claims["exp"] = time.Now().Add(-time.Minute).Unix()
	_, err := runAuth(ctxWithBearer(signToken(t, claims)), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err), "expired token must be rejected (FR-API-03 AC-1)")
}

func TestAuth_RejectsWrongSigningKey(t *testing.T) {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims())
	raw, err := tok.SignedString([]byte("a-different-key-that-is-not-trusted"))
	require.NoError(t, err)
	_, err = runAuth(ctxWithBearer(raw), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_RejectsMissingAuthHeader(t *testing.T) {
	_, err := runAuth(context.Background(), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_RejectsAlgConfusion_RS256ExpectedButHS256Sent(t *testing.T) {
	// Policy pins RS256, but the Keyfunc returns a symmetric key. A token signed
	// HS256 must be rejected by the pinned-methods parser before the Keyfunc runs
	// (the classic alg-confusion attack).
	cfg := AuthConfig{
		Issuer:     "https://issuer.test",
		Audience:   "boltrope-orchestrator",
		Algorithms: []string{"RS256"},
		Keyfunc:    func(*jwt.Token) (any, error) { return testSigningKey, nil },
	}
	_, err := runAuth(ctxWithBearer(signToken(t, validClaims())), t, cfg)
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuth_DevModeAcceptsWithoutToken(t *testing.T) {
	cfg := AuthConfig{DevInsecure: true, DevPrincipal: Principal{TenantID: "dev-tenant", Subject: "dev-user"}}
	p, err := runAuth(context.Background(), t, cfg)
	require.NoError(t, err)
	assert.Equal(t, "dev-tenant", p.TenantID)
	assert.Equal(t, "dev-user", p.Subject)
}

func TestAuth_DevModeDefaultPrincipal(t *testing.T) {
	p, err := runAuth(context.Background(), t, AuthConfig{DevInsecure: true})
	require.NoError(t, err)
	// The default dev tenant is a VALID UUID (DevTenantID), not the literal "dev",
	// so the first CreateSession does not fail the UUID-typed tenant_id column.
	assert.Equal(t, DevTenantID, p.TenantID)
	assert.Equal(t, "dev", p.Subject)
	_, perr := uuid.Parse(p.TenantID)
	assert.NoError(t, perr, "dev tenant must parse as a UUID")
}

func TestAuth_FailsClosedWithoutKeyfuncOrDevMode(t *testing.T) {
	// Neither a Keyfunc nor dev mode: construction must fail closed.
	_, err := NewAuthInterceptor(AuthConfig{Algorithms: []string{"HS256"}})
	require.Error(t, err)
}

func TestAuth_FailsClosedWithoutPinnedAlg(t *testing.T) {
	_, err := NewAuthInterceptor(AuthConfig{Keyfunc: hs256Keyfunc})
	require.Error(t, err)
}

func TestAuth_RejectsTokenMissingTenantClaim(t *testing.T) {
	claims := validClaims()
	delete(claims, "tenant_id")
	_, err := runAuth(ctxWithBearer(signToken(t, claims)), t, prodAuthConfig())
	require.Error(t, err)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
}
