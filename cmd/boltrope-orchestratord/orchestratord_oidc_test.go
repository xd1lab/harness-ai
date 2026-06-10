// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
)

// These tests close the audit finding "production auth carries a 'wired in a
// later ops task' note": the REAL pipeline — env settings → OIDC discovery →
// JWKS keyfunc → production AuthConfig → the actual edge interceptor — is
// exercised against a fake IdP (ADR-0020; FR-API-03).

const testTenant = "22222222-2222-4222-8222-222222222222"

// startFakeIDP serves an OIDC discovery document and a one-key JWKS for key.
func startFakeIDP(t *testing.T, key *rsa.PrivateKey) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var srv *httptest.Server
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   srv.URL,
			"jwks_uri": srv.URL + "/keys",
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "prod-k1",
			"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
	})
	srv = httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// mint signs an RS256 token for the fake IdP with the given claim overrides.
func mint(t *testing.T, key *rsa.PrivateKey, iss string, mutate func(jwt.MapClaims)) string {
	t.Helper()
	claims := jwt.MapClaims{
		"iss":       iss,
		"aud":       "boltrope",
		"sub":       "alice",
		"tenant_id": testTenant,
		"exp":       time.Now().Add(time.Hour).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "prod-k1"
	s, err := tok.SignedString(key)
	require.NoError(t, err)
	return s
}

// callWithBearer invokes the unary interceptor with the bearer token attached
// as gRPC metadata, returning the principal the handler observed.
func callWithBearer(t *testing.T, unary grpc.UnaryServerInterceptor, token string) (igrpc.Principal, error) {
	t.Helper()
	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs("authorization", "Bearer "+token))
	var seen igrpc.Principal
	_, err := unary(ctx, nil, &grpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		p, ok := igrpc.PrincipalFromContext(ctx)
		require.True(t, ok, "handler must observe a verified principal")
		seen = p
		return nil, nil
	})
	return seen, err
}

// TestLoadOIDCKeyfunc_ProductionRequiresIssuer pins the actionable fail-closed
// message: production without BOLTROPE_OIDC_ISSUER refuses to start and says
// exactly which knob is missing.
func TestLoadOIDCKeyfunc_ProductionRequiresIssuer(t *testing.T) {
	cfg := baseConfig(t)
	cfg.DevInsecure = false
	_, err := loadOIDCKeyfunc(context.Background(), cfg, orchSettings{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BOLTROPE_OIDC_ISSUER")
}

// TestLoadOIDCKeyfunc_DevModeNeedsNoKeyfunc pins that the dev stack does not
// contact any IdP.
func TestLoadOIDCKeyfunc_DevModeNeedsNoKeyfunc(t *testing.T) {
	cfg := baseConfig(t)
	cfg.DevInsecure = true
	kf, err := loadOIDCKeyfunc(context.Background(), cfg, orchSettings{})
	require.NoError(t, err)
	assert.Nil(t, kf)
}

// TestProductionEdgeAuth_EndToEnd wires the full production path against a
// fake IdP and asserts the interceptor's accept/reject behavior.
func TestProductionEdgeAuth_EndToEnd(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	idp := startFakeIDP(t, key)

	cfg := baseConfig(t)
	cfg.DevInsecure = false
	osettings := orchSettings{OIDCIssuer: idp.URL, OIDCAudience: "boltrope"}

	kf, err := loadOIDCKeyfunc(context.Background(), cfg, osettings)
	require.NoError(t, err, "keyfunc construction against a reachable IdP must succeed")
	require.NotNil(t, kf)

	unary, err := igrpc.NewAuthInterceptor(buildAuthConfig(cfg, osettings, kf))
	require.NoError(t, err, "production auth config with a keyfunc must construct")

	t.Run("valid token is accepted and scopes the tenant", func(t *testing.T) {
		p, err := callWithBearer(t, unary, mint(t, key, idp.URL, nil))
		require.NoError(t, err)
		assert.Equal(t, testTenant, p.TenantID)
		assert.Equal(t, "alice", p.Subject)
	})

	t.Run("expired token is rejected (FR-API-03 AC-1)", func(t *testing.T) {
		tok := mint(t, key, idp.URL, func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Hour).Unix() })
		_, err := callWithBearer(t, unary, tok)
		require.Error(t, err)
	})

	t.Run("alg=none is rejected (FR-API-03 AC-2)", func(t *testing.T) {
		none := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
			"iss": idp.URL, "aud": "boltrope", "tenant_id": testTenant,
			"exp": time.Now().Add(time.Hour).Unix(),
		})
		raw, err := none.SignedString(jwt.UnsafeAllowNoneSignatureType)
		require.NoError(t, err)
		_, err = callWithBearer(t, unary, raw)
		require.Error(t, err)
	})

	t.Run("wrong audience is rejected", func(t *testing.T) {
		tok := mint(t, key, idp.URL, func(c jwt.MapClaims) { c["aud"] = "someone-else" })
		_, err := callWithBearer(t, unary, tok)
		require.Error(t, err)
	})

	t.Run("wrong issuer is rejected", func(t *testing.T) {
		tok := mint(t, key, "https://evil.example.com", nil)
		_, err := callWithBearer(t, unary, tok)
		require.Error(t, err)
	})

	t.Run("missing tenant claim is rejected", func(t *testing.T) {
		tok := mint(t, key, idp.URL, func(c jwt.MapClaims) { delete(c, "tenant_id") })
		_, err := callWithBearer(t, unary, tok)
		require.Error(t, err)
	})

	t.Run("token signed by a foreign key is rejected", func(t *testing.T) {
		foreign, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		_, err = callWithBearer(t, unary, mint(t, foreign, idp.URL, nil))
		require.Error(t, err)
	})
}
