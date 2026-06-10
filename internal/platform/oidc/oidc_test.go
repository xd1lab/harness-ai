// SPDX-License-Identifier: Apache-2.0

package oidc_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/oidc"
)

// fakeIDP is a minimal OIDC provider: it serves the discovery document and a
// swappable JWKS, counting fetches so tests can assert refresh behavior.
type fakeIDP struct {
	srv *httptest.Server

	mu       sync.Mutex
	jwks     []map[string]any
	issuer   string // issuer to ADVERTISE in the discovery doc (defaults to srv.URL)
	discHits atomic.Int64
	jwksHits atomic.Int64
}

func newFakeIDP(t *testing.T) *fakeIDP {
	t.Helper()
	idp := &fakeIDP{}
	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		idp.discHits.Add(1)
		idp.mu.Lock()
		iss := idp.issuer
		idp.mu.Unlock()
		if iss == "" {
			iss = idp.srv.URL
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer":   iss,
			"jwks_uri": idp.srv.URL + "/keys",
		})
	})
	mux.HandleFunc("/keys", func(w http.ResponseWriter, _ *http.Request) {
		idp.jwksHits.Add(1)
		idp.mu.Lock()
		keys := idp.jwks
		idp.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	})
	idp.srv = httptest.NewServer(mux)
	t.Cleanup(idp.srv.Close)
	return idp
}

// setKeys swaps the served JWKS.
func (f *fakeIDP) setKeys(keys ...map[string]any) {
	f.mu.Lock()
	f.jwks = keys
	f.mu.Unlock()
}

// rsaKey generates a test RSA keypair.
func rsaKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return k
}

// jwkRSA renders pub as a signing JWK with the given kid.
func jwkRSA(kid string, pub *rsa.PublicKey) map[string]any {
	return map[string]any{
		"kty": "RSA",
		"use": "sig",
		"alg": "RS256",
		"kid": kid,
		"n":   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}

// signRS256 mints an RS256 token with the given kid ("" omits the header) and
// standard claims for issuer iss.
func signRS256(t *testing.T, key *rsa.PrivateKey, kid, iss string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":       iss,
		"sub":       "user-1",
		"tenant_id": "11111111-1111-4111-8111-111111111111",
		"exp":       time.Now().Add(time.Hour).Unix(),
	})
	if kid != "" {
		tok.Header["kid"] = kid
	}
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return s
}

// parseWith runs the token through the same pinned-RS256 parse the edge
// interceptor performs.
func parseWith(kf jwt.Keyfunc, raw string) error {
	_, err := jwt.Parse(raw, kf, jwt.WithValidMethods([]string{"RS256"}), jwt.WithExpirationRequired())
	return err
}

// newKeyfunc builds the keyfunc under test against idp with a fake clock.
func newKeyfunc(t *testing.T, idp *fakeIDP, clk *clocktest.Fake) jwt.Keyfunc {
	t.Helper()
	kf, err := oidc.NewKeyfunc(context.Background(), oidc.Config{
		IssuerURL: idp.srv.URL,
		Clock:     clk,
	})
	if err != nil {
		t.Fatalf("NewKeyfunc: %v", err)
	}
	return kf
}

// TestNewKeyfunc_VerifiesSignedToken is the happy path: discovery, JWKS fetch,
// kid-based resolution, and a successful pinned-RS256 verification.
func TestNewKeyfunc_VerifiesSignedToken(t *testing.T) {
	idp := newFakeIDP(t)
	key := rsaKey(t)
	idp.setKeys(jwkRSA("k1", &key.PublicKey))

	kf := newKeyfunc(t, idp, clocktest.NewFake(time.Unix(0, 0)))
	if err := parseWith(kf, signRS256(t, key, "k1", idp.srv.URL)); err != nil {
		t.Fatalf("expected valid token to verify, got: %v", err)
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Errorf("JWKS fetched %d times, want 1 (startup only)", got)
	}
}

// TestNewKeyfunc_IssuerMismatchFails pins OIDC Core issuer validation: a
// discovery document advertising a different issuer is a construction error
// (mix-up attack defense).
func TestNewKeyfunc_IssuerMismatchFails(t *testing.T) {
	idp := newFakeIDP(t)
	key := rsaKey(t)
	idp.setKeys(jwkRSA("k1", &key.PublicKey))
	idp.mu.Lock()
	idp.issuer = "https://evil.example.com"
	idp.mu.Unlock()

	_, err := oidc.NewKeyfunc(context.Background(), oidc.Config{IssuerURL: idp.srv.URL})
	if err == nil {
		t.Fatal("expected issuer-mismatch construction error, got nil")
	}
}

// TestNewKeyfunc_RejectsPlainHTTPNonLoopback pins the transport rule: a
// non-loopback http:// issuer is refused before any network I/O.
func TestNewKeyfunc_RejectsPlainHTTPNonLoopback(t *testing.T) {
	_, err := oidc.NewKeyfunc(context.Background(), oidc.Config{IssuerURL: "http://idp.example.com"})
	if err == nil {
		t.Fatal("expected non-https issuer to be rejected, got nil")
	}
}

// TestNewKeyfunc_EmptyJWKSFails pins fail-closed startup: an IdP serving zero
// usable signing keys cannot authenticate anyone — refuse to start.
func TestNewKeyfunc_EmptyJWKSFails(t *testing.T) {
	idp := newFakeIDP(t)
	idp.setKeys() // empty key set

	_, err := oidc.NewKeyfunc(context.Background(), oidc.Config{IssuerURL: idp.srv.URL})
	if err == nil {
		t.Fatal("expected empty-JWKS construction error, got nil")
	}
}

// TestKeyfunc_UnknownKidTriggersRefresh pins key rotation: a token signed by a
// key published AFTER startup verifies once the keyfunc re-fetches the JWKS.
func TestKeyfunc_UnknownKidTriggersRefresh(t *testing.T) {
	idp := newFakeIDP(t)
	k1, k2 := rsaKey(t), rsaKey(t)
	idp.setKeys(jwkRSA("k1", &k1.PublicKey))

	clk := clocktest.NewFake(time.Unix(0, 0))
	kf := newKeyfunc(t, idp, clk)

	// Rotate: the IdP now serves both keys (standard overlap), and enough time
	// passes that a refresh is permitted.
	idp.setKeys(jwkRSA("k1", &k1.PublicKey), jwkRSA("k2", &k2.PublicKey))
	clk.Advance(2 * time.Minute)

	if err := parseWith(kf, signRS256(t, k2, "k2", idp.srv.URL)); err != nil {
		t.Fatalf("expected rotated-key token to verify after refresh, got: %v", err)
	}
	if got := idp.jwksHits.Load(); got != 2 {
		t.Errorf("JWKS fetched %d times, want 2 (startup + rotation refresh)", got)
	}
	// The old key must still work (no eviction of the overlapping set).
	if err := parseWith(kf, signRS256(t, k1, "k1", idp.srv.URL)); err != nil {
		t.Errorf("pre-rotation key must still verify during overlap, got: %v", err)
	}
}

// TestKeyfunc_RefreshRateLimited pins the DoS guard: unknown kids cannot force
// a JWKS fetch more often than the minimum refresh interval.
func TestKeyfunc_RefreshRateLimited(t *testing.T) {
	idp := newFakeIDP(t)
	k1, evil := rsaKey(t), rsaKey(t)
	idp.setKeys(jwkRSA("k1", &k1.PublicKey))

	clk := clocktest.NewFake(time.Unix(0, 0))
	kf := newKeyfunc(t, idp, clk)

	// Within the interval: a forged-kid token must fail WITHOUT refetching.
	if err := parseWith(kf, signRS256(t, evil, "forged", idp.srv.URL)); err == nil {
		t.Fatal("expected forged-kid token to fail")
	}
	if got := idp.jwksHits.Load(); got != 1 {
		t.Fatalf("JWKS fetched %d times within the interval, want 1 (rate-limited)", got)
	}

	// After the interval: the refresh is permitted (and still fails — the IdP
	// never published the forged kid).
	clk.Advance(2 * time.Minute)
	if err := parseWith(kf, signRS256(t, evil, "forged", idp.srv.URL)); err == nil {
		t.Fatal("expected forged-kid token to fail after refresh too")
	}
	if got := idp.jwksHits.Load(); got != 2 {
		t.Errorf("JWKS fetched %d times after the interval, want 2", got)
	}
}

// TestKeyfunc_NoKidSingleKeyFallback pins the no-kid rule: accepted only when
// the set has exactly one signing key.
func TestKeyfunc_NoKidSingleKeyFallback(t *testing.T) {
	idp := newFakeIDP(t)
	key := rsaKey(t)
	idp.setKeys(jwkRSA("k1", &key.PublicKey))

	kf := newKeyfunc(t, idp, clocktest.NewFake(time.Unix(0, 0)))
	if err := parseWith(kf, signRS256(t, key, "", idp.srv.URL)); err != nil {
		t.Fatalf("expected no-kid token to verify against a single-key set, got: %v", err)
	}
}

// TestKeyfunc_NoKidMultipleKeysRejected pins the ambiguous case: with several
// keys and no kid there is nothing safe to try — reject.
func TestKeyfunc_NoKidMultipleKeysRejected(t *testing.T) {
	idp := newFakeIDP(t)
	k1, k2 := rsaKey(t), rsaKey(t)
	idp.setKeys(jwkRSA("k1", &k1.PublicKey), jwkRSA("k2", &k2.PublicKey))

	kf := newKeyfunc(t, idp, clocktest.NewFake(time.Unix(0, 0)))
	if err := parseWith(kf, signRS256(t, k1, "", idp.srv.URL)); err == nil {
		t.Fatal("expected no-kid token against a multi-key set to be rejected")
	}
}

// TestNewKeyfunc_SkipsNonSigningKeys pins JWK filtering: enc-use and non-RSA
// keys are ignored — they can neither authenticate anyone nor break startup
// when a usable signing key is present.
func TestNewKeyfunc_SkipsNonSigningKeys(t *testing.T) {
	idp := newFakeIDP(t)
	sig, enc := rsaKey(t), rsaKey(t)
	encJWK := jwkRSA("k-enc", &enc.PublicKey)
	encJWK["use"] = "enc"
	idp.setKeys(
		encJWK,
		map[string]any{"kty": "EC", "kid": "k-ec", "crv": "P-256", "x": "AQ", "y": "AQ"},
		jwkRSA("k-sig", &sig.PublicKey),
	)

	kf := newKeyfunc(t, idp, clocktest.NewFake(time.Unix(0, 0)))
	if err := parseWith(kf, signRS256(t, sig, "k-sig", idp.srv.URL)); err != nil {
		t.Fatalf("expected signing key to verify, got: %v", err)
	}
	// A token claiming the enc key's kid must NOT verify, even though that kid
	// appears in the raw JWKS document.
	if err := parseWith(kf, signRS256(t, enc, "k-enc", idp.srv.URL)); err == nil {
		t.Fatal("expected enc-use key to be unusable for signature verification")
	}
}
