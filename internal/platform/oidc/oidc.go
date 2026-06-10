// SPDX-License-Identifier: Apache-2.0

// Package oidc turns an OIDC issuer URL into a [jwt.Keyfunc] for the
// orchestrator's client-edge auth interceptor (FR-API-03; ADR-0020).
//
// It performs OIDC discovery (RFC 8414 / OIDC Core §4) against the issuer's
// /.well-known/openid-configuration, validates that the advertised issuer
// matches the configured one (mix-up attack defense), fetches the JWKS the
// document points at, and resolves verification keys by `kid` with
// refresh-on-miss for key rotation.
//
// # Posture
//
//   - Fail-closed startup: an unreachable IdP, an issuer mismatch, or a JWKS
//     with zero usable signing keys is a construction error — the caller
//     (orchestratord wiring) refuses to start rather than serve an edge it
//     cannot verify tokens for (NFR-SEC-01).
//   - Rotation never restarts the daemon: an unknown `kid` triggers an inline
//     JWKS re-fetch, rate-limited by [Config.MinRefreshInterval] so a flood of
//     forged kids cannot turn the IdP into a DoS amplifier.
//   - Transport hygiene: issuer and jwks_uri MUST be https (loopback hosts
//     excepted, for tests and sidecar IdPs); responses are size-capped and the
//     HTTP client carries a timeout.
//   - v1 parses RSA signing keys only (RS256 is the universal IdP default);
//     `use:"enc"` and non-RSA keys are skipped. See ADR-0020 §Alternatives.
//
// The package is dependency-light by design: stdlib + golang-jwt (already a
// module dependency) + the platform clock port. No OIDC SDK at the trust
// boundary (mirrors the MCP-client / docker-CLI decisions, ADR-0005).
package oidc

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/xd1lab/harness-ai/internal/platform/clock"
)

// Defaults applied by [NewKeyfunc] when the corresponding [Config] field is
// zero.
const (
	// DefaultMinRefreshInterval is the minimum delay between two JWKS fetches
	// triggered by unknown kids (the rotation refresh rate limit).
	DefaultMinRefreshInterval = time.Minute
	// DefaultHTTPTimeout bounds each discovery/JWKS request when no custom
	// HTTP client is supplied.
	DefaultHTTPTimeout = 10 * time.Second
	// DefaultMaxResponseBytes caps the size of a discovery or JWKS response.
	DefaultMaxResponseBytes = 1 << 20 // 1 MiB
)

// Config parameterizes [NewKeyfunc].
type Config struct {
	// IssuerURL is the OIDC issuer (e.g. "https://idp.example.com/realms/prod").
	// Required. It MUST be https unless the host is loopback. The discovery
	// document's `issuer` field must match it exactly (modulo one trailing
	// slash).
	IssuerURL string
	// HTTPClient performs the discovery and JWKS requests. Nil means a default
	// client with [DefaultHTTPTimeout].
	HTTPClient *http.Client
	// Clock drives the refresh rate limit; nil means [clock.System]. Injected
	// so rotation behavior is deterministic under test (NFR-TEST-01).
	Clock clock.Clock
	// MinRefreshInterval rate-limits unknown-kid JWKS re-fetches; zero means
	// [DefaultMinRefreshInterval].
	MinRefreshInterval time.Duration
	// MaxResponseBytes caps discovery/JWKS response sizes; zero means
	// [DefaultMaxResponseBytes].
	MaxResponseBytes int64
}

// NewKeyfunc discovers the issuer's JWKS and returns a [jwt.Keyfunc] that
// resolves verification keys by `kid`, re-fetching the JWKS on unknown kids
// (rate-limited) so IdP key rotation needs no restart. Construction performs
// the discovery and the initial JWKS fetch and fails closed on any problem.
func NewKeyfunc(ctx context.Context, cfg Config) (jwt.Keyfunc, error) {
	if cfg.IssuerURL == "" {
		return nil, errors.New("oidc: IssuerURL is required")
	}
	if err := checkTransport(cfg.IssuerURL); err != nil {
		return nil, err
	}

	p := &provider{
		client:     cfg.HTTPClient,
		clk:        cfg.Clock,
		minRefresh: cfg.MinRefreshInterval,
		maxBytes:   cfg.MaxResponseBytes,
	}
	if p.client == nil {
		p.client = &http.Client{Timeout: DefaultHTTPTimeout}
	}
	if p.clk == nil {
		p.clk = clock.System{}
	}
	if p.minRefresh <= 0 {
		p.minRefresh = DefaultMinRefreshInterval
	}
	if p.maxBytes <= 0 {
		p.maxBytes = DefaultMaxResponseBytes
	}

	jwksURI, err := p.discover(ctx, cfg.IssuerURL)
	if err != nil {
		return nil, err
	}
	p.jwksURI = jwksURI

	keys, err := p.fetchJWKS(ctx)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("oidc: JWKS at %q contains no usable RSA signing keys (fail-closed)", jwksURI)
	}
	p.keys = keys
	p.lastFetch = p.clk.Now()

	return p.keyfunc, nil
}

// provider holds the discovered JWKS state behind a mutex. The mutex also
// single-flights rotation refreshes: concurrent unknown-kid requests serialize
// on it rather than stampeding the IdP.
type provider struct {
	client     *http.Client
	clk        clock.Clock
	minRefresh time.Duration
	maxBytes   int64
	jwksURI    string

	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	lastFetch time.Time
}

// discoveryDoc is the subset of the OIDC discovery document this package
// consumes.
type discoveryDoc struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

// discover fetches the issuer's discovery document, validates the advertised
// issuer, and returns the jwks_uri.
func (p *provider) discover(ctx context.Context, issuer string) (string, error) {
	wellKnown := strings.TrimSuffix(issuer, "/") + "/.well-known/openid-configuration"
	var doc discoveryDoc
	if err := p.getJSON(ctx, wellKnown, &doc); err != nil {
		return "", fmt.Errorf("oidc: discovery at %q: %w", wellKnown, err)
	}
	// OIDC Core issuer validation: the document must assert the issuer we were
	// configured with — anything else means we are talking to the wrong (or a
	// hostile) endpoint.
	if strings.TrimSuffix(doc.Issuer, "/") != strings.TrimSuffix(issuer, "/") {
		return "", fmt.Errorf("oidc: discovery document advertises issuer %q, configured issuer is %q (mix-up defense, fail-closed)", doc.Issuer, issuer)
	}
	if doc.JWKSURI == "" {
		return "", errors.New("oidc: discovery document carries no jwks_uri")
	}
	if err := checkTransport(doc.JWKSURI); err != nil {
		return "", err
	}
	return doc.JWKSURI, nil
}

// jwk is the subset of an RFC 7517 JSON Web Key this package consumes.
type jwk struct {
	Kty string `json:"kty"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// fetchJWKS retrieves and parses the key set, keeping only RSA signing keys.
func (p *provider) fetchJWKS(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	var set struct {
		Keys []jwk `json:"keys"`
	}
	if err := p.getJSON(ctx, p.jwksURI, &set); err != nil {
		return nil, fmt.Errorf("oidc: fetch JWKS at %q: %w", p.jwksURI, err)
	}
	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "RSA" {
			continue // non-RSA keys are out of scope in v1 (ADR-0020)
		}
		if k.Use != "" && k.Use != "sig" {
			continue // encryption keys can never verify a signature
		}
		pub, err := parseRSA(k)
		if err != nil {
			// A malformed key is skipped, not fatal: the overlap set may still
			// carry a valid one; zero usable keys is caught by the caller.
			continue
		}
		keys[k.Kid] = pub
	}
	return keys, nil
}

// parseRSA builds an *rsa.PublicKey from the JWK's base64url n/e fields.
func parseRSA(k jwk) (*rsa.PublicKey, error) {
	nb, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("oidc: JWK %q: bad modulus: %w", k.Kid, err)
	}
	eb, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("oidc: JWK %q: bad exponent: %w", k.Kid, err)
	}
	if len(nb) == 0 || len(eb) == 0 || len(eb) > 4 {
		return nil, fmt.Errorf("oidc: JWK %q: implausible RSA parameters", k.Kid)
	}
	e := new(big.Int).SetBytes(eb)
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: int(e.Int64())}, nil
}

// keyfunc is the [jwt.Keyfunc] handed to the parser. It is invoked only after
// the interceptor's parser has validated the token's alg against the pinned
// set, so it never sees alg=none.
func (p *provider) keyfunc(token *jwt.Token) (any, error) {
	kid, _ := token.Header["kid"].(string)
	return p.resolve(kid)
}

// resolve returns the verification key for kid, re-fetching the JWKS once
// (rate-limited) when the kid is unknown — the rotation path.
func (p *provider) resolve(kid string) (*rsa.PublicKey, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if kid == "" {
		// No kid: safe only when there is exactly one candidate.
		if len(p.keys) == 1 {
			for _, pub := range p.keys {
				return pub, nil
			}
		}
		return nil, fmt.Errorf("oidc: token has no kid and the JWKS has %d signing keys", len(p.keys))
	}

	if pub, ok := p.keys[kid]; ok {
		return pub, nil
	}

	// Unknown kid: permit one refresh per MinRefreshInterval (rotation), so a
	// forged-kid flood cannot hammer the IdP. lastFetch advances on the ATTEMPT
	// (not only on success) so a down IdP is also shielded.
	if p.clk.Since(p.lastFetch) >= p.minRefresh {
		p.lastFetch = p.clk.Now()
		ctx, cancel := context.WithTimeout(context.Background(), p.client.Timeout)
		keys, err := p.fetchJWKS(ctx)
		cancel()
		if err == nil && len(keys) > 0 {
			p.keys = keys
		}
		if pub, ok := p.keys[kid]; ok {
			return pub, nil
		}
	}
	return nil, fmt.Errorf("oidc: no key for kid %q", kid)
}

// getJSON fetches url and decodes the (size-capped) JSON body into v.
func (p *provider) getJSON(ctx context.Context, rawURL string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", resp.Status)
	}
	body := io.LimitReader(resp.Body, p.maxBytes)
	return json.NewDecoder(body).Decode(v)
}

// checkTransport enforces https for non-loopback hosts.
func checkTransport(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("oidc: invalid URL %q: %w", rawURL, err)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		host := u.Hostname()
		if host == "localhost" {
			return nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
			return nil
		}
		return fmt.Errorf("oidc: plain http is only permitted for loopback hosts, got %q (use https)", rawURL)
	default:
		return fmt.Errorf("oidc: unsupported URL scheme %q in %q", u.Scheme, rawURL)
	}
}
