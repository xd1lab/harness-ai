// Package egressclient implements the [app.WebFetcher] port: the egress DATA
// PATH for the tool-runtime's own outbound web tools (NFR-SEC-04/FR-TOOL-06 as
// amended; ADR-0021).
//
// Before this client existed the egress broker was the policy layer with no
// data path behind it — webfetch shelled out to curl inside the --network none
// sandbox, so an allowlisted host was still unreachable. The client performs
// the fetch at the trust boundary (the toolruntimed process), hardened:
//
//   - Per-request broker mediation: the [app.EgressBroker] is consulted with
//     the bare hostname for the initial request AND for every redirect hop —
//     a redirect to a non-allowlisted host is refused before it is followed.
//   - DNS-pinned dialing: the dialer resolves the hostname itself, vets every
//     address, and dials the vetted IP literally — the checked address IS the
//     dialed address, defeating DNS-rebinding TOCTOU.
//   - Public-address-only: loopback, RFC1918/ULA, link-local (incl. the cloud
//     metadata range), multicast and unspecified destinations are refused
//     (SSRF defense). Config.AllowPrivate exists for tests and deliberate
//     intranet deployments.
//   - http/https schemes only; no proxy-from-environment; bounded response
//     size, redirect count, and wall-clock time.
//
// The sandbox itself stays --network none: in-sandbox egress remains severed,
// not proxied (the sandbox-namespace proxy is roadmap; ADR-0003).
package egressclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// Defaults for the zero [Config].
const (
	// DefaultMaxBodyBytes bounds a fetched response body (2 MiB) — large pages
	// are truncated, not refused, so the model still sees the head.
	DefaultMaxBodyBytes = 2 << 20
	// DefaultTimeout bounds a whole fetch (connect + redirects + body).
	DefaultTimeout = 30 * time.Second
	// DefaultMaxRedirects bounds a redirect chain.
	DefaultMaxRedirects = 5
)

// Config parameterizes a [Client]. The zero value gets safe defaults.
type Config struct {
	// MaxBodyBytes caps the response body; larger bodies are truncated and
	// flagged. Default [DefaultMaxBodyBytes].
	MaxBodyBytes int64
	// Timeout caps a whole fetch wall-clock. Default [DefaultTimeout].
	Timeout time.Duration
	// MaxRedirects caps the redirect chain. Default [DefaultMaxRedirects].
	MaxRedirects int
	// AllowPrivate permits fetches to loopback/private/link-local addresses.
	// Default false (the SSRF-safe production posture); tests and deliberate
	// intranet deployments (e.g. a self-hosted search backend on the cluster
	// network) opt in explicitly.
	AllowPrivate bool
}

// withDefaults fills zero fields.
func (c Config) withDefaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = DefaultMaxBodyBytes
	}
	if c.Timeout <= 0 {
		c.Timeout = DefaultTimeout
	}
	if c.MaxRedirects <= 0 {
		c.MaxRedirects = DefaultMaxRedirects
	}
	return c
}

// Client is the hardened [app.WebFetcher]. Construct with [New]; safe for
// concurrent use (the underlying transport pools connections, and every
// request is independently broker-mediated).
type Client struct {
	broker    app.EgressBroker
	cfg       Config
	transport *http.Transport
}

// compile-time assertion that Client satisfies the app port.
var _ app.WebFetcher = (*Client)(nil)

// New returns a [Client] mediating every fetch through broker.
func New(broker app.EgressBroker, cfg Config) (*Client, error) {
	if broker == nil {
		return nil, errors.New("egressclient: broker must not be nil")
	}
	cfg = cfg.withDefaults()
	c := &Client{broker: broker, cfg: cfg}
	c.transport = &http.Transport{
		// No proxy-from-environment: an env-configured proxy would be an
		// unaudited egress path around the broker's host decisions.
		Proxy:                 nil,
		DialContext:           c.dialPinned,
		MaxIdleConns:          16,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: cfg.Timeout,
	}
	return c, nil
}

// Fetch implements [app.WebFetcher.Fetch]. See the package doc for the
// hardening contract.
func (c *Client) Fetch(ctx context.Context, sessionID, rawURL string) (app.FetchResult, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return app.FetchResult{}, fmt.Errorf("egressclient: egress denied: cannot parse URL %q", rawURL)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return app.FetchResult{}, fmt.Errorf("egressclient: egress denied: URL scheme %q is not allowed (http/https only)", u.Scheme)
	}
	if u.Hostname() == "" {
		return app.FetchResult{}, fmt.Errorf("egressclient: egress denied: cannot determine host from URL %q", rawURL)
	}
	if err := c.gate(ctx, sessionID, u.Hostname()); err != nil {
		return app.FetchResult{}, err
	}

	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return app.FetchResult{}, fmt.Errorf("egressclient: build request: %w", err)
	}
	req.Header.Set("User-Agent", "boltrope-webfetch/1")

	// Per-call client value: shares the pooled transport but carries a
	// CheckRedirect closure that re-gates EVERY hop against this session's
	// allowlist before it is followed.
	httpc := &http.Client{
		Transport: c.transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= c.cfg.MaxRedirects {
				return fmt.Errorf("egressclient: redirect chain exceeds %d hops", c.cfg.MaxRedirects)
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("egressclient: egress denied: redirect to scheme %q is not allowed", req.URL.Scheme)
			}
			return c.gate(req.Context(), sessionID, req.URL.Hostname())
		},
	}

	resp, err := httpc.Do(req)
	if err != nil {
		// Unwrap the *url.Error shell so the canonical egress-denied text (from
		// the redirect gate or the pinned dialer) reaches the observation.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return app.FetchResult{}, fmt.Errorf("egressclient: fetch %q: %w", rawURL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, c.cfg.MaxBodyBytes+1))
	if err != nil {
		return app.FetchResult{}, fmt.Errorf("egressclient: read response from %q: %w", rawURL, err)
	}
	truncated := int64(len(body)) > c.cfg.MaxBodyBytes
	if truncated {
		body = body[:c.cfg.MaxBodyBytes]
	}

	return app.FetchResult{
		Status:      resp.StatusCode,
		FinalURL:    resp.Request.URL.String(),
		ContentType: resp.Header.Get("Content-Type"),
		Body:        body,
		Truncated:   truncated,
	}, nil
}

// gate consults the broker with the bare hostname, failing closed on a broker
// error and carrying the canonical denial wording (FR-TOOL-06 AC-1).
func (c *Client) gate(ctx context.Context, sessionID, host string) error {
	allowed, err := c.broker.Allow(ctx, sessionID, host)
	if err != nil {
		return fmt.Errorf("egressclient: egress denied for host %q: %w", host, err)
	}
	if !allowed {
		return fmt.Errorf("egressclient: egress denied: host %q is not on the session allowlist", host)
	}
	return nil
}

// dialPinned resolves addr's hostname itself, vets every candidate address
// against the public-only policy, and dials the FIRST vetted IP literally — so
// the address that passed the check is exactly the address dialed (no
// DNS-rebinding TOCTOU). TLS verification still runs against the original
// hostname (the transport's TLS layer uses the request URL's host).
func (c *Client) dialPinned(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("egressclient: split %q: %w", addr, err)
	}

	var ips []net.IP
	if ip := net.ParseIP(host); ip != nil {
		ips = []net.IP{ip}
	} else {
		resolved, rerr := net.DefaultResolver.LookupIPAddr(ctx, host)
		if rerr != nil {
			return nil, fmt.Errorf("egressclient: resolve %q: %w", host, rerr)
		}
		for _, r := range resolved {
			ips = append(ips, r.IP)
		}
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	for _, ip := range ips {
		if !c.cfg.AllowPrivate && !isPublicAddress(ip) {
			continue
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
	}
	return nil, fmt.Errorf("egressclient: egress denied: host %q resolves only to non-public addresses (SSRF defense)", host)
}

// isPublicAddress reports whether ip is a plausibly-public unicast address.
// Loopback, RFC1918 + ULA (IsPrivate), link-local (incl. 169.254.169.254 cloud
// metadata), multicast, and unspecified addresses are all non-public.
func isPublicAddress(ip net.IP) bool {
	switch {
	case ip.IsLoopback(),
		ip.IsPrivate(),
		ip.IsLinkLocalUnicast(),
		ip.IsLinkLocalMulticast(),
		ip.IsMulticast(),
		ip.IsUnspecified():
		return false
	}
	return true
}
