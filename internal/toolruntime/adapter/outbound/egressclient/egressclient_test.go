package egressclient_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/egressclient"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

var ctx = context.Background()

// allowBroker is a minimal EgressBroker fake recording every Allow consult and
// permitting exactly the configured hosts.
type allowBroker struct {
	mu      sync.Mutex
	allowed map[string]bool
	asked   []string
}

func newAllowBroker(hosts ...string) *allowBroker {
	m := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		m[h] = true
	}
	return &allowBroker{allowed: m}
}

func (b *allowBroker) Allow(_ context.Context, _, host string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.asked = append(b.asked, host)
	return b.allowed[host], nil
}

func (b *allowBroker) SetPolicy(context.Context, app.EgressPolicy) error { return nil }

func (b *allowBroker) askedHosts() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.asked...)
}

// hostOf extracts the hostname (no port) of a httptest server URL.
func hostOf(t *testing.T, server *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	return u.Hostname()
}

// newClient builds a Client with AllowPrivate set so tests can target the
// loopback httptest servers.
func newClient(t *testing.T, broker app.EgressBroker, cfg egressclient.Config) *egressclient.Client {
	t.Helper()
	cfg.AllowPrivate = true
	c, err := egressclient.New(broker, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// TestFetch_AllowedHostReturnsBody is the happy path: an allowlisted host is
// fetched and the body/status/content-type come back.
func TestFetch_AllowedHostReturnsBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, "<html>hello</html>")
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{})
	res, err := c.Fetch(ctx, "sess-1", server.URL)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if res.Status != http.StatusOK {
		t.Errorf("Status = %d, want 200", res.Status)
	}
	if got := string(res.Body); got != "<html>hello</html>" {
		t.Errorf("Body = %q", got)
	}
	if !strings.Contains(res.ContentType, "text/html") {
		t.Errorf("ContentType = %q, want text/html", res.ContentType)
	}
	if res.Truncated {
		t.Error("Truncated = true for a small body")
	}
}

// TestFetch_DeniedHostNeverDials proves the deny path: the broker says no, the
// canonical egress-denied message comes back, and NO request reaches the server.
func TestFetch_DeniedHostNeverDials(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	broker := newAllowBroker() // empty allowlist: deny all

	c := newClient(t, broker, egressclient.Config{})
	_, err := c.Fetch(ctx, "sess-1", server.URL)
	if err == nil {
		t.Fatal("Fetch of a denied host returned nil error")
	}
	wantMsg := fmt.Sprintf("egress denied: host %q is not on the session allowlist", hostOf(t, server))
	if !strings.Contains(err.Error(), wantMsg) {
		t.Errorf("error = %q, want it to contain %q", err, wantMsg)
	}
	if hits.Load() != 0 {
		t.Errorf("denied fetch reached the server %d times, want 0", hits.Load())
	}
}

// TestFetch_RedirectToDeniedHostBlocked: hop 1 is allowlisted, hop 2 is not —
// the redirect must be re-gated and refused, and the denied server never hit.
func TestFetch_RedirectToDeniedHostBlocked(t *testing.T) {
	var deniedHits atomic.Int64
	denied := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		deniedHits.Add(1)
	}))
	defer denied.Close()

	// Both servers listen on 127.0.0.1, and allowlists are HOST-based — so the
	// redirect targets http://localhost:<port> to give hop 2 a hostname distinct
	// from the allowlisted "127.0.0.1".
	target := strings.Replace(denied.URL, "127.0.0.1", "localhost", 1)
	allowedRedir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target, http.StatusFound)
	}))
	defer allowedRedir.Close()
	broker := newAllowBroker(hostOf(t, allowedRedir))

	c := newClient(t, broker, egressclient.Config{})
	_, err := c.Fetch(ctx, "sess-1", allowedRedir.URL)
	if err == nil {
		t.Fatal("redirect to a non-allowlisted host returned nil error")
	}
	if !strings.Contains(err.Error(), `egress denied: host "localhost" is not on the session allowlist`) {
		t.Errorf("error = %q, want the canonical denial for the redirect target", err)
	}
	if deniedHits.Load() != 0 {
		t.Errorf("denied redirect target was hit %d times, want 0", deniedHits.Load())
	}
}

// TestFetch_RedirectToAllowedHostFollowed: a same-host redirect is followed and
// FinalURL reflects the landing page.
func TestFetch_RedirectToAllowedHostFollowed(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/final", http.StatusFound)
	})
	mux.HandleFunc("/final", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "landed")
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{})
	res, err := c.Fetch(ctx, "sess-1", server.URL+"/start")
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if string(res.Body) != "landed" {
		t.Errorf("Body = %q, want %q", res.Body, "landed")
	}
	if !strings.HasSuffix(res.FinalURL, "/final") {
		t.Errorf("FinalURL = %q, want .../final", res.FinalURL)
	}
}

// TestFetch_RedirectCapEnforced: an endless redirect chain stops at the cap.
func TestFetch_RedirectCapEnforced(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// G710: the redirect target is this test server's own URL, not user
		// input — the test deliberately builds an endless same-host chain.
		http.Redirect(w, r, server.URL+r.URL.Path+"x", http.StatusFound) //nolint:gosec // G710: self-referential test redirect, not tainted input
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{MaxRedirects: 3})
	_, err := c.Fetch(ctx, "sess-1", server.URL+"/r")
	if err == nil {
		t.Fatal("endless redirect chain returned nil error")
	}
	if !strings.Contains(err.Error(), "redirect") {
		t.Errorf("error = %q, want a redirect-cap message", err)
	}
}

// TestFetch_BodyTruncatedAtCap: a body over MaxBodyBytes comes back cut with
// Truncated=true.
func TestFetch_BodyTruncatedAtCap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, strings.Repeat("a", 5000))
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{MaxBodyBytes: 1024})
	res, err := c.Fetch(ctx, "sess-1", server.URL)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if len(res.Body) != 1024 {
		t.Errorf("len(Body) = %d, want 1024", len(res.Body))
	}
	if !res.Truncated {
		t.Error("Truncated = false for an over-cap body")
	}
}

// TestFetch_NonHTTPSchemeDenied: anything but http/https is refused before any
// broker consult or dial.
func TestFetch_NonHTTPSchemeDenied(t *testing.T) {
	broker := newAllowBroker("example.com")
	c := newClient(t, broker, egressclient.Config{})

	for _, raw := range []string{"ftp://example.com/x", "file:///etc/passwd", "gopher://example.com"} {
		_, err := c.Fetch(ctx, "sess-1", raw)
		if err == nil {
			t.Fatalf("Fetch(%q) returned nil error", raw)
		}
		if !strings.Contains(err.Error(), "egress denied") {
			t.Errorf("Fetch(%q) error = %q, want an egress-denied message", raw, err)
		}
	}
	if asked := broker.askedHosts(); len(asked) != 0 {
		t.Errorf("scheme-denied fetches still consulted the broker for %v", asked)
	}
}

// TestFetch_NonPublicAddressDeniedInProduction: with AllowPrivate unset (the
// production default), an allowlisted host that resolves to a loopback/private
// address is refused at dial time (SSRF defense) — the httptest server IS the
// non-public address here.
func TestFetch_NonPublicAddressDeniedInProduction(t *testing.T) {
	var hits atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server), "localhost")

	c, err := egressclient.New(broker, egressclient.Config{}) // AllowPrivate=false
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = c.Fetch(ctx, "sess-1", server.URL)
	if err == nil {
		t.Fatal("fetch of a loopback address with AllowPrivate=false returned nil error")
	}
	if !strings.Contains(err.Error(), "non-public") {
		t.Errorf("error = %q, want a non-public-address denial", err)
	}
	if hits.Load() != 0 {
		t.Errorf("non-public fetch reached the server %d times, want 0", hits.Load())
	}
}

// TestFetch_BrokerConsultedWithHostnameOnly: the broker sees the bare hostname
// (allowlists are host-based), never host:port.
func TestFetch_BrokerConsultedWithHostnameOnly(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "ok")
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{})
	if _, err := c.Fetch(ctx, "sess-1", server.URL); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	for _, asked := range broker.askedHosts() {
		if strings.Contains(asked, ":") {
			t.Errorf("broker consulted with %q; want bare hostname", asked)
		}
	}
}

// TestFetch_Non2xxStillReturnsResult: HTTP errors are results, not transport
// errors — the tool layer decides how to surface them to the model.
func TestFetch_Non2xxStillReturnsResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusForbidden)
	}))
	defer server.Close()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{})
	res, err := c.Fetch(ctx, "sess-1", server.URL)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if res.Status != http.StatusForbidden {
		t.Errorf("Status = %d, want 403", res.Status)
	}
}

// TestFetch_TimeoutEnforced: a server slower than the configured timeout fails
// the fetch instead of hanging the tool call.
func TestFetch_TimeoutEnforced(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		_, _ = fmt.Fprint(w, "late")
	}))
	defer func() { close(release); server.Close() }()
	broker := newAllowBroker(hostOf(t, server))

	c := newClient(t, broker, egressclient.Config{Timeout: 100 * time.Millisecond})
	start := time.Now()
	_, err := c.Fetch(ctx, "sess-1", server.URL)
	if err == nil {
		t.Fatal("slow fetch returned nil error")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("timeout took %v, want ~100ms", elapsed)
	}
}

// Compile-time assertion: the client satisfies the app port.
var _ app.WebFetcher = (*egressclient.Client)(nil)
