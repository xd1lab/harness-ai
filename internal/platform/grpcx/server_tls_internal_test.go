package grpcx

import (
	"bytes"
	"context"
	"crypto/rand"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// The health service is the one service NewServer registers itself, so its two
// RPCs are what the end-to-end tests drive through the full interceptor stack
// (no generated test stubs needed).
const (
	healthCheckMethod = "/grpc.health.v1.Health/Check"
	healthWatchMethod = "/grpc.health.v1.Health/Watch"
)

// staticSPIFFESource adapts a fixed SVID + trust bundle to the [SPIFFESource]
// interface the credential constructors consume. It stands in for the live,
// auto-rotating *workloadapi.X509Source, which only a real SPIRE agent can
// provide; the constructors only ever see the two narrow interfaces, so the
// production TLS code paths are identical.
type staticSPIFFESource struct {
	svid   *x509svid.SVID
	bundle *x509bundle.Bundle
}

func (s *staticSPIFFESource) GetX509SVID() (*x509svid.SVID, error) { return s.svid, nil }

func (s *staticSPIFFESource) GetX509BundleForTrustDomain(td spiffeid.TrustDomain) (*x509bundle.Bundle, error) {
	return s.bundle.GetX509BundleForTrustDomain(td)
}

// newSPIFFETestSource derives the shared dev CA from seed and mints an SVID
// for id under it. Each call derives the CA INDEPENDENTLY — exactly like two
// separate processes — so a server source and a client source built from the
// same seed trust each other without sharing any in-memory state.
func newSPIFFETestSource(t *testing.T, td spiffeid.TrustDomain, seed []byte, id spiffeid.ID) *staticSPIFFESource {
	t.Helper()
	ca, err := newDevCA(td, seed)
	require.NoError(t, err)
	svid, bundle, err := ca.issueSVID(id, rand.Reader)
	require.NoError(t, err)
	return &staticSPIFFESource{svid: svid, bundle: bundle}
}

// syncBuffer is a goroutine-safe bytes.Buffer: the server goroutine writes log
// records while the test goroutine reads assertions from it.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// startSPIFFEServer stands up the full production server stack — [NewServer]
// with the given credentials/policy — on a bufconn listener and marks the
// registered health service as serving.
func startSPIFFEServer(t *testing.T, creds credentials.TransportCredentials, policy RBACPolicy, logger *slog.Logger) *bufconn.Listener {
	t.Helper()
	srv, hs := NewServer(ServerConfig{Creds: creds, Policy: policy, Logger: logger})
	require.NotNil(t, srv)
	require.NotNil(t, hs, "NewServer must hand back the health server for readiness flips")
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

// dialBufconn connects to lis through the production [Dial] path (mandatory
// credentials, OTel client stats handler) using a context dialer as the Extra
// seam, so the client side under test is exactly what services wire.
func dialBufconn(t *testing.T, lis *bufconn.Listener, creds credentials.TransportCredentials) *grpc.ClientConn {
	t.Helper()
	conn, err := Dial(DialConfig{
		Target: "passthrough:///bufnet",
		Creds:  creds,
		Extra: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// TestNewServerAndDial_SPIFFEMutualTLS exercises the production TLS path end to
// end: SPIFFEServerCredentials on a NewServer-built server, SPIFFE client
// credentials on a Dial-built client, real mutual TLS over bufconn, and the
// full interceptor chain (logging → recovery → RBAC) on both unary and stream
// RPCs (architecture §8.1). The sources are built independently per identity so
// the handshake proves trust-bundle verification, not shared state.
func TestNewServerAndDial_SPIFFEMutualTLS(t *testing.T) {
	seed := []byte("spiffe-e2e-shared-seed")

	serverID, err := spiffeid.FromPath(internalTestTD, "/ns/default/sa/toolruntime")
	require.NoError(t, err)
	callerID, err := spiffeid.FromPath(internalTestTD, "/ns/default/sa/orchestrator")
	require.NoError(t, err)
	strangerID, err := spiffeid.FromPath(internalTestTD, "/ns/default/sa/modelgateway")
	require.NoError(t, err)

	serverSrc := newSPIFFETestSource(t, internalTestTD, seed, serverID)
	callerSrc := newSPIFFETestSource(t, internalTestTD, seed, callerID)
	strangerSrc := newSPIFFETestSource(t, internalTestTD, seed, strangerID)

	// Only the orchestrator identity may use the health RPCs; everyone else is
	// denied by default (the §8.1 verb gate, here exercised through NewServer's
	// own chain rather than a hand-built one).
	policy := RBACPolicy{
		healthCheckMethod: {callerID},
		healthWatchMethod: {callerID},
	}

	logBuf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(logBuf, nil))

	lis := startSPIFFEServer(t, SPIFFEServerCredentials(serverSrc, internalTestTD), policy, logger)

	t.Run("pinned client completes mTLS and the allowed unary RPC", func(t *testing.T) {
		conn := dialBufconn(t, lis, SPIFFEClientCredentials(callerSrc, serverID))
		resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err, "mTLS handshake + allowed RPC must succeed")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())

		// The logging interceptor in NewServer's chain must have emitted a
		// structured record for the handled RPC (logRPC runs before the
		// response is released to the client, so this read is ordered).
		out := logBuf.String()
		assert.Contains(t, out, healthCheckMethod, "access log must carry the RPC method")
		assert.Contains(t, out, `"OK"`, "access log must carry the resolved status code")
	})

	t.Run("trust-domain-scoped client credentials also complete the handshake", func(t *testing.T) {
		conn := dialBufconn(t, lis, SPIFFEClientCredentialsForTrustDomain(callerSrc, internalTestTD))
		resp, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
	})

	t.Run("allowed caller passes the stream chain", func(t *testing.T) {
		conn := dialBufconn(t, lis, SPIFFEClientCredentials(callerSrc, serverID))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		wc, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		first, err := wc.Recv()
		require.NoError(t, err, "stream RBAC must admit the allowed caller")
		assert.Equal(t, healthpb.HealthCheckResponse_SERVING, first.GetStatus())
	})

	t.Run("authenticated but unauthorized caller is denied on unary and stream", func(t *testing.T) {
		// The stranger's SVID chains to the same trust bundle, so the
		// HANDSHAKE succeeds — only the per-RPC verb gate rejects it. That
		// split (authn at transport, authz per RPC) is the property under test.
		conn := dialBufconn(t, lis, SPIFFEClientCredentials(strangerSrc, serverID))

		_, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		wc, err := healthpb.NewHealthClient(conn).Watch(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		_, err = wc.Recv()
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err), "stream RBAC must deny before the handler runs")
	})

	t.Run("client pinned to the wrong server identity refuses the connection", func(t *testing.T) {
		// Pinning the callee's SPIFFE ID is the confused-deputy guard: the
		// server presents a valid SVID in the trust domain, but it is not the
		// pinned identity, so the client MUST abort the handshake.
		conn := dialBufconn(t, lis, SPIFFEClientCredentials(callerSrc, strangerID))
		_, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.Unavailable, status.Code(err), "a rejected TLS handshake surfaces as Unavailable")
	})
}

// TestNewServer_DefaultsLoggerAndRegistersHealth covers the configuration
// defaults: a nil Logger falls back to slog.Default (never a nil chain), and
// the grpc.health.v1.Health service is registered on every server (FR-OBS-05).
func TestNewServer_DefaultsLoggerAndRegistersHealth(t *testing.T) {
	src := newSPIFFETestSource(t, internalTestTD, []byte("defaults-seed"),
		spiffeid.RequireFromPath(internalTestTD, "/ns/default/sa/orchestrator"))

	srv, hs := NewServer(ServerConfig{Creds: SPIFFEServerCredentials(src, internalTestTD)})
	require.NotNil(t, srv)
	require.NotNil(t, hs)
	defer srv.Stop()

	_, ok := srv.GetServiceInfo()["grpc.health.v1.Health"]
	assert.True(t, ok, "NewServer must register the standard health service")
}

// TestDial_InvalidConfigurationSurfacesError covers Dial's error path: an
// option grpc.NewClient rejects at construction time (an unparsable default
// service config) must surface as a wrapped grpcx dial error, not a panic or a
// silently-degraded connection.
func TestDial_InvalidConfigurationSurfacesError(t *testing.T) {
	src := newSPIFFETestSource(t, internalTestTD, []byte("dial-err-seed"),
		spiffeid.RequireFromPath(internalTestTD, "/ns/default/sa/orchestrator"))

	conn, err := Dial(DialConfig{
		Target: "passthrough:///irrelevant",
		Creds:  SPIFFEClientCredentialsForTrustDomain(src, internalTestTD),
		Extra:  []grpc.DialOption{grpc.WithDefaultServiceConfig(`{this is not json`)},
	})
	require.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), "grpcx: dial", "the error must be wrapped with the dial target context")
}
