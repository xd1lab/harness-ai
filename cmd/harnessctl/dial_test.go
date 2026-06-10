// Tests for harnessctl's transport-security selection (dial): the dev mTLS path
// (BOLTROPE_DEV_INSECURE=1) over the shared-seed dev CA, the plaintext --insecure
// path, the no-mode error, and dev-over-insecure precedence. The dev mTLS test
// stands up a real gRPC server whose credentials come from the SAME shared-seed
// dev CA so the cross-process mutual-TLS handshake — the property the compose
// Quickstart relies on — is actually exercised (T-CMD-03 / DOD-09).
package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	boltropev1 "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

const testTrustDomain = "boltrope.local"

// grpcDialTestTimeout bounds a single RPC in the handshake tests so a wedged
// dial fails the test fast instead of hanging.
const grpcDialTestTimeout = 5 * time.Second

// TestDial_PlaintextInsecure asserts that with --insecure (and no dev env) dial
// returns a usable plaintext connection.
func TestDial_PlaintextInsecure(t *testing.T) {
	t.Setenv(devInsecureEnv, "") // ensure the dev path is off
	conn, err := dial(&cliConfig{Endpoint: "passthrough:///bufnet", Insecure: true})
	require.NoError(t, err)
	require.NotNil(t, conn)
	t.Cleanup(func() { _ = conn.Close() })
}

// TestDial_NoModeReturnsGuidanceError asserts that with neither --insecure nor
// the dev env, dial fails with a message naming BOLTROPE_DEV_INSECURE (no silent
// open/plaintext path).
func TestDial_NoModeReturnsGuidanceError(t *testing.T) {
	t.Setenv(devInsecureEnv, "")
	_, err := dial(&cliConfig{Endpoint: "localhost:9000"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), devInsecureEnv)
}

// TestDial_DevMTLSHandshakeAgainstSharedCA is the core cross-process proof: with
// BOLTROPE_DEV_INSECURE=1 the CLI dials over the shared-seed dev CA and completes
// mutual TLS against a server whose credentials are minted from the SAME shared
// CA — exactly the orchestrator compose edge. The server's RBAC admits only the
// edge identity (spiffe://<td>/edge) the CLI presents, so a successful
// CreateSession proves both the handshake and that the CLI uses the RBAC-admitted
// client identity.
func TestDial_DevMTLSHandshakeAgainstSharedCA(t *testing.T) {
	t.Setenv(devInsecureEnv, "1")

	td := spiffeid.RequireTrustDomainFromString(testTrustDomain)
	serverID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/orchestrator")
	edgeID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/edge")

	// Server credentials from the shared-seed dev CA (the same constructor the
	// daemon uses). It authorizes any peer in the trust domain at the TLS layer;
	// the RBAC interceptor is the verb gate.
	srvCreds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{
		TrustDomain: td,
		ServerID:    serverID,
	})
	require.NoError(t, err)

	// Mirror the orchestrator's RBAC: only the edge identity may call the
	// client-facing RPCs.
	policy := grpcx.RBACPolicy{
		"/boltrope.v1.OrchestratorService/CreateSession": {edgeID},
	}
	gsrv, _ := grpcx.NewServer(grpcx.ServerConfig{Creds: srvCreds, Policy: policy})
	boltropev1.RegisterOrchestratorServiceServer(gsrv, &fakeOrchestrator{createSessionID: "sess-mtls-1"})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.GracefulStop)

	// Dial through the real CLI path. TrustDomain left empty to exercise the
	// boltrope.local default.
	conn, err := dial(&cliConfig{Endpoint: lis.Addr().String()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), grpcDialTestTimeout)
	defer cancel()
	resp, err := client.CreateSession(ctx, &boltropev1.CreateSessionRequest{TenantId: "demo"})
	require.NoError(t, err, "dev mTLS CreateSession must succeed against the shared-CA server")
	assert.Equal(t, "sess-mtls-1", resp.GetSessionId())
}

// TestDial_DevMTLSWrongPinIsRejected asserts the callee pin is enforced: pinning
// a server id that does not match the server's SVID fails the handshake, proving
// the CLI is not accepting any peer in the trust domain.
func TestDial_DevMTLSWrongPinIsRejected(t *testing.T) {
	t.Setenv(devInsecureEnv, "1")

	td := spiffeid.RequireTrustDomainFromString(testTrustDomain)
	serverID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/orchestrator")
	edgeID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/edge")

	srvCreds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{TrustDomain: td, ServerID: serverID})
	require.NoError(t, err)
	policy := grpcx.RBACPolicy{"/boltrope.v1.OrchestratorService/CreateSession": {edgeID}}
	gsrv, _ := grpcx.NewServer(grpcx.ServerConfig{Creds: srvCreds, Policy: policy})
	boltropev1.RegisterOrchestratorServiceServer(gsrv, &fakeOrchestrator{createSessionID: "x"})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.GracefulStop)

	// Pin the WRONG callee id (tool-runtime), which the server's SVID
	// (orchestrator) does not satisfy → handshake must fail.
	conn, err := dial(&cliConfig{
		Endpoint: lis.Addr().String(),
		ServerID: "spiffe://" + testTrustDomain + "/tool-runtime",
	})
	require.NoError(t, err) // lazy dial: error surfaces on first RPC
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), grpcDialTestTimeout)
	defer cancel()
	_, err = client.CreateSession(ctx, &boltropev1.CreateSessionRequest{TenantId: "demo"})
	require.Error(t, err, "pinning the wrong server SPIFFE id must fail the handshake")
}

// TestDial_DevInsecureTakesPrecedenceOverInsecure asserts that when both
// BOLTROPE_DEV_INSECURE=1 and --insecure are set, the dev mTLS path wins (it is
// the documented compose path), so a plaintext-only client never silently talks
// to the mTLS edge. We assert by completing an mTLS RPC even though Insecure is
// true.
func TestDial_DevInsecureTakesPrecedenceOverInsecure(t *testing.T) {
	t.Setenv(devInsecureEnv, "1")

	td := spiffeid.RequireTrustDomainFromString(testTrustDomain)
	serverID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/orchestrator")
	edgeID := spiffeid.RequireFromString("spiffe://" + testTrustDomain + "/edge")

	srvCreds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{TrustDomain: td, ServerID: serverID})
	require.NoError(t, err)
	policy := grpcx.RBACPolicy{"/boltrope.v1.OrchestratorService/CreateSession": {edgeID}}
	gsrv, _ := grpcx.NewServer(grpcx.ServerConfig{Creds: srvCreds, Policy: policy})
	boltropev1.RegisterOrchestratorServiceServer(gsrv, &fakeOrchestrator{createSessionID: "sess-prec-1"})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = gsrv.Serve(lis) }()
	t.Cleanup(gsrv.GracefulStop)

	conn, err := dial(&cliConfig{Endpoint: lis.Addr().String(), Insecure: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := boltropev1.NewOrchestratorServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), grpcDialTestTimeout)
	defer cancel()
	resp, err := client.CreateSession(ctx, &boltropev1.CreateSessionRequest{TenantId: "demo"})
	require.NoError(t, err, "dev mTLS must take precedence over --insecure")
	assert.Equal(t, "sess-prec-1", resp.GetSessionId())
}

// TestDevClientCreds_InvalidTrustDomain asserts a malformed trust domain is a
// clear construction error (not a panic).
func TestDevClientCreds_InvalidTrustDomain(t *testing.T) {
	t.Setenv(devInsecureEnv, "1")
	_, err := devClientCreds(&cliConfig{TrustDomain: "not a domain"})
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "trust domain")
}
