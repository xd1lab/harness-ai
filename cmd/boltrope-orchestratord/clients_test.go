// Tests for the orchestrator's downstream wiring: dialDownstream must produce
// connections usable for the grpc.health.v1 readiness probe over the dev mTLS
// channel (shared-seed CA), presenting the orchestrator identity and pinning the
// model-gateway / tool-runtime SPIFFE ids the real RBAC policies admit. This is
// the inter-service-mTLS proof that /readyz would catch a shared-CA break at
// `up --wait` (FR-OBS-05).
package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// startDownstreamHealthServer stands up a shared-seed dev-mTLS gRPC server for the
// given service segment (e.g. "model-gateway"), registering the standard health
// service at the supplied status and admitting the orchestrator identity to the
// health Check (mirroring the real gatewayRBAC/toolRuntimeRBAC). It returns the
// listen address. BOLTROPE_DEV_INSECURE=1 must already be set by the caller.
func startDownstreamHealthServer(t *testing.T, td spiffeid.TrustDomain, segment string, status healthpb.HealthCheckResponse_ServingStatus) string {
	t.Helper()
	serverID, err := spiffeid.FromSegments(td, segment)
	require.NoError(t, err)
	orchID, err := spiffeid.FromSegments(td, "orchestrator")
	require.NoError(t, err)

	creds, err := grpcx.StaticDevCredentials(grpcx.StaticDevConfig{TrustDomain: td, ServerID: serverID})
	require.NoError(t, err)

	policy := grpcx.RBACPolicy{"/grpc.health.v1.Health/Check": {orchID}}
	srv, hs := grpcx.NewServer(grpcx.ServerConfig{Creds: creds, Policy: policy})
	hs.SetServingStatus("", status)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// TestDialDownstream_ReadinessChecksHandshakeOverDevMTLS dials both downstream
// services over dev mTLS and asserts the readiness checks (named per service)
// pass when both report SERVING — proving the pinned SPIFFE ids and the shared CA
// line up end-to-end.
func TestDialDownstream_ReadinessChecksHandshakeOverDevMTLS(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	td := spiffeid.RequireTrustDomainFromString("boltrope.local")

	mgwAddr := startDownstreamHealthServer(t, td, "model-gateway", healthpb.HealthCheckResponse_SERVING)
	trAddr := startDownstreamHealthServer(t, td, "tool-runtime", healthpb.HealthCheckResponse_SERVING)

	os := orchSettings{
		ModelGatewayEndpoint: mgwAddr,
		ToolRuntimeEndpoint:  trAddr,
		TrustDomain:          "boltrope.local",
	}
	down, err := dialDownstream(os, nil /*no SPIFFE source → dev fallback*/, true /*devInsecure*/)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, c := range down.closers() {
			_ = c()
		}
	})

	checks := down.readinessChecks()
	require.Len(t, checks, 2)
	assert.Equal(t, "model-gateway", checks[0].Name)
	assert.Equal(t, "tool-runtime", checks[1].Name)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, c := range checks {
		assert.NoErrorf(t, c.Probe(ctx), "downstream %q must be ready over dev mTLS", c.Name)
	}
}

// TestDialDownstream_ReadinessFailsWhenDownstreamNotServing asserts /readyz would
// stay 503 when a downstream is reachable over mTLS but reports NOT_SERVING.
func TestDialDownstream_ReadinessFailsWhenDownstreamNotServing(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")
	td := spiffeid.RequireTrustDomainFromString("boltrope.local")

	mgwAddr := startDownstreamHealthServer(t, td, "model-gateway", healthpb.HealthCheckResponse_NOT_SERVING)
	trAddr := startDownstreamHealthServer(t, td, "tool-runtime", healthpb.HealthCheckResponse_SERVING)

	os := orchSettings{ModelGatewayEndpoint: mgwAddr, ToolRuntimeEndpoint: trAddr, TrustDomain: "boltrope.local"}
	down, err := dialDownstream(os, nil, true)
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, c := range down.closers() {
			_ = c()
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	checks := down.readinessChecks()
	require.Len(t, checks, 2)
	// model-gateway is NOT_SERVING → its check must fail; tool-runtime is healthy.
	assert.Error(t, checks[0].Probe(ctx), "NOT_SERVING model-gateway must fail readiness")
	assert.NoError(t, checks[1].Probe(ctx))
}
