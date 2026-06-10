package main

import (
	"github.com/spiffe/go-spiffe/v2/spiffeid"

	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// version is the build version stamped via -ldflags at release time.
var version = "dev"

// orchestratorRBAC is the deny-by-default per-RPC SPIFFE verb gate for the
// orchestrator's inbound gRPC. The orchestrator's public edge is fronted by a
// mesh proxy / edge workload that presents the spiffe://<td>/edge identity and
// carries the client's bearer token forward; the per-call client identity is then
// established by the JWT edge-auth interceptor (architecture §8.7). The coarse
// SPIFFE gate here admits that edge identity (and the health probe) to the
// client-facing methods; cross-tenant ownership is still enforced per-RPC by the
// handler + RLS. An invalid trust domain yields an empty (deny-all) policy.
func orchestratorRBAC(trustDomain string) grpcx.RBACPolicy {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return grpcx.RBACPolicy{}
	}
	edge, err := spiffeid.FromSegments(td, "edge")
	if err != nil {
		return grpcx.RBACPolicy{}
	}
	callers := []spiffeid.ID{edge}
	return grpcx.RBACPolicy{
		"/boltrope.v1.OrchestratorService/CreateSession": callers,
		"/boltrope.v1.OrchestratorService/GetSession":    callers,
		"/boltrope.v1.OrchestratorService/Run":           callers,
		"/boltrope.v1.OrchestratorService/Control":       callers,
		"/boltrope.v1.OrchestratorService/Fork":          callers,
		"/grpc.health.v1.Health/Check":                   callers,
		"/grpc.health.v1.Health/Watch":                   callers,
	}
}
