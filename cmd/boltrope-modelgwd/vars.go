package main

import (
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	mgwgrpc "github.com/boltrope/boltrope/internal/modelgateway/adapter/inbound/grpc"
	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// version is the build version stamped via -ldflags at release time; it labels
// the OTel service.version resource attribute. It defaults to "dev" for local
// builds.
var version = "dev"

// Retry-policy defaults for the harness retry decorator wrapping the provider
// (FR-MODEL-05; NFR-REL-02). They are conservative: a few attempts with bounded
// exponential backoff + jitter.
const (
	defaultRetryMaxAttempts = 3
	defaultRetryBaseDelay   = 200 * time.Millisecond
	defaultRetryMaxDelay    = 5 * time.Second
)

// registerGatewayServer attaches the model-gateway inbound gRPC server to srv.
func registerGatewayServer(srv *grpc.Server, server *mgwgrpc.Server) {
	genproto.RegisterModelGatewayServiceServer(srv, server)
}

// gatewayRBAC is the deny-by-default per-RPC verb gate for the model gateway:
// only the orchestrator workload (spiffe://<td>/orchestrator) may invoke the
// generation RPCs (architecture §8.1). The trust domain is parsed leniently; an
// invalid value yields an empty policy that denies everything (fail-closed).
func gatewayRBAC(trustDomain string) grpcx.RBACPolicy {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return grpcx.RBACPolicy{}
	}
	orchestrator, err := spiffeid.FromSegments(td, "orchestrator")
	if err != nil {
		return grpcx.RBACPolicy{}
	}
	callers := []spiffeid.ID{orchestrator}
	return grpcx.RBACPolicy{
		"/boltrope.v1.ModelGatewayService/Generate":        callers,
		"/boltrope.v1.ModelGatewayService/CountTokens":     callers,
		"/boltrope.v1.ModelGatewayService/GetCapabilities": callers,
		"/grpc.health.v1.Health/Check":                     callers,
		"/grpc.health.v1.Health/Watch":                     callers,
	}
}
