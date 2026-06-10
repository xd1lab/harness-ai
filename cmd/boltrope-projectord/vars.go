package main

import (
	"github.com/xd1lab/harness-ai/internal/platform/grpcx"
)

// version is the build version stamped via -ldflags at release time.
var version = "dev"

// projectorRBAC is the per-RPC verb gate for projectord. projectord exposes NO
// application RPCs — only grpc.health.v1, and its real operational probes are the
// HTTP /livez and /readyz endpoints (which carry no RBAC). The gRPC surface is
// therefore deny-by-default (an empty policy): there is no inter-service RPC for
// a peer to call, so nothing needs to be allowed (architecture §8.1, §10.4).
func projectorRBAC(_ string) grpcx.RBACPolicy {
	return grpcx.RBACPolicy{}
}
