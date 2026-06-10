package main

import (
	"context"
	"fmt"
	"os/exec"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/daemon"
	"github.com/boltrope/boltrope/internal/platform/grpcx"
	trgrpc "github.com/boltrope/boltrope/internal/toolruntime/adapter/inbound/grpc"
)

// version is the build version stamped via -ldflags at release time.
var version = "dev"

// maxBlobBytes caps a single offloaded tool-output blob (16 MiB). Output larger
// than this is rejected by the blob store; the execute use-case truncates the
// inline view well below it (architecture §6.4).
const maxBlobBytes int64 = 16 * 1024 * 1024

// dockerReadinessTimeout bounds the `docker version` probe the readiness check
// runs so a hung CLI cannot stall /readyz.
const dockerReadinessTimeout = 2 * time.Second

// registerToolRuntimeServer attaches the tool-runtime inbound gRPC server to srv.
func registerToolRuntimeServer(srv *grpc.Server, server *trgrpc.Server) {
	genproto.RegisterToolRuntimeServiceServer(srv, server)
}

// dockerReadiness builds the /readyz check that gates on container-runtime
// availability (FR-OBS-05 seam; architecture §10.1): a tool-runtime whose docker
// CLI is absent or unresponsive is NOT ready, because it cannot provision a
// sandbox. It runs `docker version` (a cheap round-trip to the daemon) under a
// short deadline.
func dockerReadiness(ts toolSettings) daemon.ReadinessCheck {
	bin := ts.DockerBin
	if bin == "" {
		bin = "docker"
	}
	return daemon.ReadinessCheck{
		Name: "container-runtime",
		Probe: func(ctx context.Context) error {
			probeCtx, cancel := context.WithTimeout(ctx, dockerReadinessTimeout)
			defer cancel()
			if err := exec.CommandContext(probeCtx, bin, "version", "--format", "{{.Server.Version}}").Run(); err != nil {
				return fmt.Errorf("docker not available: %w", err)
			}
			return nil
		},
	}
}

// toolRuntimeRBAC is the deny-by-default per-RPC verb gate: only the orchestrator
// workload may invoke ExecuteTool/ListTools (architecture §8.1). An invalid trust
// domain yields an empty (deny-all) policy (fail-closed).
func toolRuntimeRBAC(trustDomain string) grpcx.RBACPolicy {
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
		"/boltrope.v1.ToolRuntimeService/ExecuteTool": callers,
		"/boltrope.v1.ToolRuntimeService/ListTools":   callers,
		"/grpc.health.v1.Health/Check":                callers,
		"/grpc.health.v1.Health/Watch":                callers,
	}
}
