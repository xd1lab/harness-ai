package main

import (
	"fmt"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/orchestrator/adapter/outbound/modelgw"
	"github.com/boltrope/boltrope/internal/orchestrator/adapter/outbound/toolrt"
	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/platform/daemon"
	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// downstream bundles the dialed gRPC client connections to the model-gateway and
// tool-runtime services so the daemon can close them on shutdown and probe their
// health for /readyz. The connections are lazy (grpc.NewClient dials on first
// RPC), so dialing here never blocks startup even if a downstream service is not
// yet up.
type downstream struct {
	model app.ModelGatewayPort
	tools app.ToolRuntimePort

	// modelConn / toolsConn are the raw connections behind the adapters, kept so
	// the orchestrator can run a grpc.health.v1 readiness probe over the SAME mTLS
	// channel it serves traffic on (so /readyz catches an inter-service mTLS break
	// — e.g. a dev-CA mismatch — at `up --wait`).
	modelConn *grpc.ClientConn
	toolsConn *grpc.ClientConn

	conns []*grpc.ClientConn
}

// readinessChecks returns the downstream gRPC-health checks gating /readyz on the
// model-gateway AND tool-runtime being reachable over the inter-service mTLS
// channel and reporting SERVING (FR-OBS-05).
func (d *downstream) readinessChecks() []daemon.ReadinessCheck {
	return []daemon.ReadinessCheck{
		daemon.GRPCHealthReadiness("model-gateway", d.modelConn),
		daemon.GRPCHealthReadiness("tool-runtime", d.toolsConn),
	}
}

// closers returns the connection-close functions for the daemon's shutdown
// closer list.
func (d *downstream) closers() []func() error {
	out := make([]func() error, 0, len(d.conns))
	for _, c := range d.conns {
		c := c
		out = append(out, c.Close)
	}
	return out
}

// dialDownstream dials the model-gateway and tool-runtime services over mTLS and
// wraps each in its orchestrator-side outbound adapter ([app.ModelGatewayPort] /
// [app.ToolRuntimePort]). The client transport credentials are selected the same
// way as the server's — SPIFFE when a source is present, the fail-closed dev
// fallback otherwise — so the inter-service channel is always mutually
// authenticated (architecture §8.1). The callee SPIFFE identities are pinned per
// service so a confused-deputy call to the wrong service is rejected.
func dialDownstream(os orchSettings, src grpcx.SPIFFESource, devInsecure bool) (*downstream, error) {
	td, err := spiffeid.TrustDomainFromString(os.TrustDomain)
	if err != nil {
		return nil, fmt.Errorf("orchestratord: invalid trust domain %q: %w", os.TrustDomain, err)
	}

	mgwID, err := spiffeid.FromSegments(td, "model-gateway")
	if err != nil {
		return nil, fmt.Errorf("orchestratord: model-gateway SPIFFE id: %w", err)
	}
	trID, err := spiffeid.FromSegments(td, "tool-runtime")
	if err != nil {
		return nil, fmt.Errorf("orchestratord: tool-runtime SPIFFE id: %w", err)
	}
	clientID, err := spiffeid.FromSegments(td, "orchestrator")
	if err != nil {
		return nil, fmt.Errorf("orchestratord: orchestrator SPIFFE id: %w", err)
	}

	mgwCreds, err := clientCreds(src, devInsecure, clientID, mgwID)
	if err != nil {
		return nil, err
	}
	trCreds, err := clientCreds(src, devInsecure, clientID, trID)
	if err != nil {
		return nil, err
	}

	mgwConn, err := grpcx.Dial(grpcx.DialConfig{Target: os.ModelGatewayEndpoint, Creds: mgwCreds})
	if err != nil {
		return nil, fmt.Errorf("orchestratord: dial model-gateway: %w", err)
	}
	trConn, err := grpcx.Dial(grpcx.DialConfig{Target: os.ToolRuntimeEndpoint, Creds: trCreds})
	if err != nil {
		_ = mgwConn.Close()
		return nil, fmt.Errorf("orchestratord: dial tool-runtime: %w", err)
	}

	return &downstream{
		model:     modelgw.NewAdapter(genproto.NewModelGatewayServiceClient(mgwConn)),
		tools:     toolrt.NewAdapter(genproto.NewToolRuntimeServiceClient(trConn)),
		modelConn: mgwConn,
		toolsConn: trConn,
		conns:     []*grpc.ClientConn{mgwConn, trConn},
	}, nil
}

// clientCreds selects the client-side mTLS credentials pinning the callee's
// SPIFFE id: SPIFFE when src is present, else the fail-closed dev fallback (which
// itself refuses unless BOLTROPE_DEV_INSECURE=1), else an error.
func clientCreds(src grpcx.SPIFFESource, devInsecure bool, clientID, serverID spiffeid.ID) (credentials.TransportCredentials, error) {
	if src != nil {
		return grpcx.SPIFFEClientCredentials(src, serverID), nil
	}
	if !devInsecure {
		return nil, fmt.Errorf("orchestratord: no SPIFFE source to dial %s and dev-insecure mode is off", serverID)
	}
	creds, err := grpcx.StaticDevClientCredentials(grpcx.StaticDevConfig{
		TrustDomain: clientID.TrustDomain(),
		ServerID:    clientID,
	}, serverID)
	if err != nil {
		return nil, fmt.Errorf("orchestratord: dev client credentials for %s: %w", serverID, err)
	}
	return creds, nil
}
