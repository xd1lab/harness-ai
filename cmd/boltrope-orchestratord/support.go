package main

import (
	igrpc "github.com/boltrope/boltrope/internal/orchestrator/adapter/inbound/grpc"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/orchestrator/policy"
	"github.com/boltrope/boltrope/internal/platform/config"
	"github.com/boltrope/boltrope/internal/platform/obs"
)

// loopMetrics adapts the shared [obs.Metrics] to the loop's
// [agent.MetricsRecorder]: a run-termination error is recorded under the "Run"
// RPC label (the orchestrator's single client-facing streaming RPC) with the
// typed termination subtype (FR-OBS-02), and a doom-loop is forwarded verbatim
// (FR-OBS-04).
type loopMetrics struct{ m *obs.Metrics }

// RecordRunError records a typed termination subtype against the Run RPC.
func (l loopMetrics) RecordRunError(subtype string) { l.m.RecordError("Run", subtype) }

// RecordDoomLoop forwards a stuck-loop detection for the given tool.
func (l loopMetrics) RecordDoomLoop(tool string) { l.m.RecordDoomLoop(tool) }

// Compile-time assertion that loopMetrics satisfies the loop's recorder port.
var _ agent.MetricsRecorder = loopMetrics{}

// buildAuthConfig assembles the client-edge auth config for the orchestrator's
// inbound gRPC server. In dev-insecure mode it returns the permissive dev path
// (a dev principal is injected, no token required); in production it returns a
// config requiring a pinned-algorithm JWKS/issuer — which, absent a configured
// Keyfunc, causes the auth interceptor construction to fail closed (NFR-SEC-01,
// FR-API-03). Wiring a real JWKS/OIDC discovery is a deployment concern beyond
// this v1 wiring; the production path is intentionally fail-closed until one is
// supplied.
func buildAuthConfig(cfg *config.Config, os orchSettings) igrpc.AuthConfig {
	if cfg.DevInsecure {
		return igrpc.AuthConfig{
			DevInsecure: true,
			// TenantID MUST be a valid UUID (the event-store tenant_id column is a
			// UUID); igrpc.DevTenantID is seeded as a tenants row by the compose
			// boltrope-grant one-shot so the session/event FK is satisfied.
			DevPrincipal: igrpc.Principal{TenantID: igrpc.DevTenantID, Subject: "dev"},
		}
	}
	// Production: pin RS256 and require issuer/audience. Keyfunc is left nil here,
	// so newAuthenticator fails closed unless a deployment supplies a JWKS-backed
	// Keyfunc (wired in a later ops task). This is the secure default.
	return igrpc.AuthConfig{
		Issuer:     os.OIDCIssuer,
		Audience:   os.OIDCAudience,
		Algorithms: []string{"RS256"},
	}
}

// defaultPolicy is the v1 baseline permission rule set: no deny/allow rules, so
// the engine's mode-driven default applies (deny→mode→allow→ask; architecture
// §8.13). A deployment overlays its own rules; this keeps the loop runnable with
// a sensible, conservative default (ModeDefault asks for mutating/external tools).
func defaultPolicy() (policy.PolicyEngine, error) {
	return policy.NewEngine(policy.Config{RuleSet: policy.RuleSet{}})
}
