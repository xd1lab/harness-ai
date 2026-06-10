package main

import (
	"context"
	"fmt"

	"github.com/golang-jwt/jwt/v5"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/obs"
	"github.com/xd1lab/harness-ai/internal/platform/oidc"
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

// loadOIDCKeyfunc builds the production JWKS-backed [jwt.Keyfunc] from the
// configured OIDC issuer (FR-API-03; ADR-0020): discovery + initial JWKS fetch
// happen here, BEFORE the daemon serves, so an unreachable IdP, an issuer
// mismatch, or an empty key set is a refused start (NFR-SEC-01). In
// dev-insecure mode no Keyfunc is needed and nil is returned. A production
// start without BOLTROPE_OIDC_ISSUER is an explicit, actionable error rather
// than the interceptor's generic fail-closed message.
func loadOIDCKeyfunc(ctx context.Context, cfg *config.Config, os orchSettings) (jwt.Keyfunc, error) {
	if cfg.DevInsecure {
		return nil, nil
	}
	if os.OIDCIssuer == "" {
		return nil, fmt.Errorf("orchestratord: production edge auth requires BOLTROPE_OIDC_ISSUER " +
			"(the OIDC issuer URL whose JWKS verifies client bearer tokens); " +
			"set it, or set BOLTROPE_DEV_INSECURE=1 for the dev stack (fail-closed; FR-API-03)")
	}
	kf, err := oidc.NewKeyfunc(ctx, oidc.Config{IssuerURL: os.OIDCIssuer})
	if err != nil {
		return nil, fmt.Errorf("orchestratord: build OIDC keyfunc (BOLTROPE_OIDC_ISSUER=%q): %w", os.OIDCIssuer, err)
	}
	return kf, nil
}

// buildAuthConfig assembles the client-edge auth config for the orchestrator's
// inbound gRPC server. In dev-insecure mode it returns the permissive dev path
// (a dev principal is injected, no token required); in production it pins
// RS256, requires issuer/audience, and carries the JWKS-backed Keyfunc built
// by [loadOIDCKeyfunc] — a nil Keyfunc still causes the auth interceptor
// construction to fail closed (NFR-SEC-01, FR-API-03).
func buildAuthConfig(cfg *config.Config, os orchSettings, kf jwt.Keyfunc) igrpc.AuthConfig {
	if cfg.DevInsecure {
		return igrpc.AuthConfig{
			DevInsecure: true,
			// TenantID MUST be a valid UUID (the event-store tenant_id column is a
			// UUID); igrpc.DevTenantID is seeded as a tenants row by the compose
			// boltrope-grant one-shot so the session/event FK is satisfied.
			DevPrincipal: igrpc.Principal{TenantID: igrpc.DevTenantID, Subject: "dev"},
		}
	}
	return igrpc.AuthConfig{
		Issuer:     os.OIDCIssuer,
		Audience:   os.OIDCAudience,
		Algorithms: []string{"RS256"},
		Keyfunc:    kf,
	}
}

// defaultPolicy is the v1 baseline permission rule set: no deny/allow rules, so
// the engine's mode-driven default applies (deny→mode→allow→ask; architecture
// §8.13). A deployment overlays its own rules; this keeps the loop runnable with
// a sensible, conservative default (ModeDefault asks for mutating/external tools).
func defaultPolicy() (policy.PolicyEngine, error) {
	return policy.NewEngine(policy.Config{RuleSet: policy.RuleSet{}})
}
