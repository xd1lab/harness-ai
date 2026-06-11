// Command boltrope-orchestratord is the orchestrator daemon: the client-facing
// agent-control edge that drives the gather→act→verify agent loop over the
// event-sourced session log (T-CMD-01; architecture §3, §4). It wires the loop's
// dependency set — the pgx event store (EventLogPort), the model-gateway client
// (ModelGatewayPort), the tool-runtime client (ToolRuntimePort), the approval
// gate, the hooks pipeline, the policy engine, the sub-agent spawner, and the
// context manager — into a LoopRunner, fronts it with the boltrope.v1
// OrchestratorService (CreateSession/GetSession/Run/Control/Fork), and serves it
// over mTLS + client-edge JWT auth with the shared
// [github.com/xd1lab/harness-ai/internal/platform/daemon] harness (health,
// dependency-gated readiness, graceful shutdown).
//
// It refuses to accept traffic before its dependencies are reachable: /readyz
// gates on the event-store DB ping (and SVID presence), so an unmigrated or
// unreachable database keeps the orchestrator out of rotation (NFR-OPS-01,
// FR-OBS-05). Orchestrator-specific knobs (downstream endpoints, default model,
// OIDC) come from the environment so the frozen shared [config.Config] stays
// service-agnostic.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/approval"
	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/rest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/eventstore"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agentctx"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/hooks"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/subagent"
	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/daemon"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/pricing"
)

const serviceName = "orchestrator"

// orchSettings are the orchestrator-specific knobs read from the environment (the
// frozen shared [config.Config] carries only the model-gateway endpoint and the
// cross-cutting infra; the rest is orchestrator-local).
type orchSettings struct {
	// ModelGatewayEndpoint is the model-gateway gRPC address (from config).
	ModelGatewayEndpoint string
	// ToolRuntimeEndpoint is the tool-runtime gRPC address (env-supplied).
	ToolRuntimeEndpoint string
	// DefaultModel is the model id used when a run does not override it.
	DefaultModel string
	// MaxContextTokens is the default context-window budget for compaction; zero
	// disables the token-pressure trigger.
	MaxContextTokens int
	// SubAgentMaxDepth bounds sub-agent recursion (FR-EXT-04).
	SubAgentMaxDepth int
	// PricingFile is the path to an optional JSON pricing-overrides document
	// (see [pricing.ParseOverrides]) layered over the built-in placeholder
	// rates for budget enforcement. Empty means defaults only; an unreadable
	// or invalid file fails startup — silently mispriced budgets are worse
	// than a crash.
	PricingFile string
	// TrustDomain is the SPIFFE trust domain for inter-service + edge mTLS.
	TrustDomain string
	// OIDCIssuer / OIDCAudience configure the client-edge JWT validation in
	// production (FR-API-03); unused in dev-insecure mode.
	OIDCIssuer   string
	OIDCAudience string
}

// loadOrchSettings reads the orchestrator-specific environment, applying the
// model-gateway endpoint from the shared config and sensible defaults.
func loadOrchSettings() orchSettings {
	return orchSettings{
		ToolRuntimeEndpoint: envOr("BOLTROPE_TOOLRUNTIME_ENDPOINT", "localhost:9002"),
		DefaultModel:        envOr("BOLTROPE_DEFAULT_MODEL", "claude-sonnet-4-6"),
		MaxContextTokens:    envInt("BOLTROPE_MAX_CONTEXT_TOKENS", 0),
		SubAgentMaxDepth:    envInt("BOLTROPE_SUBAGENT_MAX_DEPTH", 2),
		// Shared with the model-gateway so both daemons price a turn identically.
		PricingFile:  os.Getenv("BOLTROPE_PRICING_FILE"),
		TrustDomain:  envOr("BOLTROPE_TRUST_DOMAIN", "boltrope.local"),
		OIDCIssuer:   os.Getenv("BOLTROPE_OIDC_ISSUER"),
		OIDCAudience: os.Getenv("BOLTROPE_OIDC_AUDIENCE"),
	}
}

// envOr returns the value of env var key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt parses env var key as an int, falling back to def on absence/parse error.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return def
}

// loadCostFunc returns the per-turn cost function for budget enforcement:
// [pricing.Cost] when path is empty (built-in placeholder defaults, unchanged
// behavior), or the defaults overlaid with the JSON overrides document at
// path. A read or parse error is fatal — the operator explicitly asked for
// corrected rates, so running with silently wrong budget accounting is worse
// than refusing to start.
func loadCostFunc(path string) (agent.CostFunc, error) {
	if path == "" {
		return agent.CostFunc(pricing.Cost), nil
	}
	data, err := os.ReadFile(path) //nolint:gosec // G304: path is operator-configured via BOLTROPE_PRICING_FILE, not model/client-driven
	if err != nil {
		return nil, fmt.Errorf("orchestratord: read pricing overrides (BOLTROPE_PRICING_FILE=%q): %w", path, err)
	}
	overrides, err := pricing.ParseOverrides(data)
	if err != nil {
		return nil, fmt.Errorf("orchestratord: invalid pricing overrides (BOLTROPE_PRICING_FILE=%q): %w", path, err)
	}
	return overrides.Cost, nil
}

// Run wires the orchestrator and serves it until ctx is cancelled or a signal
// arrives, then shuts down gracefully (draining Run streams, closing the pool and
// downstream connections, flushing telemetry). logw is the log sink.
func Run(ctx context.Context, cfg *config.Config, logw io.Writer) error {
	tel, err := daemon.SetupTelemetry(ctx, serviceName, version, cfg, logw)
	if err != nil {
		return err
	}

	osettings := loadOrchSettings()
	osettings.ModelGatewayEndpoint = cfg.ModelGateway.Endpoint

	// Credentials (server side) and identity source, selected once.
	src := spiffeSource()
	credsCfg, err := serverCredsConfig(osettings.TrustDomain, cfg, tel)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	creds, err := daemon.ServerCredentials(credsCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	// Build the client-edge auth interceptors BEFORE anything expensive so a
	// fail-closed misconfiguration (production without a reachable OIDC issuer)
	// is reported immediately (NFR-SEC-01). In production this performs OIDC
	// discovery + the initial JWKS fetch (ADR-0020).
	keyfunc, err := loadOIDCKeyfunc(ctx, cfg, osettings)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	authCfg := buildAuthConfig(cfg, osettings, keyfunc)
	authOpts, err := authServerOptions(authCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	// The REST/SSE facade shares the SAME auth policy (and keyfunc, hence the
	// same JWKS cache) as the gRPC interceptors — identical auth per FR-API-03.
	restAuth, err := igrpc.NewAuthenticator(authCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	// Event store over a lazy pgx pool (no connection is dialed until first use,
	// so startup never blocks on Postgres; readiness gates on the DB ping).
	pool, err := eventstore.NewPgxPool(ctx, cfg.Postgres.DSN)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return fmt.Errorf("orchestratord: build event-store pool: %w", err)
	}
	store := eventstore.New(pool)

	// Downstream service clients (model-gateway + tool-runtime), lazily dialed.
	down, err := dialDownstream(osettings, src, cfg.DevInsecure)
	if err != nil {
		pool.Close()
		_ = tel.Shutdown(ctx)
		return err
	}

	// Assemble the loop dependency set + config template (the approval gate is
	// created inside so sub-agents inherit it).
	deps, loopCfg, gate, err := buildLoopDeps(store, down, tel, osettings)
	if err != nil {
		_ = closeAll(down.closers())
		pool.Close()
		_ = tel.Shutdown(ctx)
		return err
	}

	runner := igrpc.NewLoopRunner(deps, loopCfg)
	server := igrpc.NewServer(store, gate, runner, ids.System{}, igrpc.Config{DefaultModel: osettings.DefaultModel})
	// REST/JSON + SSE facade over the same server (spec §2 minimal facade:
	// CreateSession/GetSession/Run-over-SSE/Control/Fork), mounted on the
	// daemon's HTTP listener next to /livez,/readyz,/metrics.
	restHandler := rest.NewHandler(server, restAuth)

	// Shutdown closers, registered so they run LIFO (telemetry flush last).
	closers := []func() error{func() error { _ = tel.Shutdown(ctx); return nil }}
	closers = append(closers, func() error { pool.Close(); return nil })
	closers = append(closers, down.closers()...)

	return daemon.Run(ctx, daemon.RunInput{
		GRPCAddr:           cfg.Server.GRPCAddr,
		HTTPAddr:           cfg.Server.HTTPAddr,
		Creds:              creds,
		Policy:             orchestratorRBAC(osettings.TrustDomain),
		Telemetry:          tel,
		HasIdentity:        func() bool { return daemon.HasServerIdentity(credsCfg) },
		ExtraServerOptions: authOpts,
		Service: daemon.Service{
			Register: func(srv *grpc.Server) { genproto.RegisterOrchestratorServiceServer(srv, server) },
			// /readyz gates on the event-store DB AND on a grpc.health.v1 probe of
			// the model-gateway and tool-runtime over the SAME inter-service mTLS
			// channel — so the orchestrator only joins rotation once that channel
			// actually handshakes (this catches a shared-CA break at `up --wait`,
			// not on the first real turn; FR-OBS-05).
			ReadinessChecks: append([]daemon.ReadinessCheck{dbReadiness(cfg.Postgres.DSN)}, down.readinessChecks()...),
			Closers:         closers,
			HTTPRoutes:      restHandler.Routes,
		},
	})
}

// The production approval gate MUST satisfy the Run relay's ApprovalNotifier so a
// pending ask is surfaced in-band as an ApprovalRequest frame on the client's Run
// stream. Without it a default-mode tool call blocks at the gate with no
// client-visible signal — the operator never learns the call_id to approve and the
// run hangs (exactly the quickstart hang this guards against). This compile-time
// assertion fails the build if SubscribeApprovals is ever dropped from the gate.
var _ igrpc.ApprovalNotifier = (*approval.Gate)(nil)

// buildLoopDeps assembles the agent loop's [agent.Deps] and [agent.Config]
// template from the wired collaborators, returning the shared [*approval.Gate]
// the transport's Control.Approve/Deny resolves against. The Sink and Mode are
// overlaid per-run by the LoopRunner; the rest (including the approval gate, which
// sub-agents inherit) is shared across runs.
func buildLoopDeps(store *eventstore.Store, down *downstream, tel *daemon.Telemetry, os orchSettings) (agent.Deps, agent.Config, *approval.Gate, error) {
	pol, err := defaultPolicy()
	if err != nil {
		return agent.Deps{}, agent.Config{}, nil, fmt.Errorf("orchestratord: build policy engine: %w", err)
	}

	// Resolve the budget cost function (built-in placeholder defaults, optionally
	// overlaid via BOLTROPE_PRICING_FILE); a bad file fails startup.
	costFn, err := loadCostFunc(os.PricingFile)
	if err != nil {
		return agent.Deps{}, agent.Config{}, nil, err
	}

	ctxMgr := agentctx.NewManager(newGatewayTokenCounter(down.model), agentctx.Config{
		Model:            os.DefaultModel,
		MaxContextTokens: os.MaxContextTokens,
	})

	hookRunner := hooks.NewRunner(hooks.Config{}, hooks.NewOSCommandRunner())
	gate := approval.New()

	deps := agent.Deps{
		EventLog:  store,
		Model:     down.model,
		Tools:     down.tools,
		Approvals: gate,
		Hooks:     hookRunner,
		Policy:    pol,
		Context:   ctxMgr,
		Clock:     clock.System{},
		IDs:       ids.System{},
		Metrics:   loopMetrics{m: tel.Metrics},
		CostFunc:  costFn,
	}

	loopCfg := agent.Config{Model: os.DefaultModel}

	// Sub-agents reuse the same dependency set + config (now including the approval
	// gate), capped at the configured depth (FR-EXT-04). The spawner is itself a
	// loop dependency.
	deps.SubAgent = subagent.New(subagent.Config{
		MaxDepth: os.SubAgentMaxDepth,
		Deps:     deps,
		LoopCfg:  loopCfg,
	})

	return deps, loopCfg, gate, nil
}

// authServerOptions builds the gRPC server options installing the client-edge
// unary+stream auth interceptors. A fail-closed auth config (production without a
// Keyfunc) makes interceptor construction return an error, which propagates as a
// startup failure (NFR-SEC-01).
func authServerOptions(ac igrpc.AuthConfig) ([]grpc.ServerOption, error) {
	unary, err := igrpc.NewAuthInterceptor(ac)
	if err != nil {
		return nil, fmt.Errorf("orchestratord: build edge auth interceptor: %w", err)
	}
	stream, err := igrpc.NewStreamAuthInterceptor(ac)
	if err != nil {
		return nil, fmt.Errorf("orchestratord: build edge stream auth interceptor: %w", err)
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unary),
		grpc.ChainStreamInterceptor(stream),
	}, nil
}

// dbReadiness builds the /readyz check gating on the event-store DB: it pings the
// configured DSN and also verifies the pinned minimum PostgreSQL version, so an
// unreachable or too-old database keeps the orchestrator out of rotation
// (FR-OBS-05, NFR-PORT-03).
func dbReadiness(dsn string) daemon.ReadinessCheck {
	return daemon.ReadinessCheck{
		Name: "postgres",
		Probe: func(ctx context.Context) error {
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close(context.Background()) }()
			return conn.Ping(ctx)
		},
	}
}

// closeAll runs every closer, returning the first error (best-effort).
func closeAll(closers []func() error) error {
	var firstErr error
	for _, c := range closers {
		if err := c(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// serverCredsConfig assembles the [daemon.CredsConfig] for this service.
func serverCredsConfig(trustDomain string, cfg *config.Config, tel *daemon.Telemetry) (daemon.CredsConfig, error) {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("orchestratord: invalid trust domain %q: %w", trustDomain, err)
	}
	id, err := spiffeid.FromSegments(td, serviceName)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("orchestratord: build server SPIFFE id: %w", err)
	}
	return daemon.CredsConfig{
		TrustDomain: td,
		ServerID:    id,
		DevInsecure: cfg.DevInsecure,
		Source:      spiffeSource(),
		Logger:      tel.Logger,
	}, nil
}
