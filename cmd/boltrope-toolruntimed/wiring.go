// Command boltrope-toolruntimed is the tool-runtime daemon: the trust boundary
// that executes model-influenced tool calls (T-CMD-02; architecture §5.3, §7,
// §9). It wires the tool registry (native tools + lazy MCP), the container-backed
// Workspace runtime with hard limits and process-group kill, the deny-by-default
// egress broker, the durable pgx dedup ledger, the filesystem blob store, and the
// ExecuteTool use-case behind the boltrope.v1 ToolRuntimeService — served over
// mTLS with the shared [github.com/xd1lab/harness-ai/internal/platform/daemon]
// harness (health, readiness gated on container-runtime availability, graceful
// shutdown).
//
// Like the other daemons it reads its tool-runtime-specific knobs from the
// environment ([loadToolSettings]) so the frozen shared [config.Config] stays
// service-agnostic.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"google.golang.org/grpc"

	"github.com/xd1lab/harness-ai/internal/platform/blob"
	"github.com/xd1lab/harness-ai/internal/platform/config"
	"github.com/xd1lab/harness-ai/internal/platform/daemon"
	trgrpc "github.com/xd1lab/harness-ai/internal/toolruntime/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/dedup"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/egress"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/mcp"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/runtime"
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/registry"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/execute"
	"github.com/xd1lab/harness-ai/internal/toolruntime/tools"
)

const serviceName = "tool-runtime"

// defaultSessionID is the v1 single-session sandbox key the native tools' routing
// workspace binds to (see workspace.go). The dedup ledger and egress broker
// remain per-(tenant,session) keyed independently.
const defaultSessionID = "tool-runtime"

// toolSettings are the tool-runtime-specific knobs read from the environment.
type toolSettings struct {
	// DockerBin overrides the docker CLI binary name (default "docker").
	DockerBin string
	// Image overrides the sandbox base image.
	Image string
	// TrustDomain is the SPIFFE trust domain for inter-service mTLS.
	TrustDomain string
	// MCPCommand, when set, registers a single stdio MCP server launched with this
	// command (lazy-loaded; untrusted-by-default). Optional.
	MCPCommand string
	// EgressAllowlist is the operator-configured set of hosts the session's
	// egress broker policy permits (deny-by-default: empty means deny-all). It is
	// the POLICY layer; the sandbox network namespace is the actual v1 containment
	// (see egress.Broker and architecture §8.4). Sourced from
	// BOLTROPE_TOOLRT_EGRESS_ALLOWLIST (comma-separated hosts; "*.suffix" wildcards
	// per egress.Broker matching rules).
	EgressAllowlist []string
}

// loadToolSettings reads the tool-runtime-specific environment.
func loadToolSettings() toolSettings {
	return toolSettings{
		DockerBin:       os.Getenv("BOLTROPE_TOOLRT_DOCKER_BIN"),
		Image:           os.Getenv("BOLTROPE_TOOLRT_IMAGE"),
		TrustDomain:     envOr("BOLTROPE_TRUST_DOMAIN", "boltrope.local"),
		MCPCommand:      os.Getenv("BOLTROPE_TOOLRT_MCP_COMMAND"),
		EgressAllowlist: parseAllowlist(os.Getenv("BOLTROPE_TOOLRT_EGRESS_ALLOWLIST")),
	}
}

// parseAllowlist splits a comma-separated egress allowlist into trimmed,
// non-empty host entries. An empty or whitespace-only value yields nil (deny-all,
// the safe default). Entries keep their form verbatim ("*.suffix" wildcards are
// matched by the egress broker, not expanded here).
func parseAllowlist(raw string) []string {
	var hosts []string
	for _, part := range strings.Split(raw, ",") {
		if h := strings.TrimSpace(part); h != "" {
			hosts = append(hosts, h)
		}
	}
	return hosts
}

// envOr returns the value of env var key, or def when it is unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// buildRegistry builds the tool registry, registers the native tool set bound to
// the routing workspace + egress broker, and (when configured) attaches a lazy
// MCP source. It returns the populated registry. The registry validates each
// native tool's schema at registration (FR-TOOL-01); a registration failure is
// fatal wiring misconfiguration.
//
// buildRegistry constructs its own container runtime + broker for the standalone
// path; the daemon's [buildToolRuntime] reuses one shared set. It is kept
// separate so the registry can be unit-tested in isolation.
func buildRegistry(ts toolSettings) (*registry.Registry, error) {
	broker, policy := buildEgress(ts)
	rt, err := buildRuntime(ts)
	if err != nil {
		return nil, err
	}
	ws := newRoutingWorkspace(rt, defaultSessionID, policy)
	reg := newRegistry(ts)
	if err := registerNative(reg, ws, broker); err != nil {
		return nil, err
	}
	return reg, nil
}

// buildEgress constructs the deny-by-default egress [egress.Broker] and installs
// the operator-configured [app.EgressPolicy] for the v1 single session via
// [egress.Broker.SetPolicy]. This is the wiring seam that lets an operator widen
// egress from config (BOLTROPE_TOOLRT_EGRESS_ALLOWLIST): with no allowlist the
// policy is empty and the broker denies all hosts (the safe default). It returns
// both the broker and the installed policy so the routing workspace binds the
// SAME policy the broker enforces.
//
// In v1 this is the POLICY layer only: the per-session sandbox runs with
// --network none by default (the network namespace is the actual containment;
// architecture §8.4), so an allowlist installed here does not by itself grant a
// network path — gating allowlisted egress additionally requires the egress-proxy
// network path (roadmap; ADR-0003).
func buildEgress(ts toolSettings) (*egress.Broker, app.EgressPolicy) {
	broker := egress.New()
	policy := app.EgressPolicy{SessionID: defaultSessionID, AllowedHosts: ts.EgressAllowlist}
	// SetPolicy on an empty allowlist is a deliberate deny-all install; Broker
	// never returns an error from SetPolicy, so the daemon proceeds.
	_ = broker.SetPolicy(context.Background(), policy)
	return broker, policy
}

// buildRuntime constructs the container [runtime.Runtime] with conservative
// defaults overlaid by ts. The docker CLI is invoked lazily on first Create, so
// this never probes for Docker (readiness does, separately).
func buildRuntime(ts toolSettings) (*runtime.Runtime, error) {
	cfg := runtime.DefaultConfig()
	if ts.DockerBin != "" {
		cfg.DockerBin = ts.DockerBin
	}
	if ts.Image != "" {
		cfg.Image = ts.Image
	}
	rt, err := runtime.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("toolruntimed: build container runtime: %w", err)
	}
	return rt, nil
}

// newRegistry builds an empty registry with an optional lazy MCP source.
func newRegistry(ts toolSettings) *registry.Registry {
	if ts.MCPCommand == "" {
		return registry.New(nil)
	}
	client := mcp.New(mcp.WithServer("default", ts.MCPCommand, nil, nil))
	return registry.New(mcpSource{client: client})
}

// registerNative registers every native tool (bound to ws + broker) into reg,
// returning the first registration error.
func registerNative(reg *registry.Registry, ws app.Workspace, broker app.EgressBroker) error {
	for _, tool := range tools.Native(ws, broker) {
		if err := reg.Register(context.Background(), tool); err != nil {
			return fmt.Errorf("toolruntimed: register native tool %q: %w", tool.Spec().Name, err)
		}
	}
	return nil
}

// buildToolRuntime wires the full tool-runtime: a shared container runtime + the
// deny-by-default egress broker back both the native tools' routing workspace and
// the ExecuteTool use-case; the durable pgx dedup ledger and the filesystem blob
// store complete the dependency set. It returns the ExecuteTool service, the
// registry (for ListTools), and the shutdown closers (the dedup pool).
func buildToolRuntime(cfg *config.Config, ts toolSettings) (*execute.Service, *registry.Registry, []func() error, error) {
	broker, policy := buildEgress(ts)

	rt, err := buildRuntime(ts)
	if err != nil {
		return nil, nil, nil, err
	}

	ws := newRoutingWorkspace(rt, defaultSessionID, policy)
	reg := newRegistry(ts)
	if err := registerNative(reg, ws, broker); err != nil {
		return nil, nil, nil, err
	}

	// The durable dedup ledger over the event-store database (the tool_executions
	// table). The SimplePool dials lazily per acquire, so construction never
	// blocks on Postgres; readiness gates on reachability instead.
	dedupPool, err := dedup.NewSimplePool(cfg.Postgres.DSN)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("toolruntimed: build dedup pool: %w", err)
	}
	dedupStore := dedup.New(dedupPool)

	blobs := blob.NewFSStore(cfg.Blob.Dir, maxBlobBytes)

	svc, err := execute.NewService(execute.Config{
		Registry: reg,
		Runtime:  rt,
		Egress:   broker,
		Dedup:    dedupStore,
		Blobs:    blobs,
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("toolruntimed: build execute service: %w", err)
	}

	closers := []func() error{
		func() error { dedupPool.Close(); return nil },
	}
	return svc, reg, closers, nil
}

// Run wires the tool-runtime and serves it until ctx is cancelled or a signal
// arrives, then shuts down gracefully. logw is the log sink (production:
// os.Stderr).
func Run(ctx context.Context, cfg *config.Config, logw io.Writer) error {
	tel, err := daemon.SetupTelemetry(ctx, serviceName, version, cfg, logw)
	if err != nil {
		return err
	}

	ts := loadToolSettings()
	svc, reg, closers, err := buildToolRuntime(cfg, ts)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	server := trgrpc.NewServer(svc, reg)

	credsCfg, err := serverCredsConfig(ts.TrustDomain, cfg, tel)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}
	creds, err := daemon.ServerCredentials(credsCfg)
	if err != nil {
		_ = tel.Shutdown(ctx)
		return err
	}

	// Telemetry flush runs last (registered first → closed last in the LIFO
	// closer order).
	allClosers := append([]func() error{func() error { _ = tel.Shutdown(ctx); return nil }}, closers...)

	return daemon.Run(ctx, daemon.RunInput{
		GRPCAddr:    cfg.Server.GRPCAddr,
		HTTPAddr:    cfg.Server.HTTPAddr,
		Creds:       creds,
		Policy:      toolRuntimeRBAC(ts.TrustDomain),
		Telemetry:   tel,
		HasIdentity: func() bool { return daemon.HasServerIdentity(credsCfg) },
		Service: daemon.Service{
			Register:        func(srv *grpc.Server) { registerToolRuntimeServer(srv, server) },
			ReadinessChecks: []daemon.ReadinessCheck{dockerReadiness(ts)},
			Closers:         allClosers,
		},
	})
}

// serverCredsConfig assembles the [daemon.CredsConfig] for this service.
func serverCredsConfig(trustDomain string, cfg *config.Config, tel *daemon.Telemetry) (daemon.CredsConfig, error) {
	td, err := spiffeid.TrustDomainFromString(trustDomain)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("toolruntimed: invalid trust domain %q: %w", trustDomain, err)
	}
	id, err := spiffeid.FromSegments(td, serviceName)
	if err != nil {
		return daemon.CredsConfig{}, fmt.Errorf("toolruntimed: build server SPIFFE id: %w", err)
	}
	return daemon.CredsConfig{
		TrustDomain: td,
		ServerID:    id,
		DevInsecure: cfg.DevInsecure,
		Source:      spiffeSource(),
		Logger:      tel.Logger,
	}, nil
}
