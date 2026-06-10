// Package app declares the consumer-defined ports the tool-runtime's execute and
// sandbox-manager use-cases depend on (architecture §5.3): the [ToolRegistry] (tool
// registration and lazy MCP loading), the [RuntimePort] (the Workspace/Runtime
// sandbox abstraction with a real cancellation-to-kill contract), the
// [EgressBroker] (deny-by-default per-session network allowlist), the
// [MCPClientPort] (confined third-party MCP servers), and the [DedupStore] (the
// durable tool-execution ledger). Ports are small and declared where they are used,
// per the clean-architecture rule (architecture §5).
//
// IMPORTANT: nothing here imports gen/. The gRPC server adapter that exposes
// ExecuteTool/ListTools maps gen/ ⇄ these domain types at the transport edge and is
// written separately (architecture §12.4). These ports are expressed over the
// tool-runtime domain ([github.com/xd1lab/harness-ai/internal/toolruntime/domain])
// and the canonical llm kernel types only.
package app

import (
	"context"
	"errors"

	"github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// ----------------------------------------------------------------------------
// ToolRegistry — native + MCP tool registration with lazy MCP schema loading
// (architecture §2.3, §5.3 registry).
// ----------------------------------------------------------------------------

// ErrToolNotFound is the sentinel returned by [ToolRegistry.Get] when no tool is
// registered under the requested name. Recover it with [errors.Is].
var ErrToolNotFound = errors.New("toolruntime: tool not found")

// ToolRegistry holds the merged set of available tools — native tools plus tools
// discovered from MCP servers — and validates registrations. MCP tools are loaded
// LAZILY (their schemas are fetched on demand, not eagerly) and their
// descriptions/schemas are treated as untrusted content gated by
// approval-on-first-use; a newly-appearing tool is gated (ADR-0013 §"MCP server
// confinement"; architecture §5.3, §8.11). Implementations must be safe for
// concurrent use.
type ToolRegistry interface {
	// Register adds a tool to the registry. It validates the tool's [domain.ToolSpec]
	// (e.g. non-empty name, well-formed JSON Schema) and rejects a duplicate name.
	// For an MCP-sourced tool, registration is the point at which
	// approval-on-first-use is enforced before the tool becomes callable
	// (architecture §8.11).
	Register(ctx context.Context, tool domain.Tool) error

	// Get returns the registered tool by name, or [ErrToolNotFound]. For a lazily
	// loaded MCP tool, Get triggers loading its schema on first access if not
	// already loaded (architecture §5.3 "lazy schema loading").
	Get(ctx context.Context, name string) (domain.Tool, error)

	// List returns the specs of all currently registered (and, for MCP, already
	// approved/loaded) tools, so the service can answer the orchestrator's
	// ListTools and build model tool definitions.
	List(ctx context.Context) ([]domain.ToolSpec, error)
}

// ----------------------------------------------------------------------------
// RuntimePort / Workspace — the per-session sandbox abstraction (ADR-0005;
// ADR-0014; architecture §5.3, §7.5, §9.3). The container backend is the v1 impl;
// microVM/gVisor slot in behind this port later.
// ----------------------------------------------------------------------------

// ExecResult is the outcome of a [Workspace.Exec]: the captured output and exit
// status of a command run inside the sandbox.
type ExecResult struct {
	// ExitCode is the process exit code. It is meaningful only when [ExecResult.Killed]
	// is false.
	ExitCode int
	// Stdout is the captured standard output (subject to truncation/offload by the
	// caller for large output; architecture §6.4).
	Stdout []byte
	// Stderr is the captured standard error.
	Stderr []byte
	// Killed reports whether the process was terminated by the runtime rather than
	// exiting on its own — e.g. on context cancellation (cgroup/PID-namespace kill)
	// or on hitting a hard CPU/memory/PID/wall-clock limit (architecture §9.3).
	Killed bool
}

// ExecRequest describes a command to run inside a session's workspace.
type ExecRequest struct {
	// Cmd is the command and its arguments (argv form; not shell-interpreted by the
	// port itself).
	Cmd []string
	// WorkDir is the working directory inside the sandbox; empty uses the
	// workspace default.
	WorkDir string
	// Env is additional environment variables for the command, as KEY=VALUE
	// strings.
	Env []string
	// Stdin is optional standard input to feed the command.
	Stdin []byte
}

// Workspace is the per-session sandbox: a confined filesystem + execution
// environment with deny-by-default egress. It is the trust-boundary container for
// model-influenced code (architecture §2.2). The v1 backend is a per-session
// container/cgroup; the port is shaped so a future snapshotting backend can satisfy
// durable-workspace resume without changing callers (architecture §7.5).
//
// Cancellation contract (architecture §9.3): cancelling the ctx passed to [Workspace.Exec]
// MUST terminate the in-sandbox process TREE — signaling the whole process
// group/cgroup with a SIGTERM→SIGKILL deadline — not merely cancel an exec wrapper.
// A SIGTERM-trapping process, a double-forked detached child, and a fork bomb must
// each be terminated within the deadline (the required adversarial tests). Hard
// resource limits bound a non-cooperating process regardless of signal handling.
type Workspace interface {
	// Exec runs req inside this workspace and returns its [ExecResult]. On ctx
	// cancellation it kills the process group/cgroup (not just the wrapper) and
	// returns with [ExecResult.Killed] true (architecture §9.3). It enforces the
	// hard CPU/memory/PID/wall-clock limits configured for the sandbox.
	Exec(ctx context.Context, req ExecRequest) (ExecResult, error)

	// Read returns the contents of the file at path within the sandbox.
	Read(ctx context.Context, path string) ([]byte, error)
	// Write writes data to the file at path within the sandbox, creating or
	// truncating it.
	Write(ctx context.Context, path string, data []byte) error
	// Mkdir creates the directory at path within the sandbox (including parents).
	Mkdir(ctx context.Context, path string) error

	// NetworkPolicy returns the [EgressPolicy] currently in force for this
	// workspace's egress (the per-session deny-by-default allowlist; architecture
	// §8.4). In v1 the sandbox network namespace (`--network none` by default) is
	// the actual containment for all in-sandbox network; the returned policy is the
	// deny-by-default allowlist policy layer over it, which a future egress-proxy
	// data path consults to gate allowlisted egress (architecture §8.4 roadmap).
	NetworkPolicy(ctx context.Context) (EgressPolicy, error)
}

// RuntimePort manages the lifecycle of per-session [Workspace] sandboxes (architecture
// §5.3 sandboxmgr, §10.6). The sandboxmgr enforces idle/absolute TTLs and a
// max-live cap and reaps sandboxes whose session is finished/failed/abandoned
// (architecture §10.6). Implementations must be safe for concurrent use.
type RuntimePort interface {
	// Create provisions a fresh [Workspace] for sessionID with the given initial
	// [EgressPolicy], and returns it. Resume always re-attaches to a FRESH
	// workspace (clean-workspace resume; uncommitted FS state from a prior
	// container is not restored; ADR-0012 §"Clean-workspace resume"; architecture
	// §7.5). Create respects the max-live-sandboxes cap and may apply backpressure
	// (architecture §10.6).
	Create(ctx context.Context, sessionID string, egress EgressPolicy) (Workspace, error)

	// Get returns the existing [Workspace] for sessionID, or an error if none is
	// live (e.g. after reaping). Callers re-Create on resume rather than expecting
	// persistence.
	Get(ctx context.Context, sessionID string) (Workspace, error)

	// Destroy tears down sessionID's workspace and releases its resources. It is
	// idempotent: destroying an absent workspace is not an error. It is invoked by
	// the reaper when a session is finished/failed/abandoned and on orchestrator
	// crash recovery (architecture §10.6).
	Destroy(ctx context.Context, sessionID string) error
}

// ----------------------------------------------------------------------------
// EgressBroker — the single deny-by-default egress control on EVERY
// model-influenced outbound path (ADR-0013; architecture §8.4). This is an INFRA
// control, non-bypassable by policy/mode.
// ----------------------------------------------------------------------------

// EgressPolicy is a per-session egress allowlist: the set of hosts a session's
// model-influenced tools (in-sandbox bash, webfetch, websearch, MCP http) may reach.
// Everything not on the allowlist is denied (deny-by-default; ADR-0013; architecture
// §8.4).
type EgressPolicy struct {
	// SessionID is the session this policy scopes.
	SessionID string
	// AllowedHosts is the explicit allowlist of reachable hosts for the session.
	// An empty allowlist means deny all egress (the safe default), NOT allow all.
	AllowedHosts []string
}

// EgressBroker enforces the per-session deny-by-default egress allowlist for every
// model-influenced outbound request — in-sandbox bash, webfetch, websearch, and MCP
// http transports all route through it (ADR-0013 §"Egress broker"; architecture
// §8.4). It is the real exfiltration control (output masking is only best-effort
// hygiene, not containment; ADR-0013). It is an INFRA control: it remains in force
// even under [github.com/xd1lab/harness-ai/internal/orchestrator/policy.ModeBypass]
// (architecture §8.13). Implementations must be safe for concurrent use and must
// fail closed (deny) on any ambiguity.
type EgressBroker interface {
	// Allow reports whether sessionID is permitted to make an outbound connection
	// to host under its current [EgressPolicy]. The default is deny: a host absent
	// from the session's allowlist returns false. A denied decision is what the
	// required exfil test asserts when an injected page instructs exfiltration via
	// webfetch (ADR-0013 §"Taint-tracking gate"; architecture §8.4).
	Allow(ctx context.Context, sessionID, host string) (bool, error)

	// SetPolicy installs or replaces the [EgressPolicy] for a session (e.g. at
	// session start from configuration). Tightening or widening the allowlist is a
	// deliberate, logged operation; it is never driven by the model.
	SetPolicy(ctx context.Context, policy EgressPolicy) error
}

// ----------------------------------------------------------------------------
// MCPClientPort — client to confined third-party MCP servers (ADR-0013; architecture
// §5.3 mcp, §8.11).
// ----------------------------------------------------------------------------

// MCPServerRef identifies a configured MCP server with its pinned identity, so a
// changed server (or a newly-appearing tool) can be detected and gated (ADR-0013
// §"MCP server confinement").
type MCPServerRef struct {
	// Name is the local name of the configured MCP server.
	Name string
	// Transport is the transport kind, "stdio" or "http" (each runs inside its own
	// confined sandbox with deny-by-default egress; architecture §8.11).
	Transport string
	// VersionPin is the pinned identity/version hash of the server; a mismatch
	// gates the server until re-approved (architecture §8.11).
	VersionPin string
}

// MCPClientPort is the client to third-party MCP servers. Each server runs inside
// its OWN confined sandbox with deny-by-default egress through the [EgressBroker],
// never as a bare child of tool-runtime, and the SPIRE Workload API socket/SVID is
// never exposed into that namespace (ADR-0013 §"MCP server confinement"; architecture
// §8.11). Tool schemas are loaded lazily and treated as untrusted (tool-poisoning),
// gated by approval-on-first-use. Implementations must be safe for concurrent use.
type MCPClientPort interface {
	// ListTools returns the [domain.ToolSpec]s advertised by the given MCP server,
	// loaded lazily. The returned specs default to fail-safe classifications
	// ([domain.SideEffectMutating], [domain.EgressClassExternal]) unless explicitly
	// annotated, and their descriptions are untrusted pending approval (architecture
	// §8.11). A version-pin mismatch is surfaced as an error so the server is gated.
	ListTools(ctx context.Context, server MCPServerRef) ([]domain.ToolSpec, error)

	// CallTool invokes the named tool on the given MCP server for sessionID with
	// validated args and returns the [domain.Observation]. The server's egress (for
	// http tools / server-initiated fetches) is constrained by the session's
	// [EgressPolicy] via the broker (architecture §8.4, §8.11). ctx cancellation
	// aborts the call and tears down the confined process as needed.
	CallTool(ctx context.Context, server MCPServerRef, sessionID, name string, args map[string]any) (domain.Observation, error)
}

// ----------------------------------------------------------------------------
// DedupStore — the durable tool-execution ledger (ADR-0012; architecture §5.3
// dedup, §7.2). Durable, not in-memory/TTL.
// ----------------------------------------------------------------------------

// ExecutionStatus is the status of a tool execution in the durable [DedupStore]
// ledger, matching the tool_executions.status column (ADR-0011 §6.2; ADR-0012).
type ExecutionStatus string

const (
	// ExecStarted records that execution intent was persisted and dispatch began;
	// a started-but-not-completed entry on resume denotes an UNKNOWN outcome that a
	// mutating tool must not blindly re-run (ADR-0012 §"At-most-once recovery";
	// architecture §7.2).
	ExecStarted ExecutionStatus = "started"
	// ExecCompleted records a successful completion; the prior result can be
	// returned for a retried call with the same key instead of re-running.
	ExecCompleted ExecutionStatus = "completed"
	// ExecFailed records a definite failure.
	ExecFailed ExecutionStatus = "failed"
	// ExecUnknown records an indeterminate outcome surfaced for human/hook
	// adjudication (the default terminal posture for a mutating tool whose result
	// is not known; ADR-0012; architecture §7.2).
	ExecUnknown ExecutionStatus = "unknown"
)

// ExecutionRecord is one entry in the durable dedup ledger, keyed by
// (TenantID, SessionID, IdempotencyKey) — the tool_executions primary key (ADR-0011
// §6.2). The key namespace is tenant+session scoped and the key is server-derived
// (hash(session_id, seq_of_ToolCall)); a client/model-supplied value is never a
// global key (ADR-0012 §"Idempotency key scoping"; architecture §7.3, §8.8).
type ExecutionRecord struct {
	// TenantID scopes the record (and is re-checked against the caller's verified
	// tenant before any cached result is returned; architecture §7.3).
	TenantID string
	// SessionID scopes the record to the owning session.
	SessionID string
	// IdempotencyKey is the log-derived dedup key hash(session_id, seq_of_ToolCall).
	IdempotencyKey string
	// Status is the current execution status.
	Status ExecutionStatus
	// Result is the recorded terminal observation when Status is [ExecCompleted],
	// returned for a deduplicated retry instead of re-executing (architecture §7.2).
	Result domain.Observation
}

// DedupStore is the durable (PostgreSQL-backed, not in-memory/TTL) tool-execution
// ledger that makes mutating-tool side effects at-most-once across crashes
// (ADR-0012 §"Durable dedup ledger"; architecture §7.2). It survives tool-runtime
// restarts, which is precisely when an in-memory cache would be wiped. A cache hit
// re-checks authorization against the current caller's tenant/session before
// returning bytes (architecture §7.3). Implementations must be safe for concurrent
// use.
type DedupStore interface {
	// Begin records [ExecStarted] for the key, or returns the existing
	// [ExecutionRecord] if one is already present (so a retried call observes the
	// prior status/result rather than starting a duplicate). It is the durable
	// guard taken before dispatch.
	Begin(ctx context.Context, rec ExecutionRecord) (ExecutionRecord, error)

	// Complete records a terminal status and result for the key (typically
	// [ExecCompleted] with the observation, or [ExecFailed]/[ExecUnknown]).
	Complete(ctx context.Context, rec ExecutionRecord) error

	// Lookup returns the current [ExecutionRecord] for (tenantID, sessionID,
	// idempotencyKey), used on recovery to adjudicate an open execution. It returns
	// an error if no record exists. The returned record's status drives the
	// at-most-once decision for a mutating tool (architecture §7.2).
	Lookup(ctx context.Context, tenantID, sessionID, idempotencyKey string) (ExecutionRecord, error)
}
