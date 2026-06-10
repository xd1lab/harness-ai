// Package app declares the consumer-defined ports the orchestrator's agent loop
// depends on, following the clean-architecture rule that ports are small interfaces
// declared in the package that USES them (architecture §5). The concrete adapters
// that satisfy these ports live in internal/orchestrator/adapter/* and are wired in
// the infra layer; the loop itself depends only on the interfaces here plus the
// platform ports ([github.com/xd1lab/harness-ai/internal/platform/clock.Clock],
// [github.com/xd1lab/harness-ai/internal/platform/ids.IDGenerator]) and the
// canonical [github.com/xd1lab/harness-ai/internal/platform/llm] kernel types.
//
// IMPORTANT: nothing here imports the generated gen/ protobuf package. These ports
// are the in-process contract; the gRPC↔domain mapping is done by transport
// adapters that are written separately and depend on gen/ at their edge
// (architecture §12.4). The model-gateway and tool-runtime ports are expressed over
// the llm kernel types and orchestrator domain types, not over wire types.
package app

import (
	"context"
	"errors"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ----------------------------------------------------------------------------
// EventLogPort — the in-process event store (ADR-0011, ADR-0012; architecture §4.2,
// §6). It is consumer-defined here so a later extraction into a separate writer
// service is a non-breaking adapter change (ADR-0009; architecture §2.2).
// ----------------------------------------------------------------------------

// ConflictError is the sentinel returned by [EventLogPort.Append] when the
// optimistic concurrency gate fails: the supplied expected head seq did not match
// the session's current head (a genuine write race, mapped from the DB's
// FAILED_PRECONDITION). On this error the loser reloads-and-rebases if it still
// holds the lease (architecture §9.6). It is distinct from [FencedError] (lost
// lease) and from a silent idempotent no-op (a retried append with the same
// request_id succeeds; ADR-0011 §6.3). Recover it with [errors.Is].
var ConflictError = errors.New("eventlog: optimistic concurrency conflict (stale expected head seq)")

// FencedError is the sentinel returned by [EventLogPort.Append] when the writer's
// lease epoch is stale: the session lease was taken over or expired, so this writer
// is fenced out and must yield the session rather than reload-and-rebase (ADR-0011
// §6.3; ADR-0014 §"Fenced lease"; architecture §9.6). A fenced append is rejected
// even when its expected head seq is current — the fencing token protects the
// side-effect-gating log, not just liveness. Recover it with [errors.Is].
var FencedError = errors.New("eventlog: writer fenced (stale lease epoch)")

// SessionNotActiveError is the sentinel returned by [EventLogPort.Append] when the
// target session is not [domain.StatusActive] (the append transaction's status
// guard; ADR-0011 §6.3). Recover it with [errors.Is].
var SessionNotActiveError = errors.New("eventlog: session not active")

// AppendInput carries one typed event plus the per-append idempotency token for an
// [EventLogPort.Append]. The persisted coordinates that the store assigns (seq,
// created_at) are NOT supplied here; the store returns them on the resulting
// [domain.EventEnvelope].
type AppendInput struct {
	// Event is the typed payload to append (one of the domain event structs).
	Event domain.Event
	// SchemaVersion is the payload schema version for the event type; zero means
	// the current default version (ADR-0011 §"Migration policy").
	SchemaVersion int
	// Actor is the producer of the event (defaults conceptually to
	// [domain.ActorSystem] when unset; ADR-0011 §6.2).
	Actor domain.Actor
}

// EventLogPort is the consumer-defined port over the append-only event log (the
// orchestrator's in-process event store; ADR-0009). It exposes exactly the
// operations the loop and recovery need: append (optimistic + fenced + idempotent),
// load (fold), subscribe (read-side feed), and fork. There is no network here — the
// adapter is a single pgx-backed Store (architecture §4.2, §5.1).
type EventLogPort interface {
	// Append atomically appends events to sessionID's stream under optimistic
	// concurrency. expectedHeadSeq is the seq the caller believes is current; the
	// transaction bumps sessions.head_seq from it and ties each new event's seq to
	// the transition, guaranteeing contiguity by construction (ADR-0011 §6.3).
	// leaseEpoch is the writer's fencing token, checked on the same UPDATE.
	// requestID is the per-append idempotency token: a retried Append whose
	// original committed but whose ack was lost returns the previously-stored
	// envelopes as success rather than a conflict (ADR-0011 §6.3; architecture
	// §7.3). Multiple events in one call are committed together, in order, with
	// consecutive seqs.
	//
	// On the optimistic gate failing it returns [ConflictError]; on a stale lease
	// it returns [FencedError]; on a non-active session it returns
	// [SessionNotActiveError]. The returned envelopes carry the assigned seqs and
	// timestamps.
	Append(
		ctx context.Context,
		sessionID string,
		expectedHeadSeq int64,
		leaseEpoch int64,
		requestID string,
		events ...AppendInput,
	) ([]domain.EventEnvelope, error)

	// Load folds sessionID's events into an ordered slice from fromSeq (inclusive)
	// onward, oldest first. fromSeq may be the seq just after a snapshot to avoid
	// re-reading folded history (architecture §6.6 resume). For a forked session
	// the returned stream is the child's own events; the inherited parent prefix is
	// composed by the caller using [Session.ForkedFromSeq]/parent_prefix
	// (architecture §6.6). Passing fromSeq <= 1 loads the full stream.
	Load(ctx context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error)

	// LoadSession returns the current [domain.Session] aggregate state (head seq,
	// lease, status, fork lineage) without folding the event payloads. It is used
	// to obtain expectedHeadSeq/leaseEpoch before an append and by recovery.
	LoadSession(ctx context.Context, sessionID string) (domain.Session, error)

	// Subscribe streams committed envelopes for sessionID to the returned channel
	// starting just after fromSeq, then continues delivering new events as they are
	// appended, until ctx is cancelled (at which point the channel is closed). It
	// backs the resumable client Run stream / Reattach and sub-agent observation
	// (architecture §3, §7.1). It is per-session and ordered by seq; the gap-safe
	// global projection feed used by projectord is a separate read-side concern
	// (architecture §6.6, §10.4), not this method.
	Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error)

	// Fork creates a new child session branching parentID at atSeq, captured as the
	// child's immutable forked_from_seq (validated atSeq <= parent head at fork
	// time). The child's own events continue from atSeq+1 so the composed timeline
	// has a single monotonic seq namespace; the parent may keep appending past
	// atSeq without affecting the child (a fork is a new branch, never a rewrite;
	// architecture §6.6). Fork requires the caller's tenant to own the parent and
	// never crosses tenant boundaries (ADR-0013 §"Fork ownership"; architecture
	// §8.9); it returns the new child [domain.Session].
	Fork(ctx context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error)
}

// ----------------------------------------------------------------------------
// ModelGatewayPort — client-side view of the model-gateway (architecture §4.3,
// §11). Expressed over the llm kernel types, NOT gen/ wire types.
// ----------------------------------------------------------------------------

// ModelGatewayPort is the consumer-defined, client-side port the loop uses to talk
// to the model-gateway. It is a thin projection of [llm.Provider] for the
// orchestrator: Generate for non-streaming turns, Stream for streaming turns
// (returning the provider-agnostic [llm.StreamReader] the pure assembler consumes,
// architecture §4.3), CountTokens (capability-gated), and Capabilities (keyed by
// model id, architecture §11.4). The gRPC client adapter that satisfies this maps
// gen/ ⇄ llm at the transport edge only; all provider-specific stream normalization
// lives in the gateway, so this port stays provider-agnostic (architecture §11.2).
// Failures surface as [*llm.ProviderError].
type ModelGatewayPort interface {
	// Generate runs a single non-streaming generation and returns the aggregated
	// [llm.Response]. It is never auto-retried at the RPC layer (Generate is not
	// idempotent; ADR-0012 §"gRPC retry scope"). On a [llm.Pause] stop reason the
	// response carries the continuation state in ProviderRaw to echo back.
	Generate(ctx context.Context, req llm.Request) (*llm.Response, error)

	// Stream runs a streaming generation and returns a [llm.StreamReader] of
	// normalized events terminated by a [llm.Done]. The loop forwards text/thinking
	// deltas to the client live and feeds the reader to the pure assembler
	// (architecture §4.3). The context deadline carries the turn deadline plus the
	// relay-stall deadline (architecture §9.4).
	Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error)

	// CountTokens returns the input token count for req. It is capability-gated:
	// when the model's [llm.Capabilities.SupportsTokenCounting] is false it returns
	// a [*llm.ProviderError] of kind [llm.ErrUnsupported]; it is never used for
	// billing (architecture §11.6). This RPC is idempotent and may be auto-retried
	// on UNAVAILABLE/DEADLINE_EXCEEDED (ADR-0012).
	CountTokens(ctx context.Context, req llm.Request) (int, error)

	// Capabilities returns the [llm.Capabilities] for model on the gateway's
	// endpoint. The model id is an input because capabilities are per-(endpoint,
	// model) (architecture §11.4). Idempotent; auto-retryable (ADR-0012).
	Capabilities(ctx context.Context, model string) (llm.Capabilities, error)
}

// ----------------------------------------------------------------------------
// ToolRuntimePort — client-side view of the tool-runtime service (architecture
// §4.2, §7.2, §9.2).
// ----------------------------------------------------------------------------

// ToolDescriptor is the orchestrator-side view of a tool advertised by the
// tool-runtime, carrying only what the loop and policy need to plan and gate a
// call: the name/description/schema for prompting and validation, plus the
// safety-classification flags that drive scheduling (read-only parallelism vs.
// serialized mutation; architecture §9.2) and the egress/taint gate (architecture
// §8.4). The authoritative tool model lives in
// [github.com/xd1lab/harness-ai/internal/toolruntime/domain]; this is the minimal
// projection crossing the service boundary.
type ToolDescriptor struct {
	// Name is the unique tool name (matches an [llm.ToolDef.Name]).
	Name string
	// Description is the model-facing description.
	Description string
	// JSONSchema is the tool's input JSON Schema (raw), used for the model's tool
	// definition and for validation.
	JSONSchema []byte
	// SideEffect classifies the tool as read-only or mutating; it determines
	// whether the call may be dispatched concurrently in the bounded read-only pool
	// or must be serialized via the per-session mutation mutex (architecture §9.2).
	// MCP tools default to mutating (fail-safe; ADR-0014).
	SideEffect domain.SideEffect
	// EgressClass classifies whether the tool performs external communication; an
	// external tool is subject to the egress allowlist and taint gate and is NOT
	// parallelized as a harmless read (ADR-0013; architecture §8.4, §9.2). MCP
	// tools default to external (fail-safe).
	EgressClass domain.EgressClass
}

// ToolExecution is the request to execute one tool call against the tool-runtime.
// It carries the log-derived idempotency key so the durable dedup ledger
// (tool_executions) recognizes a retried call (ADR-0012 §"Durable dedup ledger";
// architecture §7.2).
type ToolExecution struct {
	// SessionID is the owning session (scopes the sandbox and the dedup ledger).
	SessionID string
	// Call is the normalized tool call to execute (name + parsed args + id).
	Call llm.ToolCall
	// IdempotencyKey is the log-derived key hash(session_id, seq_of_ToolCall) that
	// the runtime records in the tool_executions ledger; a retried call with a
	// known-completed key returns the prior result instead of re-running (ADR-0012;
	// architecture §7.2).
	IdempotencyKey string
}

// ToolEvent is one streamed event from an in-progress [ToolRuntimePort.ExecuteTool]:
// either an incremental progress/partial-output chunk or the single terminal
// result. Exactly one field is non-nil (architecture §3, §4.2: long-running tools
// stream progress then a terminal ToolResult).
type ToolEvent struct {
	// Progress is set for an incremental progress / partial stdout chunk.
	Progress *ToolProgress
	// Result is set for the single terminal result of the execution.
	Result *ToolResult
}

// ToolProgress is an incremental progress or partial-output chunk streamed during a
// tool execution.
type ToolProgress struct {
	// Output is the partial output chunk (e.g. streamed stdout) since the last
	// progress event.
	Output string
}

// ToolResult is the terminal outcome of a tool execution as returned across the
// service boundary, before the orchestrator masks output (ADR-0013 §"Output
// masking") and persists a [domain.ToolResult] event (offloading large output to a
// blob; architecture §6.4).
type ToolResult struct {
	// Content is the tool's textual result as the model should see it.
	Content string
	// IsError reports whether the execution failed.
	IsError bool
	// Truncated reports whether Content is a truncated view of a larger output
	// (the full bytes offloaded to a blob; architecture §6.4).
	Truncated bool
	// BlobRef is the tenant-scoped blob key when the full output was offloaded,
	// empty when inlined (architecture §6.4).
	BlobRef string
}

// ToolStream is the reader the loop drives over an in-progress tool execution. It
// mirrors [llm.StreamReader]'s shape (Recv/Close) for consistency, so the loop can
// relay [ToolProgress] to the client and assemble the terminal [ToolResult].
type ToolStream interface {
	// Recv returns the next [ToolEvent]. It returns [io.EOF] after the terminal
	// result event, or a non-nil error on failure. Not safe for concurrent use.
	Recv() (ToolEvent, error)
	// Close releases the stream's resources; safe to call after Recv returns an
	// error or EOF, or to abandon early on a cancelled context (which maps to a
	// real cgroup/PID-namespace kill in the runtime; architecture §9.3).
	Close() error
}

// ToolRuntimePort is the consumer-defined, client-side port the loop uses to
// execute tools and to enumerate the available tool set. Cancellation of the
// execution context propagates across the gRPC boundary and into the sandbox as a
// process-group/cgroup kill (architecture §9.3). ExecuteTool of a mutating tool is
// NEVER auto-retried at the RPC layer (ADR-0012 §"gRPC retry scope").
type ToolRuntimePort interface {
	// ExecuteTool dispatches one tool call and returns a [ToolStream] of progress
	// events terminated by a result. The orchestrator MUST have committed a
	// [domain.ToolExecutionStarted] before calling this (durable execution intent
	// before side effects; ADR-0012 §7.2). On a cancelled ctx the runtime kills the
	// in-sandbox process tree, not just the exec wrapper (architecture §9.3).
	ExecuteTool(ctx context.Context, exec ToolExecution) (ToolStream, error)

	// ListTools returns the currently registered tools (native + already-loaded
	// MCP) as orchestrator-side [ToolDescriptor]s, so the loop can build the
	// model's tool definitions and the policy/scheduler can read each tool's
	// SideEffect/EgressClass (architecture §9.2). MCP schema loading is lazy and
	// untrusted-by-default in the runtime; newly-appearing tools are gated
	// (ADR-0013 §"MCP server confinement").
	ListTools(ctx context.Context, sessionID string) ([]ToolDescriptor, error)
}

// ----------------------------------------------------------------------------
// ApprovalGate — the human-in-the-loop ask gate (architecture §3, §8.13, §9.3).
// ----------------------------------------------------------------------------

// ApprovalRequest is a request for human approval of a risk-tiered action raised by
// the policy pipeline's ask outcome (architecture §8.13). It is streamed to the
// client as an ApprovalRequest frame; the client responds out-of-band via the
// unary Control RPC (architecture §4.2), which the adapter resolves through
// [ApprovalGate.Resolve].
type ApprovalRequest struct {
	// SessionID is the session the approval is for.
	SessionID string
	// CallID is the [llm.ToolCall.ID] awaiting approval.
	CallID string
	// ToolName is the tool awaiting approval.
	ToolName string
	// Reason is a short, human-readable explanation of why approval is required
	// (e.g. mutating tool, or external comms under taint; architecture §8.4).
	Reason string
	// Args is the parsed tool-call arguments shown to the human for the approval
	// decision (mirrors the wire ApprovalRequest.args_json so the gen↔app mapping is
	// total); nil when no args apply.
	Args map[string]any
}

// ApprovalGate mediates the human ask gate. The loop calls [ApprovalGate.Request]
// and blocks (on its context) until a human decision arrives; the control-RPC
// adapter delivers the decision via [ApprovalGate.Resolve]. In tests the gate is an
// in-memory channel so the loop is exercised without real gRPC (architecture §9.3:
// "delivered through the ApprovalGate/control port so it is testable without real
// gRPC"). Interrupt is delivered as a cancellation of the loop context, not through
// this gate.
type ApprovalGate interface {
	// Request raises req and blocks until the human resolves it or ctx is
	// cancelled. It returns the [domain.AskResolution] ([domain.AskAllowed] or
	// [domain.AskDenied]); a cancelled ctx returns ctx.Err(). The loop persists the
	// outcome as a [domain.PermissionDecided] event (architecture §3, §8.13).
	Request(ctx context.Context, req ApprovalRequest) (domain.AskResolution, error)

	// Resolve delivers a human decision for the pending approval identified by
	// (sessionID, callID), unblocking the corresponding [ApprovalGate.Request]. It
	// is invoked by the Control-RPC adapter. It returns an error if no matching
	// pending request exists.
	Resolve(ctx context.Context, sessionID, callID string, resolution domain.AskResolution) error
}

// ----------------------------------------------------------------------------
// HookRunner — PreToolUse/PostToolUse/Stop/PreCompact middleware (architecture
// §2.3, §5.1 hooks).
// ----------------------------------------------------------------------------

// HookEvent identifies which lifecycle point a hook is being run for (architecture
// §5.1: PreToolUse/PostToolUse/Stop/PreCompact).
type HookEvent string

const (
	// HookPreToolUse runs before a tool is dispatched; it may block the call
	// (architecture §3: "hooks: PreToolUse -> may block").
	HookPreToolUse HookEvent = "PreToolUse"
	// HookPostToolUse runs after a tool result is received and before it is fed
	// back to the model (architecture §3).
	HookPostToolUse HookEvent = "PostToolUse"
	// HookStop runs when the loop is about to stop a turn/run, and may veto the
	// stop to continue (architecture §5.1 hooks).
	HookStop HookEvent = "Stop"
	// HookPreCompact runs before the context manager performs compaction
	// (architecture §5.1 hooks).
	HookPreCompact HookEvent = "PreCompact"
)

// HookInput is the payload handed to a hook run. The relevant fields depend on the
// [HookEvent]: tool-scoped events populate the tool fields, while Stop/PreCompact
// populate only the session/turn context.
type HookInput struct {
	// Event is the lifecycle point being run.
	Event HookEvent
	// SessionID is the session the hook runs in.
	SessionID string
	// TurnID is the current turn id, when applicable.
	TurnID string
	// CallID is the tool call id, for [HookPreToolUse]/[HookPostToolUse].
	CallID string
	// ToolName is the tool name, for tool-scoped events.
	ToolName string
	// ToolArgs is the parsed tool arguments, for [HookPreToolUse].
	ToolArgs map[string]any
	// ToolResult is the tool result content, for [HookPostToolUse].
	ToolResult string
}

// HookDecision is the allow/block outcome of a hook run (architecture §2.3:
// "Run(PreToolUse/PostToolUse/Stop/PreCompact) -> allow/block").
type HookDecision struct {
	// Allow reports whether the lifecycle action may proceed. When false the loop
	// blocks the action (e.g. refuses to dispatch the tool, or vetoes the stop).
	Allow bool
	// Reason is a short, human-readable explanation surfaced for audit/diagnostics
	// when a hook blocks (or annotates) an action.
	Reason string
}

// HookRunner runs the configured hook chain for a lifecycle event and reports the
// aggregate allow/block decision. It is an in-process port (architecture §2.3); the
// subprocess-invoking implementation lives in an adapter behind a CommandRunner
// port so loop tests stay deterministic (architecture §5.1). A blocking decision
// from any hook in the chain blocks the action.
type HookRunner interface {
	// Run executes the hook chain for in.Event and returns the aggregate
	// [HookDecision]. An error indicates the hook mechanism itself failed (distinct
	// from a clean block, which is Allow=false with no error). ctx cancellation
	// aborts the run.
	Run(ctx context.Context, in HookInput) (HookDecision, error)
}

// ----------------------------------------------------------------------------
// SubAgentPort — depth-limited sub-agents as ordinary tools (FR-EXT-04;
// architecture §5.1, §9.5).
// ----------------------------------------------------------------------------

// SubAgentSpawn is the input to spawn a child agent loop.
type SubAgentSpawn struct {
	// ParentSessionID is the spawning (parent) session.
	ParentSessionID string
	// Depth is the depth of the child to spawn — the parent's depth + 1. It MUST be
	// <= [SubAgentPort.MaxDepth] or the spawn is rejected without creating a session
	// (FR-EXT-04 AC-2).
	Depth int
	// Task is the natural-language task handed to the child agent.
	Task string
	// Model optionally overrides the model for the child; empty inherits the parent's.
	Model string
}

// SubAgentPort spawns a depth-limited child agent loop, exposed to the model as an
// ordinary tool (FR-EXT-04; architecture §5.1, §9.5). The child runs its own loop
// over a fresh child session (created via [EventLogPort.Fork] or a new session) and
// returns a condensed observation to the parent. Recursion is bounded: a spawn whose
// depth would exceed [SubAgentPort.MaxDepth] returns an error observation WITHOUT
// creating a session, so the cap is a deterministic, unit-testable property.
type SubAgentPort interface {
	// Spawn runs a child agent loop for in.Task at in.Depth and returns its condensed
	// result as a [ToolResult]. If in.Depth > MaxDepth it returns a [ToolResult] with
	// IsError set and content "max sub-agent depth exceeded" and does NOT spawn a
	// session (FR-EXT-04 AC-2). Cancelling ctx cancels the child loop.
	Spawn(ctx context.Context, in SubAgentSpawn) (ToolResult, error)
	// MaxDepth returns the configured maximum sub-agent recursion depth (FR-EXT-04).
	MaxDepth() int
}
