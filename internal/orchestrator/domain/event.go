package domain

import (
	"fmt"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// EventType is the discriminator for an event stored in the append-only log. It
// matches the events.event_type column (ADR-0011 §6.2) and is the tag carried by
// [EventEnvelope.Type]. The set is closed in the domain (new kinds are added by
// code change, not by free string), while payload evolution within a kind is
// handled by schema_version (ADR-0011 §"Migration policy").
type EventType string

const (
	// EventSessionStarted records the creation/opening of a session stream. It is
	// the conventional first event of a fresh (non-forked) session.
	EventSessionStarted EventType = "SessionStarted"
	// EventMessageAppended records a normalized user (or tool-result-bearing)
	// message appended to the conversation (events.event_type "UserMessage" family;
	// ADR-0011 §6.2).
	EventMessageAppended EventType = "MessageAppended"
	// EventTurnStarted marks a turn in flight BEFORE Generate is called, so a
	// recovered orchestrator can detect an interrupted turn (ADR-0012 §"Durable
	// turn boundaries"; architecture §7.1).
	EventTurnStarted EventType = "TurnStarted"
	// EventAssistantMessageDelta is a periodic streaming checkpoint of partial
	// assistant text, bounding lost work on crash to one checkpoint interval
	// (ADR-0012; architecture §7.1).
	EventAssistantMessageDelta EventType = "AssistantMessageDelta"
	// EventAssistantMessage records the assembled assistant message for a turn,
	// including any tool calls, usage, and the provider-raw continuation slot
	// (architecture §4.3, §11.1).
	EventAssistantMessage EventType = "AssistantMessage"
	// EventToolExecutionStarted records durable execution intent committed BEFORE
	// dispatch, carrying the log-derived idempotency key (ADR-0012 §"Durable
	// execution intent"; architecture §7.2).
	EventToolExecutionStarted EventType = "ToolExecutionStarted"
	// EventToolResult records the outcome of a tool execution fed back to the model
	// (architecture §3, §6.2).
	EventToolResult EventType = "ToolResult"
	// EventToolResultCleared supersedes a prior ToolResult append-only, referencing
	// it by (session_id, seq) so it is unambiguous after a fork (ADR-0011
	// §"Tool-result clearing"; architecture §6.5).
	EventToolResultCleared EventType = "ToolResultCleared"
	// EventTurnAborted records an in-flight turn deterministically abandoned on
	// recovery, with the usage accrued so far so cost is accounted, not
	// under-counted (ADR-0012; architecture §7.1).
	EventTurnAborted EventType = "TurnAborted"
	// EventTurnFinished records normal turn completion with usage, cost, and the
	// turn count (architecture §3).
	EventTurnFinished EventType = "TurnFinished"
	// EventCompactionPerformed records a context-compaction boundary (token
	// reclamation / cache-prefix re-marking) performed by the context manager
	// (architecture §5.1 context; events.event_type "CompactBoundary" family).
	EventCompactionPerformed EventType = "CompactionPerformed"
	// EventPermissionDecided records a permission-pipeline outcome (allow/deny/ask
	// and, for ask, the human approval grant/deny) persisted as an event
	// (architecture §3, §8.13). See [PermissionDecided].
	EventPermissionDecided EventType = "PermissionDecided"
	// EventWorkspaceReset records that resume re-attached to a fresh container and
	// uncommitted filesystem state from the prior container was lost (ADR-0012
	// §"Clean-workspace resume"; architecture §7.5).
	EventWorkspaceReset EventType = "WorkspaceReset"
	// EventBypassModeActivated records an operator activating a permission mode
	// (notably bypass), with the actor identity, for audit (FR-PERM-02 AC-3,
	// NFR-SEC-06(d); ADR-0013). See [BypassModeActivated].
	EventBypassModeActivated EventType = "BypassModeActivated"
	// EventMCPToolApprovalRequested records that an MCP server tool requires
	// first-use approval before admission to the active registry (FR-EXT-02;
	// NFR-SEC-07). See [MCPToolApprovalRequested].
	EventMCPToolApprovalRequested EventType = "MCPToolApprovalRequested"
	// EventMCPToolApprovalResolved records the human resolution of an MCP first-use
	// approval gate (FR-EXT-02). See [MCPToolApprovalResolved].
	EventMCPToolApprovalResolved EventType = "MCPToolApprovalResolved"
	// EventPlanUpdated records the model's current task plan (todo list) emitted via
	// the in-loop virtual tool todo_write (ADR-0031). It is a durable, time-travelable
	// planning primitive: the latest PlanUpdated for a session is the agent's working
	// plan, re-surfaced into the model context. It carries only non-secret plan text,
	// so the read-plane exposes it as a normal (non-redacted) descriptor.
	EventPlanUpdated EventType = "PlanUpdated"
	// EventApprovalRequested records that a per-dispatch tool call entered the ask
	// gate and is BLOCKING on a human approval, appended BEFORE the gate blocks so a
	// pending ask is durable across a crash/restart (ADR-0032). It is the pre-block
	// half of the ask; the AFTER-resolution outcome is recorded by the existing
	// [PermissionDecided] (the pair ApprovalRequested -> PermissionDecided{ask,Resolved}
	// brackets one ask). See [ApprovalRequested].
	EventApprovalRequested EventType = "ApprovalRequested"
)

// EventEnvelope is the persisted wrapper around a typed [Event] payload. It mirrors
// the non-payload columns of the events table (ADR-0011 §6.2): the per-session
// contiguous Seq, the per-append idempotency RequestID, the tenant/session scoping,
// the schema version, and the timestamp. The typed payload itself is carried in
// [EventEnvelope.Event]; the envelope's [EventEnvelope.Type] always equals the
// payload's [Event.EventType].
//
// Seq is contiguous by construction: it is the value the head-transition UPDATE
// returns, never an app-computed expected+1 (ADR-0011 §6.3). Within a forked branch
// Seq continues from the parent's forked_from_seq+1, so the composed root-to-leaf
// timeline has a single monotonic Seq namespace (architecture §6.6).
type EventEnvelope struct {
	// Type is the event discriminator; it equals Event.EventType().
	Type EventType
	// Seq is the per-session contiguous sequence number (1..N), the optimistic
	// version coordinate and the client Last-Event-ID (architecture §6.3, §7.1).
	Seq int64
	// SessionID is the owning session (the event-sourcing stream).
	SessionID string
	// TenantID is the owning tenant; every row carries it for RLS (ADR-0011 §6.7).
	TenantID string
	// RequestID is the per-append idempotency token; a retried append with the
	// same RequestID is a no-op success rather than a conflict (ADR-0011 §6.3).
	RequestID string
	// SchemaVersion is the payload schema version for this event type, enabling
	// forward-only payload evolution (ADR-0011 §"Migration policy").
	SchemaVersion int
	// Actor identifies who produced the event: user, assistant, tool, or system
	// (events.actor; ADR-0011 §6.2).
	Actor Actor
	// CreatedAt is the server-assigned append time (events.created_at).
	CreatedAt time.Time
	// Event is the typed payload. It is one of the concrete event structs in this
	// package; switch on Type or type-assert to consume it. It is decoded from
	// [PayloadCanonical] when present (so the served payload is the exact,
	// hash-protected bytes), falling back to the legacy events.payload JSONB for
	// unchained pre-0009 rows.
	Event Event
	// PayloadCanonical is the EXACT, verbatim json.Marshal bytes the append path
	// took ContentHash over and stored RAW in events.payload_canonical (ADR-0033).
	// VerifyChainIntegrity hashes these raw stored bytes directly — no
	// decode/re-marshal — so structural/additive tampering of the stored payload
	// (key reorder, whitespace, an injected extra key) changes the hashed bytes
	// and is detected. It is nil for unchained pre-0009 rows (which carry only the
	// legacy events.payload JSONB).
	PayloadCanonical []byte
	// ContentHash is the SHA-256 over the EXACT stored events.payload_canonical
	// bytes for this event (the per-event content digest; ADR-0033). It is nil for
	// unchained pre-0009 rows and populated on the append return path and on
	// every read-back. See [ContentHash].
	ContentHash []byte
	// ChainHash is the per-session running fold SHA-256( prev_chain_hash ||
	// ContentHash ) linking this event to its predecessor (ADR-0033). It is nil
	// for unchained pre-0009 rows. See [ChainHash] / [GenesisChainHash].
	ChainHash []byte
}

// Actor identifies the producer of an event, matching the events.actor column
// (ADR-0011 §6.2).
type Actor string

const (
	// ActorUser denotes an event originating from the end user.
	ActorUser Actor = "user"
	// ActorAssistant denotes an event originating from the model.
	ActorAssistant Actor = "assistant"
	// ActorTool denotes an event originating from a tool execution.
	ActorTool Actor = "tool"
	// ActorSystem denotes an event originating from the harness itself (the
	// default; ADR-0011 §6.2).
	ActorSystem Actor = "system"
)

// Event is the sealed interface implemented by every typed event payload in this
// package. It is sealed via the unexported [Event.isEvent] marker so the set of
// event kinds is closed to this package — adding an event is a deliberate code
// change, and an exhaustive type switch over events is checkable. Every payload
// reports its [EventType] so an envelope's tag and payload cannot drift apart.
//
// Payloads carry only domain data. The persisted coordinates (seq, request_id,
// tenant, timestamp, actor) live on the [EventEnvelope], not duplicated in each
// payload.
type Event interface {
	// EventType returns the discriminator for this payload; it must match the
	// [EventEnvelope.Type] the payload is wrapped in.
	EventType() EventType
	// isEvent is an unexported marker that seals the interface to this package.
	isEvent()
}

// SessionStarted is appended when a session stream is opened. For a fresh session
// it is the first event; a forked session instead inherits a parent prefix and its
// first own event continues from forked_from_seq+1 (architecture §6.6).
type SessionStarted struct {
	// ParentID is the parent session id when this session is a fork, empty
	// otherwise (sessions.parent_id; ADR-0011 §6.2).
	ParentID string
	// ForkedFromSeq is the frozen parent seq this session branched at when forked;
	// zero/unset for a non-fork (sessions.forked_from_seq; architecture §6.6).
	ForkedFromSeq int64
	// SystemPrompt is the system prompt the session was opened with. It is recorded
	// so replay is faithful and so the tenant-agnostic cache prefix can be
	// reconstructed (architecture §8.10).
	SystemPrompt string
}

// EventType identifies this payload as [EventSessionStarted].
func (SessionStarted) EventType() EventType { return EventSessionStarted }
func (SessionStarted) isEvent()             {}

// MessageAppended records a normalized message appended to the conversation —
// typically a user turn or the user-role turn that carries tool results back to the
// model. The message uses the canonical [llm.Message] kernel type (architecture
// §12.3: a single source of truth, never a mirrored copy).
type MessageAppended struct {
	// Message is the appended normalized turn.
	Message llm.Message
}

// EventType identifies this payload as [EventMessageAppended].
func (MessageAppended) EventType() EventType { return EventMessageAppended }
func (MessageAppended) isEvent()             {}

// TurnStarted marks a turn in flight before Generate is invoked. Its presence with
// no terminal [AssistantMessage]/[TurnAborted] for the same TurnID tells a recovered
// orchestrator that a turn was interrupted (ADR-0012; architecture §7.1).
type TurnStarted struct {
	// TurnID is the unique id of this turn within the session (minted via the
	// IDGenerator port). It correlates the start, deltas, and terminal event.
	TurnID string
	// Model is the target model id for the turn (llm.Request.Model).
	Model string
}

// EventType identifies this payload as [EventTurnStarted].
func (TurnStarted) EventType() EventType { return EventTurnStarted }
func (TurnStarted) isEvent()             {}

// AssistantMessageDelta is a periodic checkpoint of the partial assistant text
// produced so far during streaming, bounding lost work on crash to one checkpoint
// interval (ADR-0012; architecture §7.1). Raw byte-deltas are not stored; this is a
// coarse running-text checkpoint plus the provider's resumable cursor when one
// exists.
type AssistantMessageDelta struct {
	// TurnID is the turn this checkpoint belongs to (matches [TurnStarted.TurnID]).
	TurnID string
	// TextSoFar is the accumulated assistant text at the checkpoint instant.
	TextSoFar string
	// UsageSoFar is the partial token usage read from the provider stream at this
	// checkpoint instant. On crash recovery the fold picks the last emitted delta's
	// UsageSoFar as the recovered usage so partial turns are accounted and never
	// silently re-billed (ADR-0012 §"Durable turn boundaries"; architecture §7.1).
	// It is the zero [llm.Usage] when the provider has not yet emitted usage metadata.
	UsageSoFar llm.Usage
	// ProviderRaw is the provider's resumable continuation cursor when available
	// (e.g. Anthropic pause_turn content), used to continue rather than re-run on
	// recovery (architecture §7.1, §11.1). Nil when the provider has none.
	ProviderRaw llm.ProviderRaw
}

// EventType identifies this payload as [EventAssistantMessageDelta].
func (AssistantMessageDelta) EventType() EventType { return EventAssistantMessageDelta }
func (AssistantMessageDelta) isEvent()             {}

// AssistantMessage records the assembled assistant turn: the model-visible message
// (which may include tool-call content parts), the normalized stop reason and
// usage, and the opaque provider-raw continuation slot. ProviderRaw is the byte-
// faithful source of truth for the next provider call (Anthropic server_tool_use /
// thinking signatures, OpenAI Responses Items), so a turn can be continued or
// replayed faithfully (architecture §4.3, §11.1).
type AssistantMessage struct {
	// TurnID is the turn this message completes (matches [TurnStarted.TurnID]).
	TurnID string
	// Message is the assembled, model-visible assistant turn.
	Message llm.Message
	// StopReason is the normalized reason generation stopped; [llm.Pause] is
	// non-terminal and signals a continuation (architecture §11.3).
	StopReason llm.StopReason
	// RawStopReason is the verbatim provider stop-reason string, authoritative when
	// StopReason is [llm.StopOther] (architecture §11.3).
	RawStopReason string
	// Usage is the normalized token usage for the turn, read from the provider's
	// authoritative field (architecture §11.6).
	Usage llm.Usage
	// CostUSD is the per-turn cost computed in the gateway from Usage and model
	// pricing (architecture §11.6); recorded on the event for cost rollup.
	CostUSD float64
	// ProviderRaw is the opaque provider continuation blob, persisted to the
	// events.provider_raw column. It is echoed back via [llm.Request.ProviderRaw]
	// to continue (notably on [llm.Pause]) or to replay byte-faithfully
	// (architecture §11.1). Nil when no continuation state is needed.
	ProviderRaw llm.ProviderRaw
}

// EventType identifies this payload as [EventAssistantMessage].
func (AssistantMessage) EventType() EventType { return EventAssistantMessage }
func (AssistantMessage) isEvent()             {}

// ToolExecutionStarted records durable execution intent committed to the log BEFORE
// the tool is dispatched. The IdempotencyKey is log-derived
// (hash(session_id, seq_of_ToolCall)) so any orchestrator replaying the log
// reconstructs the same key; a ToolExecutionStarted with no terminal [ToolResult]
// marks an UNKNOWN-outcome execution that a mutating tool must NOT blindly re-run
// (ADR-0012 §"At-most-once recovery"; architecture §7.2).
type ToolExecutionStarted struct {
	// CallID is the [llm.ToolCall.ID] of the tool call being executed.
	CallID string
	// ToolName is the name of the tool being executed.
	ToolName string
	// IdempotencyKey is the log-derived dedup key hash(session_id, seq_of_ToolCall)
	// passed to the tool-runtime and recorded in the tool_executions ledger
	// (ADR-0012; architecture §7.2). It is reconstructed deterministically on
	// replay, never a fresh id.
	IdempotencyKey string
}

// EventType identifies this payload as [EventToolExecutionStarted].
func (ToolExecutionStarted) EventType() EventType { return EventToolExecutionStarted }
func (ToolExecutionStarted) isEvent()             {}

// ToolResult records the outcome of a tool execution, fed back to the model. Large
// content is offloaded to the blob store and referenced by BlobRef; the inline
// Result then holds a lightweight descriptor (architecture §6.4).
type ToolResult struct {
	// CallID matches the [ToolExecutionStarted.CallID]/[llm.ToolCall.ID] this
	// result answers.
	CallID string
	// Result is the model-visible textual result (or a lightweight descriptor when
	// the full bytes are offloaded to a blob; architecture §6.4).
	Result string
	// IsError reports whether the tool execution failed; surfaced to the model so
	// it can adapt (mirrors [llm.ToolResult.IsError]).
	IsError bool
	// Truncated reports whether Result is a truncated view of a larger output
	// retrievable via BlobRef (architecture §6.4).
	Truncated bool
	// BlobRef is the tenant-scoped blob key when the full output was offloaded
	// (events.blob_ref → blobs(tenant_id, ref); architecture §6.4). Empty when the
	// result was inlined.
	BlobRef string
}

// EventType identifies this payload as [EventToolResult].
func (ToolResult) EventType() EventType { return EventToolResult }
func (ToolResult) isEvent()             {}

// ToolResultCleared supersedes a prior [ToolResult] without deleting it
// (append-only). It references the target by the (SessionID, Seq) pair — the global
// ordering key, not a bare seq — so it is unambiguous after a fork. Clearing is
// idempotent and validated at append time (ADR-0011 §"Tool-result clearing";
// architecture §6.5).
type ToolResultCleared struct {
	// ClearedSessionID is the session of the ToolResult being cleared (part of the
	// fork-safe (session_id, seq) reference; architecture §6.5).
	ClearedSessionID string
	// ClearedSeq is the seq of the ToolResult being cleared.
	ClearedSeq int64
	// Reason is a short human/diagnostic note for why the result was cleared (e.g.
	// context reclamation by the context manager).
	Reason string
}

// EventType identifies this payload as [EventToolResultCleared].
func (ToolResultCleared) EventType() EventType { return EventToolResultCleared }
func (ToolResultCleared) isEvent()             {}

// TurnAborted records an in-flight turn deterministically abandoned during recovery
// (or by interrupt), capturing the usage accrued so far so the partial turn is
// accounted and never silently re-billed (ADR-0012; architecture §7.1).
type TurnAborted struct {
	// TurnID is the aborted turn (matches [TurnStarted.TurnID]).
	TurnID string
	// Reason is the termination reason for the abort; for an interrupt or recovery
	// abort it is typically [ErrorDuringExecution] (see [TerminationReason]).
	Reason TerminationReason
	// UsageSoFar is the provider usage read from the stream's last usage metadata
	// at the abort instant (architecture §7.1, §11.6).
	UsageSoFar llm.Usage
	// CostUSD is the cost of the partial turn computed from UsageSoFar.
	CostUSD float64
}

// EventType identifies this payload as [EventTurnAborted].
func (TurnAborted) EventType() EventType { return EventTurnAborted }
func (TurnAborted) isEvent()             {}

// TurnFinished records normal completion of a turn with final usage, cost, and the
// running turn count, and the terminal [TerminationReason] for the overall run when
// the turn ends it (architecture §3, §10.5: error rate is broken down by typed
// termination subtype).
type TurnFinished struct {
	// TurnID is the finished turn (matches [TurnStarted.TurnID]).
	TurnID string
	// Reason is the terminal reason this turn ended the run, e.g.
	// [Success] for a normal text-only completion or one of the error/refusal
	// subtypes when a cap or refusal terminated it (see [TerminationReason]).
	Reason TerminationReason
	// Usage is the normalized usage for this turn.
	Usage llm.Usage
	// CostUSD is the per-turn cost.
	CostUSD float64
	// NumTurns is the cumulative number of turns in the run up to and including
	// this one (architecture §3 TurnFinished{num_turns}).
	NumTurns int
}

// EventType identifies this payload as [EventTurnFinished].
func (TurnFinished) EventType() EventType { return EventTurnFinished }
func (TurnFinished) isEvent()             {}

// CompactionPerformed records a context-compaction boundary: the context manager
// reclaimed tokens (and/or cleared tool results) and re-marked the cache prefix
// (architecture §5.1 context, §6.2 CompactBoundary). It is append-only history of
// what the model-visible window was reduced to.
type CompactionPerformed struct {
	// BeforeTokens is the model-visible token count before compaction.
	BeforeTokens int
	// AfterTokens is the model-visible token count after compaction.
	AfterTokens int
	// Reason is a short note for why compaction ran (e.g. approaching the context
	// window, or a [llm.StopContextWindowExceeded] compact-and-retry; architecture
	// §11.3).
	Reason string
}

// EventType identifies this payload as [EventCompactionPerformed].
func (CompactionPerformed) EventType() EventType { return EventCompactionPerformed }
func (CompactionPerformed) isEvent()             {}

// PermissionDecided records the outcome of the permission pipeline for a tool
// dispatch (deny→mode→allow→ask; ADR-0013 §"Constrained bypass"; architecture
// §8.13), persisted as an event so approvals/denials are auditable. For an ask
// outcome resolved by a human, the resolved grant/deny is captured here too.
//
// NOTE (ADR-0032, un-collapsing): the architecture's
// ApprovalRequested/ApprovalGranted/ApprovalDenied family was ORIGINALLY modeled as
// this single decision event with a [PermissionDecision]. To make a blocking ask
// DURABLE across a crash/restart (the gate previously held the pending ask only in
// an in-memory map), the pre-block half is now split out into a distinct sealed
// [ApprovalRequested] event appended BEFORE the gate blocks; PermissionDecided
// remains the AFTER-resolution record. The pair
// (ApprovalRequested -> PermissionDecided{Decision:Ask, Resolved:...}) brackets one
// ask, and a lone ApprovalRequested with no matching PermissionDecided is a
// suspended-awaiting-approval turn on recovery.
type PermissionDecided struct {
	// CallID is the [llm.ToolCall.ID] the decision applies to.
	CallID string
	// ToolName is the tool the decision applies to.
	ToolName string
	// Decision is the pipeline outcome: allow, deny, or ask.
	Decision PermissionDecision
	// Resolved, set when Decision was ask, is the human resolution (allowed or
	// denied); it is the zero value for non-ask decisions.
	Resolved AskResolution
	// RuleID identifies the matched rule (or mode) that produced the decision, for
	// audit and debugging. Empty when no explicit rule matched.
	RuleID string
	// Reason is a short, human-readable explanation persisted with the decision —
	// e.g. "hook_blocked" (a PreToolUse hook blocked the call; FR-EXT-03 AC-1),
	// "egress-denied", or a deny-rule description. Distinct from RuleID so the cause
	// of a deny/hook-block is assertable from the log even when no rule id applies.
	Reason string
}

// EventType identifies this payload as [EventPermissionDecided].
func (PermissionDecided) EventType() EventType { return EventPermissionDecided }
func (PermissionDecided) isEvent()             {}

// PermissionDecision is the persisted form of a permission-pipeline outcome stored
// on a [PermissionDecided] event. It intentionally mirrors the policy package's
// decision vocabulary (allow/deny/ask) without importing it, keeping the domain
// dependency-free; the policy package owns the live decision type.
type PermissionDecision string

const (
	// PermissionAllow records that the action was permitted without a prompt.
	PermissionAllow PermissionDecision = "allow"
	// PermissionDeny records that the action was denied unconditionally.
	PermissionDeny PermissionDecision = "deny"
	// PermissionAsk records that the action required human approval; see
	// [PermissionDecided.Resolved] for how it was resolved.
	PermissionAsk PermissionDecision = "ask"
)

// AskResolution is the human resolution of an ask-gated permission, recorded on a
// [PermissionDecided] event when [PermissionDecided.Decision] is [PermissionAsk].
type AskResolution string

const (
	// AskUnresolved is the zero value, used when the decision was not an ask (or is
	// not yet resolved).
	AskUnresolved AskResolution = ""
	// AskAllowed records that the human approved the action.
	AskAllowed AskResolution = "allowed"
	// AskDenied records that the human rejected the action.
	AskDenied AskResolution = "denied"
)

// WorkspaceReset records that on resume the session re-attached to a fresh
// per-session container and uncommitted filesystem state from the prior container
// was lost; the system prompt/notice informs the agent (ADR-0012 §"Clean-workspace
// resume"; architecture §7.5).
type WorkspaceReset struct {
	// Reason is a short note for the reset (typically "resume after crash").
	Reason string
}

// EventType identifies this payload as [EventWorkspaceReset].
func (WorkspaceReset) EventType() EventType { return EventWorkspaceReset }
func (WorkspaceReset) isEvent()             {}

// PermissionMode is the persisted form of the operating mode in force when a
// permission decision or bypass activation was recorded. It mirrors the policy
// package's mode vocabulary (default/acceptEdits/plan/bypass) without importing it,
// keeping the domain dependency-free (the same pattern as [PermissionDecision]).
type PermissionMode string

const (
	// ModeDefault is the standard mode: the deny→mode→allow→ask pipeline applies in
	// full.
	ModeDefault PermissionMode = "default"
	// ModeAcceptEdits auto-approves file-edit tools while still honoring deny rules.
	ModeAcceptEdits PermissionMode = "acceptEdits"
	// ModePlan forbids mutating actions (plan-only).
	ModePlan PermissionMode = "plan"
	// ModeBypass collapses the deny→mode→allow→ask pipeline. It is operator-only,
	// audited, and forbidden under untrusted/multi-tenant (ADR-0013 §"Constrained
	// bypass"); its activation is recorded by [BypassModeActivated].
	ModeBypass PermissionMode = "bypass"
)

// BypassModeActivated records that an operator activated (or changed into) a
// permission mode, most importantly bypass mode. It is a distinct, audited event
// carrying the concrete actor identity and the mode transition so every activation
// is traceable (FR-PERM-02 AC-3, NFR-SEC-06(d); ADR-0013 §"Constrained bypass"). The
// coarse [EventEnvelope.Actor] is "system"/"user"; Principal records the concrete
// operator responsible.
type BypassModeActivated struct {
	// Principal is the concrete operator/principal identity that activated the mode,
	// distinct from the coarse envelope Actor, for the audit trail.
	Principal string
	// PriorMode is the mode in force before activation.
	PriorMode PermissionMode
	// NewMode is the mode activated (typically [ModeBypass]).
	NewMode PermissionMode
	// Reason is an optional operator-supplied justification for the activation.
	Reason string
}

// EventType identifies this payload as [EventBypassModeActivated].
func (BypassModeActivated) EventType() EventType { return EventBypassModeActivated }
func (BypassModeActivated) isEvent()             {}

// MCPToolApprovalRequested records that an MCP server's tool requires first-use
// approval before it is admitted to the active registry (FR-EXT-02; NFR-SEC-07).
// This is a registration-time gate distinct from the per-dispatch [PermissionDecided]
// gate. The MCP-supplied description is treated as UNTRUSTED (tool-poisoning defense;
// ADR-0013 §"MCP confinement") and the server identity is pinned.
type MCPToolApprovalRequested struct {
	// ServerName is the configured name of the MCP server providing the tool.
	ServerName string
	// ServerVersion is the pinned server identity/version the approval is bound to;
	// a version change requires re-approval (ADR-0013).
	ServerVersion string
	// ToolName is the MCP tool pending first-use approval.
	ToolName string
	// UntrustedDescription is the tool description as supplied by the MCP server. It
	// is UNTRUSTED input shown for the approval decision and is never trusted as
	// policy or instruction (tool-poisoning defense; ADR-0013).
	UntrustedDescription string
}

// EventType identifies this payload as [EventMCPToolApprovalRequested].
func (MCPToolApprovalRequested) EventType() EventType { return EventMCPToolApprovalRequested }
func (MCPToolApprovalRequested) isEvent()             {}

// MCPToolApprovalResolved records the human resolution of an
// [MCPToolApprovalRequested] gate. Until a grant is recorded the tool is held in the
// pending queue and is NOT available in the next Generate context (FR-EXT-02 AC-1/2).
type MCPToolApprovalResolved struct {
	// ServerName is the MCP server whose tool was being approved.
	ServerName string
	// ToolName is the tool that was approved or rejected.
	ToolName string
	// Granted reports whether the tool was approved for use.
	Granted bool
}

// EventType identifies this payload as [EventMCPToolApprovalResolved].
func (MCPToolApprovalResolved) EventType() EventType { return EventMCPToolApprovalResolved }
func (MCPToolApprovalResolved) isEvent()             {}

// PlanStatus is the closed status vocabulary for a [PlanItem]. The set is closed in
// the domain so the todo_write virtual-tool intercept and its arg parser can reject
// an out-of-range status before any [PlanUpdated] is appended (ADR-0031).
type PlanStatus = string

const (
	// PlanStatusPending marks a plan item not yet started.
	PlanStatusPending PlanStatus = "pending"
	// PlanStatusInProgress marks the single plan item currently being worked.
	PlanStatusInProgress PlanStatus = "in_progress"
	// PlanStatusCompleted marks a finished plan item.
	PlanStatusCompleted PlanStatus = "completed"
)

// PlanItem is one entry of a model-authored task plan: a short content line and its
// lifecycle status. It carries only non-secret plan text (ADR-0031).
type PlanItem struct {
	// Content is the human-readable plan step. It must be non-empty (validated by
	// [PlanUpdated.Validate]).
	Content string
	// Status is the lifecycle status; it must be one of [PlanStatusPending],
	// [PlanStatusInProgress], or [PlanStatusCompleted] (validated by
	// [PlanUpdated.Validate]).
	Status string
}

// PlanUpdated records the model's current task plan, emitted via the in-loop
// virtual tool todo_write (ADR-0031). It is a durable, time-travelable planning
// primitive: appending it overwrites the agent's working plan (the latest
// PlanUpdated for a session wins), and an empty Items slice is a valid empty plan.
// Payload is non-secret plan text, surfaced by the read-plane as a normal descriptor.
type PlanUpdated struct {
	// TurnID is the turn this plan update was emitted in (matches
	// [TurnStarted.TurnID]).
	TurnID string
	// Items is the ordered plan; an empty slice is a valid empty plan.
	Items []PlanItem
}

// EventType identifies this payload as [EventPlanUpdated].
func (PlanUpdated) EventType() EventType { return EventPlanUpdated }
func (PlanUpdated) isEvent()             {}

// Validate is the SINGLE source of truth for plan well-formedness, reused by the
// todo_write virtual-tool arg parser and the loop intercept so an invalid plan is
// never persisted (ADR-0031). It rejects any item with empty content or an
// out-of-range status; an empty Items slice is valid (an empty plan).
func (p PlanUpdated) Validate() error {
	for i, it := range p.Items {
		if it.Content == "" {
			return fmt.Errorf("plan item %d: content must not be empty", i)
		}
		switch it.Status {
		case PlanStatusPending, PlanStatusInProgress, PlanStatusCompleted:
		default:
			return fmt.Errorf("plan item %d: status %q must be one of %q, %q, %q",
				i, it.Status, PlanStatusPending, PlanStatusInProgress, PlanStatusCompleted)
		}
	}
	return nil
}

// ApprovalRequested records that a per-dispatch tool call entered the ask gate and
// is BLOCKING on a human approval. It is appended BEFORE the gate blocks (ADR-0032),
// so a pending ask survives a crash/restart and a recovered orchestrator can detect
// it (an ApprovalRequested with no matching [PermissionDecided] for the same CallID
// is a suspended-awaiting-approval turn). It is the pre-block half of the ask; the
// AFTER-resolution outcome (allow/deny) is recorded by [PermissionDecided].
//
// This is the per-dispatch sibling of the registration-time
// [MCPToolApprovalRequested] gate: distinct event, same durable-ask shape. Args is
// the tool-call arguments captured for operator audit and for re-raising the gate on
// resume; it is carried in full in the persisted payload (audit fidelity) and bounded
// only at the read-plane (the read-plane never dumps Args raw).
type ApprovalRequested struct {
	// TurnID is the turn the ask was raised in (matches [TurnStarted.TurnID]).
	TurnID string
	// CallID is the [llm.ToolCall.ID] of the tool call awaiting approval; it pairs
	// this ApprovalRequested with the AFTER-resolution [PermissionDecided.CallID].
	CallID string
	// ToolName is the tool whose dispatch is gated by the ask.
	ToolName string
	// Reason is a short, human-readable explanation for why the call is being gated
	// (the permission pipeline's ask reason), shown to the approver.
	Reason string
	// Args is the tool-call arguments captured for the approval decision and for
	// re-raising the gate on resume. It is operator-facing audit data (not
	// provider_raw/secret); the read-plane bounds rather than dumps it.
	Args map[string]any
}

// EventType identifies this payload as [EventApprovalRequested].
func (ApprovalRequested) EventType() EventType { return EventApprovalRequested }
func (ApprovalRequested) isEvent()             {}
