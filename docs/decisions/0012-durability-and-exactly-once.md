# 12. Durability and at-most-once mutating tools: durable turn/tool-execution intent, tool_executions ledger, clean-workspace resume

Date: 2026-06-10
Status: Accepted

## Context

The draft's central resume promise — "a crash anywhere lets a fresh orchestrator resume"
— was false for the two most expensive in-flight operations: model generation and
tool execution.

For model generation, no durable marker existed for a turn that had started but not
finished. A crash during streaming lost the partial generation silently; a recovered
orchestrator had no way to distinguish "this turn never started" from "this turn was in
progress" from "this turn produced a partial response." Lost generation meant silent
re-billing.

For tool execution, the draft used in-memory TTL dedup combined with declarative gRPC
retries. This was unsound in exactly the scenario that matters: a crash of the
tool-runtime process wiped the in-memory cache at the same moment the retry fired.
Additionally, gRPC auto-retry of a streaming RPC that has already received a response
frame is not retry-eligible. An idempotency key cannot make `bash` or `git push`
idempotent regardless of dedup mechanism.

For workspace state, bash side effects (installed packages, created files, git state)
are not represented as events and cannot be replayed from tool results. The draft
implied "resume just works" without acknowledging this.

## Decision

**Durable turn boundaries.** We will append `TurnStarted{turn_id}` to the event log
before calling Generate. During streaming, the loop appends
`AssistantMessageDelta{turn_id, text_so_far}` periodically (every M seconds or K tokens,
configurable) as a checkpoint, recording the provider's resumable cursor when one exists.
A recovered orchestrator that folds the log and finds an open TurnStarted with no
terminal AssistantMessage or TurnAborted knows a turn was in flight. Recovery is
explicit: either continue from the provider's resumable cursor where available, or
deterministically append `TurnAborted{usage_so_far}` and surface it as a resumable
decision. Partial generation is never silently re-billed; an aborted turn records the
provider usage read from the stream's last usage metadata.

**Durable execution intent before side effects.** We will append
`ToolExecutionStarted{call_id, idempotency_key}` and commit it to the event log before
dispatching ExecuteTool to tool-runtime. The idempotency key is log-derived:
`hash(session_id, seq_of_ToolCall)`. Any orchestrator replaying the log reconstructs
the same key deterministically.

**Durable dedup ledger.** Tool-runtime records execution status in the `tool_executions`
PostgreSQL table keyed `(tenant_id, session_id, idempotency_key)`, not in memory or
TTL cache. A retried call with a known-completed key returns the prior result instead
of re-running. The ledger survives tool-runtime restarts.

**At-most-once recovery for mutating tools.** On resume, if a ToolExecutionStarted has
no terminal ToolResult, the outcome is UNKNOWN. A Mutating tool is not blindly
re-dispatched. The orchestrator queries `tool_executions` by key; if still unknown, it
surfaces the call for human or hook adjudication, or requires the tool to be explicitly
declared idempotent or compensatable. The default posture for mutating tools is an
explicit `unknown` terminal state rather than at-least-once double execution.

**gRPC retry scope.** Declarative retryPolicy applies only to genuinely idempotent
reads (Capabilities, CountTokens, ExecuteTool of a ReadOnly tool). ExecuteTool of a
Mutating tool and Generate are never auto-retried.

**Idempotency key scoping.** Dedup keys are server-derived and namespaced
`(tenant_id, session_id, ...)`. A client-supplied or model-supplied value is never
treated as a global cache key, closing cross-session and cross-tenant leak vectors.

**Resumable client stream.** Every delivered frame carries its event seq. The
Control.Reattach{session_id, from_seq} RPC re-opens a stream from the last delivered
event. Generation is decoupled from delivery (ADR-0010) so a client disconnect never
loses the generation.

**Clean-workspace resume (v1 explicit model).** Resume re-attaches to a fresh
per-session container. Model-visible history (the event log) is intact but uncommitted
filesystem state from the prior container is not restored. The harness records a
WorkspaceReset marker on resume; the system prompt informs the agent that uncommitted
FS state was lost. The Workspace port is shaped so a future backend can snapshot the
container/volume and reference the snapshot from an event, enabling consistent FS
re-attach; this is deferred to a future gate.

## Consequences

- The "resume after crash" promise is now precisely scoped and true: the event log
  carries durable turn boundaries and execution intent, so a fresh orchestrator can
  always adjudicate recovery deterministically.
- Periodic checkpoints bound lost work for in-flight generation to one checkpoint
  interval, even for long reasoning-model turns.
- Double execution of bash/git push/patch tools on crash-and-retry is prevented by the
  durable ledger combined with the fenced lease.
- The explicit unknown outcome for mutating tools forces human or hook adjudication
  rather than silent re-run; this is the correct default posture for irreversible
  operations.
- Clean-workspace resume is honest: it does not claim FS consistency it cannot deliver;
  the seam for durable workspace snapshots is preserved for a future gate.
- The dedup ledger has a small per-execution write cost (one INSERT into tool_executions
  before dispatch, one UPDATE on completion); this is negligible compared to the tool
  execution itself.
