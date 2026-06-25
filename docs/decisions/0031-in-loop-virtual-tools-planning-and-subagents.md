<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0031: In-loop virtual tools — `spawn_subagent` + `todo_write` — and the `PlanUpdated` planning event

- **Status:** Accepted
- **Date:** 2026-06-25
- **Relates to:** [ADR-0011](0011-event-store-schema.md) (event-store schema, sealed event set, JSON codec by type tag) · [ADR-0012](0012-durability-and-exactly-once.md) (durable turn/tool-execution intent, idempotency keys) · [ADR-0013](0013-security-model.md) (tool execution guardrails, side-effect/egress classification, permission gate) · [ADR-0014](0014-concurrency-and-cancellation.md) (single-goroutine loop, gated read-only parallelism, serialized mutations) · [ADR-0015](0015-repository-layout.md) (clean-package layering, tool-runtime must not depend on the orchestrator) · [ADR-0019](0019-session-scoped-permission-mode.md) (`ModePlan` is a *guardrail*, not a planning primitive) · [ADR-0025](0025-event-log-read-and-time-travel.md) (descriptor-by-default read-plane redaction, time-travel via Load-then-fold) · [ADR-0030](0030-long-term-memory-via-tools.md) (the *other* answer to "how should a new capability reach the model" — native tools)

## Context

The capability scorecard flagged gap #3, which had **two distinct halves**:

1. **The sub-agent spawn primitive was fully built but DEAD-WIRED.** The
   `app.SubAgentPort` interface (`Spawn(ctx, SubAgentSpawn) (ToolResult, error)`
   + `MaxDepth() int`), a working `subagent.Spawner` (forks the parent event log
   at head seq, runs a bounded child loop via `agent.NewLoop`, condenses the
   child result into a `ToolResult`, and rejects `Depth > MaxDepth` *before* any
   session creation), and the production wiring
   (`cmd/boltrope-orchestratord/wiring.go` sets `deps.SubAgent`) all existed.
   But **the loop never called `deps.SubAgent.Spawn`** — `.Spawn(` appeared only
   in tests. The model had no way to delegate; the capability was unreachable.

2. **There were NO planning primitives.** No plan/todo tool, no plan event. The
   permission `ModePlan` ([ADR-0019](0019-session-scoped-permission-mode.md))
   exists but only **flips mutating tools to deny** — it is a *guardrail*, not a
   way for the model to author and track a multi-step plan. A long task had no
   durable, time-travelable record of what the agent intended to do.

The natural instinct — mirroring [ADR-0030](0030-long-term-memory-via-tools.md),
which exposed memory as native **tool-runtime tools** — does **not** work here.
Both of these capabilities need things the tool-runtime structurally **does not
have**:

- `spawn_subagent` needs the **sub-agent spawner**, which forks the **event
  log** and constructs an orchestrator **`agent.Loop`**. The tool-runtime
  (`internal/toolruntime`) has no access to either, and must not — it executes
  in a separate service/binary and depends on neither the event store nor the
  orchestrator (a layering invariant, [ADR-0015](0015-repository-layout.md)).
- `todo_write` needs to **append a domain event** to the session's event log so
  the plan is durable, replayable, and time-travelable. The tool-runtime cannot
  append events; only the orchestrator loop, which holds the single-writer
  `runState`, can.

So these two capabilities cannot be tool-runtime tools. They must be handled
**inside the orchestrator loop**, where the spawner and the event log are
reachable.

## Decision

### 1. In-loop **virtual tools** — handled inside the loop, never sent to the tool-runtime

`spawn_subagent` and `todo_write` are **virtual tools**: the model sees and
calls them exactly like any other tool, but the loop **intercepts them by name**
and handles them itself — they never reach `deps.Tools.ExecuteTool`. The seam is
a small, self-contained file (`internal/orchestrator/app/agent/virtual_tools.go`)
shared by three loop touch-points: **advertise**, **classify**, **intercept**.
Permissions are **not** a fourth special case — the virtual tools flow through
the *existing* gate unchanged (see §4).

This is the deliberate counterpart to [ADR-0030](0030-long-term-memory-via-tools.md):
memory reached the model as **tool-runtime** tools because its consumer (a
key/value store) lives cleanly behind a port; sub-agents and planning reach the
model as **in-loop virtual** tools because their dependencies (the spawner and
the event log) live *inside* the loop and cannot be pushed down to the runtime
without a layering violation. Same north star ("the model's own tool surface is
the surface"), different mechanism dictated by where the dependency lives.

### 2. ADVERTISE — appended after runtime/config defs, gated for `spawn_subagent`

`buildRequest` computes the base tool defs (from `l.cfg.ToolDefs` *or* the
runtime descriptors) and then **appends** `virtualToolDefs(...)` — the base defs
are never dropped or reordered.

- **`todo_write` is ALWAYS advertised.** It is universally useful and
  side-effect-free w.r.t. the host (it only records a durable plan note). Its
  JSON schema requires `items`: an array of `{content: string, status: enum
  pending|in_progress|completed}`, both required; an empty array is valid (it
  clears the plan).
- **`spawn_subagent` is advertised IFF `deps.SubAgent != nil` AND
  `l.cfg.Depth < deps.SubAgent.MaxDepth()`** (strict `<`). Its schema requires
  `task` (string) and an optional `model` (string).

**The strict-`<` advertise vs strict-`>` spawn-reject asymmetry is deliberate
and consistent.** The spawner rejects only `in.Depth > MaxDepth`. The loop
spawns the child at `l.cfg.Depth + 1`. Advertising at `Depth < MaxDepth` means
the parent only offers the tool when `Depth <= MaxDepth - 1`, so any child it
spawns runs at `Depth + 1 <= MaxDepth` and is **always accepted**. At
`Depth == MaxDepth` the tool is hidden, so **the model is never offered a spawn
the spawner would reject** — an advertised `spawn_subagent` call is never
depth-rejected. The two bounds meet exactly with no gap and no overlap.

`AC-4` ("`todo_write` always advertised") and `AC-17` ("default behavior
unchanged when the model emits none") are **not** in tension: `todo_write` is
always *offered*, but the event sequence is byte-identical to today's whenever
the model does not *call* it. Advertising a tool def changes the request's tool
list, not the event log.

### 3. CLASSIFY — inline classification that WINS over the runtime registry

`l.toolClasses(ctx, sessionID)` looks classifications up from the **runtime**
registry, which does not know virtual tools. So the loop merges an **inline**
classification map (`virtualClasses`) over the runtime classes
(`mergeVirtualClasses`), and the inline entries **win** over any same-named
runtime descriptor — classification stays deterministic and cannot be shadowed
by a coincidentally-named runtime tool:

- **`spawn_subagent` ⇒ `SideEffect=Mutating`, `EgressClass=None`.** A child can
  do anything, so it is gated like any mutation and **serialized** — never
  auto-parallelized as a read.
- **`todo_write` ⇒ `SideEffect=ReadOnly`, `EgressClass=None`.** It only records
  a plan note; it is harmless and classifies read-only.

A same-named *runtime* tool would be shadowed (advertised once, with virtual
semantics). That is acceptable and logged as a warning; the two stable names
(`spawn_subagent`, `todo_write`) are reserved.

### 4. GATE — permissions are NOT bypassed

Both virtual tools flow through `gateCall` **exactly like real tools**:
PreToolUse hooks → `PolicyEngine` deny/allow/ask → `ApprovalGate`. There is no
bypass. A **policy-denied (or hook-blocked) `spawn_subagent` records a
`PermissionDecided{deny}` and never calls `deps.SubAgent.Spawn`** — the loop
feeds back the synthetic denied result instead. An **ask-resolved-allowed**
`spawn_subagent` *does* call `Spawn`. The gate runs **before** dispatch, so the
intercept in §5 is only ever reached for an allowed call.

### 5. INTERCEPT — before `ExecuteTool`, with the right writer

`execOne` intercepts by name **before** consulting the runtime:

- **`spawn_subagent`** → parse args (`task` required + non-blank, rejected
  locally *without* calling Spawn when missing/empty; `model` optional) → call
  `deps.SubAgent.Spawn(ctx, app.SubAgentSpawn{ParentSessionID: sessionID,
  Depth: l.cfg.Depth + 1, Task, Model})` and use the returned `ToolResult`.
- **`todo_write`** → parse `items` into `[]domain.PlanItem`, validate
  (`domain.PlanUpdated.Validate` — the single source of truth; an invalid status
  or empty content yields an `is_error` `ToolResult` and **no** event), and on
  success return the validated plan on a side channel.

**Neither virtual tool ever calls `deps.Tools.ExecuteTool`.**

### 6. ORDERING — `PlanUpdated` appended on the serial post-dispatch path

`todo_write` is classified **read-only**, so it can be dispatched on the
**concurrent** read-only worker pool — but appending to the event log there
would race the single-writer `runState`. So `execOne` **never writes the log**.
For `todo_write` it returns the validated plan on a side channel
(`*domain.PlanUpdated`), and the **serial** result loop in `handleToolCalls`
(which holds the single writer) appends the `PlanUpdated` event deterministically,
**between** the per-call `ToolExecutionStarted` and `ToolResult`. This keeps the
golden order `ToolExecutionStarted → PlanUpdated → ToolResult` deterministic
**even though** `todo_write` is read-only/parallelizable.

### 7. AUDIT / REPLAY PARITY — virtual tools emit the same envelope as real tools

For **both** virtual tools the loop still appends a `ToolExecutionStarted`
(keyed by the same `deriveIdempotencyKey(sessionID, lastAssistantSeq)` as real
tools) **before** intercept and a `ToolResult` **after**. The event sequence and
idempotency-key derivation are therefore **unchanged** vs. a real tool, so
replay, dedup, and time-travel all work for virtual tools with no special-casing
on the read side.

### 8. The `PlanUpdated` sealed domain event

A **new sealed event** `domain.PlanUpdated` (`EventType "PlanUpdated"`):

```go
type PlanUpdated struct {
    TurnID string
    Items  []PlanItem
}
type PlanItem struct {
    Content string // required, non-empty
    Status  string // one of: pending | in_progress | completed
}
```

`PlanUpdated` implements `EventType()` (returns `EventPlanUpdated`) and the
unexported `isEvent()` marker. `PlanUpdated.Validate()` is the **single source of
truth** for status validity (rejects out-of-range status / empty content); the
`todo_write` intercept calls it and feeds back an `is_error` result rather than
appending an invalid event. An **empty `Items`** is valid — it records an empty
plan.

Adding to the **sealed** event set means **every exhaustive site** is updated so
the build and existing exhaustiveness tests stay green:

- **Domain exhaustiveness test** (`domain/domain_test.go`): one representative
  `PlanUpdated` in `eventCases()`, `EventPlanUpdated` in `allEventTypes`; tag
  seal + JSON round-trip pass.
- **Eventstore JSON codec** (`adapter/outbound/eventstore/rows.go`
  `decodePayload`): a `case domain.EventPlanUpdated:
  unmarshalInto[domain.PlanUpdated](payload)` branch; round-trip + unknown-type
  tests pass; an **integration** round-trip against real Postgres proves Append
  → Load preserves `Items`.
- **Read-plane** (`adapter/inbound/grpc/read_api.go` `summarizeEvent`): a
  `case domain.PlanUpdated` returns a **safe, bounded, NON-redacted** descriptor
  (`Redacted=false`, `hasBlob=false`) — plan text is non-secret, so unlike
  `provider_raw`/system prompts it is exposed as a normal descriptor.
- **Time-travel** (`read_api.go` `foldEventTotals`): safely **ignores**
  `PlanUpdated` (no cost/turn change), and `GetStateAtSeq` / the dev store's
  `LoadUpTo`/`LoadRange` include it in their windows.
- **Dev in-memory store** (`cmd/boltrope-dev/eventstore.go`): stores live
  `domain.Event` structs (no JSON codec), so **no codec change** is needed —
  verified that Append → Load/LoadRange/LoadUpTo returns `PlanUpdated` unchanged
  and the cost fold ignores it.
- **agentctx window builder** (see §9).

### 9. Plan re-surfacing — IMPLEMENTED (AC-18)

`agentctx.BuildWindow` re-surfaces the **latest** `PlanUpdated` for the session
as a **single** `[current plan]` context-note message appended after the live
window, mirroring how the todo/memory note is re-surfaced — so the model always
sees its current plan. `renderMessage`'s default case **skips** `PlanUpdated` on
the per-event path (no per-event plan spam, no unknown-event panic). Only the
**most recent** `PlanUpdated` contributes a note; stale plan updates never
duplicate, and a session with no `PlanUpdated` produces a **byte-identical**
window (no regression). This was implemented in this batch, **not** deferred.

### 10. No proto change

`PlanUpdated` is an **internal** domain event, and the read-plane already returns
a generic `EventDescriptor` ([ADR-0025](0025-event-log-read-and-time-travel.md)).
No `.proto` is edited and `gen/` is **not** regenerated. (If any proto change had
been needed it would have been additive + buf-breaking-clean — but none was.)

## Consequences

**Good.**

- The dead sub-agent wiring is **live**: the model can delegate via
  `spawn_subagent`, the loop calls `deps.SubAgent.Spawn` with `Depth + 1`, and
  the child's condensed `ToolResult` is fed back — proven by a loop-level test
  with a fake `SubAgentPort`.
- The agent gains a **durable, replayable, time-travelable planning primitive**:
  `todo_write` appends a `PlanUpdated` event that survives Load/replay, appears
  in the read-plane `ListSessionEvents` as a non-redacted descriptor and in
  `GetStateAtSeq`, and is re-surfaced to the model as its current plan.
- **Permissions are not bypassed**: both virtual tools go through the existing
  hooks/policy/approval gate; a denied `spawn_subagent` never spawns.
- **No proto/facade churn** and **no tool-runtime change**: both capabilities
  live entirely inside the loop, respecting the layering invariant.
- **No regression**: default behavior with the virtual tools un-called is
  byte-identical; existing loop/eventstore/projection/agentctx/read-plane/gRPC
  tests stay green.

**Bad / accepted trade-offs (honestly documented).**

- Virtual tools are a **second mechanism** alongside tool-runtime tools — a
  reader must know that `spawn_subagent`/`todo_write` are handled in the loop,
  not the runtime. This is documented here and in the seam file's comments, and
  is unavoidable given where the dependencies live.
- The two stable names (`spawn_subagent`, `todo_write`) are **reserved**: a
  same-named runtime tool is shadowed by the virtual one (logged as a warning).
- Repeated identical `todo_write` calls can trip the doom-loop **metric**. That
  is acceptable in this batch — the doom-loop signal is non-terminal — and is
  **not** special-cased here. Durable approval suspend/resume, doom-loop
  termination, and estimator-sees-tool-result are a **separate later batch**,
  explicitly out of scope for this ADR.

**Follow-up.**

- Durable approval suspend/resume so an `ask`-gated `spawn_subagent` survives a
  process restart (separate batch).
- Doom-loop **termination** (not just the metric) if repeated identical virtual
  calls become a real problem.
