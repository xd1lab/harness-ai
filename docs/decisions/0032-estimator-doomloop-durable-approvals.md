# 0032. Token estimator counts all parts; doom-loop terminates; durable approval suspend/resume

Date: 2026-06-25
Status: Accepted

## Context

Three independent "secondary but real" capability gaps surfaced on the capability
scorecard. Each is small in surface area but each is a genuine correctness or
durability hole, so they are addressed together here (in easy → hard order) while
keeping each fix in its own commit. None of the three requires a proto/`gen/`
change.

### FIX 1 — the local token estimator ignored tool results

`gatewayTokenCounter.Count` (`cmd/boltrope-orchestratord/tokencount.go`) asks the
model gateway's capability-gated `CountTokens` first and falls back to the local
`estimateTokens` on any error. Self-hosted / OpenAI-compatible endpoints (Ollama,
vLLM, …) have no count API, so those models **always** use the local estimate.

The estimate only summed `ContentPart.Text` (chars ÷ 4). It silently skipped
`ContentPart.ToolResult.Content`, `ContentPart.ToolCall` (name + args), and
`ContentPart.Thinking`. A session whose growth is dominated by large tool outputs —
the exact "noisy tool output blows the window" failure — therefore under-counted
toward ~0, and threshold compaction (`BOLTROPE_MAX_CONTEXT_TOKENS`) never fired.

### FIX 2 — doom-loop detection had no termination path

`detectDoomLoop` (`internal/orchestrator/app/agent/tools.go`), called from
`handleToolCalls`, only **recorded a metric**. A model stuck calling the same tool
with the same args forever was stopped only by `MaxTurns` (32) — wasteful, and not
the typed, auditable outcome an operator expects. The scorecard called out:
"doom-loop 無終止路徑(缺 `ErrorDoomLoop`、僅指標)".

### FIX 3 — the approval gate had no durable suspend/resume

The general per-dispatch ask gate (`adapter/inbound/approval/gate.go`) held pending
asks **only** in an in-memory map; `Request` blocked on a channel and nothing was
persisted. `gateCall` appended `PermissionDecided` only **after** resolution. On a
crash mid-ask, recovery (`app/recovery/recovery.go`) saw a generic open turn and
appended `TurnAborted` — the pending approval was **silently lost**, with no record
that a human decision had ever been pending.

`event.go` carried a design note that the architecture's
`ApprovalRequested`/`ApprovalGranted`/`ApprovalDenied` family was "modeled as this
single `PermissionDecided` event". That collapse is precisely what made a blocking
ask non-durable. A precedent already existed for the durable shape: the
`MCPToolApprovalRequested` / `MCPToolApprovalResolved` pair for MCP first-use
approval.

## Decision

### FIX 1 — count every token-bearing content part

`estimateTokens` now walks **all** token-bearing `ContentPart` variants, still at
`charsPerToken = 4`:

- `ContentPart.Text.Text` — unchanged.
- `ContentPart.ToolResult.Content` — the textual tool result fed back to the model.
- `ContentPart.ToolCall` — `Name` plus a `json.Marshal` of `Args` (`map[string]any`).
  `json.Marshal` sorts object keys, so the contribution is **deterministic regardless
  of map iteration order**; on a marshal error we fall back to `fmt.Sprintf("%v", …)`
  so a non-serializable arg still contributes non-zero chars.
- `ContentPart.Thinking.Text`.

Image bytes are deliberately **excluded** — they are not model-text tokens. The
per-tool-def loop (`Name`+`Description`+`JSONSchema`) is unchanged. `Count`'s
signature and behavior (gateway-first, estimate-on-error) are unchanged, and the
`agentctx.TokenCounter` compile-time assertion still holds. The `ContentPart` union
has exactly one field non-nil per part, so the branches are mutually exclusive and
cannot double-count.

### FIX 2 — `ErrorDoomLoop` termination, default-on, no proto change

- A new `domain.TerminationReason` constant `ErrorDoomLoop = "error_doom_loop"` is
  added to `domain/turn.go`. `IsError()` returns true for it automatically (it is
  `!= Success`).
- The wire mapping `toGenSubtype` (`adapter/inbound/grpc/mapping.go`) maps
  `ErrorDoomLoop` to the **existing** `TerminationSubtype_TERMINATION_SUBTYPE_ERROR_DURING_EXECUTION`
  enum value. **No proto/`gen/` change.** A doom loop is a generic
  execution-failure subtype on the wire; the precise reason is preserved in the
  domain `TurnFinished`/run result.
- `detectDoomLoop` becomes a control-flow signal returning `bool`. When the
  threshold is reached, `handleToolCalls` terminates via the existing return
  contract — `return turnTerminal, domain.ErrorDoomLoop, nil` — so `loop.go`'s
  `Run` switch routes `turnTerminal → l.finish(ctx, st, reason)`, appending exactly
  one `TurnFinished{ErrorDoomLoop}` on the **current open turn**. This is
  deliberately `finish`, NOT a side `l.terminate` call: `ErrorMaxTurns` fires
  *between* turns (no open turn), but a doom-loop trip happens *inside*
  `handleToolCalls` with `st.currentTurnID` already open, so `finish` avoids a
  dangling/duplicate `TurnStarted`/`TurnFinished` pair. The trip happens **before**
  dispatching the Nth identical batch (no further wasteful tool execution once
  tripped). The `RecordDoomLoop` metric is still emitted on the trip.
- **Default-on.** `DefaultDoomLoopThreshold = 3` is substituted in `NewLoop` when
  `Config.DoomLoopThreshold == 0`, so a zero-value `Config` (including sub-agent
  loops) is protected. A **negative** value is now the explicit-disable sentinel,
  and the `tools.go` guard moved from `<= 0` to `< 0` accordingly. The
  `Config.DoomLoopThreshold` doc comment documents this `0 ⇒ default 3,
  negative ⇒ disabled` contract.

### FIX 3 — un-collapse `ApprovalRequested`; durable suspend/resume (TARGET re-raise)

A new sealed `domain.Event ApprovalRequested` is added, mirroring
`MCPToolApprovalRequested`:

```go
const EventApprovalRequested EventType = "ApprovalRequested"

type ApprovalRequested struct {
    TurnID   string
    CallID   string
    ToolName string
    Reason   string
    Args     map[string]any
}
```

This **un-collapses** the pre-block half of an ask out of `PermissionDecided`.
`PermissionDecided` remains the AFTER-resolution record; the pair
`ApprovalRequested → PermissionDecided{Decision:Ask, Resolved:…}` brackets one ask,
and a lone `ApprovalRequested` with no matching `PermissionDecided` (same `CallID`)
is a suspended-awaiting-approval turn on recovery. The `event.go` design note was
amended to record this reversal explicitly.

`gateCall` (`app/agent/tools.go`) appends `ApprovalRequested` (actor `ActorSystem`)
**before** calling `l.deps.Approvals.Request` and blocking. Deny and hook-block
paths still append **no** `ApprovalRequested` (FR-PERM-01 / FR-EXT-03 preserved).
`Args` is stored in full for audit fidelity and is bounded only on read.

**Recovery** (`app/recovery/recovery.go`) gains a third output class
`Plan.SuspendedApprovals []SuspendedApproval`. The fold tracks per-`CallID` approval
state (requested-but-unresolved), closed by a matching `PermissionDecided`. An open
turn holding an unresolved `ApprovalRequested` is classified as a
`SuspendedApproval` and **excluded** from `Plan.OpenTurns`, so it is never both a
suspended approval and a generic open turn (which would double-terminate it). This
suppresses the silent generic `TurnAborted` for that turn.

**Resume level achieved: TARGET (re-raise).** `adjudicateResume`
(`app/agent/loop.go`) re-raises each `SuspendedApproval` via
`resumeSuspendedApproval`:

1. The suspended turn becomes the current turn.
2. The gated `llm.ToolCall` is reconstructed from the persisted `ApprovalRequested`
   and `l.deps.Approvals.Request` is called again — the same path the live gate
   uses — so a reconnecting client's `SubscribeApprovals` subscriber is re-notified
   and the operator can still allow/deny after a restart.
3. The block is **bounded** by `Config.ResumeApprovalTimeout` (measured on the
   injected clock). This bound is **mandatory**: the in-memory gate loses its
   pending map on a real process restart, so an unbounded re-raise with no connected
   subscriber would hang forever.
4. On resolution, the AFTER-resolution `PermissionDecided` is appended (mirroring
   live `gateCall`). If `AskAllowed`, the gated tool is dispatched on the suspended
   turn (`ToolExecutionStarted → ToolResult`, idempotency key derived from the gated
   `AssistantMessage` seq) and the run continues. If `AskDenied`, a synthetic denied
   `ToolResult` is fed back so the model can adapt.

**Fallback (durable-auditable close).** If the bounded wait elapses (no client
answered) or the gate errors, `abandonSuspendedApproval` appends an **explicit**
`PermissionDecided{Decision:Ask, Resolved:AskDenied,
Reason:"suspended-approval-abandoned-on-resume"}` followed by
`TurnAborted{ErrorDuringExecution}` — never a bare generic abort that erases the
fact a human decision was pending. This protects an unattended restart while keeping
the audit log honest.

No wire frame was required for re-raise: the existing approval control-stream frame
(carrying `call_id` + args) already covers re-emitting the ask to the reconnecting
client via the gate's `SubscribeApprovals` notifier. **No proto/`gen/` change.**

### Exhaustive-site handling for `ApprovalRequested`

The new sealed event is handled — exactly as `PlanUpdated` (ADR-0031) was — at every
exhaustive switch / codec / projection / read-plane / agentctx / dev-store site:

- `domain/event.go` — const + struct + `EventType()` / `isEvent()`.
- `domain/domain_test.go` — `eventCases()` entry (non-zero fields; numeric `Args`
  values use `float64` per the JSON-round-trip convention) + `allEventTypes` slice
  entry; `TestEventCases_CoverEveryEventType` enforces 1:1.
- `adapter/outbound/eventstore/rows.go` — `decodePayload` case
  `unmarshalInto[domain.ApprovalRequested]`.
- `adapter/inbound/grpc/read_api.go` — `summarizeEvent` explicit case returning a
  bounded, operator-facing summary `"approval requested: tool <ToolName>
  (call <CallID>)"` with a truncated reason when `includePayload`; the raw `Args`
  map is NOT dumped (`redacted=false`, `hasBlob=false` — audit data, not secret).
- `app/agentctx/agentctx.go` — a **conscious `renderMessage` skip**:
  `ApprovalRequested` is a durable control record for the ask gate, not a
  conversation message, so it is intentionally not rendered into the model window
  (mirroring how `PlanUpdated` is skipped there).

**Intentional default-no-op at the four cost/read fold sites.** `ApprovalRequested`
carries no usage/cost, so it is consciously *not* given an explicit case at the cost
folds — it falls through the existing `default:` arm at each of:
`internal/orchestrator/projection/cost.go`,
`internal/orchestrator/projection/cost_projector.go`,
`cmd/boltrope-dev/read_methods.go` (`devFoldModelCost`), and the cost fold in
`internal/orchestrator/adapter/inbound/grpc/server.go`. These were audited and left
as default-no-op deliberately — they are not missed sites.

## Consequences

- **Estimator** — self-hosted / no-count runs now grow the estimate monotonically
  with real window content, so threshold compaction fires on noisy tool output. The
  estimate is still a coarse degraded-mode measure, never billing-grade
  (architecture §11.6). `Args` accounting is order-insensitive.
- **Doom-loop** — a stuck model now terminates with `error_doom_loop` (wire:
  `ERROR_DURING_EXECUTION`) well before `MaxTurns`, default-on at threshold 3 for
  normally-constructed production *and* sub-agent loops. A model that **varies** its
  calls does not trip it (the compare is by name + args). Operators who genuinely
  want it off set a negative threshold.
- **Approvals** — an approval is now durable across a crash: the `ApprovalRequested`
  → `PermissionDecided` pair brackets every ask, recovery classifies a suspended
  approval distinctly, and resume **re-raises** the ask to the reconnecting operator
  and continues the run once answered. An unattended restart degrades gracefully to
  an explicit, auditable close rather than a silent loss. The sealed-event set grew
  by one; every exhaustive site handles it, so no codec/projection/read-plane path
  hits an "unknown event_type" error (proven by the domain exhaustiveness test and a
  real-Postgres integration round-trip).
- **No proto/`gen/` change** for any of the three fixes.
- Trade-off: `ResumeApprovalTimeout` is a new knob; too short risks abandoning an
  approval a human would have answered, too long risks holding a turn open after a
  restart. The default is conservative and the fallback close is always auditable.
