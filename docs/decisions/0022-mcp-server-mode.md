<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0022: MCP Server mode — expose Boltrope as a callable MCP server (Streamable HTTP, thin adapter, call-stays-open approvals)

- **Status:** Accepted
- **Date:** 2026-06-15
- **Relates to:** FR-API-01/02/03 (resumable client edge, control, identical auth), ADR-0009/0010 (service decomposition, gRPC client edge), ADR-0013 (security model — RLS, tenant tokens, approval gate), ADR-0019 (session-scoped permission mode), ADR-0020 (production OIDC edge auth), the REST/SSE facade (`internal/orchestrator/adapter/inbound/rest/`), the 2026-06 competitive audit ("be the callee, not just a client")

## Context

Boltrope already speaks two client-edge transports over the SAME `igrpc.Server`
methods: gRPC and a thin REST/JSON+SSE facade. Both inherit OIDC fail-closed
auth, multi-tenant RLS, the per-tenant in-flight cap, the approval gate, durable
resumable delivery, and at-most-once mutating actions BY CONSTRUCTION, because
the facade adds a wire shape and no orchestration.

The #1 strategic differentiator identified by the competitive audit is making
Boltrope the **callee** — a sandboxed, tenant-isolated, auditable, durable,
replayable execution backend that *other* agents call over the network. Neither
competitor can take this position: deepagents is an MCP **client** only
(single-operator, consumes tools); hive is a desktop **host** consuming tools
(local orchestration, not a remote multi-tenant backend). Exposing Boltrope as an
MCP **server** — whose tools are "create / run / inspect / approve / fork a real
agent session" — lets any compliant MCP client hand a whole bounded, governed
task to Boltrope and get back a durable, resumable, audited result.

The question this ADR settles: which MCP transport, how to map a long-running
run-with-approval onto MCP's request/response model, and whether to hand-roll the
server or adopt a Go MCP SDK — all without changing the frozen `proto/` and
`internal/platform/llm` contracts or re-implementing any policy.

## Decision

Add a new thin inbound adapter `internal/orchestrator/adapter/inbound/mcpserver/`
exposing Boltrope as an MCP server, mounted on the existing daemon HTTP listener
beside the REST facade and the health endpoints. It is the exact sibling of the
REST facade: it invokes the SAME `igrpc.Server` methods through the SAME
`igrpc.Authenticator` and `igrpc.ContextWithPrincipal`, so the entire moat is
inherited and cannot drift.

Three sub-decisions (recorded in full in `C:/Users/123/Documents/harness-mcp-build/DECISIONS.md`):

1. **Transport: Streamable HTTP only (no stdio).** A single MCP endpoint
   `POST /mcp` answers with `application/json` (a single JSON-RPC response) or, for
   a `tools/call` of `run` carrying `_meta.progressToken`, `text/event-stream` (an
   SSE leg carrying `notifications/progress` then the final response).
   `GET /mcp` and `DELETE /mcp` return `405 + Allow: POST`. **Rationale:** the MCP
   spec scopes OAuth/bearer authorization to HTTP transports and says stdio
   SHOULD NOT do OAuth (it uses environment credentials) — our OIDC + multi-tenant
   RLS moat only exists on HTTP. Streamable HTTP (single endpoint, POST in / SSE
   out) is structurally identical to our REST `POST .../run → text/event-stream`,
   so it drops onto the shared listener with zero logic duplication. stdio is a
   one-client/one-process/local model orthogonal to the callee position; deferred.

2. **Run + approval: the call stays OPEN; a concurrent `control` resolves it.**
   A `run` `tools/call` keeps its SSE leg open exactly like REST's `POST .../run`:
   the approval is surfaced as an in-band `notifications/progress` frame carrying
   `{call_id, tool_name, args_json, reason, after_seq}`, and the human decision
   arrives on a CONCURRENT `tools/call control` (approve/deny) on a SEPARATE HTTP
   connection while the original run SSE stays alive. The loop blocks at the gate
   under the *live* run request context, so `gate.Resolve` lands on a live
   `gate.Request` and the loop continues on the same open leg until `status:
   "completed"`. A run needing N approvals is ONE `run` call interleaved with N
   concurrent `control` calls. Disconnect recovery is the durable `after_seq`
   cursor (re-call `run` with the last seq seen). No client capability is required
   beyond holding a `tools/call` open and issuing a concurrent one — every
   Streamable-HTTP MCP client can do this. **This supersedes the originally-drafted
   "end-the-call, re-call-run after `awaiting_approval`" model**, which is
   unimplementable against the real wiring (see Alternatives).

3. **Hand-rolled minimal MCP server (no Go MCP SDK).** The adapter uses only
   `net/http` + `encoding/json` (+ existing `protojson` for proto responses),
   mirroring the existing hand-rolled MCP **client**'s JSON-RPC framing
   (`internal/toolruntime/adapter/outbound/mcp/jsonrpc.go`). It implements
   `initialize`, `tools/list`, `tools/call` (the 5 tools), and
   `notifications/progress`. **Rationale:** the official Go MCP SDK ships its own
   OAuth + `Mcp-Session-Id` session state machine, which would be a SECOND source
   of truth alongside our `VerifyBearer`/RLS and our durable `after_seq` cursor,
   plus ~6 new direct deps (including a second jsonschema and `x/oauth2`); the
   first two sub-decisions already exclude the SDK's main selling points
   (multi-transport, stdio, elicitation, full OAuth flow) from v1.

### The 5 tools (each maps 1:1 to a shared method)

| Tool | Shared method | Notes |
|---|---|---|
| `create_session` | `Server.CreateSession` | `mode` default/acceptEdits/plan; client-set `bypass` rejected server-side (operator-only) |
| `run` | `Server.Run` (server-stream) | streaming; in-band approval; synthesized `completed` result with `outputSchema` |
| `get_session` | `Server.GetSession` | idempotent projection (protojson of `Session`) |
| `control` | `Server.Control` | approve/deny/interrupt/reattach; the concurrent approval resolver |
| `fork` | `Server.Fork` | branch a child at `at_seq` |

`tenant_id` is never a tool argument: the owning tenant is the authenticated
principal's, resolved server-side. Error model: protocol failures (bad params,
unknown tool/method, auth, ownership) are JSON-RPC errors; only a genuine
mid-stream tool-execution failure is a `CallToolResult{isError:true}`. The
gRPC→HTTP status mapping reuses the REST table verbatim — notably
`FailedPrecondition → 400` (the "no pending approval" case), `PermissionDenied →
403`, `NotFound → 404`, `ResourceExhausted → 429`, `Unauthenticated → 401` (+
`WWW-Authenticate: Bearer`). The server-defined JSON-RPC code `-32001` carries the
gRPC code string in `data.grpc_code`.

### Net-new security primitive at this edge

The only new security obligation v1 implements is the MCP Streamable-HTTP
**Origin / DNS-rebinding guard** (`BOLTROPE_MCP_ALLOWED_ORIGINS`, comma list):
absent Origin → allow (non-browser clients send none); present Origin → must be
allowlisted, else 403 (an empty allowlist + present Origin fails closed).
Everything else (bearer extraction, `VerifyBearer`, RLS placement) reuses the
shared auth path. An advisory `Mcp-Session-Id` is issued on `initialize` but the
durable `seq` cursor is the authoritative continuation state.

## Alternatives considered

- **The "end-the-call, re-call-run after `awaiting_approval`" model (original
  draft option b).** Rejected as unimplementable against the real wiring: the only
  thing that makes the blocking shared `Server.Run` return is cancelling its
  request context, and `relay.run` (`relay.go`) runs the loop under a child of
  that context with `defer cancel()`. Ending the `run` call to return a terminal
  `awaiting_approval` result cancels `loopCtx` → `Gate.Request` treats the
  cancelled context as an abort and REMOVES the pending entry (`gate.go`) → a
  later `control` approve hits `gate.Resolve` → `ErrNotPending` →
  `FailedPrecondition` (`server.go`). `LoopRunner.Run` passes the RPC ctx straight
  to `loop.Run`; there is no session-scoped, RPC-decoupled loop supervisor. That
  model would require NEW orchestration in the shared server/relay, contradicting
  the thin-adapter constraint. Deferred pending a deliberate decision to add a
  detached loop runner (which would also benefit fully-async clients that cannot
  hold a `tools/call` open).
- **stdio transport.** Excluded: OAuth/RLS are only meaningful on HTTP; stdio is
  single-client/local. A future thin stdio↔HTTP bridge can be added without
  touching the core.
- **MCP `elicitation` for in-call approval.** Excluded: requires a client
  capability, the spec forbids requesting sensitive info via it, and holding a
  server→client request open across an unbounded multi-tenant human wait is
  fragile vs. the durable `after_seq` cursor. Roadmap (optional path when a client
  declares `elicitation`).
- **Adopt the official Go MCP SDK.** Rejected for v1 (see sub-decision 3): two
  parallel auth/session state machines + dead code paths for excluded features.

## Consequences

- Boltrope becomes a callable, tenant-isolated, auditable MCP execution backend
  with no new policy and no orchestration — the moat is inherited because every
  tool call lands on the same `igrpc.Server` method through the same auth/RLS
  path. The compile-time `var _ igrpc.ApprovalNotifier = (*approval.Gate)(nil)`
  assertion (which the in-band approval frame depends on) remains satisfied.
- A run needing approval is driven by ANY compliant Streamable-HTTP MCP client:
  one open `run` call + concurrent `control` calls. No client capability required.
- Honestly deferred to roadmap (each a roadmap item, not a silent gap; the v1
  surface builds, tests, and lints without them): stdio; `GET`/`DELETE /mcp`
  behaviors beyond 405; stateful `Mcp-Session-Id` / `Last-Event-ID` redelivery on
  a reconnected GET stream; the in-band approval frame for `progressToken`-less
  runs (mirrors REST's "approval_request needs an open SSE"); the rejected
  end-call/re-call model (needs a detached loop supervisor); `elicitation`; full
  OAuth Protected-Resource-Metadata discovery; the `prompts`/`resources`/`logging`/
  `completions`/`sampling` capabilities; `tools/list` pagination; MCP-side rate
  limiting beyond the inherited per-tenant cap; a standalone MCP listener.
- New operator knob: `BOLTROPE_MCP_ALLOWED_ORIGINS` (documented in
  `deploy/README.md`). No other new config; the existing OIDC/dev auth is reused.
- Frozen contracts untouched: `proto/` and `internal/platform/llm` unchanged; the
  MCP surface is hand-shaped JSON wrapping proto via `protojson` (tests assert
  decoded fields / token substrings, never exact bytes). The shared
  `relay.onApproval` cursor was made deterministic (durable head fallback when the
  live tail has not advanced) — a strict improvement shared by all transports, no
  contract change.
