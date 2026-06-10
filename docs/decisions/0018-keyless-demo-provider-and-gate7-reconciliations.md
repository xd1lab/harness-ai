# 18. Keyless demo provider is text-only; Gate-7 deploy reconciliations

Date: 2026-06-10
Status: Accepted

## Context

Gate 7 (overall review + trace/eval) drove a real verify→fix→deploy loop against the
`docker compose` stack, running the documented keyless quickstart
(`harnessctl run "..."`) end-to-end. Unlike the in-process eval (ADR-0007), which drives
the loop with a scripted fake provider, the compose smoke exercises the *deployed*
distributed system: cross-container mTLS, the pgx event store, the model-gateway and
tool-runtime gRPC boundaries, the streaming relay, and terminal `RunResult` delivery.

That run hung. Root cause, traced through the event log and per-service logs: the
built-in `stub` provider emitted a tool call on every turn (the orchestrator always
advertises the runtime's tool set to the model), and the v1 baseline policy
(empty rule set, `ModeDefault`) asks for human approval on *every* tool (deny/ask by
default; ADR-0013/0014). An unattended `harnessctl run` has no approver, so the loop
blocked forever on the approval gate; even with auto-approval the stub never stopped
calling tools, so it could only ever hit `error_max_turns`. The stub could never reach
`Success` unattended.

The same loop surfaced three adjacent inconsistencies worth recording.

## Decision

**1. The built-in `stub` provider replies with a single deterministic text turn and
never requests a tool.** It streams a fixed acknowledgement and a terminal `StopEnd`,
ignoring `Request.Tools`. Rationale: a tool call in a headless, approver-less run
deadlocks on the approval gate; a clean text-only terminal turn lets the deployed stack
reach termination `Success` deterministically and keyless — proving the full distributed
pipeline (mTLS, event log, model-gateway `Generate`, the orchestrator↔tool-runtime
`ListTools` advertisement, streaming, terminal `RunResult`). The stub is a demo/smoke
provider only (DOD-05), never production; `SupportsTools` stays `true` so the loop still
advertises tools and the request path is unchanged. Tool *execution* (sandbox + dedup +
adversarial kills) is proven exhaustively by the tool-runtime integration suite, NOT by
this network-free provider — a reader must not infer tool-path coverage from the keyless
smoke alone.

**2. The keyless default provider is `stub`, consistently.** `docker-compose.yml`
already defaulted `BOLTROPE_MODELGW_PROVIDER` to `stub`; `.env.example` was corrected
from `openaicompat` (which pointed at a host Ollama that is not present out of the box,
so the documented `cp .env.example deploy/.env` produced a non-working stack) to `stub`.
A real model (hosted provider or local OpenAI-compatible endpoint) is opt-in via the
provider + key env, documented inline.

**3. Egress "single broker" wording is amended, not rewritten.** NFR-SEC-04 / FR-TOOL-06
describe "a single deny-by-default egress broker" for all model-influenced egress. As
built this is a *logical* guarantee realized by two complementary mechanisms: the
per-session sandbox container runs with `--network none` (in-sandbox `bash` egress is
*severed*, not proxied), and the tool-runtime's `webfetch`/`websearch`/MCP-HTTP clients
enforce the deny-by-default per-session allowlist. The frozen requirement text and its
acceptance criteria are retained verbatim; a clearly-marked amendment note clarifies the
as-built mechanism (consistent with ADR-0013, which already names both enforcement
points). The load-bearing invariant — no unrestricted egress from any model-influenced
path — is satisfied.

**4. Per-run permission mode is deferred (tracked).** `RunRequest` carries no `mode`
field; `CreateSessionRequest.mode` is accepted and a client-supplied `bypass` is rejected
(operator-only), but the value is not yet persisted on the session, so every run uses the
secure default `ModeDefault`. Plumbing a session-scoped mode through the event log
(e.g. on `SessionStarted`) is an additive, contract-touching change left as a follow-up;
`ModeDefault` (the most restrictive mode) is the safe interim and is documented as a NOTE
at the `Run` handler.

> **Resolved by [ADR-0019](0019-session-scoped-permission-mode.md) (2026-06-10).** The
> mode is now persisted on the session aggregate as the additive `sessions.mode` column
> (migration 0004), stamped at `CreateSession` from the verified request and read by `Run`
> into the policy pipeline; forks inherit it and `GetSession` surfaces it. `bypass` remains
> operator-only/server-side.

## Consequences

- `docker compose up` followed by `harnessctl run "..."` succeeds keyless out of the
  box (`[result] subtype=success`), verified from a clean slate (wiped volumes, images
  rebuilt from source, fresh migrate→grant→services bootstrap). The README quickstart
  works verbatim with no API key, no model backend, and no approval step.
- The keyless smoke does NOT exercise tool execution end-to-end; that coverage lives in
  the tool-runtime integration suite. This is stated in the stub's package doc and in
  `.env.example` so the scope is not over-claimed.
- The egress amendment keeps the specification honest about the implemented mechanism
  without a silent edit of frozen acceptance text.
- Until the per-run-mode follow-up lands, non-default permission modes
  (`acceptEdits`/`plan`/`bypass`) are not selectable via the API; all runs ask for
  risk-tiered tools (interactive approval via `harnessctl approve`), which is the safe
  default. (Since resolved by [ADR-0019](0019-session-scoped-permission-mode.md).)
