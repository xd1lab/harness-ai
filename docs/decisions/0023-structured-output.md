<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0023: Structured output usable end-to-end — additive public `output_schema`/`strict` + native `response_format` with a graceful validate-retry backstop

- **Status:** Accepted
- **Date:** 2026-06-15
- **Relates to:** ADR-0004/0016 (provider abstraction, per-(endpoint,model) capabilities), ADR-0007 (network-free deterministic eval), ADR-0009/0010 (service decomposition, gRPC client edge), ADR-0011 (event-store schema), the frozen `proto/` contract and `internal/platform/llm` kernel, the agent loop's existing structured-output machinery (`agent.Config.OutputSchema`, `turn.go` validate-then-retry, `TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES`), the REST/SSE facade and MCP Server mode (ADR-0022), and the gateway `GenerationParams.output_schema`(11)/`strict`(12) wire

## Context

Boltrope's structured-output capability was half-built and **unreachable by
users**. The agent loop already validated a final response against a JSON Schema
and retried up to a cap (`agent.Config.OutputSchema`, `turn.go`
`classifyFinal`/`validateStructured`, terminating with
`TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES`), and the gateway
contract already carried `output_schema`/`strict` end to end
(`GenerationParams.output_schema=11`/`strict=12` → `modelgw` outbound → gateway
inbound map → `llm.Request.OutputSchema`/`Strict`). Two gaps remained:

1. The **public** `RunRequest` (gRPC, and the REST and MCP facades that wrap it)
   exposed no `output_schema`/`strict` field, so a client could never turn it on,
   and the client-edge `RunSpec` → `LoopRunner` seam did not overlay it per-run.
2. None of the four provider adapters read `req.OutputSchema` to set native
   provider structured output (`response_format` / `response_schema` /
   `output_config`); structured output relied entirely on the loop's blind
   validate-then-retry.

The questions this ADR settles: what carrier exposes `output_schema` to users;
how it threads to the loop per-run; where native provider structured output is
turned on (and how the gate reads authoritative capabilities without touching the
frozen `llm.Provider` interface); and what happens when native is unavailable.

## Decision

**1. Carrier — one additive proto field on the public `RunRequest`.** Add
`bytes output_schema = 5;` and `bool strict = 6;` to `message RunRequest`
(`proto/boltrope/v1/orchestrator.proto`), regenerate and commit `gen/`. The names
and types are deliberately identical to the existing
`GenerationParams.output_schema`/`strict` and `llm.Request.OutputSchema`/`Strict`,
so the whole chain needs zero vocabulary translation. The field is per-run (the
same session can run with or without a schema), so it rides `RunRequest` /
`RunSpec`, not session-level metadata. Non-proto carriers (metadata map, HTTP
header, session config) were rejected: untyped, session-scoped, transport-
asymmetric, and bypassing the contract as the single source of truth. Under
`buf.yaml`'s FILE-granularity breaking rules a brand-new optional field with a new
tag is wire-compatible — `buf breaking` passes; only rename/remove/renumber would
fail.

**2. Per-run plumbing through the existing client-edge seam.** `RunSpec` gains
`OutputSchema []byte` + `Strict bool`; `Server.Run` fills them from
`req.GetOutputSchema()`/`req.GetStrict()`; `LoopRunner.Run` overlays
`cfg.OutputSchema`/`cfg.Strict` per run exactly as it already overlays `cfg.Mode`.
The loop kernel gains one additive field `agent.Config.Strict` (it already had
`OutputSchema`), and `turn.go` `buildRequest` sets `llm.Request.Strict =
l.cfg.Strict` next to the existing `OutputSchema`. The three transports (gRPC,
REST, MCP) all assemble the SAME `genproto.RunRequest` and call the SAME
`igrpc.Server`, so behaviour cannot drift; REST adds `output_schema`(JSON object)
+ `strict` to its run body, MCP adds them to the run tool's `inputSchema` and
`runArgs`. A non-object `output_schema` is rejected at the facade boundary (REST
HTTP 400, MCP JSON-RPC InvalidParams) before any run starts — fail-closed-early.

**3. Native provider structured output where the pinned SDK truly supports it,
behind a uniform capability gate.** At request-build time each adapter applies
`useNative := len(req.OutputSchema) > 0 && caps.SupportsJSONSchemaStrict` over the
**central** per-(endpoint,model) capabilities:

- **OpenAI-Responses** (`openai-go/v3` v3.39.0): `params.Text.Format =
  json_schema`, `Strict = req.Strict && caps.SupportsJSONSchemaStrict`. Native ON.
- **Gemini** (`genai` v1.59.0): `ResponseMIMEType="application/json"` +
  `ResponseJsonSchema = <decoded schema>`. Native ON.
- **Anthropic** (stable `Messages.New` / `MessageNewParams`, `anthropic-sdk-go`
  v1.50.0): `params.OutputConfig.Format = JSONOutputFormatParam{Schema}` on the
  STABLE path — no Beta header, no Beta endpoint, no forced-tool trick. Native ON
  for the id-verified modern Claude models.
- **OpenAI-compat**: default OFF (never blind-send `json_schema` to an unknown
  self-hosted server); enabled ONLY via an explicit central-registry endpoint
  override (`SetEndpointOverride`).

**4. The capability flag is reused, its meaning converged, and the central table
is authoritative.** `Capabilities.SupportsJSONSchemaStrict` now means "native
structured output is available and strict-enforceable for this
(endpoint,model)". The central `capabilities.Registry` table is the single source
of truth; an adapter's own self-report is not the gate. The modern Claude ids that
support stable structured outputs (`claude-opus-4-0`, `claude-sonnet-4-0`,
`claude-3-7-sonnet-20250219`) are flipped `true` (evidence recorded in the
feature's TRACE.md); legacy `claude-3-*`/`claude-3-5-*` stay `false`; Gemini
2.5/2.0-flash were already `true`; OpenAI Responses models were already `true`.

**5. A new caps-injection seam — because the frozen `llm.Provider` interface
carries no capabilities.** `Provider.Generate`/`Stream(ctx, req)` is frozen and
takes no caps, and `Service.Stream` passes none; the providers' own structs hold
neither a registry nor their endpoint name. So each provider gains an injected
narrow `capabilityResolver` port (`Resolve(endpoint, model) llm.Capabilities`,
satisfied by the central `*capabilities.Registry`) plus its bound endpoint name,
wired from `cmd/boltrope-modelgwd`. ONE shared registry now backs both the
providers' native gate and the gateway `Service`'s `Capabilities` RPC. A nil
resolver leaves behaviour byte-for-byte unchanged (native simply never fires), so
this is purely additive — the frozen interface is untouched.

**6. The loop's validate-then-retry is the non-negotiable correctness backstop,
even when native is on.** Native `response_format` only reduces retries; the
loop's `validateStructured` and `TERMINATION_SUBTYPE_ERROR_MAX_STRUCTURED_OUTPUT_RETRIES`
remain the final authority, fail-safe against a provider that claims support but
silently drops the schema.

## Consequences

**Good.**

- Structured output is reachable end-to-end over all three transports with one
  additive proto field; existing clients that omit it are byte-for-byte unchanged
  (zero migration cost).
- Native structured output is genuinely active in **3 of 4** adapters
  (OpenAI-Responses, Gemini, Anthropic-stable), cutting retry rounds and reaching
  first-attempt compliance where the SDK supports it; OpenAI-compat falls back
  honestly by default and is opt-in per endpoint.
- The gate reads ONE authoritative capability table through ONE injected seam, so
  the native decision cannot drift between the adapter self-report and the
  advertised `Capabilities` RPC. The override surface is a single registry.
- No loop re-architecture, no new retry algorithm, no new termination subtype, no
  new capability flag, no change to the frozen `proto/` shape semantics or the
  `internal/platform/llm` Provider contract (only an additive `Config.Strict`
  loop field and a comment-only godoc convergence on `SupportsJSONSchemaStrict`).

**Costs / trade-offs.**

- `gen/` must be regenerated and committed in lockstep with the proto edit (CI
  builds against committed `gen/`).
- The four package-level request builders gained a `caps` parameter and each
  provider struct/constructor gained a resolver+endpoint — additive threading, but
  it touched every existing builder call-site in the provider tests (expected
  churn, not a refactor).
- `SupportsJSONSchemaStrict`'s meaning is narrowed; its godoc is updated, and the
  Anthropic adapter's local self-report (historically `true`, meaning "strict tool
  schema") is no longer the structured-output gate — the central table wins.
- Per-id Anthropic flips are evidence-gated: an unverified id stays `false` and
  conservatively falls back to the loop, so coverage grows only as ids are
  verified — never by blind-sending `output_config` to a model that 4xx's it.

**Out of scope (v1, honest).** Anthropic `OutputConfig.Effort` and non-`Format`
output knobs; native OpenAI-compat by default (opt-in only, no auto-probing);
schema-dialect/`$ref` validation beyond the loop's existing validator; mid-stream
JSON-grammar enforcement (validation stays on the final response); a typed proto
message for the schema (`output_schema` stays `bytes`).
