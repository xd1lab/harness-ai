<!-- SPDX-License-Identifier: Apache-2.0 -->

# ADR-0029: `boltrope-dev` real model + Docker local-exec behind explicit, default-OFF opt-in flags (amends ADR-0024)

- **Status:** Accepted
- **Date:** 2026-06-25
- **Amends:** [ADR-0024](0024-boltrope-dev-local-mode.md) (boltrope-dev local mode: separate single-process, loopback-only, in-memory, no-exec dev binary)
- **Relates to:** ADR-0004/0016 (multi-LLM provider strategy, per-`(endpoint, model)` capabilities), ADR-0005/0014 (sandbox isolation: containers behind a `Workspace`/`Runtime` port, docker kill / `--network none` / cgroup-PID limits), ADR-0013/0021 (security model, egress broker on model-influenced channels, deny-by-default fetcher), ADR-0023 (structured output: native `response_format` via an injected central-caps seam).

## Context

[ADR-0024](0024-boltrope-dev-local-mode.md) shipped `boltrope-dev` as a single-process,
loopback-only, in-memory dev binary that runs the **real** agent loop with two
network edges stubbed: the model is the keyless `stub` provider, and tools run
through a **no-exec** `ToolRuntimePort` whose `bash` is a refusing placeholder
(`"dev sandbox exec disabled"`). ADR-0024 §3 explicitly deferred a real
shell-capable sandbox to roadmap behind a **second explicit opt-in**
(`--enable-local-exec`, default off, *rejected in v1*).

That deferral is now closed. Developers want to point `boltrope-dev` at a **real
local OpenAI-compatible model** (e.g. Ollama serving `gemma` at
`http://localhost:11434/v1`) and have it **actually execute tools** in a
strongly-isolated Docker sandbox — without standing up the four-service
production stack and without weakening the build-time prod-exclusion invariant
that is the whole point of a separate binary.

The hazard ADR-0024 guards against is unchanged: the dev binary bypasses RLS,
mTLS, and OIDC, so it must remain **impossible to mistake for, or use as, a
production deployment**. Wiring a real model and a real sandbox into it must not
(a) make those edges the *default*, nor (b) drag any forbidden production
dependency (pgx, SPIRE/mTLS, the `modelgateway/app` Service, or the
orchestrator→model-gateway / →tool-runtime gRPC client adapters) into the dev
binary's import graph.

Two wrinkles surfaced while wiring this:

1. The OpenAI-compatible provider needs a capability resolver to decide whether
   to emit native `json_schema` structured output. That resolver lives at
   `internal/modelgateway/app/capabilities` — **under** the `modelgateway/app`
   path that ADR-0024's import-graph test forbids by *prefix*. A naive read of
   the fence would falsely flag the leaf.
2. In production the orchestrator reaches the tool-runtime over a gRPC client
   adapter (`internal/orchestrator/adapter/outbound/toolrt`) which is **forbidden**
   in the dev binary. Local-exec therefore needs an *in-process* bridge, not the
   gRPC edge.

## Decision

### 1. Two new model flags + native-schema flag, default-OFF (`--model-url` / `--model` / `--model-api-key-env` / `--enable-native-schema`)

The dev binary gains explicit opt-in flags, **all default-OFF**, that leave the
ADR-0024 default path byte-for-byte behaviorally identical:

- `--model-url <base-url>` — when set, server wiring constructs
  `openaicompat.New(openaicompat.Config{BaseURL: <url>, Capabilities: <*capabilities.Registry>, Endpoint: "openaicompat"})`
  and wraps it via `newDevModel(provider)` as the loop's `app.ModelGatewayPort`,
  **replacing** `stub.New()`. When unset, `stub.New()` is used (the ADR-0024
  default).
- `--model <id>` (default `"stub"`) — the model id threaded into **both**
  `agent.Config{Model: <id>}` and `igrpc.Config{DefaultModel: <id>}`, replacing
  the two hardcoded `"stub"` literals. The default keeps both equal to `"stub"`.
- `--model-api-key-env <ENVVAR>` (optional) — names an env var whose **value** is
  read at the point of `openaicompat.Config{APIKey: ...}` construction. The key
  value is **never** logged and **never** threaded into the banner, run-config,
  or parsed-flags structs — only the model **endpoint + id** are ever printed.
- `--enable-native-schema` (bool, default off) — calls
  `reg.SetEndpointOverride("openaicompat", capabilities.EndpointOverride{AllModels: &llm.Capabilities{SupportsJSONSchemaStrict: true}})`
  so native `json_schema` structured output is turned on for the OpenAI-compatible
  endpoint via the endpoint-wide `AllModels` override (ADR-0016/0023). When the
  flag is off, no override is set and the conservative default applies.

### 2. Local-exec behind `--enable-local-exec`, default-OFF, over an in-process bridge

`--enable-local-exec` (bool, default off) is **no longer rejected** as roadmap.
When OFF, the ADR-0024 no-exec `Runtime` is used unchanged. When ON, the server
wires an **in-process bridge** implementing orchestrator `app.ToolRuntimePort`
backed by the tool-runtime `execute.Service`, and uses it as `deps.Tools`
instead of the no-exec runtime. The bridge:

- Converts orchestrator `app.ToolExecution` → tool-runtime `execute.Request`
  (`SessionID`→`SessionID`, `Call.Name`→`ToolName`, `Call.ID`→`CallID`, parsed
  `Call.Args`→`Args`, `IdempotencyKey`→`IdempotencyKey`), sets `TenantID` to the
  dev synthetic single-tenant principal (`igrpc.DevTenantID`, the **same** id the
  dev auth path injects — never an invented literal — so the dedup ledger keys
  and any tenant re-check stay consistent), calls `execute.Service.Execute`, and
  returns an `app.ToolStream` that yields one terminal `app.ToolResult` then
  `io.EOF`.
- Maps tool-runtime `ListTools` into `[]app.ToolDescriptor`
  (Name/Description/JSONSchema/SideEffect/EgressClass).

The `execute.Service` is constructed from clean, allowed packages only:

- **Registry:** `registry.New(nil)` populated with `tools.Native(ws, fetcher, searchURL)`.
- **Runtime:** `runtime.New(runtime.DefaultConfig())` overlaid with
  `BOLTROPE_TOOLRT_IMAGE` / `BOLTROPE_TOOLRT_DOCKER_BIN` env (reusing the
  production env names). The container runs **per-session** with `--network none`
  + cgroup/PID limits via `runtime.DefaultConfig()` — **no code change** to the
  runtime package; this is the ADR-0014 isolation, opted into.
- **Egress:** `egress.New(egress.WithDefaultAllowedHosts(nil))` — **deny-by-default**
  (empty allowlist). `webfetch`/`websearch` are advertised but always denied
  (no allowlisted host + `--network none`), consistent with ADR-0013/0021.
- **Dedup:** a **new in-process in-memory `DedupStore`** (see §3).
- **Blobs:** `blob.NewFSStore(<temp dir>, <max bytes>)` — the temp dir is created
  via `os.MkdirTemp` on the local-exec path only and removed on shutdown.

Because `runtime.New`/`DefaultConfig` invoke Docker lazily (never at construction),
wiring is testable **without Docker**; only an end-to-end `bash` execution needs a
Docker daemon and a pulled `BOLTROPE_TOOLRT_IMAGE`. The default-OFF path
constructs no runtime and has **no Docker dependency**.

### 3. In-memory `DedupStore` — zero pgx, hand-rolled against the clean app port

The production dedup pool (`dedup.NewSimplePool` / `dedup.New`) requires Postgres
(pgx) and is therefore **forbidden** in the dev binary. We hand-roll a tiny
in-process `DedupStore` (new file in `cmd/boltrope-dev`) that satisfies the
tool-runtime `app.DedupStore` port (`Begin`/`Complete`/`Lookup` over
`app.ExecutionRecord` keyed by `(TenantID, SessionID, IdempotencyKey)`), with a
compile-time `var _ app.DedupStore = (*…)(nil)` assertion. It depends **only** on
`internal/toolruntime/app` (ports + `ExecutionRecord` + `ExecutionStatus`
consts) — which is clean — and **must not** import
`internal/toolruntime/adapter/outbound/dedup`, whose store imports the pgx-backed
pool. `Begin` is get-or-create; `Complete` records terminal status + result;
`Lookup` returns the stored record or an error when absent.

### 4. Import-graph fence refinement — EXACT-match the Service, ALLOW the capabilities leaf

ADR-0024's import-graph test forbade a fixed set by the rule
`dep == forbidden || HasPrefix(dep, forbidden+"/")`. Because the new dependency
`internal/modelgateway/app/capabilities` lives **under**
`internal/modelgateway/app`, the prefix rule would falsely flag it. We refine the
fence so that:

- The `modelgateway/app` **Service** package is forbidden by **EXACT match**
  (`dep == "github.com/xd1lab/harness-ai/internal/modelgateway/app"`), **not** by
  prefix.
- The other five forbidden entries keep the **prefix** rule (so a future
  `…/eventstore/foo` cannot slip through): `internal/orchestrator/adapter/outbound/eventstore`,
  `github.com/jackc/pgx/v5`, `github.com/spiffe/go-spiffe`,
  `internal/orchestrator/adapter/outbound/modelgw`,
  `internal/orchestrator/adapter/outbound/toolrt`.
- The refined test **positively asserts** that
  `internal/modelgateway/app/capabilities` **is** present in
  `go list -deps cmd/boltrope-dev` (the leaf is permitted), and adds a **negative
  guard** that `internal/toolruntime/adapter/outbound/dedup` is **absent** (so the
  hand-rolled dedup cannot regress into pulling pgx).

### 5. Why the capabilities leaf is safe

`internal/modelgateway/app/capabilities` is **pure data**: `NewRegistry`,
`SetEndpointOverride`, and `Resolve` over `llm.Capabilities`, guarded by a
`sync.RWMutex`. It performs **no I/O**, imports **no** pgx / SPIRE / Service, and
the OpenAI-compatible provider does **not** pull it in transitively (it takes the
resolver as a field) — the dev binary imports it **directly** to construct the
registry. It therefore carries none of the multi-tenant / cross-process /
fail-closed-edge machinery the `modelgateway/app` Service does, and admitting it
does not weaken the ADR-0024 invariant. This is exactly the kind of clean,
stdlib-plus-`llm` leaf the build-time fence is meant to *permit* while still
excluding the heavy Service.

### 6. Banner honesty

`writeBanner` takes the resolved local-exec / model posture. The six always-on
markers are retained: `*** NOT FOR PRODUCTION ***`, `IN-MEMORY`, `NO RLS`,
`NO mTLS`, `NO OIDC`, `LOOPBACK ONLY`. When local-exec is **OFF** the banner
prints the existing `Sandbox     : NO-EXEC …` line. When local-exec is **ON** it
**replaces** that line with
`Sandbox     : LOCAL-EXEC ENABLED (Docker isolation: per-session container, --network none, cgroup/PID limits)`.
When a real model is set, the banner adds a `Model       : <endpoint> <id>` line
(endpoint + id only; **never** the API key). The prod-signal refusal
(K8s / Postgres DSN / OIDC issuer) and the loopback fence are **unchanged** and
run **before** the success path, independent of the new flags.

### 7. Production capability override (secondary) — `BOLTROPE_MODELGW_NATIVE_SCHEMA`

So a self-hosted production endpoint can opt into native structured output
**without a code change**, `cmd/boltrope-modelgwd` wiring reads a truthy
`BOLTROPE_MODELGW_NATIVE_SCHEMA` (1/true/yes/on, reusing the existing
`truthy()` convention) and calls `caps.SetEndpointOverride(<endpoint>,
EndpointOverride{AllModels: &llm.Capabilities{SupportsJSONSchemaStrict: true}})`
on the **same** caps registry instance that backs both the provider and the
Service. Since `SetEndpointOverride` is concurrency-safe and `Resolve` reads
live, the override takes effect per request. v1 ships the simple boolean; a
structured `BOLTROPE_MODELGW_CAPS_OVERRIDE` parser is a documented follow-up,
left out here rather than half-specified.

## Consequences

**Good.**
- `boltrope-dev` can now talk to a real local OpenAI-compatible model (Ollama
  `gemma`) and execute tools in an ADR-0014-grade Docker sandbox — the missing
  half of the "feel the real loop" promise — while the **default invocation
  stays exactly as ADR-0024**: stub model (id `"stub"` in both the loop Config
  and the gRPC DefaultModel), no-exec runtime, loopback, loud banner with the
  `NO-EXEC` marker.
- The build-time prod-exclusion invariant is **preserved and sharpened**: the
  fence now exact-matches the heavy Service while admitting the clean
  capabilities leaf, and a negative guard keeps the in-memory dedup off pgx.
- Self-hosted production endpoints opt into native structured output via one env
  var, no code change.

**Bad / accepted trade-offs (honestly documented).**
- Local-exec requires a Docker daemon and a pulled `BOLTROPE_TOOLRT_IMAGE`; with
  neither, `bash` execution fails at container create (construction still
  succeeds). End-to-end exec tests are gated behind Docker availability so the
  offline unit gates stay green.
- `webfetch`/`websearch` are advertised under local-exec but **always denied**
  (deny-by-default broker + `--network none`); this is intentional and consistent
  with ADR-0013/0021, but can read as a confusing denial until the deny-by-default
  posture is understood.
- The real-model / local-exec edges still bypass RLS/mTLS/OIDC; they remain
  **dev-only**, default-OFF, fenced, and loud — never a production backend.

**Follow-up.**
- A structured `BOLTROPE_MODELGW_CAPS_OVERRIDE` parser for richer per-model
  production capability overrides (this ADR ships only the boolean
  native-schema toggle).
