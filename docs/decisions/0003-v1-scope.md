# 3. v1 scope and feature prioritization

Date: 2026-06-10
Status: Accepted

## Context

The research feature taxonomy lists MUST/SHOULD/COULD capabilities. We need an
explicit, buildable v1 boundary so the spec and TDD effort stay focused, while not
foreclosing the deferred features (the architecture must keep their seams open).

## Decision

**In v1 — MUST (the irreducible harness):**

- Single-threaded gather→act→verify agent loop; turns; max-turns + max-budget caps;
  typed termination subtypes (`success`, `error_max_turns`, `error_max_budget_usd`,
  `error_during_execution`, `error_max_structured_output_retries`).
- Provider-portable model interface (pin model ids; streaming; capability flags).
- Tool registry with JSON-Schema validation **before** execution; uniform
  Action→Execute→Observation contract; error-as-observation; core tools
  (Read/Edit/Write, Glob/Grep, Bash, web fetch/search).
- Context accounting + automatic compaction + prompt caching of stable prefixes.
- Session persistence as an append-only **event-sourced** log with resume (Postgres).
- Layered permissions (deny→mode→allow→tool) with default/acceptEdits/plan/bypass
  modes + human-in-the-loop approval callback.
- Sandboxed execution behind a Workspace abstraction (container MVP, deny-by-default
  network).
- Reliability primitives: streaming, cooperative cancellation, retries honoring
  `Retry-After`→backoff+jitter (retry only 429/5xx/529), basic rate limiting.
- Token + cost accounting on every result; structured logging (slog JSON).

**In v1 — selected SHOULD:** tool-result clearing; session fork/replay; sub-agents
as ordinary tools (depth-limited); MCP **client** with lazy schema loading;
hooks/middleware (PreToolUse/PostToolUse/Stop/PreCompact); OpenTelemetry GenAI
tracing; a bespoke eval harness wired to CI (see [ADR-0007](0007-eval-strategy.md));
parallel read-only tool execution; basic secret-registry output masking.

**Deferred to post-v1 (keep seams open, do not build):** MCP **server** mode + A2A;
native-Ollama NDJSON adapter; model routing; microVM/OS-native sandbox backends;
advanced multi-agent topologies; non-native function-calling fallback + constrained
decoding; semantic codebase indexing (a tree-sitter repo map is a likely *next*
SHOULD); LLM risk-classifier; virtual-filesystem context mounts; interactive
workspace access (VNC/embedded editor).

## Consequences

- ✅ A focused, demonstrably-useful v1 with a clear definition of done.
- ✅ The Workspace, Provider, Tool, and MCP abstractions are designed so deferred
  items slot in without re-architecture.
- ⚠️ Some headline features (microVM isolation, model routing) are absent at launch;
  the README roadmap must set expectations.
