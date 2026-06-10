# 4. Multi-LLM provider strategy

Date: 2026-06-10
Status: Accepted

## Context

The harness must drive Anthropic Claude, Google Gemini, OpenAI ("Codex"), and
self-hosted / OpenAI-compatible models from one agent loop. These APIs differ on
every axis: system-prompt placement, tool-call wrappers, stop-reason vocabularies,
streaming shapes, and token counting (see the research report's Multi-LLM section,
all facts re-verified at Gate-1).

## Decision

- Define a **normalized internal model** (messages, content parts, tool defs, tool
  calls/results, usage) and a small **`Provider` interface**:
  `Generate / Stream / CountTokens / Capabilities`. The agent loop talks only to this
  interface.
- One **adapter per provider** over the official, verified Go SDKs (pinned to exact
  versions, not "latest"):
  - Anthropic — `github.com/anthropics/anthropic-sdk-go` (MIT)
  - Gemini — `google.golang.org/genai` (Apache-2.0)
  - OpenAI — `github.com/openai/openai-go/v3` (Apache-2.0; Go 1.22+)
- **OpenAI defaults to the Responses API** (`responses` subpackage); keep Chat
  Completions (declared in the **root `openai` package** — not a subpackage) behind a
  sub-flag for compatibility.
- **Self-hosted** (vLLM, Ollama, LM Studio, llama.cpp `llama-server`, TGI) and the
  **LiteLLM** proxy are reached through the **OpenAI-compatible Chat Completions
  adapter** with a configurable base URL + placeholder key. TGI is best-effort
  (upstream entered maintenance mode 2026-03-21).
- **Capability flags are per-endpoint, not just per-provider** (e.g., LM Studio lacks
  streaming/parallel tool calls). Fail fast or degrade rather than send a request the
  backend will reject.
- Centralize **stop-reason** normalization (one `StopReason` enum) and **error**
  normalization (`ProviderError{Kind, RetryAfter, Raw}`). **Unrecognized** provider
  stop reasons map to a defined default (`StopOther`) and are logged, never silently
  dropped. One harness-level retry policy keyed on `ProviderError.Kind` honors
  `Retry-After` then exponential backoff with full jitter.
- **Token counting** is capability-gated: Anthropic/Gemini call their server
  endpoints; OpenAI uses a local `o200k_base` tokenizer or returns `Unsupported`.
  **Never** use tiktoken to estimate Anthropic/Gemini tokens; always bill from actual
  `usage`/`usageMetadata`.

## Consequences

- ✅ Adding a provider = one adapter; the loop is untouched.
- ✅ Self-hosted models work on day one via the OpenAI-compatible path.
- ⚠️ The Responses SDK surface churns faster than Chat Completions; pin versions and
  re-validate at implementation time.
- ⚠️ Capability discovery for arbitrary self-hosted endpoints is imperfect; treat the
  capability matrix as runtime-overridable config.
