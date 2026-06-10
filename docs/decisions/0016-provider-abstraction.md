# 16. Provider abstraction: per-(endpoint,model) capabilities, open stop reasons + non-terminal Pause, provider_raw opaque continuation, stateless Responses, gateway-side normalization and cost

Date: 2026-06-10
Status: Accepted

## Context

The harness targets four provider families (Anthropic, Gemini, OpenAI Responses, and
OpenAI-compatible self-hosted). Each family has a distinct wire format, streaming shape,
stop-reason vocabulary, tool-call delta encoding, capability set, and usage/cost
reporting convention. Naively mirroring any one of these in the loop produces
provider-specific logic that contaminates the orchestrator.

The draft had four concrete errors:

1. It modelled streamed tool calls 1:1 on OpenAI Chat Completions (flat integer index
   + a concatenable args_partial string). Gemini uses path-addressed partialArgs
   fragments; OpenAI Responses uses item_id/output_index-keyed deltas. The
   accumulation logic was placed in the orchestrator relay, contaminating the loop.

2. It assumed "assembled messages only" were sufficient for replay. Two of four
   providers require echoing opaque state back: Anthropic pause_turn returns a
   server_tool_use block that must be sent back as-is; Anthropic extended thinking
   returns opaque thinking signatures that must be returned unmodified. Without an
   opaque continuation slot, resume/replay for these providers was silently broken.

3. Capability flags were per-provider. One Anthropic key serves models with different
   thinking/vision/context windows; one Gemini endpoint serves models with and without
   streaming tool-call support. Per-provider flags give wrong answers.

4. Stop reasons were modelled as a closed enum that did not include Pause as a distinct
   non-terminal outcome, nor Refusal and ContextWindowExceeded as first-class reasons.

## Decision

**All stream normalization in the gateway.** Stream normalization — including tool-call
accumulation, delta encoding, and stop-reason mapping — lives entirely in model-gateway
adapters. The orchestrator relay is provider-agnostic and the partial-JSON-by-integer-
index accumulation step is deleted from the loop. The gateway has at least three stream
normalizers (Anthropic SSE, OpenAI Chat-Completions SSE, OpenAI Responses typed events)
plus the Gemini iterator, all feeding one StreamEvent oneof.

`ToolCallDelta` uses an opaque per-call identifier (string, not integer index) plus a
structured-fragment representation that encodes Gemini's jsonPath fragments and
Responses' item-scoped deltas. A per-(endpoint,model) `SupportsStreamingToolCalls` gate
makes the gateway buffer the whole call internally when streaming tool args are
unsupported rather than assuming concatenation works.

**Per-(endpoint,model) capability flags.** `CapabilitiesRequest` carries the model id.
The gateway returns model-specific flags from a static table keyed by model, overridable
per endpoint for self-hosted. Flags include SupportsStreamingToolCalls, SupportsThinking,
SupportsParallelToolCalls, SupportsVision, SupportsTokenCounting, and MaxOutputTokens.
For OpenAI-compatible self-hosted endpoints with unknown model sets, startup capability
probing is deferred; the loop never default-assumes Chat-Completions-shaped capabilities.

**Open stop-reason set with non-terminal Pause.** The normalized stop set is open: a
normalized enum for known terminal reasons plus a raw string and StopOther variant per
ADR-0004. Pause is a separate non-terminal Continue signal distinct from Done. The loop
branches on three outcomes — final, needs-tool-execution, needs-continuation(pause) —
not two. Refusal and ContextWindowExceeded are first-class normalized reasons; a refusal
maps to a distinct termination subtype, not error_during_execution.

**provider_raw opaque continuation slot.** The `AssistantMessage` event carries a
`provider_raw` JSONB column that holds provider-native opaque blocks: Anthropic
server_tool_use blocks and thinking signatures, OpenAI Responses Items. The normalized
Message is the model-visible projection; provider_raw is the source of truth for the
next provider call, so a turn can be continued or replayed byte-faithfully. This makes
the "assembled-messages-only, deterministic replay" claim true for all four providers.

**OpenAI Responses is pinned to stateless Item-passing.** The gateway passes Items
in provider_raw and replays statelessly rather than relying on server-side
`store:true`/`previous_response_id`. This preserves self-host and replay portability.
A provider-side state handle that breaks portability and self-hosting is not stored as
harness state.

**OpenAI-compatible adapter targets Chat Completions.** Self-hosted servers (vLLM,
Ollama, LM Studio, llama.cpp, TGI) implement Chat Completions, not Responses. The
`openaicompat` adapter shares the Chat-Completions normalizer. LM Studio disables
streaming and parallel tool calls in its per-endpoint capabilities.

**Native OpenAI adapter defaults to Responses** with Chat Completions behind a
sub-flag. Native adapters for Anthropic, Gemini, and OpenAI use the official Go SDKs.
One harness-level retry policy keyed on `ProviderError.Kind` operates above the SDKs'
own backoff; it is implemented with an injected Clock and jitter source for
deterministic test assertions (no direct time.Sleep).

**Usage and cost computed in the gateway.** Usage semantics differ by provider:
Anthropic streams cumulative usage under message_delta with cache_read/cache_write split;
Gemini reports usageMetadata per chunk; OpenAI Responses bills the full chained context
as input each turn. The gateway reads Usage from the authoritative field per surface
(final cumulative message_delta / last-chunk usageMetadata / response.completed usage),
normalizes to {input, output, cache_read, cache_write}, and computes cost in the gateway
(which holds model pricing). Cost is not recomputed from token counts in the
orchestrator. CountTokens is capability-gated and never used for billing on providers
that offer a streaming usage field.

## Consequences

- The orchestrator relay is provider-agnostic: no provider-specific delta accumulation,
  no provider-specific stop-reason branching, no provider-specific capability checks in
  the loop. Provider SDK churn is isolated in model-gateway.
- Replay is deterministic for all four providers because provider_raw carries the
  opaque state required for byte-faithful continuation; the "assembled-messages-only"
  claim is now true.
- Per-(endpoint,model) capability flags give correct answers for models with varying
  capabilities on the same endpoint key.
- The non-terminal Pause outcome enables the Anthropic pause_turn and extended-thinking
  protocols without special-casing them in the loop.
- The open stop-reason set means new provider-specific reasons are passed through as
  StopOther + raw string without breaking the loop; known reasons are first-class
  without being exhaustive.
- Stateless Responses Item-passing preserves portability for self-hosted and replay
  scenarios; it trades the potential provider-side context-window optimization of
  previous_response_id for correctness and portability.
- Gateway-side cost computation keeps pricing logic in one place; the orchestrator
  receives a normalized cost_usd on TurnFinished events and does not need to know
  per-model pricing.
