// Package openai implements the [llm.Provider] adapter for the native OpenAI API
// over the official openai-go/v3 SDK. It defaults to the Responses API (the
// `responses` subpackage: client.Responses.New / .NewStreaming) and keeps a Chat
// Completions sub-flag for compatibility, reusing the shared Chat-Completions
// stream normalizer in the sibling openaicompat package (ADR-0004; architecture
// §11.5).
//
// # Responses request shape
//
// The normalized [llm.Request.System] prompt is placed in the Responses
// `instructions` field; tools are flat function tools; prior assistant tool calls
// are sent back as typed `function_call` input items and tool results as
// `function_call_output` items. Conversation history maps to the Responses input
// item list.
//
// # Stateless continuation (no previous_response_id)
//
// Per ADR-0016 and architecture §11.1, this adapter is pinned to STATELESS
// Item-passing: it sets store=false and never relies on the server-side
// `previous_response_id` handle. The Response's output Items are carried verbatim
// in [llm.Response.ProviderRaw] / [llm.Done.ProviderRaw]; the next turn replays
// them as input items so a turn can be continued or replayed byte-faithfully on any
// endpoint, preserving self-host and replay portability.
//
// # Stream normalization
//
// The provider-event → [llm.StreamEvent] mapping lives in [Normalizer], a
// network-free, testable accumulator fed one [responses.ResponseStreamEventUnion]
// at a time. response.output_text.delta becomes [llm.TextDelta];
// response.reasoning_text.delta / response.reasoning_summary_text.delta become
// [llm.ThinkingDelta]; function-call argument deltas
// (response.function_call_arguments.delta) are item-scoped and buffered, and the
// terminal response.completed event authoritatively yields one complete
// [llm.ToolCallDelta] per function_call output item plus the single [llm.Done]
// carrying normalized usage and the stateless continuation blob.
//
// # Token counting
//
// CountTokens is unsupported on the Responses surface (there is no count-tokens
// endpoint); it returns a [*llm.ProviderError] of kind [llm.ErrUnsupported],
// consistent with [llm.Capabilities.SupportsTokenCounting] being false. Billing
// uses the authoritative usage on response.completed, never an estimate
// (architecture §11.6).
//
// # Purity boundary
//
// This package is the ONLY place importing the OpenAI Responses SDK surface. It
// maps SDK wire types onto the normalized [llm] kernel and returns a
// [*llm.ProviderError] on every failure; the orchestrator never sees an SDK type.
package openai
