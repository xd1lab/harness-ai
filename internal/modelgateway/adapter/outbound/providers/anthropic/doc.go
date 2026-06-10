// Package anthropic implements the Boltrope [llm.Provider] over Anthropic's
// Messages API using the official anthropic-sdk-go (v1.50.0).
//
// It is one of the model-gateway's outbound provider adapters (architecture
// §5.2, §11; ADR-0004, ADR-0016). All Anthropic-specific behavior lives here:
// request mapping ([llm.Request] -> Messages API params), stream normalization
// (the SSE event sequence -> [llm.StreamEvent]), stop-reason normalization, usage
// extraction, capability resolution, and error classification. Every method
// returns the normalized types from [github.com/xd1lab/harness-ai/internal/platform/llm]
// and a failure is always a [*llm.ProviderError], so the orchestrator stays
// provider-agnostic.
//
// # Stream normalization
//
// The Anthropic streaming protocol emits an SSE sequence — message_start,
// content_block_start, content_block_delta (text_delta / input_json_delta /
// thinking_delta / signature_delta), content_block_stop, message_delta (carrying
// stop_reason and cumulative usage), message_stop. The [streamNormalizer] is an
// isolated, network-free state machine that converts each SDK
// [anthropic.MessageStreamEventUnion] into zero or more [llm.StreamEvent]s; it is
// golden-tested directly against synthetic provider events (see
// normalizer_test.go). Tool-call arguments arrive as input_json_delta fragments
// scoped to a content-block index; the normalizer maps each index to the opaque
// tool_use id captured at content_block_start and emits append-style
// [llm.ToolCallDelta]s. Thinking text and the opaque thinking signature are
// emitted as [llm.ThinkingDelta]s, and the final signature plus any pause_turn
// continuation are captured into [llm.Done.ProviderRaw] so a paused turn or signed
// thinking can be replayed byte-faithfully (architecture §11.1).
//
// # Continuation state
//
// On a pause_turn stop reason the normalizer reports [llm.Pause] (non-terminal)
// and serializes the accumulated assistant content blocks into ProviderRaw so the
// loop can echo them back to continue. On all terminal reasons it reports the
// mapped [llm.StopReason] with the verbatim provider string preserved in
// RawStopReason.
package anthropic
