// Package openaicompat implements the [llm.Provider] adapter for OpenAI-compatible
// Chat Completions endpoints — self-hosted or proxied servers such as vLLM,
// Ollama, LM Studio, llama.cpp (llama-server), TGI, and the LiteLLM proxy.
//
// Per ADR-0004 and ADR-0016 (architecture §11.5), self-hosted servers implement
// the OpenAI Chat Completions API, not the Responses API, so this adapter targets
// Chat Completions exclusively and shares its stream normalizer with the native
// OpenAI adapter's Chat-Completions sub-flag path. The adapter is constructed with
// a configurable base URL plus a placeholder API key (most local servers ignore
// the key but the SDK requires a non-empty one) via [option.WithBaseURL].
//
// # Stream normalization
//
// The provider-event → [llm.StreamEvent] mapping lives in [Normalizer], a
// network-free, testable accumulator fed one [openai.ChatCompletionChunk] at a
// time. Streamed tool-call arguments arrive as a JSON-string fragment per chunk
// (OpenAI Chat Completions encodes them under choices[].delta.tool_calls[].
// function.arguments as a concatenable string keyed by an integer index); the
// normalizer concatenates fragments by index, parses the assembled JSON once the
// stream finishes, and emits a SINGLE complete [llm.ToolCallDelta] per tool call
// before the terminal [llm.Done]. This uniform emission also covers endpoints whose
// [llm.Capabilities.SupportsStreamingToolCalls] is false (e.g. LM Studio): the
// normalizer buffers and emits one complete tool-call delta regardless of whether
// the server streamed the call incrementally, so the orchestrator assembles tool
// calls identically in both cases.
//
// # Capabilities
//
// Capabilities are resolved per (endpoint, model) from a small per-endpoint
// profile (see [EndpointProfile]); the loop never default-assumes
// Chat-Completions-shaped capabilities for an unknown self-hosted model
// (architecture §11.4).
//
// # Purity boundary
//
// This package is a model-gateway outbound adapter: it is the ONLY place that
// imports the OpenAI Go SDK on the Chat-Completions path. It maps the SDK wire
// types onto the normalized [llm] kernel types and returns a [*llm.ProviderError]
// on every failure; the orchestrator never sees an SDK type.
package openaicompat
