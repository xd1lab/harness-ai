// Package llm defines Boltrope's canonical, provider-agnostic model of an LLM
// interaction: the normalized messages, content parts, tool definitions, tool
// calls and results, requests, responses, streaming events, capabilities, usage,
// errors, and the [Provider] interface that the agent loop talks to.
//
// # Single source of truth
//
// Per ADR-0004 (multi-LLM provider strategy) and ADR-0016 (provider abstraction),
// this package is the single source of truth for the normalized model that the
// multi-LLM abstraction exists to keep consistent. Each provider family
// (Anthropic, Google Gemini, OpenAI Responses, and OpenAI-compatible self-hosted)
// gets one model-gateway adapter that maps its wire format onto these types; the
// orchestrator and every other service import these types directly and never
// re-declare or "mirror" them (architecture §12.3). The generated protobuf wire
// types live separately under gen/; adapters map gen/ ⇄ llm at the transport edge
// only.
//
// # Purity
//
// This package is a pure, dependency-free domain kernel. It imports only the Go
// standard library (context, encoding/json, errors, fmt, time) and MUST NOT import
// any provider SDK, gRPC, protobuf, or other infrastructure package. This purity
// is enforced by depguard in CI (architecture §12.2). It contains interfaces,
// types, constants, and documentation — not provider logic. Stream normalization,
// stop-reason mapping, capability resolution, usage extraction, error
// classification, and cost computation are all the responsibility of the
// model-gateway adapters that implement [Provider]; they are deliberately absent
// here so the orchestrator stays provider-agnostic.
//
// # Design decisions captured here
//
// The types in this package bake in the hardened decisions from ADR-0016 and
// architecture §11:
//
//   - Stop reasons are an OPEN set ([StopReason]): first-class typed constants for
//     known reasons plus a [StopOther] escape hatch carrying the raw provider
//     string, so an unrecognized reason is passed through, never silently dropped.
//   - [Pause] is a separate, non-terminal continuation outcome distinct from a
//     terminal [Done]: the loop branches on three outcomes (final /
//     needs-tool-execution / needs-continuation), not two.
//   - [Request.ProviderRaw], [Response.ProviderRaw], and [Done.ProviderRaw] carry
//     an opaque, provider-scoped continuation blob so Anthropic pause_turn /
//     server_tool_use blocks and thinking signatures, and OpenAI Responses Items,
//     can be echoed back for stateless replay and continuation — with no
//     provider-side state handle.
//   - [ToolCallDelta] carries an opaque per-call identifier (not an integer index)
//     plus a structured fragment, so the gateway can normalize Gemini's
//     path-addressed fragments and OpenAI Responses' item-scoped deltas uniformly.
//   - [Capabilities] are resolved per (endpoint, model): the model id is an input
//     to [Provider.Capabilities].
package llm
