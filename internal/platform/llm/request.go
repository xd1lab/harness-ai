package llm

import "encoding/json"

// ProviderRaw is an opaque, provider-scoped continuation blob.
//
// It carries provider-native state that must be echoed back verbatim on the next
// call to make progress or to replay byte-faithfully — for example Anthropic
// pause_turn / server_tool_use blocks and extended-thinking signatures, or OpenAI
// Responses Items. It is deliberately opaque (raw JSON): the orchestrator and the
// normalized [Message] never interpret it; only the originating provider's adapter
// produces and consumes it. This enables stateless replay and continuation with NO
// provider-side state handle (no reliance on server-side store / previous_response_id),
// which preserves self-hosting and deterministic replay (ADR-0016; architecture §11.1).
//
// A nil or empty ProviderRaw means "no continuation state"; it is only meaningful
// to the same (provider, endpoint) that emitted it.
type ProviderRaw = json.RawMessage

// Request is a normalized, provider-agnostic generation request. The agent loop
// builds a Request and hands it to a [Provider]; the model-gateway adapter maps it
// onto the provider's wire format, placing System, Tools, and ToolChoice where
// that provider requires.
type Request struct {
	// Model is the provider model identifier to target (e.g. "claude-opus-4-8").
	// It is also the key for per-(endpoint,model) capability resolution.
	Model string

	// System is the system prompt as a single string. It is first-class here;
	// each adapter places it correctly for its provider (top-level system,
	// instructions, or a leading system message). Gated by
	// [Capabilities.SupportsSystemPrompt].
	System string

	// Messages is the ordered conversation history, oldest first.
	Messages []Message

	// Tools is the set of tools the model may call. Empty means no tools.
	Tools []ToolDef

	// ToolChoice constrains tool usage for this request. The empty value lets
	// the adapter apply the provider default (see [ToolChoice]).
	ToolChoice ToolChoice

	// MaxTokens is the maximum number of tokens to generate in the response. A
	// zero value lets the adapter apply a provider/model default; adapters may
	// clamp it to [Capabilities.MaxOutputTokens].
	MaxTokens int

	// Temperature is the sampling temperature. It is a pointer so that nil
	// distinctly means "use the provider default" rather than "0.0". Some models
	// reject sampling parameters entirely; adapters drop it where unsupported.
	Temperature *float64

	// Stream requests incremental delivery. It is advisory: [Provider.Generate]
	// always returns a single aggregated [Response], while [Provider.Stream]
	// always streams; this flag records the caller's intent for adapters that
	// vary request construction by streaming mode.
	Stream bool

	// ProviderRaw is the opaque continuation blob returned by a previous turn's
	// [Response.ProviderRaw], echoed back unmodified to continue or replay
	// statelessly. Nil on a fresh turn. See [ProviderRaw].
	ProviderRaw ProviderRaw

	// OutputSchema, when non-nil, requests structured output constrained to this
	// JSON Schema. The agent loop validates each response against it and retries up
	// to a configured cap, terminating the run with the
	// error_max_structured_output_retries subtype on exhaustion. Nil requests
	// free-form output. See [Capabilities.SupportsJSONSchemaStrict].
	OutputSchema json.RawMessage

	// Strict, meaningful only when OutputSchema is set, requests strict
	// provider-side schema enforcement where supported
	// ([Capabilities.SupportsJSONSchemaStrict]); when the provider cannot enforce
	// strictly, the loop falls back to validate-and-retry.
	Strict bool
}
