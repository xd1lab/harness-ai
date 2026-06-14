package llm

// Capabilities describes what a specific (endpoint, model) pair supports. Capability
// variability is per-MODEL within an endpoint — one Anthropic key serves models
// with different thinking/vision/context, one Gemini endpoint serves models with
// and without streamed tool args, one OpenAI Responses endpoint serves models with
// differing parallel-tool-call / strict-schema support — so [Provider.Capabilities]
// takes the model id and returns model-specific flags (a static table keyed by
// model, overridable per endpoint for self-hosted) (ADR-0004; ADR-0016;
// architecture §11.4).
//
// The orchestrator consults these flags to fail fast or degrade rather than
// sending a request the backend will reject.
type Capabilities struct {
	// SupportsTools reports whether the model can be given [ToolDef] tools and
	// emit [ToolCall]s.
	SupportsTools bool
	// SupportsParallelToolCalls reports whether the model may emit more than one
	// tool call in a single turn.
	SupportsParallelToolCalls bool
	// SupportsStreamingToolCalls reports whether tool-call arguments are streamed
	// incrementally. When false, the gateway buffers the whole call and emits it
	// complete rather than streaming [ToolCallDelta] fragments (architecture §11.2).
	SupportsStreamingToolCalls bool
	// SupportsVision reports whether the model accepts [ImagePart] inputs.
	SupportsVision bool
	// SupportsSystemPrompt reports whether the model accepts a [Request.System]
	// prompt as a first-class system instruction.
	SupportsSystemPrompt bool
	// SupportsThinking reports whether the model supports reasoning/extended
	// thinking ([ThinkingPart] / [ThinkingDelta]).
	SupportsThinking bool
	// SupportsTokenCounting reports whether [Provider.CountTokens] is backed by a
	// real count for this model (e.g. a provider count-tokens endpoint or a
	// local tokenizer). When false, CountTokens returns a [*ProviderError] with
	// kind [ErrUnsupported].
	SupportsTokenCounting bool
	// SupportsJSONSchemaStrict reports whether native structured output is
	// available and strict-enforceable for this (endpoint, model): i.e. the
	// provider exposes a JSON-Schema response mode — OpenAI Responses
	// text.format, Gemini response_schema, Anthropic Messages output_config.format
	// — that constrains the final response to the request's OutputSchema. When
	// false the gateway sends no native response_format and the orchestrator loop
	// falls back to validate-and-retry (the correctness backstop holds in both
	// cases). The central capability table is the single source of truth for this
	// gate; an adapter's own self-report is not authoritative.
	SupportsJSONSchemaStrict bool
	// MaxOutputTokens is the maximum number of tokens the model can generate in a
	// single response. A zero value means unknown/unspecified. Adapters may clamp
	// [Request.MaxTokens] to this value.
	MaxOutputTokens int
}
