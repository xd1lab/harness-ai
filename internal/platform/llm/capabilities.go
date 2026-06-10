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
	// SupportsJSONSchemaStrict reports whether the model enforces strict JSON
	// Schema validation of tool arguments / structured output.
	SupportsJSONSchemaStrict bool
	// MaxOutputTokens is the maximum number of tokens the model can generate in a
	// single response. A zero value means unknown/unspecified. Adapters may clamp
	// [Request.MaxTokens] to this value.
	MaxOutputTokens int
}
