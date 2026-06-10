package llm

import "encoding/json"

// ToolDef is a normalized declaration of a tool the model may call. All four
// provider families accept JSON-Schema-shaped parameters; only the wrapper around
// the schema differs per provider, so the schema is carried verbatim as raw JSON
// and the adapter supplies the per-provider envelope.
type ToolDef struct {
	// Name is the unique tool name the model uses to invoke the tool.
	Name string
	// Description tells the model what the tool does and, ideally, when to call
	// it. Adapters pass it through unchanged.
	Description string
	// JSONSchema is the tool's input schema as raw JSON Schema (a JSON object).
	// It is kept opaque here so the gateway can forward it without re-encoding;
	// strict-schema enforcement is gated by
	// [Capabilities.SupportsJSONSchemaStrict].
	JSONSchema json.RawMessage
}

// ToolChoice constrains whether and which tool the model may call on a [Request].
//
// The four sentinel values ([ToolChoiceAuto], [ToolChoiceAny], [ToolChoiceNone],
// [ToolChoiceRequired]) cover the provider-agnostic policies; any other value is
// interpreted as the name of a specific tool the model must call. An empty
// ToolChoice means "unset" and lets the adapter apply the provider default
// (equivalent to [ToolChoiceAuto]).
type ToolChoice string

const (
	// ToolChoiceAuto lets the model decide whether to call a tool. This is the
	// provider default when ToolChoice is unset.
	ToolChoiceAuto ToolChoice = "auto"
	// ToolChoiceAny requires the model to call at least one of the provided
	// tools.
	ToolChoiceAny ToolChoice = "any"
	// ToolChoiceRequired is a synonym for [ToolChoiceAny] used by some provider
	// vocabularies; adapters treat it as "must call some tool".
	ToolChoiceRequired ToolChoice = "required"
	// ToolChoiceNone forbids tool calls for this request.
	ToolChoiceNone ToolChoice = "none"
)

// ToolCall is a model request to invoke a tool. It appears as the
// [ContentPart.ToolCall] variant on an assistant [Message].
//
// ID is an opaque, provider-scoped identifier for this specific call; it is
// matched by [ToolResult.CallID] when the result is fed back. Args is the parsed
// argument object: the OpenAI Chat Completions adapter parses the JSON-string
// function.arguments into this map, while Anthropic and Gemini already deliver an
// object — so the orchestrator always sees parsed arguments regardless of provider
// (ADR-0016).
type ToolCall struct {
	// ID is the opaque per-call identifier assigned by the provider/adapter.
	ID string
	// Name is the name of the tool to invoke; it matches a [ToolDef.Name].
	Name string
	// Args is the parsed tool arguments as a JSON object decoded into a map.
	Args map[string]any
}

// ToolResult is the outcome of executing a [ToolCall], fed back to the model. It
// appears as the [ContentPart.ToolResult] variant.
type ToolResult struct {
	// CallID matches the [ToolCall.ID] this result answers.
	CallID string
	// Content is the textual result of the tool execution, as the model should
	// see it.
	Content string
	// IsError reports whether the tool execution failed; when true, the model is
	// told the call errored so it can adapt.
	IsError bool
}
