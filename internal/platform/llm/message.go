package llm

// Role identifies the author of a [Message] in a normalized conversation.
//
// The three roles are the provider-agnostic union across all four families. Each
// adapter maps a Role onto its wire representation (e.g. Anthropic merges tool
// results into a user turn; OpenAI Chat Completions uses a dedicated "tool" role).
type Role string

const (
	// RoleUser is a turn authored by the end user (or, after normalization, the
	// turn that carries tool results back to the model).
	RoleUser Role = "user"
	// RoleAssistant is a turn authored by the model, including text, thinking,
	// and tool-call content parts.
	RoleAssistant Role = "assistant"
	// RoleTool is a turn that carries the result of a tool execution. Some
	// providers model tool results as a distinct role; others fold them into a
	// user turn. Adapters normalize this difference.
	RoleTool Role = "tool"
)

// Message is one normalized turn in a conversation: a [Role] plus an ordered list
// of [ContentPart] values. The ordering of parts is significant and preserved
// across normalization (e.g. thinking before text, text before tool calls).
//
// A Message is the model-visible projection of a turn. For assistant turns that
// require provider-opaque continuation state (Anthropic server_tool_use blocks and
// thinking signatures, OpenAI Responses Items), the byte-faithful source of truth
// for the next provider call is carried separately in [Response.ProviderRaw] /
// [Request.ProviderRaw], not in the Message itself (ADR-0016; architecture §11.1).
type Message struct {
	// Role is the author of this turn.
	Role Role
	// Content is the ordered list of content parts that make up this turn.
	Content []ContentPart
}

// ContentPart is a discriminated union over the kinds of content a [Message] turn
// can carry. Exactly one field is non-nil; that field identifies the variant.
//
// Modeling content as a union (rather than a flat string) lets the normalized
// representation carry interleaved text, reasoning, images, tool calls, and tool
// results in a single turn, which every provider family requires in some form.
type ContentPart struct {
	// Text is set for a plain text fragment.
	Text *TextPart
	// Image is set for an image input (vision).
	Image *ImagePart
	// Thinking is set for a reasoning/extended-thinking fragment.
	Thinking *ThinkingPart
	// ToolCall is set for a model request to invoke a tool.
	ToolCall *ToolCall
	// ToolResult is set for the result of a previously requested tool call.
	ToolResult *ToolResult
}

// TextPart is a plain text content fragment.
type TextPart struct {
	// Text is the literal text.
	Text string
}

// ThinkingPart is a reasoning / extended-thinking content fragment.
//
// Signature carries any provider-opaque thinking signature that must be returned
// unmodified on a subsequent call (e.g. Anthropic extended-thinking signatures).
// The model-gateway adapter is responsible for populating and echoing it; the
// authoritative copy for replay rides in the provider-raw continuation slot
// (architecture §11.1). It is kept here so a single thinking part is
// self-describing when present.
type ThinkingPart struct {
	// Text is the human-readable reasoning text. It may be empty when the
	// provider omits or summarizes thinking content.
	Text string
	// Signature is the opaque, provider-scoped signature for this thinking
	// block, to be returned unmodified on continuation. Empty when the provider
	// does not use signed thinking.
	Signature string
}

// ImagePart is an image input, normalized across the providers' differing image
// block shapes (Anthropic image block, Gemini inlineData Blob, OpenAI image_url /
// input_image). Exactly one of Data, URL, or FileRef is set; MediaType always
// accompanies inline Data so the adapter can render the correct wire shape.
type ImagePart struct {
	// MediaType is the IANA media type of the image, e.g. "image/png" or
	// "image/jpeg". It is required when Data is set.
	MediaType string
	// Data is the raw (un-encoded) image bytes for an inline image. Adapters
	// base64-encode as the wire format requires.
	Data []byte
	// URL is a remote image URL, for providers and models that accept image
	// references by URL.
	URL string
	// FileRef is a provider-side file identifier for a previously uploaded
	// image.
	FileRef string
}
