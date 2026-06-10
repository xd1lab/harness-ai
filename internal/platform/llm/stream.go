package llm

import "encoding/json"

// StreamEvent is one normalized event in a streamed generation. It is a
// discriminated union: exactly one field is non-nil, identifying the variant.
//
// ALL provider stream normalization is the model-gateway's job. The gateway runs
// the provider-specific stream normalizers (Anthropic SSE, OpenAI Chat-Completions
// SSE, OpenAI Responses typed events, the Gemini iterator) and emits this single
// event shape, so the orchestrator relay stays provider-agnostic — there is no
// provider-specific delta accumulation or stop-reason branching in the loop
// (ADR-0016; architecture §4.3, §11.2).
type StreamEvent struct {
	// TextDelta is set for an incremental chunk of assistant text.
	TextDelta *TextDelta
	// ThinkingDelta is set for an incremental chunk of reasoning/thinking text.
	ThinkingDelta *ThinkingDelta
	// ToolCallDelta is set for an incremental fragment of a tool call.
	ToolCallDelta *ToolCallDelta
	// Done is set for the single terminal event of a stream.
	Done *Done
}

// TextDelta is an incremental chunk of assistant text to append to the visible
// response.
type TextDelta struct {
	// Text is the text fragment.
	Text string
}

// ThinkingDelta is an incremental chunk of reasoning/thinking text. Its content
// may be empty when the provider streams thinking blocks without surfacing their
// text.
type ThinkingDelta struct {
	// Text is the thinking-text fragment.
	Text string
	// Signature is an opaque thinking-signature fragment, when the provider
	// emits one. The gateway accumulates and carries the final signature in the
	// provider-raw continuation slot; it is exposed here only for completeness.
	Signature string
}

// ToolCallDelta is an incremental fragment of a single tool call, identified by an
// OPAQUE per-call identifier rather than an integer index.
//
// Providers encode streamed tool-call arguments differently — OpenAI Chat
// Completions sends a concatenable JSON-string fragment, Gemini sends
// path-addressed (jsonPath) partialArgs fragments, and OpenAI Responses sends
// item-scoped deltas. Modeling the fragment with an opaque CallID plus an optional
// ArgsPath and a raw ArgsFragment lets the gateway normalize all of these without
// the brittle "accumulate by index" assumption that was deleted from the
// orchestrator (architecture §11.2).
//
// When a (endpoint, model) lacks streaming tool-call support
// ([Capabilities.SupportsStreamingToolCalls] is false), the gateway buffers the
// whole call internally and emits it as a SINGLE complete ToolCallDelta — CallID +
// Name + the full arguments in ArgsFragment with ArgsPath empty — before [Done].
// [Done] never carries tool-call content, so the orchestrator assembles tool calls
// from ToolCallDelta events uniformly, whether streamed or buffered.
type ToolCallDelta struct {
	// CallID is the opaque, provider-scoped identifier of the tool call this
	// fragment belongs to. Fragments sharing a CallID belong to the same call.
	CallID string
	// Name is the tool name, when known for this call. It may be empty on
	// fragments that carry only argument data.
	Name string
	// ArgsPath is the address of this fragment within the arguments object for
	// path-addressed encodings (e.g. Gemini jsonPath). Empty for whole-object or
	// append-style encodings.
	ArgsPath string
	// ArgsFragment is the raw argument fragment for this delta, as provided by
	// the normalizer. Its interpretation (append vs. set-at-path) is determined
	// by ArgsPath; the gateway assembles fragments into the final parsed
	// [ToolCall.Args]. It is opaque to the orchestrator.
	ArgsFragment json.RawMessage
}

// Done is the single terminal event of a stream. It mirrors the terminal fields of
// a non-streaming [Response] so the orchestrator handles streamed and unary
// outcomes uniformly.
//
// Note that Done is emitted for terminal outcomes; a [Pause] continuation is
// likewise reported via StopReason here (with the continuation state in
// ProviderRaw), so the loop can distinguish needs-continuation from final using
// [StopReason.IsTerminal].
type Done struct {
	// StopReason is the normalized reason generation stopped (see [StopReason]).
	StopReason StopReason
	// RawStopReason is the verbatim provider stop-reason string, authoritative
	// when StopReason is [StopOther].
	RawStopReason string
	// Usage is the normalized token usage for the turn, read from the provider's
	// authoritative usage field (architecture §11.6).
	Usage Usage
	// ProviderRaw is the opaque, provider-scoped continuation blob for the turn,
	// echoed back via [Request.ProviderRaw] to continue (notably on [Pause]) or
	// to replay byte-faithfully. Nil when no continuation state is needed.
	ProviderRaw ProviderRaw
}

// StreamReader is a provider-agnostic reader over a streamed generation. The
// model-gateway returns one from [Provider.Stream]; the orchestrator relay only
// calls Recv in a loop and Close when done, mapping [StreamEvent] values through
// without provider-specific handling (architecture §4.3).
type StreamReader interface {
	// Recv returns the next [StreamEvent]. It returns [io.EOF] when the stream is
	// exhausted after the terminal [Done] event, or a non-nil error (such as a
	// [*ProviderError]) on failure. Recv is not safe for concurrent use.
	Recv() (StreamEvent, error)
	// Close releases the stream's resources. It is safe to call after Recv has
	// returned an error or [io.EOF], and may be called to abandon a stream early
	// (e.g. on a cancelled context).
	Close() error
}
