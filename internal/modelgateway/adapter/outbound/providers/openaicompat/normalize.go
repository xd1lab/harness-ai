package openaicompat

import (
	"encoding/json"

	openai "github.com/openai/openai-go/v3"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Normalizer converts a stream of OpenAI Chat Completions chunks
// ([openai.ChatCompletionChunk], object "chat.completion.chunk") into normalized
// [llm.StreamEvent] values. It is a pure, network-free accumulator: feed each chunk
// to [Normalizer.Next] in arrival order and call [Normalizer.Finish] once the SSE
// stream is exhausted. This makes the most defect-prone provider logic — delta and
// stop-reason normalization plus the JSON-string tool-argument concatenation —
// directly unit-testable with synthetic chunks and no HTTP.
//
// Text and refusal content are emitted live as [llm.TextDelta] events. Tool calls
// are buffered: Chat Completions streams a tool call as an integer-indexed sequence
// of fragments whose function.arguments is a concatenable JSON STRING (and whose id
// and name typically appear only on the first fragment), so the Normalizer
// accumulates fragments by index and emits a SINGLE complete [llm.ToolCallDelta]
// per call — CallID + Name + the assembled arguments in ArgsFragment with ArgsPath
// empty — just before the terminal [llm.Done]. This matches the contract that
// [llm.Done] never carries tool-call content and that buffered and streamed tool
// calls are assembled uniformly by the orchestrator (architecture §11.2).
//
// A Normalizer is single-use and not safe for concurrent use.
type Normalizer struct {
	// calls accumulates tool-call fragments keyed by the streaming integer index.
	calls map[int64]*toolCallAccumulator
	// order records the first-seen ordering of indices so buffered tool-call
	// deltas are emitted deterministically in the order the model produced them.
	order []int64

	// finishReason is the verbatim provider finish_reason captured from the
	// terminating choice, used to derive the normalized [llm.StopReason].
	finishReason string
	// usage holds the last non-empty usage block observed on the stream (Chat
	// Completions reports usage on a trailing chunk when stream_options include
	// usage). Nil until seen.
	usage *openai.CompletionUsage
	// model is the model id echoed by the provider, retained for the continuation
	// blob.
	model string
	// text accumulates assembled assistant text so the continuation blob can
	// reconstruct the turn for stateless replay; it is independent of the live
	// text deltas emitted from Next.
	text []byte
}

// toolCallAccumulator buffers the fragments of one streamed tool call.
type toolCallAccumulator struct {
	id   string
	name string
	args []byte
}

// NewNormalizer returns an empty Chat Completions [Normalizer] ready to receive
// chunks.
func NewNormalizer() *Normalizer {
	return &Normalizer{calls: make(map[int64]*toolCallAccumulator)}
}

// Next folds one Chat Completions chunk into the Normalizer's state and returns the
// [llm.StreamEvent] values it produces immediately — incremental text and thinking
// deltas. Tool-call fragments produce no events here; they are buffered and emitted
// by [Normalizer.Finish]. A chunk carrying a finish_reason or a usage block updates
// the terminal state consumed by Finish. Empty-choice chunks (e.g. the usage-only
// trailer) are handled without producing events.
func (n *Normalizer) Next(chunk openai.ChatCompletionChunk) []llm.StreamEvent {
	if chunk.Model != "" {
		n.model = chunk.Model
	}
	// A trailing usage-only chunk has no choices; capture usage when present.
	if chunk.Usage.TotalTokens != 0 || chunk.Usage.PromptTokens != 0 || chunk.Usage.CompletionTokens != 0 {
		u := chunk.Usage
		n.usage = &u
	}

	var out []llm.StreamEvent
	for _, choice := range chunk.Choices {
		if choice.FinishReason != "" {
			n.finishReason = choice.FinishReason
		}
		delta := choice.Delta
		if delta.Content != "" {
			n.text = append(n.text, delta.Content...)
			out = append(out, llm.StreamEvent{TextDelta: &llm.TextDelta{Text: delta.Content}})
		}
		// A refusal is surfaced as visible text so the assembled message is
		// non-empty; the normalized stop reason still reflects the provider's
		// finish_reason.
		if delta.Refusal != "" {
			out = append(out, llm.StreamEvent{TextDelta: &llm.TextDelta{Text: delta.Refusal}})
		}
		for _, tc := range delta.ToolCalls {
			n.accumulateToolCall(tc)
		}
	}
	return out
}

// accumulateToolCall merges one streamed tool-call fragment into the buffer keyed by
// its integer index, concatenating the JSON-string argument fragment and capturing
// the id and name when they appear (typically only on the first fragment).
func (n *Normalizer) accumulateToolCall(tc openai.ChatCompletionChunkChoiceDeltaToolCall) {
	acc, ok := n.calls[tc.Index]
	if !ok {
		acc = &toolCallAccumulator{}
		n.calls[tc.Index] = acc
		n.order = append(n.order, tc.Index)
	}
	if tc.ID != "" {
		acc.id = tc.ID
	}
	if tc.Function.Name != "" {
		acc.name = tc.Function.Name
	}
	if tc.Function.Arguments != "" {
		acc.args = append(acc.args, tc.Function.Arguments...)
	}
}

// Finish flushes the buffered tool calls as complete [llm.ToolCallDelta] events
// (one per call, in first-seen order) followed by the single terminal [llm.Done].
// It is called once after the last chunk has been passed to [Normalizer.Next]. The
// returned slice is the tail of the normalized event stream; the caller emits these
// after every event returned from Next.
//
// The assembled per-call argument bytes are emitted verbatim in
// [llm.ToolCallDelta.ArgsFragment]; if a call produced no argument bytes, an empty
// JSON object is substituted so downstream parsing always sees a valid object. The
// continuation blob in [llm.Done.ProviderRaw] carries the assembled tool calls and
// finish_reason so a turn can be reconstructed for stateless replay.
func (n *Normalizer) Finish() []llm.StreamEvent {
	out := make([]llm.StreamEvent, 0, len(n.order)+1)

	// n.order is first-seen index order, which is the emission contract: tool
	// calls are emitted in the order the model began producing them.
	for _, idx := range n.order {
		acc := n.calls[idx]
		args := acc.args
		if len(args) == 0 {
			args = []byte("{}")
		}
		out = append(out, llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID:       acc.id,
			Name:         acc.name,
			ArgsFragment: json.RawMessage(append([]byte(nil), args...)),
		}})
	}

	done := &llm.Done{
		StopReason:    mapFinishReason(n.finishReason),
		RawStopReason: n.finishReason,
	}
	if n.usage != nil {
		done.Usage = normalizeChatUsage(*n.usage)
	}
	if raw, err := n.continuationRaw(); err == nil && raw != nil {
		done.ProviderRaw = raw
	}
	out = append(out, llm.StreamEvent{Done: done})
	return out
}

// mapFinishReason maps a Chat Completions finish_reason onto the normalized open
// [llm.StopReason] set. Unknown reasons map to [llm.StopOther] with the raw value
// preserved by the caller in [llm.Done.RawStopReason] (architecture §11.3).
func mapFinishReason(reason string) llm.StopReason {
	switch reason {
	case "stop":
		return llm.StopEnd
	case "length":
		return llm.StopMaxTokens
	case "tool_calls", "function_call":
		return llm.StopToolUse
	case "content_filter":
		return llm.StopContentFilter
	case "":
		// No finish_reason was reported (e.g. an aborted stream); treat as a
		// normal end rather than inventing an error reason.
		return llm.StopEnd
	default:
		return llm.StopOther
	}
}

// normalizeChatUsage converts an OpenAI [openai.CompletionUsage] into the normalized
// [llm.Usage]. Cached prompt tokens are reported separately as cache reads and are
// excluded from InputTokens per the [llm.Usage] convention; reasoning tokens are a
// subset of output tokens (architecture §11.6).
func normalizeChatUsage(u openai.CompletionUsage) llm.Usage {
	cacheRead := int(u.PromptTokensDetails.CachedTokens)
	input := int(u.PromptTokens) - cacheRead
	if input < 0 {
		input = 0
	}
	return llm.Usage{
		InputTokens:     input,
		OutputTokens:    int(u.CompletionTokens),
		CacheReadTokens: cacheRead,
		ReasoningTokens: int(u.CompletionTokensDetails.ReasoningTokens),
	}
}
