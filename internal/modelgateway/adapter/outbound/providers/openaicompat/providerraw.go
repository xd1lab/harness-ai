package openaicompat

import (
	"encoding/json"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// continuationState is the openaicompat-scoped continuation blob carried in
// [llm.Done.ProviderRaw] / [llm.Response.ProviderRaw]. Chat Completions has no
// server-side conversation handle, so continuation is fully stateless: the gateway
// reconstructs the prior assistant turn from this blob and replays it as an
// assistant message (plus tool messages for results) on the next request. It is
// opaque to the orchestrator and meaningful only to this adapter.
//
// It is deliberately a small, self-describing JSON document rather than a raw SDK
// type so that it remains stable across SDK upgrades and is byte-faithfully
// round-trippable for deterministic replay.
type continuationState struct {
	// Surface identifies the producing adapter surface; it guards against feeding
	// a blob from a different provider back into this one.
	Surface string `json:"surface"`
	// Model is the model id the turn ran against.
	Model string `json:"model,omitempty"`
	// FinishReason is the verbatim provider finish_reason for the turn.
	FinishReason string `json:"finish_reason,omitempty"`
	// Text is the assembled assistant text for the turn.
	Text string `json:"text,omitempty"`
	// ToolCalls are the assembled tool calls the assistant emitted, in order.
	ToolCalls []continuationToolCall `json:"tool_calls,omitempty"`
}

// continuationToolCall is one assembled tool call within a [continuationState].
type continuationToolCall struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Args string `json:"args"`
}

// surfaceChat is the [continuationState.Surface] tag for the Chat Completions
// surface shared by openaicompat and the native OpenAI Chat-Completions sub-flag.
const surfaceChat = "openai.chat"

// continuationRaw builds the continuation blob for the assembled turn from the
// Normalizer's buffered state. It returns nil when there is nothing worth carrying
// (no text and no tool calls), so [llm.Done.ProviderRaw] stays nil in that case.
func (n *Normalizer) continuationRaw() (llm.ProviderRaw, error) {
	st := continuationState{
		Surface:      surfaceChat,
		Model:        n.model,
		FinishReason: n.finishReason,
		Text:         string(n.text),
	}
	for _, idx := range n.order {
		acc := n.calls[idx]
		st.ToolCalls = append(st.ToolCalls, continuationToolCall{
			ID:   acc.id,
			Name: acc.name,
			Args: string(acc.args),
		})
	}
	if st.Text == "" && len(st.ToolCalls) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil, err
	}
	return b, nil
}
