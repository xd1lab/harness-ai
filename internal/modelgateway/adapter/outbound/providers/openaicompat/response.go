package openaicompat

import (
	"encoding/json"
	"errors"

	openai "github.com/openai/openai-go/v3"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// assembleResponse converts a non-streaming [openai.ChatCompletion] into the
// normalized [llm.Response]. It builds the ordered content (text then tool calls),
// maps the finish_reason, normalizes usage, and carries the same stateless
// continuation blob the streaming path produces. A response with no choices is a
// provider protocol violation and yields a [*llm.ProviderError].
func assembleResponse(c *openai.ChatCompletion) (*llm.Response, error) {
	if c == nil || len(c.Choices) == 0 {
		return nil, &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New("openaicompat: response had no choices")}
	}
	choice := c.Choices[0]
	msg := choice.Message

	var content []llm.ContentPart
	text := msg.Content
	if text == "" && msg.Refusal != "" {
		text = msg.Refusal
	}
	if text != "" {
		content = append(content, llm.ContentPart{Text: &llm.TextPart{Text: text}})
	}

	var contToolCalls []continuationToolCall
	for _, tc := range msg.ToolCalls {
		fn := tc.Function
		args, parseErr := parseArgs(fn.Arguments)
		if parseErr != nil {
			return nil, newInvalidRequest(parseErr)
		}
		content = append(content, llm.ContentPart{ToolCall: &llm.ToolCall{
			ID:   tc.ID,
			Name: fn.Name,
			Args: args,
		}})
		contToolCalls = append(contToolCalls, continuationToolCall{ID: tc.ID, Name: fn.Name, Args: fn.Arguments})
	}

	resp := &llm.Response{
		Content:       content,
		StopReason:    mapFinishReason(choice.FinishReason),
		RawStopReason: choice.FinishReason,
		Usage:         normalizeChatUsage(c.Usage),
	}
	if raw := buildContinuation(c.Model, choice.FinishReason, text, contToolCalls); raw != nil {
		resp.ProviderRaw = raw
	}
	return resp, nil
}

// parseArgs decodes a Chat Completions tool-call argument JSON string into the
// parsed map [llm.ToolCall.Args] requires. An empty string decodes to an empty map.
func parseArgs(s string) (map[string]any, error) {
	if s == "" {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

// buildContinuation constructs the stateless continuation blob shared with the
// streaming path. It returns nil when there is nothing to carry.
func buildContinuation(model, finishReason, text string, toolCalls []continuationToolCall) llm.ProviderRaw {
	if text == "" && len(toolCalls) == 0 {
		return nil
	}
	st := continuationState{
		Surface:      surfaceChat,
		Model:        model,
		FinishReason: finishReason,
		Text:         text,
		ToolCalls:    toolCalls,
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil
	}
	return b
}
