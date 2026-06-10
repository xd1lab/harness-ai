package openai

import (
	"encoding/json"
	"errors"

	"github.com/openai/openai-go/v3/responses"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// assembleResponse converts a non-streaming Responses [responses.Response] into the
// normalized [llm.Response]: ordered content (text/thinking parts then tool calls),
// derived stop reason, normalized usage, and the stateless continuation blob. A
// failed response status yields a [*llm.ProviderError].
func assembleResponse(resp *responses.Response) (*llm.Response, error) {
	if resp == nil {
		return nil, &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New("openai: nil response")}
	}
	if resp.Status == responses.ResponseStatusFailed {
		msg := resp.Error.Message
		if msg == "" {
			msg = "openai: response failed"
		}
		return nil, &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New(msg)}
	}

	var content []llm.ContentPart
	for _, item := range resp.Output {
		switch item.Type {
		case itemTypeReasoning:
			if text := reasoningText(item); text != "" {
				content = append(content, llm.ContentPart{Thinking: &llm.ThinkingPart{Text: text}})
			}
		case itemTypeMessage:
			if text := outputMessageText(item); text != "" {
				content = append(content, llm.ContentPart{Text: &llm.TextPart{Text: text}})
			}
		case itemTypeFunctionCall:
			args, err := parseArgs(item.Arguments.OfString)
			if err != nil {
				return nil, newInvalidRequest(err)
			}
			content = append(content, llm.ContentPart{ToolCall: &llm.ToolCall{
				ID:   item.CallID,
				Name: item.Name,
				Args: args,
			}})
		}
	}

	out := &llm.Response{
		Content:       content,
		StopReason:    stopReasonFromResponse(*resp),
		RawStopReason: rawStopReason(*resp),
		Usage:         normalizeResponsesUsage(resp.Usage),
	}
	if raw := continuationFromResponse(*resp); raw != nil {
		out.ProviderRaw = raw
	}
	return out, nil
}

// reasoningText concatenates the summary text of a reasoning output item.
func reasoningText(item responses.ResponseOutputItemUnion) string {
	var b []byte
	for _, s := range item.Summary {
		if s.Text != "" {
			b = append(b, s.Text...)
		}
	}
	return string(b)
}

// parseArgs decodes a function-call argument JSON string into the parsed map
// [llm.ToolCall.Args] requires; an empty string decodes to an empty map.
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
