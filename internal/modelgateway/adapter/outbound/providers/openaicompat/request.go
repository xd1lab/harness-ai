package openaicompat

import (
	"encoding/json"
	"fmt"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/shared"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// buildParams maps a normalized [llm.Request] onto OpenAI Chat Completions request
// parameters. The system prompt is placed as a leading system-role message,
// conversation messages are translated turn by turn, and tools are wrapped as
// function tools (architecture §11.5). It returns a [*llm.ProviderError] of kind
// [llm.ErrInvalidRequest] if a tool schema cannot be decoded.
func buildParams(req llm.Request) (openai.ChatCompletionNewParams, error) {
	params := openai.ChatCompletionNewParams{
		Model: shared.ChatModel(req.Model),
	}

	msgs := make([]openai.ChatCompletionMessageParamUnion, 0, len(req.Messages)+1)
	if req.System != "" {
		msgs = append(msgs, openai.SystemMessage(req.System))
	}
	for _, m := range req.Messages {
		converted, err := convertMessage(m)
		if err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
		msgs = append(msgs, converted...)
	}
	params.Messages = msgs

	if len(req.Tools) > 0 {
		tools, err := convertTools(req.Tools)
		if err != nil {
			return openai.ChatCompletionNewParams{}, err
		}
		params.Tools = tools
	}

	if tc, ok := convertToolChoice(req.ToolChoice); ok {
		params.ToolChoice = tc
	}

	if req.MaxTokens > 0 {
		// Chat Completions deprecated max_tokens in favor of
		// max_completion_tokens for newer models; set the modern field so reasoning
		// models account output correctly. Self-hosted servers accept it as a plain
		// generation cap.
		params.MaxCompletionTokens = param.NewOpt(int64(req.MaxTokens))
	}
	if req.Temperature != nil {
		params.Temperature = param.NewOpt(*req.Temperature)
	}

	return params, nil
}

// convertMessage translates one normalized [llm.Message] into one or more Chat
// Completions message params. A single assistant turn maps to one assistant
// message (text + tool_calls); a tool turn maps to one tool message per result
// (Chat Completions models each tool result as a distinct tool-role message keyed
// by tool_call_id).
func convertMessage(m llm.Message) ([]openai.ChatCompletionMessageParamUnion, error) {
	switch m.Role {
	case llm.RoleUser:
		return []openai.ChatCompletionMessageParamUnion{openai.UserMessage(joinText(m.Content))}, nil

	case llm.RoleAssistant:
		assistant := openai.ChatCompletionAssistantMessageParam{}
		if text := joinText(m.Content); text != "" {
			assistant.Content.OfString = param.NewOpt(text)
		}
		var toolCalls []openai.ChatCompletionMessageToolCallUnionParam
		for _, part := range m.Content {
			if part.ToolCall == nil {
				continue
			}
			args, err := marshalArgs(part.ToolCall.Args)
			if err != nil {
				return nil, newInvalidRequest(fmt.Errorf("marshal tool-call args for %q: %w", part.ToolCall.Name, err))
			}
			toolCalls = append(toolCalls, openai.ChatCompletionMessageToolCallUnionParam{
				OfFunction: &openai.ChatCompletionMessageFunctionToolCallParam{
					ID: part.ToolCall.ID,
					Function: openai.ChatCompletionMessageFunctionToolCallFunctionParam{
						Name:      part.ToolCall.Name,
						Arguments: string(args),
					},
				},
			})
		}
		assistant.ToolCalls = toolCalls
		return []openai.ChatCompletionMessageParamUnion{{OfAssistant: &assistant}}, nil

	case llm.RoleTool:
		var out []openai.ChatCompletionMessageParamUnion
		for _, part := range m.Content {
			if part.ToolResult == nil {
				continue
			}
			out = append(out, openai.ToolMessage(part.ToolResult.Content, part.ToolResult.CallID))
		}
		return out, nil

	default:
		return nil, newInvalidRequest(fmt.Errorf("unsupported message role %q", m.Role))
	}
}

// convertTools wraps each normalized [llm.ToolDef] as a Chat Completions function
// tool, decoding the raw JSON Schema into the SDK's parameters map.
func convertTools(defs []llm.ToolDef) ([]openai.ChatCompletionToolUnionParam, error) {
	out := make([]openai.ChatCompletionToolUnionParam, 0, len(defs))
	for _, d := range defs {
		params, err := decodeSchema(d.JSONSchema)
		if err != nil {
			return nil, newInvalidRequest(fmt.Errorf("decode JSON schema for tool %q: %w", d.Name, err))
		}
		fn := shared.FunctionDefinitionParam{
			Name:       d.Name,
			Parameters: shared.FunctionParameters(params),
		}
		if d.Description != "" {
			fn.Description = param.NewOpt(d.Description)
		}
		out = append(out, openai.ChatCompletionFunctionTool(fn))
	}
	return out, nil
}

// convertToolChoice maps the normalized [llm.ToolChoice] onto the Chat Completions
// tool_choice union. The boolean result is false when the choice is unset, leaving
// the provider default. A non-sentinel value is treated as a specific function name
// the model must call.
func convertToolChoice(choice llm.ToolChoice) (openai.ChatCompletionToolChoiceOptionUnionParam, bool) {
	switch choice {
	case "":
		return openai.ChatCompletionToolChoiceOptionUnionParam{}, false
	case llm.ToolChoiceAuto:
		return openai.ChatCompletionToolChoiceOptionUnionParam{OfAuto: param.NewOpt("auto")}, true
	case llm.ToolChoiceNone:
		return openai.ChatCompletionToolChoiceOptionUnionParam{OfAuto: param.NewOpt("none")}, true
	case llm.ToolChoiceAny, llm.ToolChoiceRequired:
		return openai.ChatCompletionToolChoiceOptionUnionParam{OfAuto: param.NewOpt("required")}, true
	default:
		return openai.ToolChoiceOptionFunctionToolChoice(
			openai.ChatCompletionNamedToolChoiceFunctionParam{Name: string(choice)},
		), true
	}
}

// joinText concatenates the text parts of a message in order. Non-text parts are
// ignored here; tool calls and results are handled by the caller.
func joinText(parts []llm.ContentPart) string {
	var b []byte
	for _, p := range parts {
		if p.Text != nil {
			b = append(b, p.Text.Text...)
		}
	}
	return string(b)
}

// marshalArgs serializes a parsed tool-argument map to the JSON string Chat
// Completions expects. A nil map marshals to an empty object.
func marshalArgs(args map[string]any) ([]byte, error) {
	if args == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(args)
}

// decodeSchema decodes a raw JSON Schema document into a map for the SDK. A nil or
// empty schema decodes to an empty object (a function with no declared parameters).
func decodeSchema(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}
