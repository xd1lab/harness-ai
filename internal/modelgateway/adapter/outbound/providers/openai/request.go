package openai

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go/v3/packages/param"
	"github.com/openai/openai-go/v3/responses"
	"github.com/openai/openai-go/v3/shared"
	"github.com/openai/openai-go/v3/shared/constant"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// buildParams maps a normalized [llm.Request] onto OpenAI Responses request
// parameters. The system prompt is placed in `instructions`; tools are flat
// function tools; conversation messages map to input items, with prior assistant
// tool calls sent as typed `function_call` items and tool results as
// `function_call_output` items. Per ADR-0016 the request is STATELESS: store is set
// to false and any continuation Items from a prior turn's [llm.Request.ProviderRaw]
// are replayed as leading input items rather than relying on previous_response_id.
//
// It returns a [*llm.ProviderError] of kind [llm.ErrInvalidRequest] when a tool
// schema, tool-call arguments, or a continuation blob cannot be decoded.
func buildParams(req llm.Request) (responses.ResponseNewParams, error) {
	params := responses.ResponseNewParams{
		Model: shared.ResponsesModel(req.Model),
		// Stateless Item-passing: never persist server-side conversation state.
		Store: param.NewOpt(false),
	}
	if req.System != "" {
		params.Instructions = param.NewOpt(req.System)
	}
	if req.MaxTokens > 0 {
		params.MaxOutputTokens = param.NewOpt(int64(req.MaxTokens))
	}
	if req.Temperature != nil {
		params.Temperature = param.NewOpt(*req.Temperature)
	}

	items := make([]responses.ResponseInputItemUnionParam, 0, len(req.Messages)+1)

	// Replay prior-turn continuation Items first (stateless continuation, §11.1).
	contItems, err := continuationInputItems(req.ProviderRaw)
	if err != nil {
		return responses.ResponseNewParams{}, err
	}
	items = append(items, contItems...)

	for _, m := range req.Messages {
		converted, err := convertMessage(m)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		items = append(items, converted...)
	}
	params.Input = responses.ResponseNewParamsInputUnion{OfInputItemList: items}

	if len(req.Tools) > 0 {
		tools, err := convertTools(req.Tools)
		if err != nil {
			return responses.ResponseNewParams{}, err
		}
		params.Tools = tools
	}
	if tc, ok := convertToolChoice(req.ToolChoice); ok {
		params.ToolChoice = tc
	}

	return params, nil
}

// convertMessage translates one normalized [llm.Message] into Responses input
// items. Assistant text becomes a message item; assistant tool calls become typed
// function_call items; tool results become function_call_output items. A single
// message may yield several items (e.g. text plus multiple tool calls).
func convertMessage(m llm.Message) ([]responses.ResponseInputItemUnionParam, error) {
	switch m.Role {
	case llm.RoleUser:
		return []responses.ResponseInputItemUnionParam{
			responses.ResponseInputItemParamOfMessage(joinText(m.Content), responses.EasyInputMessageRoleUser),
		}, nil

	case llm.RoleAssistant:
		var out []responses.ResponseInputItemUnionParam
		if text := joinText(m.Content); text != "" {
			out = append(out, responses.ResponseInputItemParamOfMessage(text, responses.EasyInputMessageRoleAssistant))
		}
		for _, part := range m.Content {
			if part.ToolCall == nil {
				continue
			}
			args, err := marshalArgs(part.ToolCall.Args)
			if err != nil {
				return nil, newInvalidRequest(fmt.Errorf("marshal tool-call args for %q: %w", part.ToolCall.Name, err))
			}
			out = append(out, responses.ResponseInputItemParamOfFunctionCall(string(args), part.ToolCall.ID, part.ToolCall.Name))
		}
		return out, nil

	case llm.RoleTool:
		var out []responses.ResponseInputItemUnionParam
		for _, part := range m.Content {
			if part.ToolResult == nil {
				continue
			}
			out = append(out, responses.ResponseInputItemParamOfFunctionCallOutput(part.ToolResult.CallID, part.ToolResult.Content))
		}
		return out, nil

	default:
		return nil, newInvalidRequest(fmt.Errorf("unsupported message role %q", m.Role))
	}
}

// continuationInputItems decodes a prior-turn continuation blob and rebuilds the
// Responses input items to replay (assistant messages, reasoning items, and
// function_call items). A blob from another surface or an empty blob yields no
// items.
func continuationInputItems(raw llm.ProviderRaw) ([]responses.ResponseInputItemUnionParam, error) {
	st, ok, err := decodeContinuation(raw)
	if err != nil {
		return nil, newInvalidRequest(fmt.Errorf("decode continuation blob: %w", err))
	}
	if !ok {
		return nil, nil
	}
	var out []responses.ResponseInputItemUnionParam
	for _, item := range st.Items {
		switch item.Type {
		case itemTypeMessage:
			if item.Text == "" {
				continue
			}
			out = append(out, responses.ResponseInputItemParamOfMessage(item.Text, responses.EasyInputMessageRoleAssistant))
		case itemTypeFunctionCall:
			args := item.Arguments
			if args == "" {
				args = "{}"
			}
			out = append(out, responses.ResponseInputItemParamOfFunctionCall(args, item.CallID, item.Name))
		case itemTypeReasoning:
			if item.ID == "" {
				continue
			}
			out = append(out, responses.ResponseInputItemParamOfReasoning(item.ID, nil))
		}
	}
	return out, nil
}

// convertTools maps each normalized [llm.ToolDef] to a flat Responses function
// tool, decoding the raw JSON Schema into the SDK parameters map.
func convertTools(defs []llm.ToolDef) ([]responses.ToolUnionParam, error) {
	out := make([]responses.ToolUnionParam, 0, len(defs))
	for _, d := range defs {
		schema, err := decodeSchema(d.JSONSchema)
		if err != nil {
			return nil, newInvalidRequest(fmt.Errorf("decode JSON schema for tool %q: %w", d.Name, err))
		}
		fn := responses.FunctionToolParam{
			Name:       d.Name,
			Parameters: schema,
			// Do not force strict schema enforcement; the loop validates and
			// retries (architecture §11.3). Strict defaults true in the SDK, so
			// set it false explicitly.
			Strict: param.NewOpt(false),
			Type:   constant.ValueOf[constant.Function](),
		}
		if d.Description != "" {
			fn.Description = param.NewOpt(d.Description)
		}
		out = append(out, responses.ToolUnionParam{OfFunction: &fn})
	}
	return out, nil
}

// convertToolChoice maps the normalized [llm.ToolChoice] onto the Responses
// tool_choice union. The boolean result is false when the choice is unset, leaving
// the provider default. A non-sentinel value names a specific function the model
// must call.
func convertToolChoice(choice llm.ToolChoice) (responses.ResponseNewParamsToolChoiceUnion, bool) {
	switch choice {
	case "":
		return responses.ResponseNewParamsToolChoiceUnion{}, false
	case llm.ToolChoiceAuto:
		return responses.ResponseNewParamsToolChoiceUnion{OfToolChoiceMode: param.NewOpt(responses.ToolChoiceOptionsAuto)}, true
	case llm.ToolChoiceNone:
		return responses.ResponseNewParamsToolChoiceUnion{OfToolChoiceMode: param.NewOpt(responses.ToolChoiceOptionsNone)}, true
	case llm.ToolChoiceAny, llm.ToolChoiceRequired:
		return responses.ResponseNewParamsToolChoiceUnion{OfToolChoiceMode: param.NewOpt(responses.ToolChoiceOptionsRequired)}, true
	default:
		return responses.ResponseNewParamsToolChoiceUnion{
			OfFunctionTool: &responses.ToolChoiceFunctionParam{Name: string(choice)},
		}, true
	}
}

// joinText concatenates the text parts of a message in order.
func joinText(parts []llm.ContentPart) string {
	var b []byte
	for _, p := range parts {
		if p.Text != nil {
			b = append(b, p.Text.Text...)
		}
	}
	return string(b)
}

// marshalArgs serializes a parsed tool-argument map to a JSON string; a nil map
// becomes an empty object.
func marshalArgs(args map[string]any) ([]byte, error) {
	if args == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(args)
}

// decodeSchema decodes a raw JSON Schema document into a map for the SDK; a nil or
// empty schema becomes an empty object.
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
