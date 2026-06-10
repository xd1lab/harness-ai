package anthropic

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/param"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// base64Encode standard-base64-encodes raw image bytes for the Anthropic
// base64 image source.
func base64Encode(b []byte) string { return base64.StdEncoding.EncodeToString(b) }

// buildMessageParams maps a normalized [llm.Request] onto the Anthropic Messages
// API [sdk.MessageNewParams]. System is placed in the top-level system parameter,
// tools are mapped to {name, description, input_schema}, and ToolChoice is
// translated to the provider's union. MaxTokens, when zero, is left to a
// caller-supplied default in defaultMaxTokens so the SDK's required field is
// always populated (the API rejects max_tokens <= 0).
func buildMessageParams(req llm.Request, defaultMaxTokens int64) (sdk.MessageNewParams, error) {
	msgs, err := buildMessages(req.Messages)
	if err != nil {
		return sdk.MessageNewParams{}, err
	}

	params := sdk.MessageNewParams{
		Model:    sdk.Model(req.Model),
		Messages: msgs,
	}

	maxTokens := int64(req.MaxTokens)
	if maxTokens <= 0 {
		maxTokens = defaultMaxTokens
	}
	params.MaxTokens = maxTokens

	if req.System != "" {
		params.System = []sdk.TextBlockParam{{Text: req.System}}
	}

	if len(req.Tools) > 0 {
		tps, terr := buildToolParams(req.Tools)
		if terr != nil {
			return sdk.MessageNewParams{}, terr
		}
		tools := make([]sdk.ToolUnionParam, len(tps))
		for i := range tps {
			tp := tps[i]
			tools[i] = sdk.ToolUnionParam{OfTool: &tp}
		}
		params.Tools = tools
	}

	if tc, ok := buildToolChoice(req.ToolChoice); ok {
		params.ToolChoice = tc
	}

	if req.Temperature != nil {
		params.Temperature = param.NewOpt(*req.Temperature)
	}

	return params, nil
}

// buildCountTokensParams maps a normalized [llm.Request] onto
// [sdk.MessageCountTokensParams] for the count_tokens endpoint. It mirrors
// buildMessageParams minus the generation-only fields (max_tokens, temperature).
func buildCountTokensParams(req llm.Request) (sdk.MessageCountTokensParams, error) {
	msgs, err := buildMessages(req.Messages)
	if err != nil {
		return sdk.MessageCountTokensParams{}, err
	}
	params := sdk.MessageCountTokensParams{
		Model:    sdk.Model(req.Model),
		Messages: msgs,
	}
	if req.System != "" {
		params.System = sdk.MessageCountTokensParamsSystemUnion{
			OfTextBlockArray: []sdk.TextBlockParam{{Text: req.System}},
		}
	}
	if len(req.Tools) > 0 {
		tps, terr := buildToolParams(req.Tools)
		if terr != nil {
			return sdk.MessageCountTokensParams{}, terr
		}
		tools := make([]sdk.MessageCountTokensToolUnionParam, len(tps))
		for i := range tps {
			tp := tps[i]
			tools[i] = sdk.MessageCountTokensToolUnionParam{OfTool: &tp}
		}
		params.Tools = tools
	}
	return params, nil
}

// buildMessages maps the normalized conversation onto Anthropic message params.
// RoleTool turns and RoleUser turns both produce a user message (Anthropic folds
// tool results into a user turn); RoleAssistant turns produce an assistant
// message. Content parts are mapped 1:1: text, thinking (with signature), tool
// calls, tool results, and images.
func buildMessages(in []llm.Message) ([]sdk.MessageParam, error) {
	out := make([]sdk.MessageParam, 0, len(in))
	for i := range in {
		m := in[i]
		blocks, err := buildContentBlocks(m.Content)
		if err != nil {
			return nil, fmt.Errorf("anthropic: message %d: %w", i, err)
		}
		switch m.Role {
		case llm.RoleAssistant:
			out = append(out, sdk.NewAssistantMessage(blocks...))
		case llm.RoleUser, llm.RoleTool:
			out = append(out, sdk.NewUserMessage(blocks...))
		default:
			return nil, fmt.Errorf("anthropic: message %d: unknown role %q", i, m.Role)
		}
	}
	return out, nil
}

// buildContentBlocks maps normalized content parts onto Anthropic content-block
// params.
func buildContentBlocks(parts []llm.ContentPart) ([]sdk.ContentBlockParamUnion, error) {
	blocks := make([]sdk.ContentBlockParamUnion, 0, len(parts))
	for i := range parts {
		p := parts[i]
		switch {
		case p.Text != nil:
			blocks = append(blocks, sdk.NewTextBlock(p.Text.Text))
		case p.Thinking != nil:
			blocks = append(blocks, sdk.NewThinkingBlock(p.Thinking.Signature, p.Thinking.Text))
		case p.ToolCall != nil:
			input := anyForToolCall(p.ToolCall.Args)
			blocks = append(blocks, sdk.NewToolUseBlock(p.ToolCall.ID, input, p.ToolCall.Name))
		case p.ToolResult != nil:
			blocks = append(blocks, sdk.NewToolResultBlock(p.ToolResult.CallID, p.ToolResult.Content, p.ToolResult.IsError))
		case p.Image != nil:
			blk, err := buildImageBlock(p.Image)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, blk)
		default:
			return nil, fmt.Errorf("anthropic: content part %d has no variant set", i)
		}
	}
	return blocks, nil
}

// anyForToolCall returns the tool-call arguments in a form the SDK marshals
// correctly. A nil map marshals to an empty object.
func anyForToolCall(args map[string]any) any {
	if args == nil {
		return map[string]any{}
	}
	return args
}

// buildImageBlock maps a normalized [llm.ImagePart] onto an Anthropic image
// block. Exactly one of Data, URL, or FileRef is expected.
func buildImageBlock(img *llm.ImagePart) (sdk.ContentBlockParamUnion, error) {
	switch {
	case len(img.Data) > 0:
		enc := base64Encode(img.Data)
		return sdk.NewImageBlockBase64(img.MediaType, enc), nil
	case img.URL != "":
		return sdk.NewImageBlock(sdk.URLImageSourceParam{URL: img.URL}), nil
	default:
		return sdk.ContentBlockParamUnion{}, fmt.Errorf("anthropic: image part has neither inline data nor URL (file refs unsupported)")
	}
}

// buildToolParams maps normalized tool definitions onto Anthropic custom-tool
// params, projecting each tool's raw JSON Schema into the {name, description,
// input_schema} envelope. The schema's properties / required keys are lifted into
// the typed fields and any remaining schema keywords (e.g. additionalProperties,
// $defs) are preserved via ExtraFields so strict-schema enforcement is not lost.
// The result is wrapped into the per-endpoint tool union by each caller (the
// Messages and CountTokens APIs use distinct union types that both embed
// [sdk.ToolParam]).
func buildToolParams(in []llm.ToolDef) ([]sdk.ToolParam, error) {
	out := make([]sdk.ToolParam, 0, len(in))
	for i := range in {
		td := in[i]
		schema, err := buildInputSchema(td.JSONSchema)
		if err != nil {
			return nil, fmt.Errorf("anthropic: tool %q: %w", td.Name, err)
		}
		tool := sdk.ToolParam{
			Name:        td.Name,
			InputSchema: schema,
		}
		if td.Description != "" {
			tool.Description = param.NewOpt(td.Description)
		}
		out = append(out, tool)
	}
	return out, nil
}

// buildInputSchema converts a raw JSON Schema object into a
// [sdk.ToolInputSchemaParam]. An empty schema yields a bare object schema. The
// "type", "properties", and "required" keywords map to typed fields; all other
// keywords are carried through ExtraFields.
func buildInputSchema(raw json.RawMessage) (sdk.ToolInputSchemaParam, error) {
	schema := sdk.ToolInputSchemaParam{}
	if len(raw) == 0 {
		return schema, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return schema, fmt.Errorf("invalid JSON schema: %w", err)
	}

	if props, ok := m["properties"]; ok {
		var p any
		if err := json.Unmarshal(props, &p); err != nil {
			return schema, fmt.Errorf("invalid schema properties: %w", err)
		}
		schema.Properties = p
		delete(m, "properties")
	}
	if reqRaw, ok := m["required"]; ok {
		var r []string
		if err := json.Unmarshal(reqRaw, &r); err != nil {
			return schema, fmt.Errorf("invalid schema required: %w", err)
		}
		schema.Required = r
		delete(m, "required")
	}
	// "type" is fixed to object by the SDK; drop a redundant declaration.
	delete(m, "type")

	if len(m) > 0 {
		extra := make(map[string]any, len(m))
		for k, v := range m {
			var val any
			if err := json.Unmarshal(v, &val); err != nil {
				return schema, fmt.Errorf("invalid schema keyword %q: %w", k, err)
			}
			extra[k] = val
		}
		schema.ExtraFields = extra
	}
	return schema, nil
}

// buildToolChoice translates the normalized [llm.ToolChoice] into the Anthropic
// tool_choice union. The second result is false when ToolChoice is unset, leaving
// the provider default. A value that is not one of the four sentinels is treated
// as a specific tool name the model must call.
func buildToolChoice(tc llm.ToolChoice) (sdk.ToolChoiceUnionParam, bool) {
	switch tc {
	case "":
		return sdk.ToolChoiceUnionParam{}, false
	case llm.ToolChoiceAuto:
		return sdk.ToolChoiceUnionParam{OfAuto: &sdk.ToolChoiceAutoParam{}}, true
	case llm.ToolChoiceAny, llm.ToolChoiceRequired:
		return sdk.ToolChoiceUnionParam{OfAny: &sdk.ToolChoiceAnyParam{}}, true
	case llm.ToolChoiceNone:
		return sdk.ToolChoiceUnionParam{OfNone: &sdk.ToolChoiceNoneParam{}}, true
	default:
		return sdk.ToolChoiceParamOfTool(string(tc)), true
	}
}
