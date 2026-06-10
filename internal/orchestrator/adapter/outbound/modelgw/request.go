package modelgw

import (
	"encoding/json"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// toGenerationParams maps an llm.Request to a gen.GenerationParams for the
// Generate and CountTokens RPCs. This is the only place in the adapter that
// reads llm.Request fields; it lives here so the main adapter stays thin.
func toGenerationParams(req llm.Request) *genproto.GenerationParams {
	p := &genproto.GenerationParams{
		Model:        req.Model,
		System:       req.System,
		MaxTokens:    int64(req.MaxTokens),
		Stream:       req.Stream,
		ProviderRaw:  req.ProviderRaw,
		OutputSchema: req.OutputSchema,
		Strict:       req.Strict,
	}
	if req.Temperature != nil {
		t := *req.Temperature
		p.Temperature = &t
	}
	for _, m := range req.Messages {
		p.Messages = append(p.Messages, toMessage(m))
	}
	for _, td := range req.Tools {
		p.Tools = append(p.Tools, toToolDefinition(td))
	}
	p.ToolChoice, p.ToolName = toToolChoice(req.ToolChoice)
	return p
}

// toMessage maps an llm.Message to a gen.Message.
func toMessage(m llm.Message) *genproto.Message {
	gm := &genproto.Message{Role: toRole(m.Role)}
	for _, cp := range m.Content {
		gm.Content = append(gm.Content, toContentPart(cp))
	}
	return gm
}

// toRole maps llm.Role to the gen Role enum.
func toRole(r llm.Role) genproto.Role {
	switch r {
	case llm.RoleUser:
		return genproto.Role_ROLE_USER
	case llm.RoleAssistant:
		return genproto.Role_ROLE_ASSISTANT
	case llm.RoleTool:
		return genproto.Role_ROLE_TOOL
	default:
		return genproto.Role_ROLE_UNSPECIFIED
	}
}

// toContentPart maps an llm.ContentPart to a gen.ContentPart.
func toContentPart(cp llm.ContentPart) *genproto.ContentPart {
	switch {
	case cp.Text != nil:
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_Text{
				Text: &genproto.TextPart{Text: cp.Text.Text},
			},
		}
	case cp.Thinking != nil:
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_Thinking{
				Thinking: &genproto.ThinkingPart{
					Text:      cp.Thinking.Text,
					Signature: cp.Thinking.Signature,
				},
			},
		}
	case cp.Image != nil:
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_Image{
				Image: &genproto.ImagePart{
					MediaType: cp.Image.MediaType,
					Data:      cp.Image.Data,
					Url:       cp.Image.URL,
					FileRef:   cp.Image.FileRef,
				},
			},
		}
	case cp.ToolCall != nil:
		argsJSON, _ := json.Marshal(cp.ToolCall.Args)
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_ToolCall{
				ToolCall: &genproto.ToolCall{
					Id:       cp.ToolCall.ID,
					Name:     cp.ToolCall.Name,
					ArgsJson: string(argsJSON),
				},
			},
		}
	case cp.ToolResult != nil:
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_ToolResult{
				ToolResult: &genproto.ToolResult{
					CallId:  cp.ToolResult.CallID,
					Content: cp.ToolResult.Content,
					IsError: cp.ToolResult.IsError,
				},
			},
		}
	default:
		// empty / unknown part: return an empty text part as a safe fallback
		return &genproto.ContentPart{
			Part: &genproto.ContentPart_Text{
				Text: &genproto.TextPart{},
			},
		}
	}
}

// toToolDefinition maps an llm.ToolDef to a gen.ToolDefinition.
func toToolDefinition(td llm.ToolDef) *genproto.ToolDefinition {
	return &genproto.ToolDefinition{
		Name:        td.Name,
		Description: td.Description,
		JsonSchema:  string(td.JSONSchema),
	}
}

// toToolChoice maps an llm.ToolChoice to (gen.ToolChoice, toolName).
// When the ToolChoice is a specific tool name (not a sentinel), we return
// TOOL_CHOICE_TOOL and the name separately.
func toToolChoice(tc llm.ToolChoice) (genproto.ToolChoice, string) {
	switch tc {
	case llm.ToolChoiceAuto:
		return genproto.ToolChoice_TOOL_CHOICE_AUTO, ""
	case llm.ToolChoiceAny:
		return genproto.ToolChoice_TOOL_CHOICE_ANY, ""
	case llm.ToolChoiceRequired:
		return genproto.ToolChoice_TOOL_CHOICE_REQUIRED, ""
	case llm.ToolChoiceNone:
		return genproto.ToolChoice_TOOL_CHOICE_NONE, ""
	case "":
		return genproto.ToolChoice_TOOL_CHOICE_UNSPECIFIED, ""
	default:
		// A non-sentinel string is interpreted as a specific tool name.
		return genproto.ToolChoice_TOOL_CHOICE_TOOL, string(tc)
	}
}
