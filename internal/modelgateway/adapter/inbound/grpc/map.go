package grpc

import (
	"encoding/json"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// This file holds the gen ⇄ llm mapping for the model-gateway inbound server.
// It is the transport edge: gen wire types are mapped to the canonical llm
// kernel types on the way IN (GenerationParams → llm.Request) and llm types are
// mapped back to gen on the way OUT (llm.StreamEvent → gen.StreamEvent,
// llm.Capabilities → gen.Capabilities). No provider-specific logic lives here;
// all normalization is already done behind the injected provider (architecture
// §4.3, §12.3; ADR-0016).

// ---- request: gen → llm ------------------------------------------------------

// toLLMRequest maps a gen.GenerationParams to a normalized llm.Request. A nil
// params yields a zero llm.Request (the provider rejects it as appropriate).
func toLLMRequest(p *genproto.GenerationParams) llm.Request {
	if p == nil {
		return llm.Request{}
	}
	req := llm.Request{
		Model:        p.GetModel(),
		System:       p.GetSystem(),
		MaxTokens:    int(p.GetMaxTokens()),
		Stream:       p.GetStream(),
		ProviderRaw:  rawOrNil(p.GetProviderRaw()),
		OutputSchema: rawOrNil(p.GetOutputSchema()),
		Strict:       p.GetStrict(),
		ToolChoice:   toLLMToolChoice(p.GetToolChoice(), p.GetToolName()),
	}
	// Temperature is optional on the wire; a present value maps to a non-nil
	// pointer so "unset" stays distinct from 0.0.
	if p.Temperature != nil {
		t := p.GetTemperature()
		req.Temperature = &t
	}
	for _, m := range p.GetMessages() {
		req.Messages = append(req.Messages, toLLMMessage(m))
	}
	for _, td := range p.GetTools() {
		req.Tools = append(req.Tools, toLLMToolDef(td))
	}
	return req
}

// rawOrNil returns nil for an empty byte slice so an absent provider_raw /
// output_schema stays nil (matching llm's "no continuation / free-form" sense)
// rather than an empty-but-non-nil json.RawMessage.
func rawOrNil(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return b
}

// toLLMMessage maps a gen.Message to an llm.Message.
func toLLMMessage(m *genproto.Message) llm.Message {
	if m == nil {
		return llm.Message{}
	}
	out := llm.Message{Role: toLLMRole(m.GetRole())}
	for _, cp := range m.GetContent() {
		out.Content = append(out.Content, toLLMContentPart(cp))
	}
	return out
}

// toLLMRole maps the gen Role enum to an llm.Role.
func toLLMRole(r genproto.Role) llm.Role {
	switch r {
	case genproto.Role_ROLE_USER:
		return llm.RoleUser
	case genproto.Role_ROLE_ASSISTANT:
		return llm.RoleAssistant
	case genproto.Role_ROLE_TOOL:
		return llm.RoleTool
	default:
		// ROLE_UNSPECIFIED — leave empty; the provider adapter handles it.
		return ""
	}
}

// toLLMContentPart maps a gen.ContentPart oneof to an llm.ContentPart. Exactly
// one field is set on the result; an unrecognized/empty part yields a zero
// llm.ContentPart.
func toLLMContentPart(cp *genproto.ContentPart) llm.ContentPart {
	if cp == nil {
		return llm.ContentPart{}
	}
	switch v := cp.GetPart().(type) {
	case *genproto.ContentPart_Text:
		return llm.ContentPart{Text: &llm.TextPart{Text: v.Text.GetText()}}
	case *genproto.ContentPart_Image:
		img := v.Image
		return llm.ContentPart{Image: &llm.ImagePart{
			MediaType: img.GetMediaType(),
			Data:      img.GetData(),
			URL:       img.GetUrl(),
			FileRef:   img.GetFileRef(),
		}}
	case *genproto.ContentPart_Thinking:
		return llm.ContentPart{Thinking: &llm.ThinkingPart{
			Text:      v.Thinking.GetText(),
			Signature: v.Thinking.GetSignature(),
		}}
	case *genproto.ContentPart_ToolCall:
		return llm.ContentPart{ToolCall: &llm.ToolCall{
			ID:   v.ToolCall.GetId(),
			Name: v.ToolCall.GetName(),
			Args: parseArgs(v.ToolCall.GetArgsJson()),
		}}
	case *genproto.ContentPart_ToolResult:
		return llm.ContentPart{ToolResult: &llm.ToolResult{
			CallID:  v.ToolResult.GetCallId(),
			Content: v.ToolResult.GetContent(),
			IsError: v.ToolResult.GetIsError(),
		}}
	default:
		return llm.ContentPart{}
	}
}

// parseArgs decodes a JSON-object args string into a map[string]any. An empty or
// invalid string yields nil — the orchestrator always supplies a valid object;
// a malformed one is treated as no args rather than a hard error at this edge.
func parseArgs(s string) map[string]any {
	if s == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return m
}

// toLLMToolDef maps a gen.ToolDefinition to an llm.ToolDef. The JSON schema is
// carried verbatim as raw bytes.
func toLLMToolDef(td *genproto.ToolDefinition) llm.ToolDef {
	if td == nil {
		return llm.ToolDef{}
	}
	return llm.ToolDef{
		Name:        td.GetName(),
		Description: td.GetDescription(),
		JSONSchema:  rawOrNil([]byte(td.GetJsonSchema())),
	}
}

// toLLMToolChoice maps the gen ToolChoice enum (+ tool_name) to an
// llm.ToolChoice. TOOL_CHOICE_TOOL carries the specific tool name in the string;
// TOOL_CHOICE_UNSPECIFIED maps to the empty "unset" value.
func toLLMToolChoice(tc genproto.ToolChoice, name string) llm.ToolChoice {
	switch tc {
	case genproto.ToolChoice_TOOL_CHOICE_AUTO:
		return llm.ToolChoiceAuto
	case genproto.ToolChoice_TOOL_CHOICE_ANY:
		return llm.ToolChoiceAny
	case genproto.ToolChoice_TOOL_CHOICE_REQUIRED:
		return llm.ToolChoiceRequired
	case genproto.ToolChoice_TOOL_CHOICE_NONE:
		return llm.ToolChoiceNone
	case genproto.ToolChoice_TOOL_CHOICE_TOOL:
		return llm.ToolChoice(name)
	default:
		// TOOL_CHOICE_UNSPECIFIED → unset; adapter applies the provider default.
		return ""
	}
}

// ---- stream events: llm → gen ------------------------------------------------

// toGenStreamEvent maps a normalized llm.StreamEvent to a gen.StreamEvent oneof.
// Exactly one variant is set on the result. A zero llm.StreamEvent (no field
// set) yields a nil event, which the caller skips.
func toGenStreamEvent(ev llm.StreamEvent) *genproto.StreamEvent {
	switch {
	case ev.TextDelta != nil:
		return &genproto.StreamEvent{Event: &genproto.StreamEvent_TextDelta{
			TextDelta: &genproto.TextDelta{Text: ev.TextDelta.Text},
		}}
	case ev.ThinkingDelta != nil:
		return &genproto.StreamEvent{Event: &genproto.StreamEvent_ThinkingDelta{
			ThinkingDelta: &genproto.ThinkingDelta{
				Text:      ev.ThinkingDelta.Text,
				Signature: ev.ThinkingDelta.Signature,
			},
		}}
	case ev.ToolCallDelta != nil:
		tcd := ev.ToolCallDelta
		return &genproto.StreamEvent{Event: &genproto.StreamEvent_ToolCallDelta{
			ToolCallDelta: &genproto.ToolCallDelta{
				CallId:       tcd.CallID,
				Name:         tcd.Name,
				ArgsPath:     tcd.ArgsPath,
				ArgsFragment: tcd.ArgsFragment,
			},
		}}
	case ev.Done != nil:
		return &genproto.StreamEvent{Event: &genproto.StreamEvent_Done{
			Done: toGenDone(ev.Done),
		}}
	default:
		return nil
	}
}

// toGenDone maps an llm.Done to a gen.Done, including usage and provider_raw.
func toGenDone(d *llm.Done) *genproto.Done {
	return &genproto.Done{
		StopReason:    toGenStopReason(d.StopReason),
		RawStopReason: d.RawStopReason,
		Usage:         toGenUsage(d.Usage),
		ProviderRaw:   d.ProviderRaw,
	}
}

// toGenStopReason maps the open-set llm.StopReason to the gen StopReason enum.
// llm.StopOther (and any unmapped value) maps to STOP_REASON_OTHER; the verbatim
// provider string travels in Done.raw_stop_reason (architecture §11.3).
func toGenStopReason(r llm.StopReason) genproto.StopReason {
	switch r {
	case llm.StopEnd:
		return genproto.StopReason_STOP_REASON_END
	case llm.StopMaxTokens:
		return genproto.StopReason_STOP_REASON_MAX_TOKENS
	case llm.StopToolUse:
		return genproto.StopReason_STOP_REASON_TOOL_USE
	case llm.StopStopSequence:
		return genproto.StopReason_STOP_REASON_STOP_SEQUENCE
	case llm.StopContentFilter:
		return genproto.StopReason_STOP_REASON_CONTENT_FILTER
	case llm.StopRefusal:
		return genproto.StopReason_STOP_REASON_REFUSAL
	case llm.StopContextWindowExceeded:
		return genproto.StopReason_STOP_REASON_CONTEXT_WINDOW_EXCEEDED
	case llm.Pause:
		return genproto.StopReason_STOP_REASON_PAUSE
	default:
		// StopOther or any unrecognized value.
		return genproto.StopReason_STOP_REASON_OTHER
	}
}

// toGenUsage maps an llm.Usage to a gen.Usage.
func toGenUsage(u llm.Usage) *genproto.Usage {
	return &genproto.Usage{
		InputTokens:      int64(u.InputTokens),
		OutputTokens:     int64(u.OutputTokens),
		CacheReadTokens:  int64(u.CacheReadTokens),
		CacheWriteTokens: int64(u.CacheWriteTokens),
		ReasoningTokens:  int64(u.ReasoningTokens),
	}
}

// ---- capabilities: llm → gen -------------------------------------------------

// toGenCapabilities maps an llm.Capabilities to a gen.Capabilities. It always
// sets supports_server_side_tools to false: provider-native server-side tools
// are DISABLED by a hard gateway policy switch in v1 so all tools flow through
// tool-runtime's controls (architecture §8.12). The flag is carried for forward
// compatibility only.
func toGenCapabilities(c llm.Capabilities) *genproto.Capabilities {
	return &genproto.Capabilities{
		SupportsTools:              c.SupportsTools,
		SupportsParallelToolCalls:  c.SupportsParallelToolCalls,
		SupportsStreamingToolCalls: c.SupportsStreamingToolCalls,
		SupportsVision:             c.SupportsVision,
		SupportsSystemPrompt:       c.SupportsSystemPrompt,
		SupportsThinking:           c.SupportsThinking,
		SupportsTokenCounting:      c.SupportsTokenCounting,
		SupportsJsonSchemaStrict:   c.SupportsJSONSchemaStrict,
		SupportsServerSideTools:    false, // hard-off in v1 (§8.12)
		MaxOutputTokens:            int64(c.MaxOutputTokens),
	}
}

// ---- error mapping: llm.ProviderError → gRPC status --------------------------

// statusFromError maps an error returned by the use-case (a *llm.ProviderError
// at this edge) to a gRPC status error. The mapping mirrors the orchestrator's
// inbound expectation (architecture §4.4):
//
//   - ErrUnsupported    → UNIMPLEMENTED   (capability-gated CountTokens)
//   - ErrRateLimited    → RESOURCE_EXHAUSTED
//   - ErrAuth           → UNAUTHENTICATED
//   - ErrInvalidRequest → INVALID_ARGUMENT
//   - ErrTimeout        → DEADLINE_EXCEEDED
//   - ErrServer/ErrOverloaded/other → UNAVAILABLE
//
// A nil error returns nil; a non-ProviderError is reported as INTERNAL.
func statusFromError(err error) error {
	if err == nil {
		return nil
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		return status.Error(codes.Internal, err.Error())
	}
	var code codes.Code
	switch pe.Kind {
	case llm.ErrUnsupported:
		code = codes.Unimplemented
	case llm.ErrRateLimited:
		code = codes.ResourceExhausted
	case llm.ErrAuth:
		code = codes.Unauthenticated
	case llm.ErrInvalidRequest:
		code = codes.InvalidArgument
	case llm.ErrTimeout:
		code = codes.DeadlineExceeded
	case llm.ErrServer, llm.ErrOverloaded:
		code = codes.Unavailable
	default:
		code = codes.Unavailable
	}
	return status.Error(code, err.Error())
}
