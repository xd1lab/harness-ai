// Package grpc tests — pure, table-driven tests for the gen ⇄ llm mapping edge
// (map.go). The bufconn server tests exercise the happy round-trips; these
// cover the mapping layer exhaustively with no network: every enum value in
// both directions, the unknown-value fallbacks, nil payloads, and the
// llm.ProviderError → gRPC status mapping (architecture §4.4, §12.3).
package grpc

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// ---- request: gen → llm --------------------------------------------------------

func TestToLLMRequest_Nil(t *testing.T) {
	got := toLLMRequest(nil)
	if got.Model != "" || got.Messages != nil || got.Tools != nil || got.Temperature != nil {
		t.Errorf("toLLMRequest(nil) = %+v, want zero llm.Request", got)
	}
}

func TestToLLMRequest_FullParams(t *testing.T) {
	temp := 0.9
	p := &genproto.GenerationParams{
		Model:  "claude-3-5-sonnet-20241022",
		System: "be brief",
		Messages: []*genproto.Message{
			{Role: genproto.Role_ROLE_USER, Content: []*genproto.ContentPart{
				{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "hi"}}},
			}},
		},
		Tools: []*genproto.ToolDefinition{
			{Name: "write", Description: "writes", JsonSchema: `{"type":"object"}`},
		},
		ToolChoice:   genproto.ToolChoice_TOOL_CHOICE_NONE,
		MaxTokens:    128,
		Temperature:  &temp,
		Stream:       true,
		ProviderRaw:  []byte(`{"prev":1}`),
		OutputSchema: []byte(`{"type":"object"}`),
		Strict:       true,
	}

	got := toLLMRequest(p)

	if got.Model != "claude-3-5-sonnet-20241022" || got.System != "be brief" {
		t.Errorf("Model/System = %q/%q", got.Model, got.System)
	}
	if got.MaxTokens != 128 || !got.Stream || !got.Strict {
		t.Errorf("MaxTokens/Stream/Strict = %d/%v/%v", got.MaxTokens, got.Stream, got.Strict)
	}
	if got.Temperature == nil || *got.Temperature != 0.9 {
		t.Errorf("Temperature = %v, want 0.9 (present wire value maps to non-nil)", got.Temperature)
	}
	if string(got.ProviderRaw) != `{"prev":1}` || string(got.OutputSchema) != `{"type":"object"}` {
		t.Errorf("ProviderRaw/OutputSchema = %s/%s", got.ProviderRaw, got.OutputSchema)
	}
	if got.ToolChoice != llm.ToolChoiceNone {
		t.Errorf("ToolChoice = %q, want none", got.ToolChoice)
	}
	if len(got.Messages) != 1 || got.Messages[0].Role != llm.RoleUser ||
		len(got.Messages[0].Content) != 1 || got.Messages[0].Content[0].Text == nil ||
		got.Messages[0].Content[0].Text.Text != "hi" {
		t.Errorf("Messages = %+v", got.Messages)
	}
	if len(got.Tools) != 1 || got.Tools[0].Name != "write" || string(got.Tools[0].JSONSchema) != `{"type":"object"}` {
		t.Errorf("Tools = %+v", got.Tools)
	}
}

func TestToLLMRequest_AbsentOptionalsStayUnset(t *testing.T) {
	got := toLLMRequest(&genproto.GenerationParams{Model: "m"})
	if got.Temperature != nil {
		t.Errorf("Temperature = %v, want nil (unset stays distinct from 0.0)", got.Temperature)
	}
	if got.ProviderRaw != nil {
		t.Errorf("ProviderRaw = %v, want nil (absent blob stays nil, not empty)", got.ProviderRaw)
	}
	if got.OutputSchema != nil {
		t.Errorf("OutputSchema = %v, want nil", got.OutputSchema)
	}
	if got.ToolChoice != "" {
		t.Errorf("ToolChoice = %q, want unset", got.ToolChoice)
	}
}

func TestRawOrNil(t *testing.T) {
	if got := rawOrNil(nil); got != nil {
		t.Errorf("rawOrNil(nil) = %v, want nil", got)
	}
	if got := rawOrNil([]byte{}); got != nil {
		t.Errorf("rawOrNil(empty) = %v, want nil", got)
	}
	if got := rawOrNil([]byte(`{}`)); string(got) != `{}` {
		t.Errorf("rawOrNil({}) = %s, want {}", got)
	}
}

func TestToLLMMessage_Nil(t *testing.T) {
	got := toLLMMessage(nil)
	if got.Role != "" || got.Content != nil {
		t.Errorf("toLLMMessage(nil) = %+v, want zero", got)
	}
}

func TestToLLMRole_Exhaustive(t *testing.T) {
	cases := []struct {
		in   genproto.Role
		want llm.Role
	}{
		{genproto.Role_ROLE_USER, llm.RoleUser},
		{genproto.Role_ROLE_ASSISTANT, llm.RoleAssistant},
		{genproto.Role_ROLE_TOOL, llm.RoleTool},
		// UNSPECIFIED and unknown future values stay empty; the provider adapter
		// decides, never this edge.
		{genproto.Role_ROLE_UNSPECIFIED, ""},
		{genproto.Role(99), ""},
	}
	for _, tc := range cases {
		if got := toLLMRole(tc.in); got != tc.want {
			t.Errorf("toLLMRole(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestToLLMContentPart_Variants(t *testing.T) {
	t.Run("nil part is zero", func(t *testing.T) {
		if got := toLLMContentPart(nil); got != (llm.ContentPart{}) {
			t.Errorf("got %+v, want zero", got)
		}
	})

	t.Run("empty oneof is zero", func(t *testing.T) {
		if got := toLLMContentPart(&genproto.ContentPart{}); got != (llm.ContentPart{}) {
			t.Errorf("got %+v, want zero", got)
		}
	})

	t.Run("text", func(t *testing.T) {
		got := toLLMContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: "hi"}},
		})
		if got.Text == nil || got.Text.Text != "hi" {
			t.Errorf("got %+v, want Text{hi}", got)
		}
	})

	t.Run("image", func(t *testing.T) {
		got := toLLMContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Image{Image: &genproto.ImagePart{
				MediaType: "image/jpeg", Data: []byte{0xff}, Url: "https://x/i.jpg", FileRef: "f1",
			}},
		})
		if got.Image == nil || got.Image.MediaType != "image/jpeg" || string(got.Image.Data) != "\xff" ||
			got.Image.URL != "https://x/i.jpg" || got.Image.FileRef != "f1" {
			t.Errorf("got %+v, want full ImagePart", got)
		}
	})

	t.Run("thinking", func(t *testing.T) {
		got := toLLMContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_Thinking{Thinking: &genproto.ThinkingPart{Text: "hm", Signature: "sig"}},
		})
		if got.Thinking == nil || got.Thinking.Text != "hm" || got.Thinking.Signature != "sig" {
			t.Errorf("got %+v, want Thinking{hm,sig}", got)
		}
	})

	t.Run("tool call parses args", func(t *testing.T) {
		got := toLLMContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_ToolCall{ToolCall: &genproto.ToolCall{
				Id: "c1", Name: "bash", ArgsJson: `{"cmd":"ls"}`,
			}},
		})
		if got.ToolCall == nil || got.ToolCall.ID != "c1" || got.ToolCall.Name != "bash" {
			t.Fatalf("got %+v, want ToolCall{c1,bash}", got)
		}
		if got.ToolCall.Args["cmd"] != "ls" {
			t.Errorf("Args = %v, want cmd=ls", got.ToolCall.Args)
		}
	})

	t.Run("tool result", func(t *testing.T) {
		got := toLLMContentPart(&genproto.ContentPart{
			Part: &genproto.ContentPart_ToolResult{ToolResult: &genproto.ToolResult{
				CallId: "c2", Content: "out", IsError: true,
			}},
		})
		if got.ToolResult == nil || got.ToolResult.CallID != "c2" || got.ToolResult.Content != "out" || !got.ToolResult.IsError {
			t.Errorf("got %+v, want ToolResult{c2,out,err}", got)
		}
	})
}

func TestParseArgs(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want map[string]any
	}{
		{"empty is nil", "", nil},
		{"malformed is nil, not a hard error at this edge", `{"broken`, nil},
		{"non-object is nil", `"str"`, nil},
		{"valid object parses", `{"k":"v"}`, map[string]any{"k": "v"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseArgs(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("parseArgs(%q) = %v, want %v", tc.in, got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("parseArgs(%q)[%q] = %v, want %v", tc.in, k, got[k], v)
				}
			}
		})
	}
}

func TestToLLMToolDef(t *testing.T) {
	t.Run("nil is zero", func(t *testing.T) {
		got := toLLMToolDef(nil)
		if got.Name != "" || got.Description != "" || got.JSONSchema != nil {
			t.Errorf("got %+v, want zero", got)
		}
	})

	t.Run("fields carry verbatim, empty schema stays nil", func(t *testing.T) {
		got := toLLMToolDef(&genproto.ToolDefinition{Name: "w", Description: "d"})
		if got.Name != "w" || got.Description != "d" {
			t.Errorf("got %+v", got)
		}
		if got.JSONSchema != nil {
			t.Errorf("JSONSchema = %v, want nil for an empty wire schema", got.JSONSchema)
		}
	})
}

func TestToLLMToolChoice_Exhaustive(t *testing.T) {
	cases := []struct {
		in   genproto.ToolChoice
		name string
		want llm.ToolChoice
	}{
		{genproto.ToolChoice_TOOL_CHOICE_AUTO, "", llm.ToolChoiceAuto},
		{genproto.ToolChoice_TOOL_CHOICE_ANY, "", llm.ToolChoiceAny},
		{genproto.ToolChoice_TOOL_CHOICE_REQUIRED, "", llm.ToolChoiceRequired},
		{genproto.ToolChoice_TOOL_CHOICE_NONE, "", llm.ToolChoiceNone},
		// TOOL carries the specific name; the enum alone is not enough.
		{genproto.ToolChoice_TOOL_CHOICE_TOOL, "write", llm.ToolChoice("write")},
		// UNSPECIFIED and unknown future values mean "unset" (provider default).
		{genproto.ToolChoice_TOOL_CHOICE_UNSPECIFIED, "ignored", ""},
		{genproto.ToolChoice(99), "ignored", ""},
	}
	for _, tc := range cases {
		if got := toLLMToolChoice(tc.in, tc.name); got != tc.want {
			t.Errorf("toLLMToolChoice(%v, %q) = %q, want %q", tc.in, tc.name, got, tc.want)
		}
	}
}

// ---- stream events: llm → gen ----------------------------------------------------

func TestToGenStreamEvent_Variants(t *testing.T) {
	t.Run("zero event yields nil (caller skips)", func(t *testing.T) {
		if got := toGenStreamEvent(llm.StreamEvent{}); got != nil {
			t.Errorf("got %+v, want nil", got)
		}
	})

	t.Run("text delta", func(t *testing.T) {
		got := toGenStreamEvent(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "hi"}})
		if got.GetTextDelta() == nil || got.GetTextDelta().GetText() != "hi" {
			t.Errorf("got %+v, want TextDelta{hi}", got)
		}
	})

	t.Run("thinking delta carries signature", func(t *testing.T) {
		got := toGenStreamEvent(llm.StreamEvent{ThinkingDelta: &llm.ThinkingDelta{Text: "hm", Signature: "s"}})
		td := got.GetThinkingDelta()
		if td == nil || td.GetText() != "hm" || td.GetSignature() != "s" {
			t.Errorf("got %+v, want ThinkingDelta{hm,s}", got)
		}
	})

	t.Run("tool call delta carries path and fragment", func(t *testing.T) {
		got := toGenStreamEvent(llm.StreamEvent{ToolCallDelta: &llm.ToolCallDelta{
			CallID: "c1", Name: "w", ArgsPath: "$.x", ArgsFragment: []byte(`{"x":1}`),
		}})
		tcd := got.GetToolCallDelta()
		if tcd == nil || tcd.GetCallId() != "c1" || tcd.GetName() != "w" ||
			tcd.GetArgsPath() != "$.x" || string(tcd.GetArgsFragment()) != `{"x":1}` {
			t.Errorf("got %+v, want full ToolCallDelta", got)
		}
	})

	t.Run("done carries stop reason, usage, provider_raw", func(t *testing.T) {
		got := toGenStreamEvent(llm.StreamEvent{Done: &llm.Done{
			StopReason:    llm.Pause,
			RawStopReason: "pause_turn",
			Usage:         llm.Usage{InputTokens: 1, OutputTokens: 2, CacheReadTokens: 3, CacheWriteTokens: 4, ReasoningTokens: 5},
			ProviderRaw:   []byte(`{"cont":true}`),
		}})
		d := got.GetDone()
		if d == nil {
			t.Fatalf("got %+v, want Done", got)
		}
		if d.GetStopReason() != genproto.StopReason_STOP_REASON_PAUSE || d.GetRawStopReason() != "pause_turn" {
			t.Errorf("StopReason = %v/%q", d.GetStopReason(), d.GetRawStopReason())
		}
		if string(d.GetProviderRaw()) != `{"cont":true}` {
			t.Errorf("ProviderRaw = %s", d.GetProviderRaw())
		}
		u := d.GetUsage()
		if u.GetInputTokens() != 1 || u.GetOutputTokens() != 2 || u.GetCacheReadTokens() != 3 ||
			u.GetCacheWriteTokens() != 4 || u.GetReasoningTokens() != 5 {
			t.Errorf("Usage = %+v", u)
		}
	})
}

// TestToGenStopReason_Exhaustive covers every llm.StopReason constant plus the
// open-set escape hatch: an unrecognized provider value maps to OTHER and the
// verbatim string travels in raw_stop_reason (architecture §11.3).
func TestToGenStopReason_Exhaustive(t *testing.T) {
	cases := []struct {
		in   llm.StopReason
		want genproto.StopReason
	}{
		{llm.StopEnd, genproto.StopReason_STOP_REASON_END},
		{llm.StopMaxTokens, genproto.StopReason_STOP_REASON_MAX_TOKENS},
		{llm.StopToolUse, genproto.StopReason_STOP_REASON_TOOL_USE},
		{llm.StopStopSequence, genproto.StopReason_STOP_REASON_STOP_SEQUENCE},
		{llm.StopContentFilter, genproto.StopReason_STOP_REASON_CONTENT_FILTER},
		{llm.StopRefusal, genproto.StopReason_STOP_REASON_REFUSAL},
		{llm.StopContextWindowExceeded, genproto.StopReason_STOP_REASON_CONTEXT_WINDOW_EXCEEDED},
		{llm.Pause, genproto.StopReason_STOP_REASON_PAUSE},
		{llm.StopOther, genproto.StopReason_STOP_REASON_OTHER},
		{llm.StopReason("provider_specific"), genproto.StopReason_STOP_REASON_OTHER},
		{llm.StopReason(""), genproto.StopReason_STOP_REASON_OTHER},
	}
	for _, tc := range cases {
		if got := toGenStopReason(tc.in); got != tc.want {
			t.Errorf("toGenStopReason(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// ---- capabilities: llm → gen -------------------------------------------------------

func TestToGenCapabilities_ForcesServerSideToolsOff(t *testing.T) {
	got := toGenCapabilities(llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            4096,
	})
	if !got.GetSupportsTools() || !got.GetSupportsParallelToolCalls() || !got.GetSupportsStreamingToolCalls() ||
		!got.GetSupportsVision() || !got.GetSupportsSystemPrompt() || !got.GetSupportsThinking() ||
		!got.GetSupportsTokenCounting() || !got.GetSupportsJsonSchemaStrict() {
		t.Errorf("capability flags lost in mapping: %+v", got)
	}
	if got.GetMaxOutputTokens() != 4096 {
		t.Errorf("MaxOutputTokens = %d, want 4096", got.GetMaxOutputTokens())
	}
	// The v1 hard policy switch: provider-native server-side tools are DISABLED
	// regardless of what the resolver reports (architecture §8.12).
	if got.GetSupportsServerSideTools() {
		t.Error("supports_server_side_tools must always be false in v1 (§8.12)")
	}
}

// ---- error mapping: llm.ProviderError → gRPC status ---------------------------------

func TestStatusFromError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		if got := statusFromError(nil); got != nil {
			t.Errorf("statusFromError(nil) = %v, want nil", got)
		}
	})

	t.Run("non-ProviderError is Internal", func(t *testing.T) {
		got := statusFromError(errors.New("bug"))
		if status.Code(got) != codes.Internal {
			t.Errorf("code = %v, want Internal", status.Code(got))
		}
	})

	t.Run("wrapped ProviderError still maps (errors.As)", func(t *testing.T) {
		got := statusFromError(fmt.Errorf("generate: %w", &llm.ProviderError{Kind: llm.ErrRateLimited}))
		if status.Code(got) != codes.ResourceExhausted {
			t.Errorf("code = %v, want ResourceExhausted", status.Code(got))
		}
	})

	// Kind table mirroring the orchestrator's inbound expectation (§4.4).
	cases := []struct {
		kind llm.ErrorKind
		want codes.Code
	}{
		{llm.ErrUnsupported, codes.Unimplemented},
		{llm.ErrRateLimited, codes.ResourceExhausted},
		{llm.ErrAuth, codes.Unauthenticated},
		{llm.ErrInvalidRequest, codes.InvalidArgument},
		{llm.ErrTimeout, codes.DeadlineExceeded},
		{llm.ErrServer, codes.Unavailable},
		{llm.ErrOverloaded, codes.Unavailable},
		// An unknown kind degrades to the retryable UNAVAILABLE, never a silent OK.
		{llm.ErrorKind("future_kind"), codes.Unavailable},
	}
	for _, tc := range cases {
		t.Run(string(tc.kind), func(t *testing.T) {
			got := statusFromError(&llm.ProviderError{Kind: tc.kind})
			if status.Code(got) != tc.want {
				t.Errorf("statusFromError(kind=%q) code = %v, want %v", tc.kind, status.Code(got), tc.want)
			}
		})
	}
}
