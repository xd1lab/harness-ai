// Package modelgw tests — pure mapping tests for the llm → gen request mapping
// (request.go), the gRPC-status → *llm.ProviderError mapping (adapter.go), and
// the normalize.go branches the bufconn integration tests do not reach (nil
// payloads and nil oneof inners). No network is needed; every function under
// test is a pure mapping, so enums are asserted exhaustively including the
// unknown-value fallbacks.
package modelgw

import (
	"errors"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// ---- toRole -------------------------------------------------------------------

func TestToRole_Exhaustive(t *testing.T) {
	cases := []struct {
		in   llm.Role
		want genproto.Role
	}{
		{llm.RoleUser, genproto.Role_ROLE_USER},
		{llm.RoleAssistant, genproto.Role_ROLE_ASSISTANT},
		{llm.RoleTool, genproto.Role_ROLE_TOOL},
		// The zero value and an unknown role both map to UNSPECIFIED; the gateway
		// decides how to treat it (never an invented role).
		{llm.Role(""), genproto.Role_ROLE_UNSPECIFIED},
		{llm.Role("narrator"), genproto.Role_ROLE_UNSPECIFIED},
	}
	for _, tc := range cases {
		if got := toRole(tc.in); got != tc.want {
			t.Errorf("toRole(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// ---- toToolChoice ---------------------------------------------------------------

func TestToToolChoice_Exhaustive(t *testing.T) {
	cases := []struct {
		in       llm.ToolChoice
		want     genproto.ToolChoice
		wantName string
	}{
		{llm.ToolChoiceAuto, genproto.ToolChoice_TOOL_CHOICE_AUTO, ""},
		{llm.ToolChoiceAny, genproto.ToolChoice_TOOL_CHOICE_ANY, ""},
		{llm.ToolChoiceRequired, genproto.ToolChoice_TOOL_CHOICE_REQUIRED, ""},
		{llm.ToolChoiceNone, genproto.ToolChoice_TOOL_CHOICE_NONE, ""},
		// Empty means "unset" (provider default), distinct from any sentinel.
		{llm.ToolChoice(""), genproto.ToolChoice_TOOL_CHOICE_UNSPECIFIED, ""},
		// A non-sentinel string is a specific tool name and travels separately.
		{llm.ToolChoice("write_file"), genproto.ToolChoice_TOOL_CHOICE_TOOL, "write_file"},
	}
	for _, tc := range cases {
		got, name := toToolChoice(tc.in)
		if got != tc.want || name != tc.wantName {
			t.Errorf("toToolChoice(%q) = (%v, %q), want (%v, %q)", tc.in, got, name, tc.want, tc.wantName)
		}
	}
}

// ---- toContentPart ---------------------------------------------------------------

func TestToContentPart_Variants(t *testing.T) {
	t.Run("text", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{Text: &llm.TextPart{Text: "hi"}})
		if got.GetText() == nil || got.GetText().GetText() != "hi" {
			t.Errorf("got %+v, want Text{hi}", got)
		}
	})

	t.Run("thinking carries signature", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{Thinking: &llm.ThinkingPart{Text: "hm", Signature: "sig"}})
		th := got.GetThinking()
		if th == nil || th.GetText() != "hm" || th.GetSignature() != "sig" {
			t.Errorf("got %+v, want Thinking{hm,sig}", got)
		}
	})

	t.Run("image carries every reference form", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{Image: &llm.ImagePart{
			MediaType: "image/png", Data: []byte{1, 2}, URL: "https://x/i.png", FileRef: "f-1",
		}})
		img := got.GetImage()
		if img == nil || img.GetMediaType() != "image/png" || string(img.GetData()) != "\x01\x02" ||
			img.GetUrl() != "https://x/i.png" || img.GetFileRef() != "f-1" {
			t.Errorf("got %+v, want full ImagePart", got)
		}
	})

	t.Run("tool call marshals args to JSON", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{ToolCall: &llm.ToolCall{
			ID: "c1", Name: "bash", Args: map[string]any{"cmd": "ls"},
		}})
		tc := got.GetToolCall()
		if tc == nil || tc.GetId() != "c1" || tc.GetName() != "bash" {
			t.Fatalf("got %+v, want ToolCall{c1,bash}", got)
		}
		if tc.GetArgsJson() != `{"cmd":"ls"}` {
			t.Errorf("ArgsJson = %s, want {\"cmd\":\"ls\"}", tc.GetArgsJson())
		}
	})

	t.Run("tool result", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{ToolResult: &llm.ToolResult{CallID: "c2", Content: "out", IsError: true}})
		tr := got.GetToolResult()
		if tr == nil || tr.GetCallId() != "c2" || tr.GetContent() != "out" || !tr.GetIsError() {
			t.Errorf("got %+v, want ToolResult{c2,out,err}", got)
		}
	})

	t.Run("empty part falls back to an empty text part, never nil", func(t *testing.T) {
		got := toContentPart(llm.ContentPart{})
		if got == nil || got.GetText() == nil || got.GetText().GetText() != "" {
			t.Errorf("got %+v, want safe empty TextPart fallback", got)
		}
	})
}

// ---- toMessage / toToolDefinition / toGenerationParams ---------------------------

func TestToMessage(t *testing.T) {
	got := toMessage(llm.Message{
		Role: llm.RoleAssistant,
		Content: []llm.ContentPart{
			{Text: &llm.TextPart{Text: "a"}},
			{ToolCall: &llm.ToolCall{ID: "c1", Name: "t"}},
		},
	})
	if got.GetRole() != genproto.Role_ROLE_ASSISTANT {
		t.Errorf("Role = %v, want ASSISTANT", got.GetRole())
	}
	if len(got.GetContent()) != 2 || got.GetContent()[0].GetText() == nil || got.GetContent()[1].GetToolCall() == nil {
		t.Errorf("Content = %+v, want [Text, ToolCall] in order", got.GetContent())
	}
}

func TestToToolDefinition(t *testing.T) {
	got := toToolDefinition(llm.ToolDef{
		Name:        "write",
		Description: "writes a file",
		JSONSchema:  []byte(`{"type":"object"}`),
	})
	if got.GetName() != "write" || got.GetDescription() != "writes a file" || got.GetJsonSchema() != `{"type":"object"}` {
		t.Errorf("got %+v", got)
	}
}

func TestToGenerationParams_FullRequest(t *testing.T) {
	temp := 0.3
	req := llm.Request{
		Model:  "claude-3-5-sonnet-20241022",
		System: "be terse",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}},
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "yo"}}}},
		},
		Tools:        []llm.ToolDef{{Name: "write", JSONSchema: []byte(`{}`)}},
		ToolChoice:   llm.ToolChoice("write"),
		MaxTokens:    512,
		Temperature:  &temp,
		Stream:       true,
		ProviderRaw:  []byte(`{"prev":1}`),
		OutputSchema: []byte(`{"type":"object"}`),
		Strict:       true,
	}

	p := toGenerationParams(req)

	if p.GetModel() != req.Model || p.GetSystem() != req.System {
		t.Errorf("Model/System = %q/%q", p.GetModel(), p.GetSystem())
	}
	if p.GetMaxTokens() != 512 || !p.GetStream() || !p.GetStrict() {
		t.Errorf("MaxTokens/Stream/Strict = %d/%v/%v", p.GetMaxTokens(), p.GetStream(), p.GetStrict())
	}
	if string(p.GetProviderRaw()) != `{"prev":1}` || string(p.GetOutputSchema()) != `{"type":"object"}` {
		t.Errorf("ProviderRaw/OutputSchema = %s/%s", p.GetProviderRaw(), p.GetOutputSchema())
	}
	// Temperature must be copied (not aliased) and preserve the set-vs-unset bit.
	if p.Temperature == nil || p.GetTemperature() != 0.3 {
		t.Errorf("Temperature = %v, want 0.3", p.Temperature)
	}
	if p.Temperature == &temp {
		t.Error("Temperature pointer must be a copy, not alias the request's")
	}
	if len(p.GetMessages()) != 2 || p.GetMessages()[0].GetRole() != genproto.Role_ROLE_USER ||
		p.GetMessages()[1].GetRole() != genproto.Role_ROLE_ASSISTANT {
		t.Errorf("Messages = %+v", p.GetMessages())
	}
	if len(p.GetTools()) != 1 || p.GetTools()[0].GetName() != "write" {
		t.Errorf("Tools = %+v", p.GetTools())
	}
	if p.GetToolChoice() != genproto.ToolChoice_TOOL_CHOICE_TOOL || p.GetToolName() != "write" {
		t.Errorf("ToolChoice/ToolName = %v/%q", p.GetToolChoice(), p.GetToolName())
	}
}

func TestToGenerationParams_UnsetTemperatureStaysNil(t *testing.T) {
	p := toGenerationParams(llm.Request{Model: "m"})
	if p.Temperature != nil {
		t.Errorf("Temperature = %v, want nil (unset must stay distinct from 0.0)", p.Temperature)
	}
}

// ---- mapGRPCError -----------------------------------------------------------------

func TestMapGRPCError(t *testing.T) {
	t.Run("nil passes through", func(t *testing.T) {
		if got := mapGRPCError(nil); got != nil {
			t.Errorf("mapGRPCError(nil) = %v, want nil", got)
		}
	})

	t.Run("non-status error wraps as retryable ErrServer preserving Raw", func(t *testing.T) {
		in := errors.New("conn reset")
		got := mapGRPCError(in)
		var pe *llm.ProviderError
		if !errors.As(got, &pe) {
			t.Fatalf("got %T, want *llm.ProviderError", got)
		}
		if pe.Kind != llm.ErrServer {
			t.Errorf("Kind = %q, want %q", pe.Kind, llm.ErrServer)
		}
		if !errors.Is(pe.Raw, in) {
			t.Errorf("Raw = %v, want the original error preserved", pe.Raw)
		}
	})

	// Status-code table per architecture §4.4 retryability semantics.
	cases := []struct {
		code codes.Code
		want llm.ErrorKind
	}{
		{codes.ResourceExhausted, llm.ErrRateLimited},
		{codes.Unauthenticated, llm.ErrAuth},
		{codes.PermissionDenied, llm.ErrAuth},
		{codes.InvalidArgument, llm.ErrInvalidRequest},
		{codes.FailedPrecondition, llm.ErrInvalidRequest},
		{codes.Unimplemented, llm.ErrUnsupported},
		{codes.DeadlineExceeded, llm.ErrTimeout},
		// Everything else is a retryable server error.
		{codes.Unavailable, llm.ErrServer},
		{codes.Internal, llm.ErrServer},
		{codes.Unknown, llm.ErrServer},
		{codes.Aborted, llm.ErrServer},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			got := mapGRPCError(status.Error(tc.code, "x"))
			var pe *llm.ProviderError
			if !errors.As(got, &pe) {
				t.Fatalf("got %T, want *llm.ProviderError", got)
			}
			if pe.Kind != tc.want {
				t.Errorf("Kind = %q, want %q", pe.Kind, tc.want)
			}
		})
	}
}

// ---- normalize.go gap branches ------------------------------------------------------

func TestNormalizeDone_NilAndZero(t *testing.T) {
	got := normalizeDone(nil)
	if got == nil {
		t.Fatal("normalizeDone(nil) must return a zero *llm.Done, not nil")
	}
	if got.StopReason != "" || got.RawStopReason != "" || got.Usage != (llm.Usage{}) || got.ProviderRaw != nil {
		t.Errorf("normalizeDone(nil) = %+v, want zero", *got)
	}

	// Done with no usage payload: usage normalizes to zero, not a panic.
	got = normalizeDone(&genproto.Done{StopReason: genproto.StopReason_STOP_REASON_END})
	if got.StopReason != llm.StopEnd || got.Usage != (llm.Usage{}) {
		t.Errorf("normalizeDone(no usage) = %+v", got)
	}
}

func TestNormalizeCapabilities_Nil(t *testing.T) {
	if got := normalizeCapabilities(nil); got != (llm.Capabilities{}) {
		t.Errorf("normalizeCapabilities(nil) = %+v, want zero", got)
	}
}

// TestNormalizeEvent_NilOneofInner asserts a oneof wrapper whose inner message is
// nil (a malformed but decodable frame) normalizes to the zero event, never a
// panic — the stream reader feeds these straight from the wire.
func TestNormalizeEvent_NilOneofInner(t *testing.T) {
	cases := []struct {
		name string
		in   *genproto.StreamEvent
	}{
		{"text delta", &genproto.StreamEvent{Event: &genproto.StreamEvent_TextDelta{}}},
		{"thinking delta", &genproto.StreamEvent{Event: &genproto.StreamEvent_ThinkingDelta{}}},
		{"tool call delta", &genproto.StreamEvent{Event: &genproto.StreamEvent_ToolCallDelta{}}},
		{"done", &genproto.StreamEvent{Event: &genproto.StreamEvent_Done{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeEvent(tc.in)
			if got.TextDelta != nil || got.ThinkingDelta != nil || got.ToolCallDelta != nil || got.Done != nil {
				t.Errorf("normalizeEvent(%s with nil inner) = %+v, want zero event", tc.name, got)
			}
		})
	}
}

// ---- assembleResponse error paths -----------------------------------------------

// scriptedReader is a minimal llm.StreamReader: it yields the scripted events
// in order, then the terminal error (io.EOF for a clean end).
type scriptedReader struct {
	events []llm.StreamEvent
	errAt  error
}

func (r *scriptedReader) Recv() (llm.StreamEvent, error) {
	if len(r.events) == 0 {
		return llm.StreamEvent{}, r.errAt
	}
	ev := r.events[0]
	r.events = r.events[1:]
	return ev, nil
}

func (r *scriptedReader) Close() error { return nil }

// TestAssembleResponse_NoDoneIsError asserts the no-Done guard: a stream that
// ends cleanly (io.EOF) without a terminal Done is a protocol violation and must
// fail rather than fabricate a Response with a zero stop reason.
func TestAssembleResponse_NoDoneIsError(t *testing.T) {
	r := &scriptedReader{
		events: []llm.StreamEvent{{TextDelta: &llm.TextDelta{Text: "half"}}},
		errAt:  io.EOF,
	}
	resp, err := assembleResponse(r)
	if err == nil {
		t.Fatalf("expected error for a Done-less stream, got resp %+v", resp)
	}
	if !strings.Contains(err.Error(), "without a Done event") {
		t.Errorf("err = %v, want the no-Done diagnostic", err)
	}
}

// TestAssembleResponse_MidStreamErrorPropagates asserts a non-EOF reader error
// aborts assembly and is returned verbatim (the retry layer above decides).
func TestAssembleResponse_MidStreamErrorPropagates(t *testing.T) {
	in := &llm.ProviderError{Kind: llm.ErrOverloaded}
	r := &scriptedReader{
		events: []llm.StreamEvent{{TextDelta: &llm.TextDelta{Text: "x"}}},
		errAt:  in,
	}
	_, err := assembleResponse(r)
	if !errors.Is(err, in) {
		t.Errorf("err = %v, want the reader's error propagated", err)
	}
}

// TestAssembleResponse_LateNamedFragments asserts fragment accumulation by
// CallID: an unnamed leading fragment is keyed correctly, the name is filled by
// a later fragment, args concatenate across fragments, and first-seen call
// order is preserved.
func TestAssembleResponse_LateNamedFragments(t *testing.T) {
	r := &scriptedReader{
		events: []llm.StreamEvent{
			{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", ArgsFragment: []byte(`{"pa`)}},
			{ToolCallDelta: &llm.ToolCallDelta{CallID: "c2", Name: "second", ArgsFragment: []byte(`{}`)}},
			{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", Name: "first", ArgsFragment: []byte(`th":"/x"}`)}},
			{Done: &llm.Done{StopReason: llm.StopToolUse}},
		},
		errAt: io.EOF,
	}
	resp, err := assembleResponse(r)
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if len(resp.Content) != 2 || resp.Content[0].ToolCall == nil || resp.Content[1].ToolCall == nil {
		t.Fatalf("Content = %+v, want two tool calls", resp.Content)
	}
	c1, c2 := resp.Content[0].ToolCall, resp.Content[1].ToolCall
	if c1.ID != "c1" || c1.Name != "first" {
		t.Errorf("call 1 = %+v, want id c1 with late-filled name", c1)
	}
	if c1.Args["path"] != "/x" {
		t.Errorf("call 1 args = %v, want fragments concatenated into path=/x", c1.Args)
	}
	if c2.ID != "c2" || c2.Name != "second" {
		t.Errorf("call 2 = %+v (first-seen order must be preserved)", c2)
	}
}

// ---- parseArgs (tool-call argument assembly) ----------------------------------------

func TestParseArgs(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want map[string]any
	}{
		{"empty is nil", nil, nil},
		{"malformed is nil (raw survives via ProviderRaw)", []byte(`{"broken`), nil},
		{"non-object is nil", []byte(`[1]`), nil},
		{"valid object parses", []byte(`{"x":42}`), map[string]any{"x": float64(42)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseArgs(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("parseArgs(%s) = %v, want %v", tc.in, got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("parseArgs(%s)[%q] = %v, want %v", tc.in, k, got[k], v)
				}
			}
		})
	}
}
