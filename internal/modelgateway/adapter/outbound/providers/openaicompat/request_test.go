package openaicompat

import (
	"encoding/json"
	"testing"

	"github.com/openai/openai-go/v3/packages/param"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// marshalParams renders the built params to JSON so the wire shape can be asserted
// without a live endpoint.
func marshalParams(t *testing.T, req llm.Request) map[string]any {
	t.Helper()
	params, err := buildParams(req)
	if err != nil {
		t.Fatalf("buildParams: %v", err)
	}
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	return m
}

func TestBuildParams_SystemMessageIsLeadingSystemRole(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model:  "llama3",
		System: "you are helpful",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}},
		},
	})
	msgs, ok := m["messages"].([]any)
	if !ok || len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %#v", m["messages"])
	}
	first := msgs[0].(map[string]any)
	if first["role"] != "system" {
		t.Fatalf("first message must be system role, got %v", first["role"])
	}
	if first["content"] != "you are helpful" {
		t.Fatalf("system content wrong: %v", first["content"])
	}
	second := msgs[1].(map[string]any)
	if second["role"] != "user" || second["content"] != "hi" {
		t.Fatalf("second message wrong: %#v", second)
	}
}

func TestBuildParams_FunctionWrappedTools(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}},"required":["q"]}`)
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Tools: []llm.ToolDef{{Name: "search", Description: "search the web", JSONSchema: schema}},
	})
	tools, ok := m["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("want 1 tool, got %#v", m["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["type"] != "function" {
		t.Fatalf("tool must be function-wrapped, got type %v", tool["type"])
	}
	fn := tool["function"].(map[string]any)
	if fn["name"] != "search" || fn["description"] != "search the web" {
		t.Fatalf("function name/description wrong: %#v", fn)
	}
	params := fn["parameters"].(map[string]any)
	if params["type"] != "object" {
		t.Fatalf("parameters schema not carried through: %#v", params)
	}
}

func TestBuildParams_AssistantToolCallsAndToolResults(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "weather?"}}}},
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: "c1", Name: "get_weather", Args: map[string]any{"city": "Paris"}}},
			}},
			{Role: llm.RoleTool, Content: []llm.ContentPart{
				{ToolResult: &llm.ToolResult{CallID: "c1", Content: "sunny"}},
			}},
		},
	})
	msgs := m["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("want 3 messages, got %d", len(msgs))
	}
	assistant := msgs[1].(map[string]any)
	if assistant["role"] != "assistant" {
		t.Fatalf("msg 1 must be assistant, got %v", assistant["role"])
	}
	tcs := assistant["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("want 1 tool_call, got %d", len(tcs))
	}
	tc := tcs[0].(map[string]any)
	if tc["id"] != "c1" {
		t.Fatalf("tool_call id wrong: %v", tc["id"])
	}
	fn := tc["function"].(map[string]any)
	if fn["name"] != "get_weather" {
		t.Fatalf("tool_call function name wrong: %v", fn["name"])
	}
	// arguments must be a JSON STRING, not an object.
	argStr, ok := fn["arguments"].(string)
	if !ok {
		t.Fatalf("tool_call arguments must be a JSON string, got %T", fn["arguments"])
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(argStr), &parsed); err != nil || parsed["city"] != "Paris" {
		t.Fatalf("tool_call arguments string wrong: %q (%v)", argStr, err)
	}
	toolMsg := msgs[2].(map[string]any)
	if toolMsg["role"] != "tool" || toolMsg["tool_call_id"] != "c1" || toolMsg["content"] != "sunny" {
		t.Fatalf("tool result message wrong: %#v", toolMsg)
	}
}

func TestConvertToolChoice_Table(t *testing.T) {
	cases := []struct {
		in       llm.ToolChoice
		wantSet  bool
		wantJSON string // substring expected in marshaled choice when set
	}{
		{"", false, ""},
		{llm.ToolChoiceAuto, true, `"auto"`},
		{llm.ToolChoiceNone, true, `"none"`},
		{llm.ToolChoiceAny, true, `"required"`},
		{llm.ToolChoiceRequired, true, `"required"`},
		{llm.ToolChoice("my_func"), true, `"my_func"`},
	}
	for _, c := range cases {
		got, ok := convertToolChoice(c.in)
		if ok != c.wantSet {
			t.Errorf("convertToolChoice(%q) set=%v, want %v", c.in, ok, c.wantSet)
			continue
		}
		if !c.wantSet {
			continue
		}
		b, err := json.Marshal(got)
		if err != nil {
			t.Errorf("marshal choice %q: %v", c.in, err)
			continue
		}
		if !containsJSON(string(b), c.wantJSON) {
			t.Errorf("convertToolChoice(%q) = %s, want substring %s", c.in, b, c.wantJSON)
		}
	}
}

// TestConvertMessage_NilToolArgs_EmptyObject asserts a tool call with nil parsed
// args replays as an empty JSON object, never an empty string or "null".
func TestConvertMessage_NilToolArgs_EmptyObject(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: "c1", Name: "noargs"}},
			}},
		},
	})
	msgs := m["messages"].([]any)
	tcs := msgs[0].(map[string]any)["tool_calls"].([]any)
	fn := tcs[0].(map[string]any)["function"].(map[string]any)
	if fn["arguments"] != "{}" {
		t.Fatalf("nil args must marshal to {}, got %v", fn["arguments"])
	}
}

// TestConvertMessage_ToolMessageSkipsNonResultParts asserts stray non-result parts
// in a tool turn are dropped rather than failing or producing bogus messages.
func TestConvertMessage_ToolMessageSkipsNonResultParts(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Messages: []llm.Message{
			{Role: llm.RoleTool, Content: []llm.ContentPart{
				{Text: &llm.TextPart{Text: "stray text"}},
				{ToolResult: &llm.ToolResult{CallID: "c1", Content: "ok"}},
			}},
		},
	})
	msgs := m["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("want only the tool message, got %d: %#v", len(msgs), msgs)
	}
	if msg := msgs[0].(map[string]any); msg["role"] != "tool" || msg["tool_call_id"] != "c1" {
		t.Fatalf("tool message wrong: %#v", msg)
	}
}

// TestBuildParams_ToolWithoutSchema_EmptyObject asserts a tool with no declared
// JSON Schema still sends a valid empty parameters object.
func TestBuildParams_ToolWithoutSchema_EmptyObject(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Tools: []llm.ToolDef{{Name: "noparams"}},
	})
	tools := m["tools"].([]any)
	fn := tools[0].(map[string]any)["function"].(map[string]any)
	params, ok := fn["parameters"].(map[string]any)
	if !ok || len(params) != 0 {
		t.Fatalf("schema-less tool must carry an empty parameters object, got %#v", fn["parameters"])
	}
}

// TestBuildParams_ToolChoiceWired asserts the converted tool_choice actually lands
// on the request params (the union conversion itself is covered by the table).
func TestBuildParams_ToolChoiceWired(t *testing.T) {
	m := marshalParams(t, llm.Request{Model: "llama3", ToolChoice: llm.ToolChoiceAuto})
	if m["tool_choice"] != "auto" {
		t.Fatalf("tool_choice not wired through buildParams, got %v", m["tool_choice"])
	}
}

func TestBuildParams_TemperatureAndMaxTokens(t *testing.T) {
	temp := 0.4
	params, err := buildParams(llm.Request{Model: "llama3", MaxTokens: 256, Temperature: &temp})
	if err != nil {
		t.Fatalf("buildParams: %v", err)
	}
	if params.MaxCompletionTokens != param.NewOpt(int64(256)) {
		t.Fatalf("max_completion_tokens not set: %#v", params.MaxCompletionTokens)
	}
	if params.Temperature != param.NewOpt(0.4) {
		t.Fatalf("temperature not set: %#v", params.Temperature)
	}
}

func TestBuildParams_BadToolSchema_InvalidRequest(t *testing.T) {
	_, err := buildParams(llm.Request{
		Model: "llama3",
		Tools: []llm.ToolDef{{Name: "bad", JSONSchema: json.RawMessage(`{not json`)}},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

func TestConvertMessage_UnsupportedRole_InvalidRequest(t *testing.T) {
	_, err := buildParams(llm.Request{
		Model:    "llama3",
		Messages: []llm.Message{{Role: llm.Role("system")}},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

// TestConvertMessage_AssistantTextAndToolCall asserts a mixed assistant turn maps
// to ONE assistant message carrying both the text content and the tool_calls —
// Chat Completions models the whole turn as a single message, unlike Responses.
func TestConvertMessage_AssistantTextAndToolCall(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "llama3",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{Text: &llm.TextPart{Text: "let me check"}},
				{ToolCall: &llm.ToolCall{ID: "c9", Name: "calc", Args: map[string]any{"n": float64(1)}}},
			}},
		},
	})
	msgs := m["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("mixed assistant turn must be one message, got %d: %#v", len(msgs), msgs)
	}
	assistant := msgs[0].(map[string]any)
	if assistant["role"] != "assistant" || assistant["content"] != "let me check" {
		t.Fatalf("assistant text wrong: %#v", assistant)
	}
	tcs, ok := assistant["tool_calls"].([]any)
	if !ok || len(tcs) != 1 {
		t.Fatalf("assistant tool_calls wrong: %#v", assistant["tool_calls"])
	}
}

func TestConvertMessage_UnmarshalableToolArgs_InvalidRequest(t *testing.T) {
	// A func value cannot be marshaled to JSON; the failure must be reported as a
	// non-retryable invalid request, not bubble up as a raw marshal error.
	_, err := buildParams(llm.Request{
		Model: "llama3",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: "c1", Name: "bad", Args: map[string]any{"f": func() {}}}},
			}},
		},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

func containsJSON(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) > 0 && indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
