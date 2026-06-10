package openai

import (
	"encoding/json"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// marshalParams renders the built Responses params to a generic map so the wire
// shape can be asserted without a live endpoint.
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

func TestBuildParams_SystemBecomesInstructions(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model:  "gpt-5",
		System: "you are helpful",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}},
		},
	})
	if m["instructions"] != "you are helpful" {
		t.Fatalf("system prompt must map to instructions, got %v", m["instructions"])
	}
	// Stateless: store must be false.
	if m["store"] != false {
		t.Fatalf("store must be false for stateless Item-passing, got %v", m["store"])
	}
}

func TestBuildParams_FlatFunctionTools(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
		Tools: []llm.ToolDef{{Name: "search", Description: "find things", JSONSchema: schema}},
	})
	tools, ok := m["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("want 1 tool, got %#v", m["tools"])
	}
	tool := tools[0].(map[string]any)
	// Responses function tools are FLAT: name/parameters at the top level, not
	// nested under a "function" key.
	if tool["type"] != "function" {
		t.Fatalf("tool type must be function, got %v", tool["type"])
	}
	if tool["name"] != "search" {
		t.Fatalf("flat function tool must carry name at top level, got %#v", tool)
	}
	if tool["description"] != "find things" {
		t.Fatalf("description wrong: %v", tool["description"])
	}
	if _, nested := tool["function"]; nested {
		t.Fatalf("Responses function tool must be flat, not nested under 'function': %#v", tool)
	}
	params := tool["parameters"].(map[string]any)
	if params["type"] != "object" {
		t.Fatalf("parameters schema not carried through: %#v", params)
	}
}

func TestBuildParams_TypedFunctionCallAndOutputItems(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
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
	input, ok := m["input"].([]any)
	if !ok {
		t.Fatalf("input must be an item list, got %T", m["input"])
	}
	// user message, function_call item, function_call_output item.
	var sawFuncCall, sawFuncOutput bool
	for _, it := range input {
		item := it.(map[string]any)
		switch item["type"] {
		case "function_call":
			sawFuncCall = true
			if item["call_id"] != "c1" || item["name"] != "get_weather" {
				t.Fatalf("function_call item wrong: %#v", item)
			}
			argStr, isStr := item["arguments"].(string)
			if !isStr {
				t.Fatalf("function_call arguments must be a JSON string, got %T", item["arguments"])
			}
			var parsed map[string]any
			if err := json.Unmarshal([]byte(argStr), &parsed); err != nil || parsed["city"] != "Paris" {
				t.Fatalf("function_call arguments wrong: %q", argStr)
			}
		case "function_call_output":
			sawFuncOutput = true
			if item["call_id"] != "c1" {
				t.Fatalf("function_call_output call_id wrong: %#v", item)
			}
		}
	}
	if !sawFuncCall {
		t.Fatalf("expected a typed function_call input item; input=%#v", input)
	}
	if !sawFuncOutput {
		t.Fatalf("expected a typed function_call_output input item; input=%#v", input)
	}
}

// TestBuildParams_StatelessContinuationReplay asserts a prior-turn continuation
// blob (Responses Items) is replayed as leading input items, proving stateless
// continuation without previous_response_id.
func TestBuildParams_StatelessContinuationReplay(t *testing.T) {
	// Build a continuation blob as the normalizer would emit it.
	blob, err := json.Marshal(continuationState{
		Surface: surfaceResponses,
		Items: []continuationItem{
			{Type: itemTypeMessage, Text: "prior assistant text"},
			{Type: itemTypeFunctionCall, CallID: "pc1", Name: "prior_tool", Arguments: `{"x":1}`},
		},
	})
	if err != nil {
		t.Fatalf("marshal blob: %v", err)
	}

	m := marshalParams(t, llm.Request{
		Model:       "gpt-5",
		ProviderRaw: blob,
		Messages: []llm.Message{
			{Role: llm.RoleTool, Content: []llm.ContentPart{{ToolResult: &llm.ToolResult{CallID: "pc1", Content: "result"}}}},
		},
	})

	input := m["input"].([]any)
	// The first items must be the replayed continuation items, then the new tool
	// result. There must be NO previous_response_id.
	if _, ok := m["previous_response_id"]; ok {
		t.Fatalf("previous_response_id must never be set (stateless Item-passing)")
	}
	if len(input) < 3 {
		t.Fatalf("want >=3 input items (2 replayed + 1 new), got %d: %#v", len(input), input)
	}
	first := input[0].(map[string]any)
	if first["role"] != "assistant" {
		t.Fatalf("first replayed item should be the assistant message, got %#v", first)
	}
	second := input[1].(map[string]any)
	if second["type"] != "function_call" || second["call_id"] != "pc1" {
		t.Fatalf("second replayed item should be the prior function_call, got %#v", second)
	}
}

func TestBuildParams_ForeignContinuationIgnored(t *testing.T) {
	// A continuation blob from another surface must be ignored, not replayed.
	blob := json.RawMessage(`{"surface":"openai.chat","text":"x"}`)
	m := marshalParams(t, llm.Request{
		Model:       "gpt-5",
		ProviderRaw: blob,
		Messages:    []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
	})
	input := m["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("foreign continuation must be ignored; want only the 1 user item, got %d", len(input))
	}
}

func TestBuildParams_ToolChoiceTable(t *testing.T) {
	cases := []struct {
		in       llm.ToolChoice
		wantMode string // expected tool_choice when a simple mode; "" => function name
	}{
		{llm.ToolChoiceAuto, "auto"},
		{llm.ToolChoiceNone, "none"},
		{llm.ToolChoiceAny, "required"},
		{llm.ToolChoiceRequired, "required"},
	}
	for _, c := range cases {
		m := marshalParams(t, llm.Request{Model: "gpt-5", ToolChoice: c.in})
		if m["tool_choice"] != c.wantMode {
			t.Errorf("ToolChoice %q => tool_choice %v, want %q", c.in, m["tool_choice"], c.wantMode)
		}
	}
	// A specific function name.
	m := marshalParams(t, llm.Request{Model: "gpt-5", ToolChoice: llm.ToolChoice("my_func")})
	tc, ok := m["tool_choice"].(map[string]any)
	if !ok || tc["type"] != "function" || tc["name"] != "my_func" {
		t.Fatalf("specific tool choice wrong: %#v", m["tool_choice"])
	}
}

func TestBuildParams_BadToolSchema_InvalidRequest(t *testing.T) {
	_, err := buildParams(llm.Request{
		Model: "gpt-5",
		Tools: []llm.ToolDef{{Name: "bad", JSONSchema: json.RawMessage(`{nope`)}},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

func TestBuildParams_MaxTokensAndTemperature(t *testing.T) {
	temp := 0.4
	m := marshalParams(t, llm.Request{Model: "gpt-5", MaxTokens: 256, Temperature: &temp})
	if m["max_output_tokens"] != float64(256) {
		t.Fatalf("max_output_tokens not set: %v", m["max_output_tokens"])
	}
	if m["temperature"] != 0.4 {
		t.Fatalf("temperature not set: %v", m["temperature"])
	}
	// Zero values must leave the provider defaults in place.
	m = marshalParams(t, llm.Request{Model: "gpt-5"})
	if _, ok := m["max_output_tokens"]; ok {
		t.Fatalf("max_output_tokens must be omitted when unset: %v", m["max_output_tokens"])
	}
	if _, ok := m["temperature"]; ok {
		t.Fatalf("temperature must be omitted when unset: %v", m["temperature"])
	}
}

func TestBuildParams_MalformedContinuationBlob_InvalidRequest(t *testing.T) {
	// A truncated blob is a decode failure, unlike a well-formed foreign-surface
	// blob which is silently ignored.
	_, err := buildParams(llm.Request{Model: "gpt-5", ProviderRaw: json.RawMessage(`{"surface":`)})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

// TestBuildParams_ContinuationItemEdgeCases pins the replay rules for degenerate
// continuation items: empty assistant text and id-less reasoning are dropped,
// empty function-call arguments are defaulted to "{}", and unknown item types are
// skipped rather than failing the request.
func TestBuildParams_ContinuationItemEdgeCases(t *testing.T) {
	blob, err := json.Marshal(continuationState{
		Surface: surfaceResponses,
		Items: []continuationItem{
			{Type: itemTypeMessage, Text: ""},
			{Type: itemTypeFunctionCall, CallID: "c1", Name: "tool"},
			{Type: itemTypeReasoning, ID: "rs_1"},
			{Type: itemTypeReasoning},
			{Type: "web_search_call"},
		},
	})
	if err != nil {
		t.Fatalf("marshal blob: %v", err)
	}
	m := marshalParams(t, llm.Request{Model: "gpt-5", ProviderRaw: blob})
	input, ok := m["input"].([]any)
	if !ok || len(input) != 2 {
		t.Fatalf("want 2 replayed items (function_call + reasoning), got %#v", m["input"])
	}
	fc := input[0].(map[string]any)
	if fc["type"] != "function_call" || fc["call_id"] != "c1" {
		t.Fatalf("first replayed item wrong: %#v", fc)
	}
	if fc["arguments"] != "{}" {
		t.Fatalf("empty function-call arguments must default to {}, got %v", fc["arguments"])
	}
	rs := input[1].(map[string]any)
	if rs["type"] != "reasoning" || rs["id"] != "rs_1" {
		t.Fatalf("second replayed item must be the reasoning item, got %#v", rs)
	}
}

func TestConvertMessage_UnsupportedRole_InvalidRequest(t *testing.T) {
	_, err := buildParams(llm.Request{
		Model:    "gpt-5",
		Messages: []llm.Message{{Role: llm.Role("system")}},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

// TestConvertMessage_AssistantTextAndToolCall asserts a mixed assistant turn maps
// to TWO input items: the text message followed by the typed function_call.
func TestConvertMessage_AssistantTextAndToolCall(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{Text: &llm.TextPart{Text: "let me check"}},
				{ToolCall: &llm.ToolCall{ID: "c9", Name: "calc", Args: map[string]any{"n": float64(1)}}},
			}},
		},
	})
	input := m["input"].([]any)
	if len(input) != 2 {
		t.Fatalf("want assistant message + function_call, got %d items: %#v", len(input), input)
	}
	first := input[0].(map[string]any)
	if first["role"] != "assistant" {
		t.Fatalf("first item must be the assistant text message, got %#v", first)
	}
	second := input[1].(map[string]any)
	if second["type"] != "function_call" || second["call_id"] != "c9" || second["name"] != "calc" {
		t.Fatalf("second item must be the function_call, got %#v", second)
	}
}

// TestConvertMessage_NilToolArgs_EmptyObject asserts a tool call with nil parsed
// args replays as an empty JSON object, never an empty string or "null".
func TestConvertMessage_NilToolArgs_EmptyObject(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: "c1", Name: "noargs"}},
			}},
		},
	})
	input := m["input"].([]any)
	fc := input[0].(map[string]any)
	if fc["arguments"] != "{}" {
		t.Fatalf("nil args must marshal to {}, got %v", fc["arguments"])
	}
}

// TestConvertMessage_ToolMessageSkipsNonResultParts asserts stray non-result parts
// in a tool turn are dropped rather than failing or producing bogus items.
func TestConvertMessage_ToolMessageSkipsNonResultParts(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
		Messages: []llm.Message{
			{Role: llm.RoleTool, Content: []llm.ContentPart{
				{Text: &llm.TextPart{Text: "stray text"}},
				{ToolResult: &llm.ToolResult{CallID: "c1", Content: "ok"}},
			}},
		},
	})
	input := m["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("want only the function_call_output item, got %d: %#v", len(input), input)
	}
	if item := input[0].(map[string]any); item["type"] != "function_call_output" {
		t.Fatalf("tool turn item wrong: %#v", item)
	}
}

// TestBuildParams_ToolWithoutSchema_EmptyObject asserts a tool with no declared
// JSON Schema still sends a valid empty parameters object.
func TestBuildParams_ToolWithoutSchema_EmptyObject(t *testing.T) {
	m := marshalParams(t, llm.Request{
		Model: "gpt-5",
		Tools: []llm.ToolDef{{Name: "noparams"}},
	})
	tools := m["tools"].([]any)
	tool := tools[0].(map[string]any)
	params, ok := tool["parameters"].(map[string]any)
	if !ok || len(params) != 0 {
		t.Fatalf("schema-less tool must carry an empty parameters object, got %#v", tool["parameters"])
	}
}

func TestConvertMessage_UnmarshalableToolArgs_InvalidRequest(t *testing.T) {
	// A func value cannot be marshaled to JSON; the failure must be reported as a
	// non-retryable invalid request, not bubble up as a raw marshal error.
	_, err := buildParams(llm.Request{
		Model: "gpt-5",
		Messages: []llm.Message{
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{
				{ToolCall: &llm.ToolCall{ID: "c1", Name: "bad", Args: map[string]any{"f": func() {}}}},
			}},
		},
	})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}
