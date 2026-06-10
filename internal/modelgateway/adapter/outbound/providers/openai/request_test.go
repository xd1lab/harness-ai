package openai

import (
	"encoding/json"
	"testing"

	"github.com/boltrope/boltrope/internal/platform/llm"
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
