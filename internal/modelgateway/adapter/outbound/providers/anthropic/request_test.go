package anthropic

import (
	"encoding/json"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// marshalParams renders the SDK params to their wire JSON so tests can assert the
// request shape the adapter produces (the param structs are otherwise opaque).
func marshalParams(t *testing.T, p sdk.MessageNewParams) map[string]any {
	t.Helper()
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	return m
}

// TestBuildMessageParams_SystemTopLevel asserts System maps to the top-level
// system parameter, not a message.
func TestBuildMessageParams_SystemTopLevel(t *testing.T) {
	req := llm.Request{
		Model:    "claude-opus-4-8",
		System:   "You are helpful.",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
	}
	params, err := buildMessageParams(req, 4096, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	m := marshalParams(t, params)
	sys, ok := m["system"]
	if !ok {
		t.Fatal("system not present at top level")
	}
	// system renders as an array of text blocks.
	arr, ok := sys.([]any)
	if !ok || len(arr) == 0 {
		t.Fatalf("system = %T %v, want non-empty array", sys, sys)
	}
	block := arr[0].(map[string]any)
	if block["text"] != "You are helpful." {
		t.Errorf("system text = %v, want 'You are helpful.'", block["text"])
	}
	if m["model"] != "claude-opus-4-8" {
		t.Errorf("model = %v, want claude-opus-4-8", m["model"])
	}
}

// TestBuildMessageParams_DefaultMaxTokens asserts a zero MaxTokens is replaced by
// the supplied default (the API rejects max_tokens <= 0).
func TestBuildMessageParams_DefaultMaxTokens(t *testing.T) {
	req := llm.Request{
		Model:    "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
	}
	params, err := buildMessageParams(req, 1234, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	if params.MaxTokens != 1234 {
		t.Errorf("MaxTokens = %d, want default 1234", params.MaxTokens)
	}

	req.MaxTokens = 999
	params, err = buildMessageParams(req, 1234, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	if params.MaxTokens != 999 {
		t.Errorf("MaxTokens = %d, want explicit 999", params.MaxTokens)
	}
}

// TestBuildMessageParams_Tools asserts a tool maps to {name, description,
// input_schema} with properties/required lifted and additionalProperties
// preserved.
func TestBuildMessageParams_Tools(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"],"additionalProperties":false}`)
	req := llm.Request{
		Model:    "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
		Tools:    []llm.ToolDef{{Name: "get_weather", Description: "Look up weather", JSONSchema: schema}},
	}
	params, err := buildMessageParams(req, 4096, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	m := marshalParams(t, params)
	tools, ok := m["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v, want one tool", m["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["name"] != "get_weather" {
		t.Errorf("tool name = %v, want get_weather", tool["name"])
	}
	if tool["description"] != "Look up weather" {
		t.Errorf("tool description = %v, want 'Look up weather'", tool["description"])
	}
	is, ok := tool["input_schema"].(map[string]any)
	if !ok {
		t.Fatalf("input_schema = %v, want object", tool["input_schema"])
	}
	if is["type"] != "object" {
		t.Errorf("input_schema.type = %v, want object", is["type"])
	}
	props, ok := is["properties"].(map[string]any)
	if !ok || props["city"] == nil {
		t.Errorf("input_schema.properties = %v, want city property", is["properties"])
	}
	reqArr, ok := is["required"].([]any)
	if !ok || len(reqArr) != 1 || reqArr[0] != "city" {
		t.Errorf("input_schema.required = %v, want [city]", is["required"])
	}
	if is["additionalProperties"] != false {
		t.Errorf("input_schema.additionalProperties = %v, want false (preserved)", is["additionalProperties"])
	}
}

// TestBuildToolChoice covers the tool-choice translation table.
func TestBuildToolChoice(t *testing.T) {
	cases := []struct {
		in       llm.ToolChoice
		wantSet  bool
		wantType string // expected "type" in marshaled tool_choice; "" if unset
		wantName string
	}{
		{"", false, "", ""},
		{llm.ToolChoiceAuto, true, "auto", ""},
		{llm.ToolChoiceAny, true, "any", ""},
		{llm.ToolChoiceRequired, true, "any", ""},
		{llm.ToolChoiceNone, true, "none", ""},
		{llm.ToolChoice("get_weather"), true, "tool", "get_weather"},
	}
	for _, tc := range cases {
		t.Run(string(tc.in), func(t *testing.T) {
			got, ok := buildToolChoice(tc.in)
			if ok != tc.wantSet {
				t.Fatalf("set = %v, want %v", ok, tc.wantSet)
			}
			if !tc.wantSet {
				return
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("marshal tool_choice: %v", err)
			}
			var m map[string]any
			if err := json.Unmarshal(b, &m); err != nil {
				t.Fatalf("unmarshal tool_choice: %v", err)
			}
			if m["type"] != tc.wantType {
				t.Errorf("tool_choice type = %v, want %q", m["type"], tc.wantType)
			}
			if tc.wantName != "" && m["name"] != tc.wantName {
				t.Errorf("tool_choice name = %v, want %q", m["name"], tc.wantName)
			}
		})
	}
}

// TestBuildMessages_ToolResultIsUserTurn asserts a RoleTool turn becomes a user
// message carrying a tool_result block (Anthropic folds results into user turns).
func TestBuildMessages_ToolResultIsUserTurn(t *testing.T) {
	req := llm.Request{
		Model: "claude-opus-4-8",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "weather?"}}}},
			{Role: llm.RoleAssistant, Content: []llm.ContentPart{{ToolCall: &llm.ToolCall{ID: "toolu_1", Name: "get_weather", Args: map[string]any{"city": "Paris"}}}}},
			{Role: llm.RoleTool, Content: []llm.ContentPart{{ToolResult: &llm.ToolResult{CallID: "toolu_1", Content: "sunny"}}}},
		},
	}
	params, err := buildMessageParams(req, 4096, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	m := marshalParams(t, params)
	msgs := m["messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("messages = %d, want 3", len(msgs))
	}
	// Assistant turn carries a tool_use block.
	asst := msgs[1].(map[string]any)
	if asst["role"] != "assistant" {
		t.Errorf("msg[1] role = %v, want assistant", asst["role"])
	}
	asstContent := asst["content"].([]any)
	if asstContent[0].(map[string]any)["type"] != "tool_use" {
		t.Errorf("assistant content[0] type = %v, want tool_use", asstContent[0])
	}
	// Tool result turn is a user message with a tool_result block.
	toolTurn := msgs[2].(map[string]any)
	if toolTurn["role"] != "user" {
		t.Errorf("tool result turn role = %v, want user (folded)", toolTurn["role"])
	}
	tc := toolTurn["content"].([]any)[0].(map[string]any)
	if tc["type"] != "tool_result" {
		t.Errorf("tool result block type = %v, want tool_result", tc["type"])
	}
	if tc["tool_use_id"] != "toolu_1" {
		t.Errorf("tool_use_id = %v, want toolu_1", tc["tool_use_id"])
	}
}

// TestBuildMessageParams_Temperature asserts a nil Temperature is omitted and a
// set one is forwarded.
func TestBuildMessageParams_Temperature(t *testing.T) {
	base := llm.Request{
		Model:    "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
	}
	// nil -> absent
	params, _ := buildMessageParams(base, 4096, llm.Capabilities{})
	if _, present := marshalParams(t, params)["temperature"]; present {
		t.Error("temperature present when Request.Temperature is nil")
	}
	// set -> present
	temp := 0.5
	base.Temperature = &temp
	params, _ = buildMessageParams(base, 4096, llm.Capabilities{})
	if got := marshalParams(t, params)["temperature"]; got != 0.5 {
		t.Errorf("temperature = %v, want 0.5", got)
	}
}

// TestBuildCountTokensParams asserts the count_tokens request carries the system
// prompt (as a text-block array) and the tools, and omits generation-only fields.
func TestBuildCountTokensParams(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`)
	req := llm.Request{
		Model:    "claude-opus-4-8",
		System:   "sys",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
		Tools:    []llm.ToolDef{{Name: "search", Description: "search", JSONSchema: schema}},
	}
	params, err := buildCountTokensParams(req)
	if err != nil {
		t.Fatalf("buildCountTokensParams: %v", err)
	}
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal count-tokens params: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m["model"] != "claude-opus-4-8" {
		t.Errorf("model = %v, want claude-opus-4-8", m["model"])
	}
	// system serializes (string or array form both acceptable, just present).
	if _, ok := m["system"]; !ok {
		t.Error("system missing from count_tokens params")
	}
	tools, ok := m["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("tools = %v, want one tool", m["tools"])
	}
	if tools[0].(map[string]any)["name"] != "search" {
		t.Errorf("tool name = %v, want search", tools[0])
	}
	// max_tokens is generation-only and must not appear.
	if _, present := m["max_tokens"]; present {
		t.Error("max_tokens must not be present on count_tokens params")
	}
}

// TestBuildContentBlocks_Image asserts inline image data and image URLs map to
// Anthropic image blocks, and a file-ref-only image is rejected.
func TestBuildContentBlocks_Image(t *testing.T) {
	// Inline base64 image.
	inline := llm.Request{
		Model: "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{
			{Image: &llm.ImagePart{MediaType: "image/png", Data: []byte{0x89, 0x50, 0x4e, 0x47}}},
			{Text: &llm.TextPart{Text: "describe"}},
		}}},
	}
	params, err := buildMessageParams(inline, 4096, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams(inline image): %v", err)
	}
	m := marshalParams(t, params)
	content := m["messages"].([]any)[0].(map[string]any)["content"].([]any)
	img := content[0].(map[string]any)
	if img["type"] != "image" {
		t.Errorf("content[0] type = %v, want image", img["type"])
	}
	src := img["source"].(map[string]any)
	if src["type"] != "base64" || src["media_type"] != "image/png" || src["data"] == "" {
		t.Errorf("image source = %v, want base64/image-png with data", src)
	}

	// URL image.
	urlReq := llm.Request{
		Model: "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{
			{Image: &llm.ImagePart{URL: "https://example.test/a.png"}},
		}}},
	}
	params, err = buildMessageParams(urlReq, 4096, llm.Capabilities{})
	if err != nil {
		t.Fatalf("buildMessageParams(url image): %v", err)
	}
	m = marshalParams(t, params)
	src = m["messages"].([]any)[0].(map[string]any)["content"].([]any)[0].(map[string]any)["source"].(map[string]any)
	if src["type"] != "url" || src["url"] != "https://example.test/a.png" {
		t.Errorf("url image source = %v, want url type", src)
	}

	// File-ref-only image is unsupported and must error.
	fileRef := llm.Request{
		Model: "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{
			{Image: &llm.ImagePart{FileRef: "file_123"}},
		}}},
	}
	if _, err := buildMessageParams(fileRef, 4096, llm.Capabilities{}); err == nil {
		t.Error("expected an error for a file-ref-only image (unsupported)")
	}
}

// TestBuildMessages_UnknownRole asserts an unrecognized role is rejected rather
// than silently mapped.
func TestBuildMessages_UnknownRole(t *testing.T) {
	req := llm.Request{
		Model:    "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.Role("system"), Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "x"}}}}},
	}
	if _, err := buildMessageParams(req, 4096, llm.Capabilities{}); err == nil {
		t.Error("expected an error for an unknown message role")
	}
}

// TestBuildInputSchema_Empty asserts an empty/nil schema yields a bare object
// schema without error.
func TestBuildInputSchema_Empty(t *testing.T) {
	s, err := buildInputSchema(nil)
	if err != nil {
		t.Fatalf("buildInputSchema(nil): %v", err)
	}
	b, _ := json.Marshal(s)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["type"] != "object" {
		t.Errorf("empty schema type = %v, want object", m["type"])
	}
}

// TestBuildInputSchema_Invalid asserts malformed schema JSON is reported.
func TestBuildInputSchema_Invalid(t *testing.T) {
	if _, err := buildInputSchema(json.RawMessage(`{not json`)); err == nil {
		t.Error("expected an error for malformed schema JSON")
	}
}
