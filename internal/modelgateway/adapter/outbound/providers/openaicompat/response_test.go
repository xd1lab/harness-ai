package openaicompat

import (
	"encoding/json"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// chatCompletion builds a synthetic non-streaming completion with one choice.
func chatCompletion(content, refusal, finish string, toolCalls ...openai.ChatCompletionMessageToolCallUnion) *openai.ChatCompletion {
	return &openai.ChatCompletion{
		Model: "m-test",
		Choices: []openai.ChatCompletionChoice{{
			FinishReason: finish,
			Message: openai.ChatCompletionMessage{
				Content:   content,
				Refusal:   refusal,
				ToolCalls: toolCalls,
			},
		}},
		Usage: openai.CompletionUsage{PromptTokens: 9, CompletionTokens: 3, TotalTokens: 12},
	}
}

// fnToolCall builds a synthetic function tool call for a completion message.
func fnToolCall(id, name, args string) openai.ChatCompletionMessageToolCallUnion {
	return openai.ChatCompletionMessageToolCallUnion{
		ID:   id,
		Type: "function",
		Function: openai.ChatCompletionMessageFunctionToolCallFunction{
			Name:      name,
			Arguments: args,
		},
	}
}

func TestAssembleResponse_NoChoices_IsServerError(t *testing.T) {
	// A nil or empty-choice completion is a provider protocol violation, not a
	// client bug: it must map to a retryable server-kind error.
	_, err := assembleResponse(nil)
	assertProviderErrorKind(t, err, llm.ErrServer)

	_, err = assembleResponse(&openai.ChatCompletion{})
	assertProviderErrorKind(t, err, llm.ErrServer)
}

func TestAssembleResponse_TextOnly(t *testing.T) {
	got, err := assembleResponse(chatCompletion("hello there", "", "stop"))
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if len(got.Content) != 1 || got.Content[0].Text == nil || got.Content[0].Text.Text != "hello there" {
		t.Fatalf("content wrong: %#v", got.Content)
	}
	if got.StopReason != llm.StopEnd || got.RawStopReason != "stop" {
		t.Fatalf("stop reason wrong: %q (%q)", got.StopReason, got.RawStopReason)
	}
	if (got.Usage != llm.Usage{InputTokens: 9, OutputTokens: 3}) {
		t.Fatalf("usage wrong: %#v", got.Usage)
	}
	// Text alone is worth carrying for stateless replay.
	var st continuationState
	if err := json.Unmarshal(got.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if st.Surface != surfaceChat || st.Model != "m-test" || st.Text != "hello there" || st.FinishReason != "stop" {
		t.Fatalf("continuation blob wrong: %#v", st)
	}
}

// TestAssembleResponse_RefusalFallback asserts a refusal-only message surfaces as
// visible text so the assembled response is never silently empty.
func TestAssembleResponse_RefusalFallback(t *testing.T) {
	got, err := assembleResponse(chatCompletion("", "I cannot help with that", "stop"))
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if len(got.Content) != 1 || got.Content[0].Text == nil || got.Content[0].Text.Text != "I cannot help with that" {
		t.Fatalf("refusal must surface as text, got %#v", got.Content)
	}
}

func TestAssembleResponse_ToolCallsAndContinuation(t *testing.T) {
	got, err := assembleResponse(chatCompletion("", "", "tool_calls",
		fnToolCall("call_0", "first", `{"a":1}`),
		fnToolCall("call_1", "second", `{"b":2}`),
	))
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if got.StopReason != llm.StopToolUse {
		t.Fatalf("want StopToolUse, got %q", got.StopReason)
	}
	if len(got.Content) != 2 {
		t.Fatalf("want 2 tool-call parts, got %d: %#v", len(got.Content), got.Content)
	}
	tc0, tc1 := got.Content[0].ToolCall, got.Content[1].ToolCall
	if tc0 == nil || tc0.ID != "call_0" || tc0.Name != "first" || tc0.Args["a"].(float64) != 1 {
		t.Fatalf("first tool call wrong: %#v", got.Content[0])
	}
	if tc1 == nil || tc1.ID != "call_1" || tc1.Name != "second" {
		t.Fatalf("second tool call wrong: %#v", got.Content[1])
	}
	// The continuation must carry the verbatim argument strings for replay.
	var st continuationState
	if err := json.Unmarshal(got.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if len(st.ToolCalls) != 2 || st.ToolCalls[0].Args != `{"a":1}` || st.ToolCalls[1].ID != "call_1" {
		t.Fatalf("continuation tool calls wrong: %#v", st.ToolCalls)
	}
}

func TestAssembleResponse_BadToolArgs_InvalidRequest(t *testing.T) {
	_, err := assembleResponse(chatCompletion("", "", "tool_calls", fnToolCall("c", "broken", `{"q":`)))
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

// TestAssembleResponse_EmptyTurn_NilProviderRaw asserts a turn with nothing to
// replay keeps ProviderRaw nil so the orchestrator does not echo a useless blob.
func TestAssembleResponse_EmptyTurn_NilProviderRaw(t *testing.T) {
	got, err := assembleResponse(chatCompletion("", "", "stop"))
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if got.ProviderRaw != nil {
		t.Fatalf("empty turn must not carry a continuation blob, got %s", string(got.ProviderRaw))
	}
	if len(got.Content) != 0 {
		t.Fatalf("empty turn must have no content, got %#v", got.Content)
	}
}

func TestParseArgs_Table(t *testing.T) {
	// Empty arguments decode to an empty (non-nil) map so downstream code can
	// index without nil checks.
	got, err := parseArgs("")
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("parseArgs(\"\") = (%#v, %v), want empty map", got, err)
	}
	got, err = parseArgs(`{"a":1,"b":"x"}`)
	if err != nil || got["a"].(float64) != 1 || got["b"] != "x" {
		t.Fatalf("parseArgs object = (%#v, %v)", got, err)
	}
	// Non-object and truncated payloads are decode errors, not silent zeros.
	if _, err := parseArgs(`[1,2]`); err == nil {
		t.Fatalf("parseArgs must reject a non-object payload")
	}
	if _, err := parseArgs(`{"a":`); err == nil {
		t.Fatalf("parseArgs must reject truncated JSON")
	}
}

func TestBuildContinuation_NilWhenEmpty(t *testing.T) {
	if raw := buildContinuation("m", "stop", "", nil); raw != nil {
		t.Fatalf("no text and no tool calls must yield a nil blob, got %s", string(raw))
	}
	raw := buildContinuation("m", "stop", "some text", nil)
	if raw == nil {
		t.Fatalf("text alone must yield a continuation blob")
	}
	var st continuationState
	if err := json.Unmarshal(raw, &st); err != nil || st.Text != "some text" || st.Model != "m" {
		t.Fatalf("blob round-trip wrong: %#v (%v)", st, err)
	}
}
