package openai

import (
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

func TestAssembleResponse_TextAndToolCall(t *testing.T) {
	resp := &responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{
			messageItem("m1", "here you go"),
			functionCallItem("c1", "do_it", `{"k":"v"}`),
		},
		Usage: usage(12, 4, 0, 0),
	}
	got, err := assembleResponse(resp)
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if got.StopReason != llm.StopToolUse {
		t.Fatalf("want StopToolUse (function_call present), got %q", got.StopReason)
	}
	if len(got.Content) != 2 {
		t.Fatalf("want 2 content parts, got %d", len(got.Content))
	}
	if got.Content[0].Text == nil || got.Content[0].Text.Text != "here you go" {
		t.Fatalf("first part must be text, got %#v", got.Content[0])
	}
	tc := got.Content[1].ToolCall
	if tc == nil || tc.ID != "c1" || tc.Name != "do_it" {
		t.Fatalf("second part must be the tool call, got %#v", got.Content[1])
	}
	if tc.Args["k"] != "v" {
		t.Fatalf("tool call args wrong: %#v", tc.Args)
	}
	if got.ProviderRaw == nil {
		t.Fatalf("expected stateless continuation blob in ProviderRaw")
	}
}

func TestAssembleResponse_FailedStatusIsProviderError(t *testing.T) {
	resp := &responses.Response{Status: responses.ResponseStatusFailed}
	resp.Error.Message = "kaboom"
	_, err := assembleResponse(resp)
	assertProviderErrorKind(t, err, llm.ErrServer)

	// A failed status without a provider message still yields a server error
	// (with the generic fallback message), and so does a nil response.
	_, err = assembleResponse(&responses.Response{Status: responses.ResponseStatusFailed})
	assertProviderErrorKind(t, err, llm.ErrServer)
	_, err = assembleResponse(nil)
	assertProviderErrorKind(t, err, llm.ErrServer)
}

func TestAssembleResponse_BadToolArgs_InvalidRequest(t *testing.T) {
	resp := &responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{functionCallItem("c1", "broken", `{"q":`)},
	}
	_, err := assembleResponse(resp)
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

// TestAssembleResponse_EmptyToolArgs_EmptyMap asserts a function call with no
// argument payload decodes to an empty (non-nil) args map, so downstream code can
// index without nil checks.
func TestAssembleResponse_EmptyToolArgs_EmptyMap(t *testing.T) {
	resp := &responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{functionCallItem("c1", "noargs", "")},
	}
	got, err := assembleResponse(resp)
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	tc := got.Content[0].ToolCall
	if tc == nil || tc.Args == nil || len(tc.Args) != 0 {
		t.Fatalf("empty arguments must decode to an empty map, got %#v", tc)
	}
}

// TestAssembleResponse_RefusalSurfacesAsText asserts a refusal-only output
// message is carried as visible text so the response is never silently empty.
func TestAssembleResponse_RefusalSurfacesAsText(t *testing.T) {
	item := responses.ResponseOutputItemUnion{
		Type: itemTypeMessage,
		ID:   "m1",
		Content: []responses.ResponseOutputMessageContentUnion{
			{Type: "refusal", Refusal: "I cannot help with that"},
		},
	}
	resp := &responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{item},
	}
	got, err := assembleResponse(resp)
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if len(got.Content) != 1 || got.Content[0].Text == nil || got.Content[0].Text.Text != "I cannot help with that" {
		t.Fatalf("refusal must surface as text, got %#v", got.Content)
	}
}

func TestAssembleResponse_ReasoningBecomesThinkingPart(t *testing.T) {
	item := responses.ResponseOutputItemUnion{Type: itemTypeReasoning, ID: "r1"}
	item.Summary = []responses.ResponseReasoningItemSummary{{Text: "because"}}
	resp := &responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{item, messageItem("m1", "answer")},
	}
	got, err := assembleResponse(resp)
	if err != nil {
		t.Fatalf("assembleResponse: %v", err)
	}
	if len(got.Content) != 2 {
		t.Fatalf("want thinking + text, got %d parts", len(got.Content))
	}
	if got.Content[0].Thinking == nil || got.Content[0].Thinking.Text != "because" {
		t.Fatalf("first part must be thinking, got %#v", got.Content[0])
	}
}
