package openai

import (
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/boltrope/boltrope/internal/platform/llm"
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
