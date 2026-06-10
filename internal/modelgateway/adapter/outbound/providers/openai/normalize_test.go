package openai

import (
	"encoding/json"
	"testing"

	"github.com/openai/openai-go/v3/responses"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// runNormalizer feeds the synthetic Responses events through a fresh Normalizer and
// returns the full ordered event slice.
func runNormalizer(events []responses.ResponseStreamEventUnion) []llm.StreamEvent {
	n := NewNormalizer()
	var got []llm.StreamEvent
	for _, e := range events {
		got = append(got, n.Next(e)...)
	}
	return got
}

func textDeltaEvent(delta string) responses.ResponseStreamEventUnion {
	return responses.ResponseStreamEventUnion{Type: evtOutputTextDelta, Delta: delta}
}

func reasoningDeltaEvent(delta string) responses.ResponseStreamEventUnion {
	return responses.ResponseStreamEventUnion{Type: evtReasoningTextDelta, Delta: delta}
}

func funcArgsDeltaEvent(itemID string, outputIndex int64, delta string) responses.ResponseStreamEventUnion {
	return responses.ResponseStreamEventUnion{
		Type:        evtFunctionCallArgsDelta,
		ItemID:      itemID,
		OutputIndex: outputIndex,
		Delta:       delta,
	}
}

// completedEvent builds a synthetic response.completed terminal event carrying the
// authoritative output items and usage.
func completedEvent(resp responses.Response) responses.ResponseStreamEventUnion {
	return responses.ResponseStreamEventUnion{Type: evtCompleted, Response: resp}
}

func functionCallItem(callID, name, args string) responses.ResponseOutputItemUnion {
	item := responses.ResponseOutputItemUnion{
		Type:   itemTypeFunctionCall,
		CallID: callID,
		Name:   name,
	}
	item.Arguments.OfString = args
	return item
}

func messageItem(id, text string) responses.ResponseOutputItemUnion {
	return responses.ResponseOutputItemUnion{
		Type: itemTypeMessage,
		ID:   id,
		Content: []responses.ResponseOutputMessageContentUnion{
			{Type: "output_text", Text: text},
		},
	}
}

func usage(input, output, cached, reasoning int64) responses.ResponseUsage {
	u := responses.ResponseUsage{
		InputTokens:  input,
		OutputTokens: output,
		TotalTokens:  input + output,
	}
	u.InputTokensDetails.CachedTokens = cached
	u.OutputTokensDetails.ReasoningTokens = reasoning
	return u
}

func TestNormalizer_TextThenCompleted_EndStop(t *testing.T) {
	resp := responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{messageItem("msg_1", "Hello world")},
		Usage:  usage(10, 5, 0, 0),
	}
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		textDeltaEvent("Hello "),
		textDeltaEvent("world"),
		completedEvent(resp),
	})

	if len(got) != 3 {
		t.Fatalf("want 3 events (2 text + Done), got %d: %s", len(got), dump(got))
	}
	if got[0].TextDelta == nil || got[0].TextDelta.Text != "Hello " {
		t.Fatalf("event 0 wrong: %s", dump(got[:1]))
	}
	if got[1].TextDelta == nil || got[1].TextDelta.Text != "world" {
		t.Fatalf("event 1 wrong: %s", dump(got[1:2]))
	}
	done := got[2].Done
	if done == nil {
		t.Fatalf("event 2 must be Done")
	}
	if done.StopReason != llm.StopEnd {
		t.Fatalf("want StopEnd, got %q", done.StopReason)
	}
	if (done.Usage != llm.Usage{InputTokens: 10, OutputTokens: 5}) {
		t.Fatalf("usage wrong: %#v", done.Usage)
	}
}

func TestNormalizer_ReasoningDelta_BecomesThinking(t *testing.T) {
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		reasoningDeltaEvent("let me think"),
		completedEvent(responses.Response{Status: responses.ResponseStatusCompleted}),
	})
	if got[0].ThinkingDelta == nil || got[0].ThinkingDelta.Text != "let me think" {
		t.Fatalf("expected ThinkingDelta, got %s", dump(got[:1]))
	}
}

// TestNormalizer_ItemScopedFunctionCall is the headline Responses case: function
// argument deltas are item-scoped (keyed by item_id / output_index) and the
// terminal response.completed carries the authoritative, fully-formed function_call
// item. The normalizer must emit ONE complete ToolCallDelta (parsed args) before
// Done, with StopToolUse.
func TestNormalizer_ItemScopedFunctionCall(t *testing.T) {
	resp := responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{
			functionCallItem("call_abc", "get_weather", `{"city":"Paris","unit":"c"}`),
		},
		Usage: usage(20, 8, 0, 0),
	}
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		// Item-scoped argument deltas (ignored for assembly; authoritative item
		// comes from the terminal event).
		funcArgsDeltaEvent("fc_1", 0, `{"city":`),
		funcArgsDeltaEvent("fc_1", 0, `"Paris"}`),
		completedEvent(resp),
	})

	var tcDeltas []*llm.ToolCallDelta
	var done *llm.Done
	for _, ev := range got {
		switch {
		case ev.ToolCallDelta != nil:
			tcDeltas = append(tcDeltas, ev.ToolCallDelta)
		case ev.Done != nil:
			done = ev.Done
		case ev.TextDelta != nil:
			t.Fatalf("unexpected text delta: %q", ev.TextDelta.Text)
		}
	}
	if len(tcDeltas) != 1 {
		t.Fatalf("want exactly 1 ToolCallDelta, got %d: %s", len(tcDeltas), dump(got))
	}
	tc := tcDeltas[0]
	if tc.CallID != "call_abc" || tc.Name != "get_weather" {
		t.Fatalf("tool call id/name wrong: id=%q name=%q", tc.CallID, tc.Name)
	}
	if tc.ArgsPath != "" {
		t.Fatalf("ArgsPath must be empty, got %q", tc.ArgsPath)
	}
	var parsed map[string]any
	if err := json.Unmarshal(tc.ArgsFragment, &parsed); err != nil {
		t.Fatalf("assembled args must parse: %v (%q)", err, string(tc.ArgsFragment))
	}
	if parsed["city"] != "Paris" || parsed["unit"] != "c" {
		t.Fatalf("args wrong: %#v", parsed)
	}
	if done == nil || done.StopReason != llm.StopToolUse {
		t.Fatalf("want Done with StopToolUse, got %#v", done)
	}
	// ToolCallDelta must precede Done.
	if got[len(got)-1].Done == nil {
		t.Fatalf("Done must be the final event")
	}
}

func TestNormalizer_MultipleToolCalls_Order(t *testing.T) {
	resp := responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{
			functionCallItem("call_0", "first", `{"a":1}`),
			functionCallItem("call_1", "second", `{"b":2}`),
		},
	}
	got := runNormalizer([]responses.ResponseStreamEventUnion{completedEvent(resp)})
	var ids []string
	for _, ev := range got {
		if ev.ToolCallDelta != nil {
			ids = append(ids, ev.ToolCallDelta.CallID)
		}
	}
	if len(ids) != 2 || ids[0] != "call_0" || ids[1] != "call_1" {
		t.Fatalf("want [call_0 call_1] in output order, got %v", ids)
	}
}

func TestNormalizer_Incomplete_MaxOutputTokens(t *testing.T) {
	resp := responses.Response{Status: responses.ResponseStatusIncomplete}
	resp.IncompleteDetails.Reason = incompleteMaxOutputTokens
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		textDeltaEvent("partial"),
		{Type: evtIncomplete, Response: resp},
	})
	done := lastDone(t, got)
	if done.StopReason != llm.StopMaxTokens {
		t.Fatalf("want StopMaxTokens, got %q", done.StopReason)
	}
	if done.RawStopReason != incompleteMaxOutputTokens {
		t.Fatalf("raw reason wrong: %q", done.RawStopReason)
	}
}

func TestNormalizer_Incomplete_ContentFilter(t *testing.T) {
	resp := responses.Response{Status: responses.ResponseStatusIncomplete}
	resp.IncompleteDetails.Reason = incompleteContentFilter
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		{Type: evtIncomplete, Response: resp},
	})
	if lastDone(t, got).StopReason != llm.StopContentFilter {
		t.Fatalf("want StopContentFilter, got %q", lastDone(t, got).StopReason)
	}
}

func TestNormalizer_FailedEvent_StopOtherWithRaw(t *testing.T) {
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		{Type: evtError, Message: "rate boom"},
	})
	done := lastDone(t, got)
	if done.StopReason != llm.StopOther {
		t.Fatalf("want StopOther, got %q", done.StopReason)
	}
	if done.RawStopReason != "rate boom" {
		t.Fatalf("raw reason not preserved: %q", done.RawStopReason)
	}
}

func TestNormalizer_UsageCacheAndReasoningSplit(t *testing.T) {
	resp := responses.Response{
		Status: responses.ResponseStatusCompleted,
		Usage:  usage(100, 40, 30, 12),
	}
	got := runNormalizer([]responses.ResponseStreamEventUnion{completedEvent(resp)})
	done := lastDone(t, got)
	want := llm.Usage{InputTokens: 70, OutputTokens: 40, CacheReadTokens: 30, ReasoningTokens: 12}
	if done.Usage != want {
		t.Fatalf("usage normalization wrong:\n got %#v\nwant %#v", done.Usage, want)
	}
}

// TestNormalizer_ContinuationCarriesItems asserts the stateless continuation blob
// in Done.ProviderRaw carries the output Items (message + function_call) for replay
// without previous_response_id.
func TestNormalizer_ContinuationCarriesItems(t *testing.T) {
	resp := responses.Response{
		Status: responses.ResponseStatusCompleted,
		Output: []responses.ResponseOutputItemUnion{
			messageItem("msg_1", "thinking out loud"),
			functionCallItem("call_z", "lookup", `{"q":"go"}`),
		},
	}
	got := runNormalizer([]responses.ResponseStreamEventUnion{completedEvent(resp)})
	done := lastDone(t, got)
	if done.ProviderRaw == nil {
		t.Fatalf("expected non-nil ProviderRaw continuation blob")
	}
	var st continuationState
	if err := json.Unmarshal(done.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if st.Surface != surfaceResponses {
		t.Fatalf("surface tag wrong: %q", st.Surface)
	}
	if len(st.Items) != 2 {
		t.Fatalf("want 2 continuation items, got %d: %#v", len(st.Items), st.Items)
	}
	if st.Items[0].Type != itemTypeMessage || st.Items[0].Text != "thinking out loud" {
		t.Fatalf("message item wrong: %#v", st.Items[0])
	}
	if st.Items[1].Type != itemTypeFunctionCall || st.Items[1].CallID != "call_z" || st.Items[1].Arguments != `{"q":"go"}` {
		t.Fatalf("function_call item wrong: %#v", st.Items[1])
	}
}

func TestNormalizer_IgnoresEventsAfterTerminal(t *testing.T) {
	got := runNormalizer([]responses.ResponseStreamEventUnion{
		completedEvent(responses.Response{Status: responses.ResponseStatusCompleted}),
		textDeltaEvent("should be ignored"),
	})
	// Exactly one Done, nothing after.
	if len(got) != 1 || got[0].Done == nil {
		t.Fatalf("expected a single Done and no trailing events, got %s", dump(got))
	}
}

// --- helpers ---

func lastDone(t *testing.T, evs []llm.StreamEvent) *llm.Done {
	t.Helper()
	if len(evs) == 0 || evs[len(evs)-1].Done == nil {
		t.Fatalf("last event is not Done: %s", dump(evs))
	}
	return evs[len(evs)-1].Done
}

func dump(evs []llm.StreamEvent) string {
	b, _ := json.Marshal(evs)
	return string(b)
}
