package openaicompat

import (
	"encoding/json"
	"testing"

	openai "github.com/openai/openai-go/v3"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// runNormalizer feeds the synthetic chunks through a fresh Normalizer and returns
// the full ordered event slice (live events from Next followed by the Finish tail),
// exactly as the streaming adapter assembles them.
func runNormalizer(chunks []openai.ChatCompletionChunk) []llm.StreamEvent {
	n := NewNormalizer()
	var got []llm.StreamEvent
	for _, c := range chunks {
		got = append(got, n.Next(c)...)
	}
	got = append(got, n.Finish()...)
	return got
}

// textChunk is a synthetic content-delta chunk.
func textChunk(text string) openai.ChatCompletionChunk {
	return openai.ChatCompletionChunk{
		Model: "test-model",
		Choices: []openai.ChatCompletionChunkChoice{{
			Delta: openai.ChatCompletionChunkChoiceDelta{Content: text},
		}},
	}
}

// toolFragChunk is a synthetic tool-call-fragment chunk for the given streaming
// index. id and name are typically only set on the first fragment.
func toolFragChunk(index int64, id, name, argsFragment string) openai.ChatCompletionChunk {
	return openai.ChatCompletionChunk{
		Model: "test-model",
		Choices: []openai.ChatCompletionChunkChoice{{
			Delta: openai.ChatCompletionChunkChoiceDelta{
				ToolCalls: []openai.ChatCompletionChunkChoiceDeltaToolCall{{
					Index: index,
					ID:    id,
					Type:  "function",
					Function: openai.ChatCompletionChunkChoiceDeltaToolCallFunction{
						Name:      name,
						Arguments: argsFragment,
					},
				}},
			},
		}},
	}
}

// finishChunk is a synthetic terminating chunk carrying a finish_reason.
func finishChunk(reason string) openai.ChatCompletionChunk {
	return openai.ChatCompletionChunk{
		Model: "test-model",
		Choices: []openai.ChatCompletionChunkChoice{{
			FinishReason: reason,
			Delta:        openai.ChatCompletionChunkChoiceDelta{},
		}},
	}
}

// usageChunk is a synthetic trailing usage-only chunk (no choices), as emitted when
// stream_options request usage.
func usageChunk(prompt, completion, cached, reasoning int64) openai.ChatCompletionChunk {
	return openai.ChatCompletionChunk{
		Model: "test-model",
		Usage: openai.CompletionUsage{
			PromptTokens:     prompt,
			CompletionTokens: completion,
			TotalTokens:      prompt + completion,
			PromptTokensDetails: openai.CompletionUsagePromptTokensDetails{
				CachedTokens: cached,
			},
			CompletionTokensDetails: openai.CompletionUsageCompletionTokensDetails{
				ReasoningTokens: reasoning,
			},
		},
	}
}

func TestNormalizer_TextOnly_EndStop(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		textChunk("Hello, "),
		textChunk("world"),
		finishChunk("stop"),
		usageChunk(10, 3, 0, 0),
	})

	// Two text deltas, then exactly one Done.
	want := []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "Hello, "}},
		{TextDelta: &llm.TextDelta{Text: "world"}},
		{Done: &llm.Done{
			StopReason:    llm.StopEnd,
			RawStopReason: "stop",
			Usage:         llm.Usage{InputTokens: 10, OutputTokens: 3},
		}},
	}
	assertTextAndDone(t, got, want)
}

// TestNormalizer_JSONStringArgConcatenation is the headline case: tool-call
// arguments arrive as a JSON STRING split across several fragments, the id and name
// only on the first. The normalizer must concatenate, emit ONE complete
// ToolCallDelta before Done, and the assembled ArgsFragment must parse to the whole
// object.
func TestNormalizer_JSONStringArgConcatenation(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		toolFragChunk(0, "call_abc", "get_weather", `{"ci`),
		toolFragChunk(0, "", "", `ty":"Par`),
		toolFragChunk(0, "", "", `is","unit":"c"}`),
		finishChunk("tool_calls"),
	})

	// Expect: one ToolCallDelta then Done. No text events.
	var tcDeltas []*llm.ToolCallDelta
	var dones []*llm.Done
	for _, ev := range got {
		switch {
		case ev.ToolCallDelta != nil:
			tcDeltas = append(tcDeltas, ev.ToolCallDelta)
		case ev.Done != nil:
			dones = append(dones, ev.Done)
		case ev.TextDelta != nil:
			t.Fatalf("unexpected text delta in tool-only stream: %q", ev.TextDelta.Text)
		}
	}
	if len(tcDeltas) != 1 {
		t.Fatalf("want exactly 1 ToolCallDelta, got %d", len(tcDeltas))
	}
	tc := tcDeltas[0]
	if tc.CallID != "call_abc" || tc.Name != "get_weather" {
		t.Fatalf("tool call id/name wrong: id=%q name=%q", tc.CallID, tc.Name)
	}
	if tc.ArgsPath != "" {
		t.Fatalf("ArgsPath must be empty for Chat Completions append-style args, got %q", tc.ArgsPath)
	}
	var parsed map[string]any
	if err := json.Unmarshal(tc.ArgsFragment, &parsed); err != nil {
		t.Fatalf("assembled args must be valid JSON: %v (raw=%q)", err, string(tc.ArgsFragment))
	}
	if parsed["city"] != "Paris" || parsed["unit"] != "c" {
		t.Fatalf("assembled args wrong: %#v", parsed)
	}
	if len(dones) != 1 {
		t.Fatalf("want exactly 1 Done, got %d", len(dones))
	}
	if dones[0].StopReason != llm.StopToolUse {
		t.Fatalf("want StopToolUse, got %q", dones[0].StopReason)
	}
	// The ToolCallDelta must be emitted BEFORE Done.
	if got[len(got)-1].Done == nil {
		t.Fatalf("Done must be the final event")
	}
}

// TestNormalizer_BufferedToolCall_LMStudio models a server that does not stream
// tool-call arguments incrementally (SupportsStreamingToolCalls=false): the entire
// call arrives in a single fragment. The emission must be identical — one complete
// ToolCallDelta before Done.
func TestNormalizer_BufferedToolCall_LMStudio(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		toolFragChunk(0, "call_x", "do_thing", `{"a":1,"b":2}`),
		finishChunk("tool_calls"),
	})
	var tc *llm.ToolCallDelta
	for _, ev := range got {
		if ev.ToolCallDelta != nil {
			if tc != nil {
				t.Fatalf("expected a single complete ToolCallDelta, got a second one")
			}
			tc = ev.ToolCallDelta
		}
	}
	if tc == nil {
		t.Fatalf("expected one ToolCallDelta")
	}
	var parsed map[string]any
	if err := json.Unmarshal(tc.ArgsFragment, &parsed); err != nil {
		t.Fatalf("args must parse: %v", err)
	}
	if parsed["a"].(float64) != 1 || parsed["b"].(float64) != 2 {
		t.Fatalf("args wrong: %#v", parsed)
	}
}

// TestNormalizer_ParallelToolCalls verifies two interleaved tool calls keyed by
// distinct indices are each assembled and emitted in first-seen order.
func TestNormalizer_ParallelToolCalls(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		toolFragChunk(0, "call_0", "first", `{"x":`),
		toolFragChunk(1, "call_1", "second", `{"y":`),
		toolFragChunk(0, "", "", `1}`),
		toolFragChunk(1, "", "", `2}`),
		finishChunk("tool_calls"),
	})
	var ids []string
	for _, ev := range got {
		if ev.ToolCallDelta != nil {
			ids = append(ids, ev.ToolCallDelta.CallID)
		}
	}
	if len(ids) != 2 || ids[0] != "call_0" || ids[1] != "call_1" {
		t.Fatalf("want [call_0 call_1] in first-seen order, got %v", ids)
	}
}

func TestNormalizer_UsageCacheAndReasoningSplit(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		textChunk("hi"),
		finishChunk("stop"),
		usageChunk(100, 40, 30, 12),
	})
	done := lastDone(t, got)
	// InputTokens excludes the 30 cached tokens.
	want := llm.Usage{InputTokens: 70, OutputTokens: 40, CacheReadTokens: 30, ReasoningTokens: 12}
	if done.Usage != want {
		t.Fatalf("usage normalization wrong:\n got %#v\nwant %#v", done.Usage, want)
	}
}

func TestMapFinishReason_Table(t *testing.T) {
	cases := []struct {
		reason string
		want   llm.StopReason
	}{
		{"stop", llm.StopEnd},
		{"length", llm.StopMaxTokens},
		{"tool_calls", llm.StopToolUse},
		{"function_call", llm.StopToolUse},
		{"content_filter", llm.StopContentFilter},
		{"", llm.StopEnd},
		{"some_new_reason", llm.StopOther},
	}
	for _, c := range cases {
		if got := mapFinishReason(c.reason); got != c.want {
			t.Errorf("mapFinishReason(%q) = %q, want %q", c.reason, got, c.want)
		}
	}
}

// TestNormalizer_UnknownFinishReason_PreservesRaw asserts an unrecognized
// finish_reason maps to StopOther while preserving the verbatim provider string.
func TestNormalizer_UnknownFinishReason_PreservesRaw(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		textChunk("x"),
		finishChunk("guardrail_intervened"),
	})
	done := lastDone(t, got)
	if done.StopReason != llm.StopOther {
		t.Fatalf("want StopOther, got %q", done.StopReason)
	}
	if done.RawStopReason != "guardrail_intervened" {
		t.Fatalf("raw stop reason not preserved: %q", done.RawStopReason)
	}
}

// TestNormalizer_ContinuationRoundTrip asserts the continuation blob carried in
// Done.ProviderRaw captures the assembled text + tool calls for stateless replay.
func TestNormalizer_ContinuationRoundTrip(t *testing.T) {
	got := runNormalizer([]openai.ChatCompletionChunk{
		textChunk("partial answer"),
		toolFragChunk(0, "call_z", "lookup", `{"q":"go"}`),
		finishChunk("tool_calls"),
	})
	done := lastDone(t, got)
	if done.ProviderRaw == nil {
		t.Fatalf("expected non-nil ProviderRaw continuation blob")
	}
	var st continuationState
	if err := json.Unmarshal(done.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if st.Surface != surfaceChat {
		t.Fatalf("surface tag wrong: %q", st.Surface)
	}
	if st.Text != "partial answer" {
		t.Fatalf("continuation text wrong: %q", st.Text)
	}
	if len(st.ToolCalls) != 1 || st.ToolCalls[0].ID != "call_z" || st.ToolCalls[0].Name != "lookup" {
		t.Fatalf("continuation tool calls wrong: %#v", st.ToolCalls)
	}
	if st.ToolCalls[0].Args != `{"q":"go"}` {
		t.Fatalf("continuation tool args wrong: %q", st.ToolCalls[0].Args)
	}
}

// --- helpers ---

func lastDone(t *testing.T, evs []llm.StreamEvent) *llm.Done {
	t.Helper()
	if len(evs) == 0 || evs[len(evs)-1].Done == nil {
		t.Fatalf("last event is not Done")
	}
	return evs[len(evs)-1].Done
}

func assertTextAndDone(t *testing.T, got, want []llm.StreamEvent) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event count mismatch: got %d want %d\ngot=%s", len(got), len(want), dump(got))
	}
	for i := range want {
		switch {
		case want[i].TextDelta != nil:
			if got[i].TextDelta == nil || got[i].TextDelta.Text != want[i].TextDelta.Text {
				t.Fatalf("event %d: want text %q, got %s", i, want[i].TextDelta.Text, dump(got[i:i+1]))
			}
		case want[i].Done != nil:
			if got[i].Done == nil {
				t.Fatalf("event %d: want Done, got %s", i, dump(got[i:i+1]))
			}
			gd, wd := got[i].Done, want[i].Done
			if gd.StopReason != wd.StopReason || gd.RawStopReason != wd.RawStopReason || gd.Usage != wd.Usage {
				t.Fatalf("event %d Done mismatch:\n got %#v\nwant %#v", i, gd, wd)
			}
		}
	}
}

func dump(evs []llm.StreamEvent) string {
	b, _ := json.Marshal(evs)
	return string(b)
}
