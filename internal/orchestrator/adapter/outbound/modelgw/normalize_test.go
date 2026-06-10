// Package modelgw tests — stream normalizer golden tests (network-free).
//
// These tests drive normalizeEvent directly with synthetic gen.StreamEvent
// values and assert the mapped llm.StreamEvent output. No gRPC dial is
// required; the normalizer is a pure function.
package modelgw

import (
	"encoding/json"
	"testing"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// TestNormalizeEvent_TextDelta verifies that a gen TextDelta maps to
// llm.StreamEvent{TextDelta}.
func TestNormalizeEvent_TextDelta(t *testing.T) {
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_TextDelta{
			TextDelta: &genproto.TextDelta{Text: "hello"},
		},
	}
	got := normalizeEvent(in)
	if got.TextDelta == nil {
		t.Fatal("expected TextDelta, got nil")
	}
	if got.TextDelta.Text != "hello" {
		t.Errorf("TextDelta.Text = %q, want %q", got.TextDelta.Text, "hello")
	}
	if got.ThinkingDelta != nil || got.ToolCallDelta != nil || got.Done != nil {
		t.Error("expected only TextDelta set, other fields non-nil")
	}
}

// TestNormalizeEvent_ThinkingDelta verifies that a gen ThinkingDelta maps to
// llm.StreamEvent{ThinkingDelta}, carrying both text and signature.
func TestNormalizeEvent_ThinkingDelta(t *testing.T) {
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_ThinkingDelta{
			ThinkingDelta: &genproto.ThinkingDelta{
				Text:      "reasoning",
				Signature: "sig-abc",
			},
		},
	}
	got := normalizeEvent(in)
	if got.ThinkingDelta == nil {
		t.Fatal("expected ThinkingDelta, got nil")
	}
	if got.ThinkingDelta.Text != "reasoning" {
		t.Errorf("ThinkingDelta.Text = %q, want %q", got.ThinkingDelta.Text, "reasoning")
	}
	if got.ThinkingDelta.Signature != "sig-abc" {
		t.Errorf("ThinkingDelta.Signature = %q, want %q", got.ThinkingDelta.Signature, "sig-abc")
	}
}

// TestNormalizeEvent_ToolCallDelta verifies that a gen ToolCallDelta maps
// faithfully to llm.StreamEvent{ToolCallDelta}, preserving CallID, Name,
// ArgsPath and ArgsFragment as raw JSON bytes.
func TestNormalizeEvent_ToolCallDelta(t *testing.T) {
	argsJSON := []byte(`{"x":1}`)
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_ToolCallDelta{
			ToolCallDelta: &genproto.ToolCallDelta{
				CallId:       "call-1",
				Name:         "my_tool",
				ArgsPath:     "$.args",
				ArgsFragment: argsJSON,
			},
		},
	}
	got := normalizeEvent(in)
	if got.ToolCallDelta == nil {
		t.Fatal("expected ToolCallDelta, got nil")
	}
	tcd := got.ToolCallDelta
	if tcd.CallID != "call-1" {
		t.Errorf("CallID = %q, want %q", tcd.CallID, "call-1")
	}
	if tcd.Name != "my_tool" {
		t.Errorf("Name = %q, want %q", tcd.Name, "my_tool")
	}
	if tcd.ArgsPath != "$.args" {
		t.Errorf("ArgsPath = %q, want %q", tcd.ArgsPath, "$.args")
	}
	if string(tcd.ArgsFragment) != string(argsJSON) {
		t.Errorf("ArgsFragment = %s, want %s", tcd.ArgsFragment, argsJSON)
	}
}

// TestNormalizeEvent_Done_End verifies that a terminal Done with
// STOP_REASON_END maps to llm.StopEnd with Usage and ProviderRaw preserved.
func TestNormalizeEvent_Done_End(t *testing.T) {
	provRaw := json.RawMessage(`{"k":"v"}`)
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_Done{
			Done: &genproto.Done{
				StopReason:    genproto.StopReason_STOP_REASON_END,
				RawStopReason: "end",
				Usage: &genproto.Usage{
					InputTokens:      10,
					OutputTokens:     20,
					CacheReadTokens:  3,
					CacheWriteTokens: 4,
					ReasoningTokens:  5,
				},
				ProviderRaw: []byte(provRaw),
			},
		},
	}
	got := normalizeEvent(in)
	if got.Done == nil {
		t.Fatal("expected Done, got nil")
	}
	d := got.Done
	if d.StopReason != llm.StopEnd {
		t.Errorf("StopReason = %q, want %q", d.StopReason, llm.StopEnd)
	}
	if d.RawStopReason != "end" {
		t.Errorf("RawStopReason = %q, want %q", d.RawStopReason, "end")
	}
	u := d.Usage
	if u.InputTokens != 10 || u.OutputTokens != 20 || u.CacheReadTokens != 3 ||
		u.CacheWriteTokens != 4 || u.ReasoningTokens != 5 {
		t.Errorf("Usage mismatch: %+v", u)
	}
	if string(d.ProviderRaw) != string(provRaw) {
		t.Errorf("ProviderRaw = %s, want %s", d.ProviderRaw, provRaw)
	}
}

// TestNormalizeEvent_Done_Pause verifies that STOP_REASON_PAUSE maps to
// llm.Pause (non-terminal).
func TestNormalizeEvent_Done_Pause(t *testing.T) {
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_Done{
			Done: &genproto.Done{
				StopReason:    genproto.StopReason_STOP_REASON_PAUSE,
				RawStopReason: "pause_turn",
			},
		},
	}
	got := normalizeEvent(in)
	if got.Done == nil {
		t.Fatal("expected Done, got nil")
	}
	if got.Done.StopReason != llm.Pause {
		t.Errorf("StopReason = %q, want %q", got.Done.StopReason, llm.Pause)
	}
	if got.Done.StopReason.IsTerminal() {
		t.Error("Pause must not be terminal")
	}
}

// TestNormalizeEvent_Done_Other verifies that an unknown STOP_REASON maps to
// llm.StopOther and preserves the raw string.
func TestNormalizeEvent_Done_Other(t *testing.T) {
	in := &genproto.StreamEvent{
		Event: &genproto.StreamEvent_Done{
			Done: &genproto.Done{
				StopReason:    genproto.StopReason_STOP_REASON_OTHER,
				RawStopReason: "provider_specific_reason",
			},
		},
	}
	got := normalizeEvent(in)
	if got.Done == nil {
		t.Fatal("expected Done, got nil")
	}
	if got.Done.StopReason != llm.StopOther {
		t.Errorf("StopReason = %q, want %q", got.Done.StopReason, llm.StopOther)
	}
	if got.Done.RawStopReason != "provider_specific_reason" {
		t.Errorf("RawStopReason = %q, want %q", got.Done.RawStopReason, "provider_specific_reason")
	}
}

// TestNormalizeStopReason is a table-driven exhaustive test for every
// proto StopReason enum value → llm.StopReason constant.
func TestNormalizeStopReason(t *testing.T) {
	cases := []struct {
		proto genproto.StopReason
		want  llm.StopReason
	}{
		{genproto.StopReason_STOP_REASON_UNSPECIFIED, llm.StopOther},
		{genproto.StopReason_STOP_REASON_END, llm.StopEnd},
		{genproto.StopReason_STOP_REASON_MAX_TOKENS, llm.StopMaxTokens},
		{genproto.StopReason_STOP_REASON_TOOL_USE, llm.StopToolUse},
		{genproto.StopReason_STOP_REASON_STOP_SEQUENCE, llm.StopStopSequence},
		{genproto.StopReason_STOP_REASON_CONTENT_FILTER, llm.StopContentFilter},
		{genproto.StopReason_STOP_REASON_REFUSAL, llm.StopRefusal},
		{genproto.StopReason_STOP_REASON_CONTEXT_WINDOW_EXCEEDED, llm.StopContextWindowExceeded},
		{genproto.StopReason_STOP_REASON_PAUSE, llm.Pause},
		{genproto.StopReason_STOP_REASON_OTHER, llm.StopOther},
	}
	for _, tc := range cases {
		got := normalizeStopReason(tc.proto)
		if got != tc.want {
			t.Errorf("normalizeStopReason(%v) = %q, want %q", tc.proto, got, tc.want)
		}
	}
}

// TestNormalizeEvent_Nil verifies that a nil or empty gen.StreamEvent
// returns an empty llm.StreamEvent without panicking.
func TestNormalizeEvent_Nil(t *testing.T) {
	// nil input
	got := normalizeEvent(nil)
	if got.TextDelta != nil || got.ThinkingDelta != nil || got.ToolCallDelta != nil || got.Done != nil {
		t.Error("normalizeEvent(nil) should return zero StreamEvent")
	}
	// event oneof unset
	got2 := normalizeEvent(&genproto.StreamEvent{})
	if got2.TextDelta != nil || got2.ThinkingDelta != nil || got2.ToolCallDelta != nil || got2.Done != nil {
		t.Error("normalizeEvent(empty) should return zero StreamEvent")
	}
}
