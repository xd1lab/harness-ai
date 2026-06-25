package main

// RED tests for FIX 1 (context estimator counts ALL token-bearing content
// parts, not just Text). These pin AC-1.1..AC-1.4: a message whose only content
// is a large ToolResult must yield a proportional, non-trivial estimate (today
// estimateTokens skips ToolResult/ToolCall/Thinking and returns ~0). The test is
// in package main so it can call the unexported estimateTokens directly.
//
// All cases use the local estimator directly (no gateway), so they are pure and
// hermetic (NFR-TEST-01).

import (
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// msgWith builds a single-part assistant message carrying one content part.
func msgWith(p llm.ContentPart) llm.Message {
	return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentPart{p}}
}

// TestEstimateTokens_CountsLargeToolResult is the headline RED case (AC-1.3): a
// message whose ONLY content is a large ToolResult must estimate proportional to
// len/4 — non-trivial, NOT ~0 as the pre-fix Text-only logic would yield.
func TestEstimateTokens_CountsLargeToolResult(t *testing.T) {
	const n = 4000
	content := strings.Repeat("x", n)
	msgs := []llm.Message{msgWith(llm.ContentPart{ToolResult: &llm.ToolResult{
		CallID:  "c1",
		Content: content,
	}})}

	got := estimateTokens(msgs, nil)

	// Pre-fix behavior: ToolResult is skipped entirely, so got == 0. Assert it is
	// strictly positive and within a sane band of len(content)/charsPerToken (=1000).
	if got <= 0 {
		t.Fatalf("estimateTokens over a large ToolResult = %d, want > 0 (the noisy-tool-output window must be counted)", got)
	}
	if got < 900 || got > 1100 {
		t.Errorf("estimateTokens over a %d-char ToolResult = %d, want within [900,1100] (~len/4)", n, got)
	}
}

// TestEstimateTokens_CountsToolCallNameAndArgs asserts a ToolCall contributes
// non-zero from its Name + a JSON rendering of its Args (AC-1.1(c), AC-1.4).
func TestEstimateTokens_CountsToolCallNameAndArgs(t *testing.T) {
	msgs := []llm.Message{msgWith(llm.ContentPart{ToolCall: &llm.ToolCall{
		ID:   "c1",
		Name: "search_the_codebase_thoroughly",
		Args: map[string]any{"query": strings.Repeat("q", 400), "limit": float64(10)},
	}})}

	got := estimateTokens(msgs, nil)
	if got <= 0 {
		t.Fatalf("estimateTokens over a ToolCall = %d, want > 0 (name+args must be counted)", got)
	}
	// Name(30) + an args JSON of ~420+ chars => well over 100 tokens.
	if got < 100 {
		t.Errorf("estimateTokens over a ToolCall with a 400-char arg = %d, want >= 100 (~len/4)", got)
	}
}

// TestEstimateTokens_CountsThinkingText asserts a Thinking part contributes
// non-zero from its text (AC-1.1(d), AC-1.4).
func TestEstimateTokens_CountsThinkingText(t *testing.T) {
	const n = 2000
	msgs := []llm.Message{msgWith(llm.ContentPart{Thinking: &llm.ThinkingPart{
		Text: strings.Repeat("t", n),
	}})}

	got := estimateTokens(msgs, nil)
	if got <= 0 {
		t.Fatalf("estimateTokens over Thinking = %d, want > 0 (thinking text must be counted)", got)
	}
	if got < 400 || got > 600 {
		t.Errorf("estimateTokens over %d chars of thinking = %d, want within [400,600] (~len/4)", n, got)
	}
}

// TestEstimateTokens_ArgsOrderInsensitive asserts the Args contribution is
// deterministic regardless of map iteration order (AC-1.2): two messages with
// identical Args content produce identical estimates.
func TestEstimateTokens_ArgsOrderInsensitive(t *testing.T) {
	a := []llm.Message{msgWith(llm.ContentPart{ToolCall: &llm.ToolCall{
		Name: "tool",
		Args: map[string]any{"alpha": "1", "beta": "2", "gamma": "3", "delta": "4"},
	}})}
	b := []llm.Message{msgWith(llm.ContentPart{ToolCall: &llm.ToolCall{
		Name: "tool",
		Args: map[string]any{"delta": "4", "gamma": "3", "beta": "2", "alpha": "1"},
	}})}

	if ga, gb := estimateTokens(a, nil), estimateTokens(b, nil); ga != gb {
		t.Errorf("estimateTokens not order-insensitive for Args: %d != %d", ga, gb)
	}
}

// TestEstimateTokens_TextOnlyUnchanged is the regression guard (AC-1.4): a
// pure-text message must estimate exactly len(text)/charsPerToken — unchanged
// from the pre-fix behavior. (charsPerToken=4.)
func TestEstimateTokens_TextOnlyUnchanged(t *testing.T) {
	const n = 4000 // exactly divisible by 4
	msgs := []llm.Message{msgWith(llm.ContentPart{Text: &llm.TextPart{Text: strings.Repeat("a", n)}})}

	if got, want := estimateTokens(msgs, nil), n/4; got != want {
		t.Errorf("estimateTokens over a pure-text message = %d, want %d (unchanged from prior behavior)", got, want)
	}
}

// TestEstimateTokens_ImageBytesNotCounted asserts image bytes are NOT counted as
// model-text tokens (AC-1.1): an image-only message contributes zero text.
func TestEstimateTokens_ImageBytesNotCounted(t *testing.T) {
	msgs := []llm.Message{msgWith(llm.ContentPart{Image: &llm.ImagePart{
		MediaType: "image/png",
		Data:      make([]byte, 10000),
	}})}

	if got := estimateTokens(msgs, nil); got != 0 {
		t.Errorf("estimateTokens over an image-only message = %d, want 0 (image bytes are not model-text tokens)", got)
	}
}

// TestEstimateTokens_ToolDefLoopUnchanged guards that the per-tool-def loop
// (Name+Description+JSONSchema) still contributes (AC-1.1).
func TestEstimateTokens_ToolDefLoopUnchanged(t *testing.T) {
	tools := []llm.ToolDef{{
		Name:        "read",
		Description: strings.Repeat("d", 100),
		JSONSchema:  []byte(strings.Repeat("s", 300)),
	}}
	// Name(4) + Description(100) + JSONSchema(300) = 404 chars => 101 tokens.
	if got, want := estimateTokens(nil, tools), (4+100+300)/4; got != want {
		t.Errorf("estimateTokens over one tool def = %d, want %d", got, want)
	}
}
