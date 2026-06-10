package stub_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/stub"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// drain reads r to completion and returns all events collected before io.EOF.
// It fails the test on any non-EOF error.
func drain(t *testing.T, r llm.StreamReader) []llm.StreamEvent {
	t.Helper()
	var out []llm.StreamEvent
	for {
		ev, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Recv: unexpected error: %v", err)
		}
		out = append(out, ev)
	}
}

// TestProvider_ImplementsInterface asserts the compile-time assertion is sound
// and that New() returns a non-nil Provider.
func TestProvider_ImplementsInterface(_ *testing.T) {
	var _ llm.Provider = stub.New()
}

// TestStream_NoTools_EmitsTextThenDone asserts that without tools the script
// yields: TextDelta → Done(StopEnd) → EOF.
func TestStream_NoTools_EmitsTextThenDone(t *testing.T) {
	p := stub.New()
	r, err := p.Stream(context.Background(), llm.Request{Model: "stub-v1"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()

	events := drain(t, r)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (TextDelta + Done)", len(events))
	}

	ev0 := events[0]
	if ev0.TextDelta == nil {
		t.Fatalf("events[0].TextDelta is nil; event = %+v", ev0)
	}
	if ev0.TextDelta.Text == "" {
		t.Error("TextDelta.Text is empty, want non-empty acknowledgement")
	}

	ev1 := events[1]
	if ev1.Done == nil {
		t.Fatalf("events[1].Done is nil; event = %+v", ev1)
	}
	if ev1.Done.StopReason != llm.StopEnd {
		t.Errorf("Done.StopReason = %q, want %q", ev1.Done.StopReason, llm.StopEnd)
	}
	if ev1.Done.Usage.InputTokens == 0 || ev1.Done.Usage.OutputTokens == 0 {
		t.Errorf("Done.Usage = %+v, want non-zero counts", ev1.Done.Usage)
	}
}

// TestStream_WithTools_IgnoresToolsEmitsText asserts that the stub IGNORES any
// advertised tools and still yields a terminal text-only turn: TextDelta →
// Done(StopEnd) → EOF, with NO ToolCallDelta. This is the deliberate keyless-demo
// contract (see stub.stubText): a tool call in an unattended, approver-less run
// would deadlock on the human approval gate, so the demo provider never requests
// one. The tool EXECUTION path is covered by the tool-runtime integration suite.
func TestStream_WithTools_IgnoresToolsEmitsText(t *testing.T) {
	p := stub.New()
	schema := json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}}}`)
	req := llm.Request{
		Model: "stub-v1",
		Tools: []llm.ToolDef{{Name: "my_tool", Description: "does stuff", JSONSchema: schema}},
	}
	r, err := p.Stream(context.Background(), req)
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()

	events := drain(t, r)
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (TextDelta + Done); the stub must ignore tools", len(events))
	}

	if events[0].TextDelta == nil {
		t.Errorf("events[0] is not TextDelta: %+v", events[0])
	}

	for i, ev := range events {
		if ev.ToolCallDelta != nil {
			t.Errorf("events[%d] has a ToolCallDelta %+v; the stub must never request a tool", i, ev.ToolCallDelta)
		}
	}

	done := events[1].Done
	if done == nil {
		t.Fatalf("events[1].Done is nil; event = %+v", events[1])
	}
	if done.StopReason != llm.StopEnd {
		t.Errorf("Done.StopReason = %q, want %q", done.StopReason, llm.StopEnd)
	}
}

// TestStream_EOFAfterLastEvent asserts that Recv returns io.EOF after the Done
// event has been delivered (stream is exhausted).
func TestStream_EOFAfterLastEvent(t *testing.T) {
	p := stub.New()
	r, err := p.Stream(context.Background(), llm.Request{Model: "stub-v1"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()

	// Drain all events.
	drain(t, r)

	// One more Recv must return io.EOF.
	_, err = r.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("Recv after EOF = %v, want io.EOF", err)
	}
}

// TestStream_Deterministic asserts that two Stream calls on the same request
// return identical event sequences (idempotent / deterministic).
func TestStream_Deterministic(t *testing.T) {
	p := stub.New()
	req := llm.Request{Model: "stub-v1"}

	r1, _ := p.Stream(context.Background(), req)
	r2, _ := p.Stream(context.Background(), req)
	defer func() { _ = r1.Close(); _ = r2.Close() }()

	ev1 := drain(t, r1)
	ev2 := drain(t, r2)

	if len(ev1) != len(ev2) {
		t.Fatalf("stream lengths differ: %d vs %d", len(ev1), len(ev2))
	}
	for i := range ev1 {
		// Compare discriminator fields only (struct contains slices, so avoid reflect.DeepEqual on Done.Usage which is fine here).
		switch {
		case ev1[i].TextDelta != nil && ev2[i].TextDelta != nil:
			if ev1[i].TextDelta.Text != ev2[i].TextDelta.Text {
				t.Errorf("events[%d] TextDelta text differs", i)
			}
		case ev1[i].Done != nil && ev2[i].Done != nil:
			if ev1[i].Done.StopReason != ev2[i].Done.StopReason {
				t.Errorf("events[%d] Done StopReason differs", i)
			}
		default:
			if (ev1[i].TextDelta == nil) != (ev2[i].TextDelta == nil) ||
				(ev1[i].Done == nil) != (ev2[i].Done == nil) {
				t.Errorf("events[%d] discriminator mismatch", i)
			}
		}
	}
}

// TestGenerate_NoTools_ReturnsTextResponse asserts Generate returns a text
// response with StopEnd when no tools are provided.
func TestGenerate_NoTools_ReturnsTextResponse(t *testing.T) {
	p := stub.New()
	resp, err := p.Generate(context.Background(), llm.Request{Model: "stub-v1"})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp == nil {
		t.Fatal("Generate returned nil response")
	}
	if resp.StopReason != llm.StopEnd {
		t.Errorf("StopReason = %q, want %q", resp.StopReason, llm.StopEnd)
	}
	if len(resp.Content) == 0 {
		t.Fatal("Content is empty")
	}
	if resp.Content[0].Text == nil || resp.Content[0].Text.Text == "" {
		t.Errorf("Content[0].Text = %+v, want non-empty text", resp.Content[0].Text)
	}
	if resp.Usage.InputTokens == 0 || resp.Usage.OutputTokens == 0 {
		t.Errorf("Usage = %+v, want non-zero counts", resp.Usage)
	}
}

// TestGenerate_WithTools_IgnoresToolsReturnsText asserts Generate IGNORES
// advertised tools and returns a terminal text-only response (StopEnd, no
// ToolCall part) — the same keyless-demo contract as the streaming path.
func TestGenerate_WithTools_IgnoresToolsReturnsText(t *testing.T) {
	p := stub.New()
	req := llm.Request{
		Model: "stub-v1",
		Tools: []llm.ToolDef{{Name: "search", JSONSchema: json.RawMessage(`{}`)}},
	}
	resp, err := p.Generate(context.Background(), req)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.StopReason != llm.StopEnd {
		t.Errorf("StopReason = %q, want %q", resp.StopReason, llm.StopEnd)
	}
	for _, part := range resp.Content {
		if part.ToolCall != nil {
			t.Errorf("response contains a ToolCall %+v; the stub must never request a tool", part.ToolCall)
		}
	}
	if len(resp.Content) == 0 || resp.Content[0].Text == nil || resp.Content[0].Text.Text == "" {
		t.Errorf("want a non-empty text content part; content = %+v", resp.Content)
	}
}

// TestCountTokens_ReturnsFixedEstimate asserts CountTokens always returns a
// positive count with no error.
func TestCountTokens_ReturnsFixedEstimate(t *testing.T) {
	p := stub.New()
	n, err := p.CountTokens(context.Background(), llm.Request{Model: "stub-v1"})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Errorf("CountTokens = %d, want > 0", n)
	}
}

// TestCapabilities_SupportsTools asserts the stub reports tools as supported (so
// the end-to-end agent loop exercises the tool path).
func TestCapabilities_SupportsTools(t *testing.T) {
	p := stub.New()
	caps, err := p.Capabilities(context.Background(), "stub-v1")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !caps.SupportsTools {
		t.Error("Capabilities.SupportsTools = false, want true")
	}
	if caps.MaxOutputTokens <= 0 {
		t.Errorf("MaxOutputTokens = %d, want > 0", caps.MaxOutputTokens)
	}
}

// TestCapabilities_SupportsSystemPrompt asserts the stub supports system prompts.
func TestCapabilities_SupportsSystemPrompt(t *testing.T) {
	p := stub.New()
	caps, err := p.Capabilities(context.Background(), "stub-v1")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !caps.SupportsSystemPrompt {
		t.Error("Capabilities.SupportsSystemPrompt = false, want true")
	}
}
