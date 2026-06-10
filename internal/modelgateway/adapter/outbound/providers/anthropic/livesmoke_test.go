//go:build livesmoke

// Package anthropic live smoke test. Build-tagged so it is excluded from the
// default unit suite; run with `go test -tags livesmoke ./...`. It SKIPS unless
// ANTHROPIC_API_KEY is set, so it is safe to leave in the tree and never makes a
// network call without an explicit key (architecture §14; task requirement).
package anthropic

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// liveModel is the model exercised by the smoke test; overridable via
// ANTHROPIC_SMOKE_MODEL.
func liveModel() string {
	if m := os.Getenv("ANTHROPIC_SMOKE_MODEL"); m != "" {
		return m
	}
	return "claude-haiku-4-5"
}

func liveProvider(t *testing.T) *Provider {
	t.Helper()
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY not set; skipping live smoke test")
	}
	return New(WithAPIKey(key))
}

// TestLive_Generate runs a single real non-streaming generation and asserts a
// normalized terminal response comes back.
func TestLive_Generate(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := contextWithTimeout(t)
	defer cancel()

	resp, err := p.Generate(ctx, llm.Request{
		Model:     liveModel(),
		MaxTokens: 64,
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Reply with the single word: pong"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if resp.StopReason == "" || resp.RawStopReason == "" {
		t.Errorf("missing stop reason: %+v", resp)
	}
	if resp.Usage.InputTokens == 0 {
		t.Errorf("expected non-zero input usage, got %+v", resp.Usage)
	}
	if len(resp.Content) == 0 {
		t.Error("expected at least one content part")
	}
}

// TestLive_Stream runs a single real streaming generation and asserts the stream
// terminates with a Done after at least one text delta.
func TestLive_Stream(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := contextWithTimeout(t)
	defer cancel()

	reader, err := p.Stream(ctx, llm.Request{
		Model:     liveModel(),
		MaxTokens: 64,
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Count: one two three"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	var sawText, sawDone bool
	for {
		ev, rerr := reader.Recv()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			t.Fatalf("Recv: %v", rerr)
		}
		if ev.TextDelta != nil {
			sawText = true
		}
		if ev.Done != nil {
			sawDone = true
			if ev.Done.StopReason == "" {
				t.Error("Done with empty stop reason")
			}
		}
	}
	if !sawDone {
		t.Error("stream ended without a Done event")
	}
	if !sawText {
		t.Error("stream produced no text deltas")
	}
}

// TestLive_CountTokens exercises the count_tokens endpoint.
func TestLive_CountTokens(t *testing.T) {
	p := liveProvider(t)
	ctx, cancel := contextWithTimeout(t)
	defer cancel()

	n, err := p.CountTokens(ctx, llm.Request{
		Model:    liveModel(),
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "How many tokens is this?"}}}}},
	})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Errorf("token count = %d, want > 0", n)
	}
}

func contextWithTimeout(t *testing.T) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.Context(), 60*time.Second)
}
