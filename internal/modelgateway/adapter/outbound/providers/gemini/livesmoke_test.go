//go:build livesmoke

// Package gemini live smoke test. Build-tagged behind `livesmoke` so it never runs in
// the default unit suite; it additionally SKIPS at runtime when neither GEMINI_API_KEY
// nor GOOGLE_API_KEY is set, so a keyless `go test -tags livesmoke ./...` is a no-op.
//
// Run with: go test -tags livesmoke ./internal/modelgateway/adapter/outbound/providers/gemini/...
package gemini

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// apiKeyOrSkip returns the configured Gemini API key, skipping the test when neither
// GEMINI_API_KEY nor GOOGLE_API_KEY is present.
func apiKeyOrSkip(t *testing.T) string {
	t.Helper()
	for _, name := range []string{"GEMINI_API_KEY", "GOOGLE_API_KEY"} {
		if v := os.Getenv(name); v != "" {
			return v
		}
	}
	t.Skip("live smoke skipped: set GEMINI_API_KEY or GOOGLE_API_KEY to run")
	return ""
}

// liveModel is the model the smoke test targets; overridable for local runs.
func liveModel() string {
	if v := os.Getenv("GEMINI_SMOKE_MODEL"); v != "" {
		return v
	}
	return "gemini-2.5-flash"
}

// TestLiveGenerate exercises a real non-streaming generation end-to-end.
func TestLiveGenerate(t *testing.T) {
	key := apiKeyOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	p, err := New(ctx, Config{APIKey: key})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	resp, err := p.Generate(ctx, llm.Request{
		Model:     liveModel(),
		System:    "Reply with a single word.",
		Messages:  []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Say hello."}}}}},
		MaxTokens: 32,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(resp.Content) == 0 {
		t.Errorf("empty content")
	}
	if resp.StopReason == "" {
		t.Errorf("empty stop reason")
	}
	t.Logf("stop=%s usage=%+v", resp.StopReason, resp.Usage)
}

// TestLiveStream exercises a real streamed generation end-to-end and asserts a
// terminal Done is delivered before io.EOF.
func TestLiveStream(t *testing.T) {
	key := apiKeyOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	p, err := New(ctx, Config{APIKey: key})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	sr, err := p.Stream(ctx, llm.Request{
		Model:     liveModel(),
		Messages:  []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Count to three."}}}}},
		MaxTokens: 64,
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = sr.Close() }()

	var sawDone bool
	for {
		ev, rerr := sr.Recv()
		if errors.Is(rerr, errEOF) {
			break
		}
		if rerr != nil {
			t.Fatalf("Recv: %v", rerr)
		}
		if ev.Done != nil {
			sawDone = true
		}
	}
	if !sawDone {
		t.Errorf("stream ended without a Done event")
	}
}

// TestLiveCountTokens exercises the real countTokens endpoint.
func TestLiveCountTokens(t *testing.T) {
	key := apiKeyOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p, err := New(ctx, Config{APIKey: key})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n, err := p.CountTokens(ctx, llm.Request{
		Model:    liveModel(),
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "How many tokens is this?"}}}}},
	})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n <= 0 {
		t.Errorf("CountTokens = %d, want > 0", n)
	}
}
