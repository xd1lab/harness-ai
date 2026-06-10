//go:build livesmoke

// Package openai live smoke test. Build-tagged so it never runs in the default unit
// suite; it hits the real OpenAI Responses API and is skipped when OPENAI_API_KEY is
// unset. Run with:
//
//	go test -tags livesmoke ./internal/modelgateway/adapter/outbound/providers/openai/...
package openai

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

func liveModel() string {
	if v := os.Getenv("OPENAI_MODEL"); v != "" {
		return v
	}
	return "gpt-5.4-mini"
}

func TestLive_ResponsesStream(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY unset; skipping live OpenAI Responses smoke test")
	}
	p, err := New(Config{APIKey: os.Getenv("OPENAI_API_KEY")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	reader, err := p.Stream(ctx, llm.Request{
		Model:    liveModel(),
		System:   "You are concise.",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Say hello in one word."}}}}},
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer reader.Close()

	var sawText, sawDone bool
	for {
		ev, err := reader.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.TextDelta != nil {
			sawText = true
		}
		if ev.Done != nil {
			sawDone = true
			if ev.Done.Usage.OutputTokens == 0 {
				t.Errorf("expected non-zero output usage on Done")
			}
		}
	}
	if !sawText {
		t.Errorf("expected at least one text delta")
	}
	if !sawDone {
		t.Fatalf("stream ended without a Done event")
	}
}

func TestLive_ResponsesGenerate(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY unset; skipping live OpenAI Responses smoke test")
	}
	p, err := New(Config{APIKey: os.Getenv("OPENAI_API_KEY")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	resp, err := p.Generate(ctx, llm.Request{
		Model:    liveModel(),
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "Reply with the word OK."}}}}},
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(resp.Content) == 0 {
		t.Fatalf("expected non-empty content")
	}
}
