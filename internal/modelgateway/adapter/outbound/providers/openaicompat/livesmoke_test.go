//go:build livesmoke

// Package openaicompat live smoke test. Build-tagged so it never runs in the
// default unit suite; it requires a reachable OpenAI-compatible endpoint and is
// skipped when the API key env is unset. Run with:
//
//	go test -tags livesmoke ./internal/modelgateway/adapter/outbound/providers/openaicompat/...
package openaicompat

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// liveBaseURL is the endpoint to exercise; defaults to a local Ollama OpenAI shim.
func liveBaseURL() string {
	if v := os.Getenv("OPENAI_COMPAT_BASE_URL"); v != "" {
		return v
	}
	return "http://localhost:11434/v1"
}

func liveModel() string {
	if v := os.Getenv("OPENAI_COMPAT_MODEL"); v != "" {
		return v
	}
	return "llama3"
}

func TestLive_Stream(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY unset; skipping live OpenAI-compatible smoke test")
	}
	p, err := New(Config{BaseURL: liveBaseURL(), APIKey: os.Getenv("OPENAI_API_KEY")})
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

	var sawDone bool
	for {
		ev, err := reader.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if ev.Done != nil {
			sawDone = true
		}
	}
	if !sawDone {
		t.Fatalf("stream ended without a Done event")
	}
}
