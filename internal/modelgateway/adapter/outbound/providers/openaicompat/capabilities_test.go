package openaicompat

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestOllamaProfile_TracksGeneric pins the deliberate choice that current Ollama
// versions get the full generic profile (tools, parallel and streaming tool calls,
// system prompt) rather than a degraded one.
func TestOllamaProfile_TracksGeneric(t *testing.T) {
	got := OllamaProfile()
	if got != GenericProfile() {
		t.Fatalf("OllamaProfile diverged from GenericProfile:\n got %#v\nwant %#v", got, GenericProfile())
	}
	if !got.SupportsTools || !got.SupportsParallelToolCalls || !got.SupportsStreamingToolCalls || !got.SupportsSystemPrompt {
		t.Fatalf("Ollama must support tools + parallel + streaming + system prompt: %#v", got)
	}
}

// TestEndpointProfile_Capabilities asserts the profile-to-capabilities rendering:
// every profile flag passes through, while token counting and strict JSON-Schema
// enforcement are forced off for ALL OpenAI-compatible endpoints — there is no
// portable count-tokens endpoint or guaranteed strict-schema support.
func TestEndpointProfile_Capabilities(t *testing.T) {
	p := EndpointProfile{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		MaxOutputTokens:            4096,
	}
	want := llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            4096,
	}
	if got := p.capabilities(); got != want {
		t.Fatalf("capabilities mapping wrong:\n got %#v\nwant %#v", got, want)
	}
}
