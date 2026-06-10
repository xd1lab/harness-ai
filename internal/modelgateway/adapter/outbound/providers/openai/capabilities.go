package openai

import (
	"github.com/xd1lab/harness-ai/internal/modelgateway/adapter/outbound/providers/openaicompat"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// DefaultCapabilities returns the capability set for a current tool-capable OpenAI
// Responses model. Token counting is OFF — the harness bills from authoritative
// usage and there is no count-tokens endpoint (architecture §11.6) — and
// provider-native server-side tools are not advertised here; all tools flow through
// tool-runtime in v1 (architecture §8.12). The model-gateway capabilities resolver
// (T-MGW-02) may override these per model; this is the adapter's default when no
// explicit set is supplied.
func DefaultCapabilities() llm.Capabilities {
	return llm.Capabilities{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            0,
	}
}

// chatProfileFromCaps derives an OpenAI-compatible endpoint profile from the
// provider capabilities, used when delegating the Chat-Completions sub-flag path to
// the shared adapter. Token counting and strict schema enforcement remain governed
// by the shared adapter's Chat-surface policy.
func chatProfileFromCaps(caps llm.Capabilities) openaicompat.EndpointProfile {
	return openaicompat.EndpointProfile{
		SupportsTools:              caps.SupportsTools,
		SupportsParallelToolCalls:  caps.SupportsParallelToolCalls,
		SupportsStreamingToolCalls: caps.SupportsStreamingToolCalls,
		SupportsVision:             caps.SupportsVision,
		SupportsSystemPrompt:       caps.SupportsSystemPrompt,
		SupportsThinking:           caps.SupportsThinking,
		MaxOutputTokens:            caps.MaxOutputTokens,
	}
}
