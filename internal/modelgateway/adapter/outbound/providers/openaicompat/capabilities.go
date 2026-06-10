package openaicompat

import "github.com/boltrope/boltrope/internal/platform/llm"

// EndpointProfile captures the capability defaults for one OpenAI-compatible
// endpoint kind. Self-hosted servers vary in what they support — notably LM Studio
// does not stream tool-call arguments or run tool calls in parallel — so the
// adapter resolves capabilities from a per-endpoint profile rather than assuming a
// single Chat-Completions-shaped capability set (architecture §11.4, §11.5).
//
// The zero EndpointProfile is a conservative generic profile: tools and a system
// prompt are supported, streaming tool calls are assumed (most servers stream),
// and token counting and strict schema enforcement are off. Construct named
// profiles with the helpers below or supply [Config.Profile] directly.
type EndpointProfile struct {
	// SupportsTools reports whether the endpoint accepts function tools.
	SupportsTools bool
	// SupportsParallelToolCalls reports whether more than one tool call may be
	// emitted per turn.
	SupportsParallelToolCalls bool
	// SupportsStreamingToolCalls reports whether tool-call arguments stream
	// incrementally. When false, the normalizer still emits one complete
	// ToolCallDelta per call; this flag lets the gateway advertise the limitation.
	SupportsStreamingToolCalls bool
	// SupportsVision reports whether image inputs are accepted.
	SupportsVision bool
	// SupportsSystemPrompt reports whether a system-role message is honored.
	SupportsSystemPrompt bool
	// SupportsThinking reports whether the endpoint surfaces reasoning content.
	SupportsThinking bool
	// MaxOutputTokens is the per-response generation cap, or zero if unknown.
	MaxOutputTokens int
}

// capabilities renders the profile as normalized [llm.Capabilities] for a model.
// Token counting and strict JSON-Schema enforcement are always off for the
// OpenAI-compatible path: there is no portable count-tokens endpoint or guaranteed
// strict-schema support across self-hosted servers (architecture §11.6, §8.12).
func (p EndpointProfile) capabilities() llm.Capabilities {
	return llm.Capabilities{
		SupportsTools:              p.SupportsTools,
		SupportsParallelToolCalls:  p.SupportsParallelToolCalls,
		SupportsStreamingToolCalls: p.SupportsStreamingToolCalls,
		SupportsVision:             p.SupportsVision,
		SupportsSystemPrompt:       p.SupportsSystemPrompt,
		SupportsThinking:           p.SupportsThinking,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            p.MaxOutputTokens,
	}
}

// GenericProfile is the default profile for an unspecified OpenAI-compatible
// endpoint (e.g. vLLM, llama.cpp, TGI, LiteLLM): tools, parallel and streaming
// tool calls, and a system prompt are supported.
func GenericProfile() EndpointProfile {
	return EndpointProfile{
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsSystemPrompt:       true,
	}
}

// OllamaProfile is the profile for an Ollama endpoint. Ollama supports function
// tools and a system prompt; tool-call streaming is supported on current versions.
func OllamaProfile() EndpointProfile {
	p := GenericProfile()
	return p
}

// LMStudioProfile is the profile for an LM Studio endpoint, which does NOT stream
// tool-call arguments and does NOT run tool calls in parallel; the gateway buffers
// the whole call and emits it complete (architecture §11.2, §11.4).
func LMStudioProfile() EndpointProfile {
	return EndpointProfile{
		SupportsTools:              true,
		SupportsParallelToolCalls:  false,
		SupportsStreamingToolCalls: false,
		SupportsSystemPrompt:       true,
	}
}
