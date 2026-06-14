// Package capabilities provides the per-(endpoint, model) capability resolver
// for the model-gateway (T-MGW-02; FR-MODEL-03; architecture §11.4; ADR-0016).
//
// Capability variability is per-model within one endpoint: one Anthropic key
// serves models with different thinking/vision/context windows; one Gemini
// endpoint serves models with and without streaming tool-call support; one
// OpenAI Responses endpoint serves models with differing parallel-tool-call /
// strict-schema support. A request carries the model id; the registry returns
// model-specific flags.
//
// Resolution precedence (highest wins):
//
//  1. Per-model endpoint override for the (endpoint, model) pair.
//  2. All-models endpoint override for the endpoint.
//  3. Built-in per-model default from the static table.
//  4. Conservative default (all flags false, MaxOutputTokens 0).
//
// Both the initial override table and per-entry updates can be applied at
// runtime via [Registry.SetEndpointOverride], supporting config-driven and
// live-reloadable capability injection for self-hosted / OpenAI-compatible
// endpoints (architecture §11.4 open question: startup probe vs. static
// config).
package capabilities

import (
	"sync"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// EndpointOverride holds capability overrides for all (or specific) models on
// one endpoint. When AllModels is non-nil it applies to every model on that
// endpoint that does not have a more specific PerModel entry. PerModel entries
// win over AllModels (precedence documented in package godoc).
type EndpointOverride struct {
	// AllModels, when non-nil, applies to every model on the endpoint for which
	// no PerModel override exists. It is the canonical way to configure a
	// self-hosted / OpenAI-compatible endpoint (e.g. LM Studio) where the
	// capability set is endpoint-wide rather than per-model.
	AllModels *llm.Capabilities

	// PerModel maps a model id to its specific capabilities on this endpoint.
	// An entry here takes precedence over AllModels for that model id.
	PerModel map[string]*llm.Capabilities
}

// Registry is the capabilities resolver. It is safe for concurrent use.
//
// Construct one with [NewRegistry], supplying any initial endpoint overrides.
// Apply runtime overrides via [SetEndpointOverride].
type Registry struct {
	mu        sync.RWMutex
	overrides map[string]EndpointOverride // keyed by endpoint name
}

// NewRegistry returns a Registry pre-loaded with the given endpoint overrides.
// Pass nil or an empty map to start with only the built-in model table.
func NewRegistry(overrides map[string]EndpointOverride) *Registry {
	r := &Registry{
		overrides: make(map[string]EndpointOverride, len(overrides)),
	}
	for k, v := range overrides {
		r.overrides[k] = v
	}
	return r
}

// SetEndpointOverride installs or replaces the override configuration for one
// endpoint. It is safe to call concurrently. Calling with a zero-value
// EndpointOverride removes any meaningful override (AllModels and PerModel are
// both nil/empty) leaving the endpoint to resolve via the built-in model table.
func (r *Registry) SetEndpointOverride(endpoint string, override EndpointOverride) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.overrides[endpoint] = override
}

// Resolve returns the [llm.Capabilities] for the given (endpoint, model) pair.
// It applies the precedence order documented in the package overview:
// per-model endpoint override > all-models endpoint override >
// built-in model default > conservative default.
func (r *Registry) Resolve(endpoint, model string) llm.Capabilities {
	r.mu.RLock()
	eo, hasOverride := r.overrides[endpoint]
	r.mu.RUnlock()

	if hasOverride {
		// 1. Per-model endpoint override.
		if eo.PerModel != nil {
			if caps, ok := eo.PerModel[model]; ok && caps != nil {
				return *caps
			}
		}
		// 2. All-models endpoint override.
		if eo.AllModels != nil {
			return *eo.AllModels
		}
	}

	// 3. Built-in per-model default.
	if caps, ok := builtinCaps[model]; ok {
		return caps
	}

	// 4. Conservative default.
	return llm.Capabilities{}
}

// ---------------------------------------------------------------------------
// Built-in model capability table
// ---------------------------------------------------------------------------
//
// This table maps well-known model ids to their documented capabilities.
// It is seeded from:
//   - The implementation plan T-MGW-02 support-matrix
//   - ADR-0004 / ADR-0016
//   - Architecture §6 provider matrix, §11.4 capability flags
//   - Provider documentation verified at Gate 3 (2026-06-10)
//
// When a model id is NOT in this table, Resolve returns a conservative default
// (all flags false, MaxOutputTokens 0) — the loop must never assume capabilities
// the registry has not confirmed (architecture §11.4).
//
// Per-endpoint overrides (e.g. LM Studio disabling streaming/parallel tool
// calls) are supplied externally via [EndpointOverride] rather than hard-coded
// here, since any OpenAI-compatible endpoint may need them.
var builtinCaps = map[string]llm.Capabilities{
	// -----------------------------------------------------------------------
	// Anthropic Claude models
	// -----------------------------------------------------------------------

	// Claude 3.5 Sonnet (2024-10-22) — flagship multimodal, extended thinking
	"claude-3-5-sonnet-20241022": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false, // thinking is on claude-3-7-sonnet+
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            8192,
	},

	// Claude 3.5 Haiku (2024-11-05) — fast / low-cost
	"claude-3-5-haiku-20241105": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            8192,
	},

	// Claude 3 Haiku (2024-03-07) — legacy fast model
	"claude-3-haiku-20240307": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            4096,
	},

	// Claude 3 Opus (2024-02-29) — high-capability legacy
	"claude-3-opus-20240229": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            4096,
	},

	// Claude 3.7 Sonnet — first model with extended thinking
	"claude-3-7-sonnet-20250219": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		// Native structured output (stable Messages OutputConfig.Format) verified
		// against Anthropic's structured-outputs supported-models list (TRACE.md,
		// Feature S T-9). Flipped from the conservative default.
		SupportsJSONSchemaStrict: true,
		MaxOutputTokens:          16000,
	},

	// Claude 4 Opus — reasoning flagship (ADR-0004 research matrix)
	"claude-opus-4-0": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		// Native structured output verified (TRACE.md, Feature S T-9).
		SupportsJSONSchemaStrict: true,
		MaxOutputTokens:          32000,
	},

	// Claude 4 Sonnet
	"claude-sonnet-4-0": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		// Native structured output verified (TRACE.md, Feature S T-9).
		SupportsJSONSchemaStrict: true,
		MaxOutputTokens:          16000,
	},

	// -----------------------------------------------------------------------
	// Google Gemini models
	// -----------------------------------------------------------------------

	// Gemini 2.5 Pro — top-tier reasoning model
	"gemini-2.5-pro": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            65536,
	},

	// Gemini 2.5 Flash — fast, cost-effective, with thinking
	"gemini-2.5-flash": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           true,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            65536,
	},

	// Gemini 2.0 Flash — fast without thinking
	"gemini-2.0-flash": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            8192,
	},

	// Gemini 2.0 Flash-Lite — fastest/cheapest; NOTE: no streaming tool calls
	// per architecture §11.4 and T-MGW-02 support matrix.
	"gemini-2.0-flash-lite": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  false, // Flash-Lite limitation
		SupportsStreamingToolCalls: false, // Flash-Lite limitation (architecture §11.4)
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      true,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            8192,
	},

	// -----------------------------------------------------------------------
	// OpenAI models (Responses API primary; Chat Completions sub-flag)
	// -----------------------------------------------------------------------

	// GPT-4o — flagship multimodal
	"gpt-4o": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      false, // uses local o200k_base tokenizer or returns Unsupported
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            16384,
	},

	// GPT-4o Mini — cost-effective, small
	"gpt-4o-mini": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            16384,
	},

	// o3 — OpenAI reasoning model (Responses API, no system prompt, extended thinking)
	"o3": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       false, // o-series uses developer prompt
		SupportsThinking:           true,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            100000,
	},

	// o4-mini — compact reasoning model
	"o4-mini": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       false,
		SupportsThinking:           true,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   true,
		MaxOutputTokens:            65536,
	},

	// GPT-4 Turbo (legacy but still used)
	"gpt-4-turbo": {
		SupportsTools:              true,
		SupportsParallelToolCalls:  true,
		SupportsStreamingToolCalls: true,
		SupportsVision:             true,
		SupportsSystemPrompt:       true,
		SupportsThinking:           false,
		SupportsTokenCounting:      false,
		SupportsJSONSchemaStrict:   false,
		MaxOutputTokens:            4096,
	},
}
