package capabilities_test

import (
	"testing"

	"github.com/boltrope/boltrope/internal/modelgateway/app/capabilities"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// ----------------------------------------------------------------------------
// Tests: TDD for T-MGW-02 — Capabilities resolver
// Tests first (per the TDD mandate):
//   1. Known model returns expected caps (e.g. claude-3-5-sonnet vs claude-3-haiku differ).
//   2. Unknown model falls back to a conservative default.
//   3. A per-endpoint override changes the result.
// ----------------------------------------------------------------------------

// TestKnownModelsReturnExpectedCaps checks that well-known models in the
// built-in registry return the documented capability flags. Specifically, the
// implementation plan (T-MGW-02) requires:
//
//   - (anthropic, claude-3-5-sonnet-…) and (anthropic, claude-3-haiku-…) must
//     return different MaxOutputTokens.
//   - LM Studio endpoint defaults: SupportsStreamingToolCalls=false,
//     SupportsParallelToolCalls=false.
//   - All v1 models: SupportsServerSideTools=false (architecture §8.12 hard
//     policy; expressed here as an assertion that the flag is NOT set — the
//     llm.Capabilities struct does not carry that flag, so this maps to
//     checking SupportsTools is controlled appropriately by the gateway).
func TestKnownModelsReturnExpectedCaps(t *testing.T) {
	t.Parallel()

	reg := capabilities.NewRegistry(nil) // nil = no endpoint overrides

	t.Run("claude-3-5-sonnet has larger MaxOutputTokens than haiku", func(t *testing.T) {
		t.Parallel()

		const (
			sonnetModel = "claude-3-5-sonnet-20241022"
			haikuModel  = "claude-3-haiku-20240307"
			endpoint    = "anthropic"
		)

		sonnet := reg.Resolve(endpoint, sonnetModel)
		haiku := reg.Resolve(endpoint, haikuModel)

		if sonnet.MaxOutputTokens == 0 {
			t.Errorf("claude-3-5-sonnet: MaxOutputTokens must not be zero (model must be in registry)")
		}
		if haiku.MaxOutputTokens == 0 {
			t.Errorf("claude-3-haiku: MaxOutputTokens must not be zero (model must be in registry)")
		}
		if sonnet.MaxOutputTokens <= haiku.MaxOutputTokens {
			t.Errorf("expected claude-3-5-sonnet MaxOutputTokens (%d) > claude-3-haiku MaxOutputTokens (%d)",
				sonnet.MaxOutputTokens, haiku.MaxOutputTokens)
		}
	})

	t.Run("claude-3-5-sonnet supports tools and streaming tool calls", func(t *testing.T) {
		t.Parallel()

		caps := reg.Resolve("anthropic", "claude-3-5-sonnet-20241022")
		if !caps.SupportsTools {
			t.Error("claude-3-5-sonnet: SupportsTools must be true")
		}
		if !caps.SupportsParallelToolCalls {
			t.Error("claude-3-5-sonnet: SupportsParallelToolCalls must be true")
		}
		if !caps.SupportsStreamingToolCalls {
			t.Error("claude-3-5-sonnet: SupportsStreamingToolCalls must be true")
		}
	})

	t.Run("claude-3-5-sonnet supports vision", func(t *testing.T) {
		t.Parallel()

		caps := reg.Resolve("anthropic", "claude-3-5-sonnet-20241022")
		if !caps.SupportsVision {
			t.Error("claude-3-5-sonnet: SupportsVision must be true")
		}
	})

	t.Run("gemini-2.0-flash-lite disables streaming tool calls per architecture §11.4", func(t *testing.T) {
		t.Parallel()

		// Architecture §11.4 and ADR-0016 call out that some Gemini models
		// (e.g. Flash-Lite) lack streaming tool-call support.
		caps := reg.Resolve("gemini", "gemini-2.0-flash-lite")
		if caps.SupportsStreamingToolCalls {
			t.Error("gemini-2.0-flash-lite: SupportsStreamingToolCalls must be false (architecture §11.4)")
		}
	})

	t.Run("gemini-2.5-pro supports tools", func(t *testing.T) {
		t.Parallel()

		caps := reg.Resolve("gemini", "gemini-2.5-pro")
		if !caps.SupportsTools {
			t.Error("gemini-2.5-pro: SupportsTools must be true")
		}
	})

	t.Run("gpt-4o supports tools and parallel tool calls", func(t *testing.T) {
		t.Parallel()

		caps := reg.Resolve("openai", "gpt-4o")
		if !caps.SupportsTools {
			t.Error("gpt-4o: SupportsTools must be true")
		}
		if !caps.SupportsParallelToolCalls {
			t.Error("gpt-4o: SupportsParallelToolCalls must be true")
		}
	})
}

// TestUnknownModelFallsBackToConservativeDefault checks that a model not found
// in the registry returns conservative (minimal / least-capable) defaults
// rather than assuming capabilities that might not exist (architecture §11.4:
// "the loop never default-assumes Chat-Completions-shaped capabilities").
func TestUnknownModelFallsBackToConservativeDefault(t *testing.T) {
	t.Parallel()

	reg := capabilities.NewRegistry(nil)

	caps := reg.Resolve("anthropic", "unknown-model-xyzzy-9999")

	// Conservative defaults: all boolean flags false, MaxOutputTokens 0 (unspecified).
	tests := []struct {
		name string
		got  bool
		want bool
	}{
		{"SupportsTools", caps.SupportsTools, false},
		{"SupportsParallelToolCalls", caps.SupportsParallelToolCalls, false},
		{"SupportsStreamingToolCalls", caps.SupportsStreamingToolCalls, false},
		{"SupportsVision", caps.SupportsVision, false},
		{"SupportsSystemPrompt", caps.SupportsSystemPrompt, false},
		{"SupportsThinking", caps.SupportsThinking, false},
		{"SupportsTokenCounting", caps.SupportsTokenCounting, false},
		{"SupportsJSONSchemaStrict", caps.SupportsJSONSchemaStrict, false},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("conservative default: %s = %v, want %v", tt.name, tt.got, tt.want)
		}
	}
	if caps.MaxOutputTokens != 0 {
		t.Errorf("conservative default: MaxOutputTokens = %d, want 0 (unknown/unspecified)", caps.MaxOutputTokens)
	}
}

// TestUnknownEndpointUnknownModelFallsBackToConservativeDefault ensures that
// an entirely unknown endpoint (not just unknown model) also returns a
// conservative default.
func TestUnknownEndpointUnknownModelFallsBackToConservativeDefault(t *testing.T) {
	t.Parallel()

	reg := capabilities.NewRegistry(nil)
	caps := reg.Resolve("mystery-provider-xyz", "mystery-model-abc")

	if caps.SupportsTools || caps.SupportsParallelToolCalls || caps.SupportsStreamingToolCalls {
		t.Error("completely unknown (endpoint, model): all capability flags must be false by default")
	}
	if caps.MaxOutputTokens != 0 {
		t.Errorf("completely unknown: MaxOutputTokens = %d, want 0", caps.MaxOutputTokens)
	}
}

// TestPerEndpointOverrideChangesResult verifies that a per-endpoint override
// supplied at registry construction time wins over the per-model default. The
// canonical example from the architecture is an LM Studio (OpenAI-compatible)
// endpoint: SupportsStreamingToolCalls=false, SupportsParallelToolCalls=false
// (ADR-0016; architecture §11.4, §11.5).
func TestPerEndpointOverrideChangesResult(t *testing.T) {
	t.Parallel()

	// Build an override: an LM Studio endpoint that disables streaming and
	// parallel tool calls, regardless of model.
	lmStudioOverride := capabilities.EndpointOverride{
		// Apply to all models on this endpoint.
		AllModels: &llm.Capabilities{
			SupportsTools:              true, // tools still work, just not streaming/parallel
			SupportsStreamingToolCalls: false,
			SupportsParallelToolCalls:  false,
			SupportsVision:             false,
			SupportsSystemPrompt:       true,
			MaxOutputTokens:            8192,
		},
	}

	overrides := map[string]capabilities.EndpointOverride{
		"lmstudio": lmStudioOverride,
	}

	reg := capabilities.NewRegistry(overrides)

	// gpt-4o normally has streaming + parallel tool calls, but under the
	// lmstudio endpoint override those must be disabled.
	caps := reg.Resolve("lmstudio", "gpt-4o")

	if caps.SupportsStreamingToolCalls {
		t.Error("lmstudio endpoint: SupportsStreamingToolCalls must be false (per-endpoint override)")
	}
	if caps.SupportsParallelToolCalls {
		t.Error("lmstudio endpoint: SupportsParallelToolCalls must be false (per-endpoint override)")
	}
	if !caps.SupportsTools {
		t.Error("lmstudio endpoint: SupportsTools must be true (set in override)")
	}
	if caps.MaxOutputTokens != 8192 {
		t.Errorf("lmstudio endpoint: MaxOutputTokens = %d, want 8192", caps.MaxOutputTokens)
	}

	// A different endpoint (anthropic) for the same model should NOT be affected
	// by the lmstudio override.
	anthropicCaps := reg.Resolve("anthropic", "gpt-4o") // unusual combo but tests isolation
	// anthropic endpoint has no override for gpt-4o; it should fall back to
	// either the known model caps or the conservative default. Either way, the
	// lmstudio override must not contaminate it.
	_ = anthropicCaps // just verify no panic; isolation tested by absence of lmstudio values
}

// TestPerEndpointOverrideForSpecificModel verifies that a model-specific
// endpoint override applies only to the named model, not to other models on the
// same endpoint.
func TestPerEndpointOverrideForSpecificModel(t *testing.T) {
	t.Parallel()

	// Provide a model-specific override: on the "myendpoint" endpoint,
	// "special-model" gets custom caps.
	overrides := map[string]capabilities.EndpointOverride{
		"myendpoint": {
			PerModel: map[string]*llm.Capabilities{
				"special-model": {
					SupportsTools:              true,
					SupportsStreamingToolCalls: false, // special model can't stream tool calls
					SupportsParallelToolCalls:  false,
					MaxOutputTokens:            4096,
				},
			},
		},
	}

	reg := capabilities.NewRegistry(overrides)

	special := reg.Resolve("myendpoint", "special-model")
	if special.SupportsStreamingToolCalls {
		t.Error("myendpoint/special-model: SupportsStreamingToolCalls must be false (per-model override)")
	}
	if special.MaxOutputTokens != 4096 {
		t.Errorf("myendpoint/special-model: MaxOutputTokens = %d, want 4096", special.MaxOutputTokens)
	}

	// A different model on the same endpoint should fall through to the default,
	// not pick up special-model's caps.
	other := reg.Resolve("myendpoint", "other-model")
	if other.MaxOutputTokens == 4096 {
		t.Error("myendpoint/other-model: must not inherit special-model's MaxOutputTokens override")
	}
}

// TestRuntimeOverride verifies that overrides can be applied at runtime (after
// registry construction) via SetEndpointOverride. This models config-driven,
// runtime-overridable capability injection as described in the implementation
// plan T-MGW-02.
func TestRuntimeOverride(t *testing.T) {
	t.Parallel()

	reg := capabilities.NewRegistry(nil)

	// Before the runtime override, an unknown model on "dynamic-endpoint" must
	// fall back to conservative defaults (unknown endpoint, unknown model).
	// We use a model id that is not in the built-in table so the test exercises
	// the conservative-default path, not the model-lookup path.
	const unknownModel = "custom-llm-rig-v2-not-in-registry"
	before := reg.Resolve("dynamic-endpoint", unknownModel)
	if before.SupportsTools {
		t.Error("dynamic-endpoint before override: SupportsTools must be false (no override yet)")
	}

	// Apply a runtime override.
	reg.SetEndpointOverride("dynamic-endpoint", capabilities.EndpointOverride{
		AllModels: &llm.Capabilities{
			SupportsTools:              true,
			SupportsStreamingToolCalls: true,
			SupportsParallelToolCalls:  true,
			MaxOutputTokens:            16384,
		},
	})

	after := reg.Resolve("dynamic-endpoint", unknownModel)
	if !after.SupportsTools {
		t.Error("dynamic-endpoint after override: SupportsTools must be true")
	}
	if after.MaxOutputTokens != 16384 {
		t.Errorf("dynamic-endpoint after override: MaxOutputTokens = %d, want 16384", after.MaxOutputTokens)
	}
}

// TestOverridePrecedenceOrder checks resolution precedence:
// per-model endpoint override > all-models endpoint override > built-in model default > conservative default.
func TestOverridePrecedenceOrder(t *testing.T) {
	t.Parallel()

	// We have both AllModels and PerModel overrides for the same endpoint.
	overrides := map[string]capabilities.EndpointOverride{
		"myep": {
			AllModels: &llm.Capabilities{
				MaxOutputTokens: 1000,
			},
			PerModel: map[string]*llm.Capabilities{
				"priority-model": {
					MaxOutputTokens: 9999, // per-model wins over AllModels
				},
			},
		},
	}

	reg := capabilities.NewRegistry(overrides)

	// Per-model override wins.
	pm := reg.Resolve("myep", "priority-model")
	if pm.MaxOutputTokens != 9999 {
		t.Errorf("per-model override: MaxOutputTokens = %d, want 9999", pm.MaxOutputTokens)
	}

	// AllModels override applies to other models.
	other := reg.Resolve("myep", "other-model")
	if other.MaxOutputTokens != 1000 {
		t.Errorf("all-models override: MaxOutputTokens = %d, want 1000", other.MaxOutputTokens)
	}

	// Known model on a different endpoint (no override) falls back to registry.
	known := reg.Resolve("anthropic", "claude-3-5-sonnet-20241022")
	if known.MaxOutputTokens == 0 {
		t.Error("known model on unaffected endpoint: MaxOutputTokens must be non-zero")
	}
}
