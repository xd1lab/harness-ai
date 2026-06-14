package capabilities_test

// Feature S (structured output) — TDD red, AC-9..AC-12 (table) / TASKS T-9.
//
// The central table is the SINGLE source of truth for the native structured-output
// gate (SupportsJSONSchemaStrict, meaning converged to "native structured output —
// response_format/output_config/response_schema — available and strict-enforceable").
// T-9 flips ONLY id-verified modern Claude ids to true; legacy claude-3-* / 3.5-*
// stay false; Gemini 2.5/2.0-flash already true; openaicompat default false.
//
// These assertions are RED until T-9 flips the verified Claude ids in
// capabilities.go (today all 7 Claude entries are false). The per-id flip MUST be
// evidence-gated against Anthropic's structured-outputs docs (recorded in
// TRACE.md) — this test locks the matrix so the provider native gates have a
// deterministic input.

import (
	"testing"

	"github.com/xd1lab/harness-ai/internal/modelgateway/app/capabilities"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// TestStructuredOutputCaps_ModernClaudeFlipped pins T-9: the verified modern Claude
// ids resolve SupportsJSONSchemaStrict=true centrally so the Anthropic native gate
// can fire.
func TestStructuredOutputCaps_ModernClaudeFlipped(t *testing.T) {
	t.Parallel()
	reg := capabilities.NewRegistry(nil)
	for _, model := range []string{"claude-opus-4-0", "claude-sonnet-4-0", "claude-3-7-sonnet-20250219"} {
		caps := reg.Resolve("anthropic", model)
		if !caps.SupportsJSONSchemaStrict {
			t.Errorf("%s: SupportsJSONSchemaStrict must be true centrally after the verified flip", model)
		}
	}
}

// TestStructuredOutputCaps_LegacyClaudeStayFalse pins the conservative default: an
// unverified legacy id stays false (fall back to loop validate-retry).
func TestStructuredOutputCaps_LegacyClaudeStayFalse(t *testing.T) {
	t.Parallel()
	reg := capabilities.NewRegistry(nil)
	for _, model := range []string{"claude-3-5-sonnet-20241022", "claude-3-haiku-20240307", "claude-3-opus-20240229"} {
		caps := reg.Resolve("anthropic", model)
		if caps.SupportsJSONSchemaStrict {
			t.Errorf("%s: legacy id must stay SupportsJSONSchemaStrict=false unless verified", model)
		}
	}
}

// TestStructuredOutputCaps_GeminiStrictTrue pins the Gemini natives stay true (the
// adapter gate must read THIS, not its hard-coded false self-report).
func TestStructuredOutputCaps_GeminiStrictTrue(t *testing.T) {
	t.Parallel()
	reg := capabilities.NewRegistry(nil)
	for _, model := range []string{"gemini-2.5-pro", "gemini-2.5-flash", "gemini-2.0-flash"} {
		caps := reg.Resolve("gemini", model)
		if !caps.SupportsJSONSchemaStrict {
			t.Errorf("%s: SupportsJSONSchemaStrict must be true centrally", model)
		}
	}
}

// TestStructuredOutputCaps_OpenAICompatDefaultFalse_OverrideTrue pins T-14's source
// of truth: the openaicompat endpoint default is false, and only SetEndpointOverride
// flips it true (the mechanism the adapter reads through the T-9b seam).
func TestStructuredOutputCaps_OpenAICompatDefaultFalse_OverrideTrue(t *testing.T) {
	t.Parallel()
	reg := capabilities.NewRegistry(nil)
	if reg.Resolve("openaicompat", "local-model").SupportsJSONSchemaStrict {
		t.Errorf("openaicompat default must be SupportsJSONSchemaStrict=false")
	}

	strictCaps := llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true}
	reg.SetEndpointOverride("openaicompat", capabilities.EndpointOverride{
		AllModels: &strictCaps,
	})
	if !reg.Resolve("openaicompat", "local-model").SupportsJSONSchemaStrict {
		t.Errorf("after SetEndpointOverride(AllModels strict=true), openaicompat must resolve strict=true")
	}
}
