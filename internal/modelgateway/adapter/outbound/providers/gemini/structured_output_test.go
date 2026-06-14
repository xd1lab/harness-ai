package gemini

// Feature S (structured output) — TDD red, AC-10 / TASKS T-11.
//
// Gemini native structured output: when the request carries an OutputSchema AND
// the resolved CENTRAL caps mark SupportsJSONSchemaStrict, buildConfig must set
// cfg.ResponseMIMEType="application/json" and cfg.ResponseJsonSchema to the decoded
// schema; otherwise both stay unset and the loop's validate-retry is the backstop.
// The gate MUST read the threaded central caps, NOT the adapter's hard-coded
// Capabilities() self-report (which returns SupportsJSONSchemaStrict=false).
//
// RED until buildConfig is threaded with caps (new signature
// `buildConfig(req llm.Request, caps llm.Capabilities)`, T-11). This file calls the
// NEW signature, so it does not COMPILE against the current 1-arg buildConfig —
// that compile failure is the red state.

import (
	"encoding/json"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

func strictCapsGemini() llm.Capabilities {
	return llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true}
}

// TestBuildConfig_NativeResponseSchema_WhenCapsStrict pins AC-10 (native ON):
// strict central caps + schema → ResponseMIMEType "application/json" +
// ResponseJsonSchema set to the decoded schema.
func TestBuildConfig_NativeResponseSchema_WhenCapsStrict(t *testing.T) {
	t.Parallel()
	schema := json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	cfg, err := buildConfig(llm.Request{ // RED: buildConfig takes only req today.
		Model:        "gemini-2.5-pro",
		OutputSchema: schema,
		Strict:       true,
	}, strictCapsGemini())
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.ResponseMIMEType != "application/json" {
		t.Errorf("ResponseMIMEType = %q, want application/json", cfg.ResponseMIMEType)
	}
	if cfg.ResponseJsonSchema == nil {
		t.Errorf("ResponseJsonSchema must carry the decoded schema when native is on")
	}
}

// TestBuildConfig_NoNative_WhenCapsNotStrict pins AC-10 (fallback): caps without
// strict (e.g. an unsupported model) leaves both response fields unset even with a
// schema — the gate reads the threaded central caps.
func TestBuildConfig_NoNative_WhenCapsNotStrict(t *testing.T) {
	t.Parallel()
	cfg, err := buildConfig(llm.Request{
		Model:        "gemini-1.0-pro",
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Strict:       true,
	}, llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: false})
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.ResponseMIMEType != "" {
		t.Errorf("ResponseMIMEType must be unset when caps strict is false, got %q", cfg.ResponseMIMEType)
	}
	if cfg.ResponseJsonSchema != nil {
		t.Errorf("ResponseJsonSchema must be unset when caps strict is false")
	}
}

// TestBuildConfig_NoNative_WhenNoSchema pins that an empty schema never sets native
// output regardless of caps.
func TestBuildConfig_NoNative_WhenNoSchema(t *testing.T) {
	t.Parallel()
	cfg, err := buildConfig(llm.Request{Model: "gemini-2.5-pro"}, strictCapsGemini())
	if err != nil {
		t.Fatalf("buildConfig: %v", err)
	}
	if cfg.ResponseMIMEType != "" || cfg.ResponseJsonSchema != nil {
		t.Errorf("no native output when OutputSchema is empty; got mime=%q schema=%v", cfg.ResponseMIMEType, cfg.ResponseJsonSchema)
	}
}
