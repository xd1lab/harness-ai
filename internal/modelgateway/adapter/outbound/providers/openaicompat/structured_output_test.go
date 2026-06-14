package openaicompat

// Feature S (structured output) — TDD red, AC-12 / TASKS T-14.
//
// OpenAI-compat NEVER blind-sends json_schema to an unknown self-hosted server.
// buildParams only sets params.ResponseFormat (json_schema) when the resolved caps
// mark SupportsJSONSchemaStrict — which by default is FALSE for every model on this
// path and is only flipped true via the central Registry endpoint override
// (capabilities.Registry.SetEndpointOverride), threaded in as caps. With default
// caps + a schema → NO ResponseFormat (fall back to loop validate-retry).
//
// RED until buildParams is threaded with caps (new signature
// `buildParams(req llm.Request, caps llm.Capabilities)`, T-14). This file calls the
// NEW signature, so it does not COMPILE against the current 1-arg buildParams —
// that compile failure is the red state.

import (
	"encoding/json"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

func marshalParamsCaps(t *testing.T, req llm.Request, caps llm.Capabilities) map[string]any {
	t.Helper()
	params, err := buildParams(req, caps) // RED: buildParams takes only req today.
	if err != nil {
		t.Fatalf("buildParams: %v", err)
	}
	b, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	return m
}

// TestBuildParams_NoNativeByDefault pins AC-12 (the honest default): a schema with
// DEFAULT caps (SupportsJSONSchemaStrict=false, the only value the default profile
// can yield) must NOT set response_format — the request shape is unchanged and the
// loop validates.
func TestBuildParams_NoNativeByDefault(t *testing.T) {
	m := marshalParamsCaps(t, llm.Request{
		Model:        "local-model",
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Strict:       true,
	}, llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: false})

	if _, ok := m["response_format"]; ok {
		t.Errorf("response_format must NOT be sent by default for a self-hosted endpoint, got %v", m["response_format"])
	}
}

// TestBuildParams_NativeWhenOverrideStrict pins AC-12 (opt-in): when an endpoint
// override marks the caps strict (the value the central Registry resolves after
// SetEndpointOverride), a schema DOES set a json_schema response_format.
func TestBuildParams_NativeWhenOverrideStrict(t *testing.T) {
	m := marshalParamsCaps(t, llm.Request{
		Model:        "local-model",
		OutputSchema: json.RawMessage(`{"type":"object","required":["answer"]}`),
		Strict:       true,
	}, llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true})

	rf, ok := m["response_format"].(map[string]any)
	if !ok {
		t.Fatalf("response_format must be set when the override marks strict, got %v", m["response_format"])
	}
	if rf["type"] != "json_schema" {
		t.Errorf("response_format.type = %v, want json_schema", rf["type"])
	}
}

// TestBuildParams_NoNative_WhenNoSchema pins that an empty schema never sets native
// output even when caps are strict.
func TestBuildParams_NoNative_WhenNoSchema(t *testing.T) {
	m := marshalParamsCaps(t, llm.Request{Model: "local-model"},
		llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true})
	if _, ok := m["response_format"]; ok {
		t.Errorf("response_format must be unset when no OutputSchema is present, got %v", m["response_format"])
	}
}
