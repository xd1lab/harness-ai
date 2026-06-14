package openai

// Feature S (structured output) — TDD red, AC-9 / TASKS T-10.
//
// OpenAI-Responses native structured output: when the request carries an
// OutputSchema AND the resolved caps mark SupportsJSONSchemaStrict, buildParams
// must set params.Text.Format to a json_schema config whose Strict equals
// (req.Strict && caps.SupportsJSONSchemaStrict); otherwise Text.Format is left
// unset and the loop's validate-retry remains the backstop.
//
// RED until buildParams is threaded with caps (new signature
// `buildParams(req llm.Request, caps llm.Capabilities)`, T-10). This file calls the
// NEW signature, so it does not COMPILE against the current 1-arg buildParams —
// that compile failure is the red state. The tool-schema Strict=false invariant
// (request.go:167) is INDEPENDENT of output strict and is asserted unchanged.

import (
	"encoding/json"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// marshalParamsCaps renders the built Responses params (with caps threaded) to a
// generic map so the wire shape can be asserted without a live endpoint.
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

// strictCaps returns caps with native structured output enabled.
func strictCaps() llm.Capabilities {
	return llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true}
}

// TestBuildParams_NativeJSONSchema_WhenCapsStrict pins AC-9 (native ON): an
// OutputSchema + strict caps + req.Strict yields a text.format json_schema config
// with strict true and the schema embedded.
func TestBuildParams_NativeJSONSchema_WhenCapsStrict(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	m := marshalParamsCaps(t, llm.Request{
		Model:        "gpt-4o",
		OutputSchema: schema,
		Strict:       true,
	}, strictCaps())

	text, ok := m["text"].(map[string]any)
	if !ok {
		t.Fatalf("text config must be set for native structured output, got %v", m["text"])
	}
	format, ok := text["format"].(map[string]any)
	if !ok {
		t.Fatalf("text.format must be a json_schema config, got %v", text["format"])
	}
	if format["type"] != "json_schema" {
		t.Errorf("text.format.type = %v, want json_schema", format["type"])
	}
	if format["strict"] != true {
		t.Errorf("text.format.strict = %v, want true (req.Strict && caps strict)", format["strict"])
	}
	if format["schema"] == nil {
		t.Errorf("text.format.schema must carry the decoded JSON schema")
	}
}

// TestBuildParams_NativeStrictFalse_WhenReqStrictFalse pins that strict is the AND
// of req.Strict and caps: caps strict but req.Strict false → format present but
// strict false (native schema, no hard strict enforcement).
func TestBuildParams_NativeStrictFalse_WhenReqStrictFalse(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	m := marshalParamsCaps(t, llm.Request{
		Model:        "gpt-4o",
		OutputSchema: schema,
		Strict:       false,
	}, strictCaps())

	text, ok := m["text"].(map[string]any)
	if !ok {
		t.Fatalf("text config must still be set when a schema is present, got %v", m["text"])
	}
	format := text["format"].(map[string]any)
	if format["strict"] == true {
		t.Errorf("text.format.strict must be false when req.Strict is false")
	}
}

// TestBuildParams_NoNative_WhenCapsNotStrict pins AC-9 (fallback): caps without
// strict leaves text.format unset even with a schema (the loop validates instead).
func TestBuildParams_NoNative_WhenCapsNotStrict(t *testing.T) {
	schema := json.RawMessage(`{"type":"object"}`)
	m := marshalParamsCaps(t, llm.Request{
		Model:        "gpt-4o",
		OutputSchema: schema,
		Strict:       true,
	}, llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: false})

	if _, ok := m["text"]; ok {
		t.Errorf("text.format must be unset when caps.SupportsJSONSchemaStrict is false, got %v", m["text"])
	}
}

// TestBuildParams_NoNative_WhenNoSchema pins that an empty OutputSchema never sets
// native output, regardless of caps.
func TestBuildParams_NoNative_WhenNoSchema(t *testing.T) {
	m := marshalParamsCaps(t, llm.Request{Model: "gpt-4o"}, strictCaps())
	if _, ok := m["text"]; ok {
		t.Errorf("text.format must be unset when no OutputSchema is present, got %v", m["text"])
	}
}

// TestBuildParams_ToolStrictStaysFalse pins R7: the tool-schema Strict invariant
// (tools strict=false; the loop validates tool args) is INDEPENDENT of the new
// output-format strict and must remain false even when output native is on.
func TestBuildParams_ToolStrictStaysFalse(t *testing.T) {
	m := marshalParamsCaps(t, llm.Request{
		Model:        "gpt-4o",
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Strict:       true,
		Tools: []llm.ToolDef{{
			Name:       "search",
			JSONSchema: json.RawMessage(`{"type":"object","properties":{"q":{"type":"string"}}}`),
		}},
	}, strictCaps())

	tools, ok := m["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("want 1 tool, got %#v", m["tools"])
	}
	tool := tools[0].(map[string]any)
	if tool["strict"] == true {
		t.Errorf("tool-schema strict must stay false (independent of output strict)")
	}
}
