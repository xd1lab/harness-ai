package anthropic

// Feature S (structured output) — TDD red, AC-11 / AC-11b / TASKS T-12.
//
// Anthropic native structured output on the STABLE Messages path: when the request
// carries an OutputSchema AND the resolved CENTRAL caps mark
// SupportsJSONSchemaStrict, buildMessageParams must set
// params.OutputConfig.Format = sdk.JSONOutputFormatParam{Schema: <decoded map>}
// (verified stable types: MessageNewParams.OutputConfig message.go:9398 →
// OutputConfigParam.Format message.go:4210 → JSONOutputFormatParam.Schema
// map[string]any message.go:3373; message.go is `package anthropic`, the stable
// package — NO Beta). Otherwise OutputConfig stays the zero value and the schema
// reaches the loop for validate-and-retry.
//
// The gate MUST read the threaded central caps, NOT the adapter's local
// resolveCapabilities (anthropicCommon self-reports strict=true for ALL modern
// Claude prefixes — the WRONG gate). So a legacy/unflipped id whose central caps
// are false must NOT get OutputConfig even though resolveCapabilities says true.
//
// RED until buildMessageParams is threaded with caps (new signature
// `buildMessageParams(req llm.Request, defaultMaxTokens int64, caps llm.Capabilities)`,
// T-12). This file calls the NEW 3-arg signature, so it does not COMPILE against
// the current 2-arg builder — that compile failure is the red state.

import (
	"encoding/json"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

func strictCapsAnthropic() llm.Capabilities {
	return llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: true}
}

// TestBuildMessageParams_NativeOutputConfig_WhenCapsStrict pins AC-11 (native ON):
// strict central caps + schema → OutputConfig.Format.Schema equals the decoded
// schema, set on the STABLE MessageNewParams.
func TestBuildMessageParams_NativeOutputConfig_WhenCapsStrict(t *testing.T) {
	schema := json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	req := llm.Request{
		Model:        "claude-opus-4-0",
		Messages:     []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
		OutputSchema: schema,
		Strict:       true,
	}
	params, err := buildMessageParams(req, 4096, strictCapsAnthropic()) // RED: 3rd arg does not exist today.
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}

	// Typed assertion: the decoded schema must be carried on the stable
	// OutputConfig.Format.Schema (map[string]any).
	got := params.OutputConfig.Format.Schema
	if got == nil {
		t.Fatalf("OutputConfig.Format.Schema must be set when native is on")
	}
	if got["type"] != "object" {
		t.Errorf("OutputConfig.Format.Schema.type = %v, want object", got["type"])
	}

	// Wire assertion: the params serialize an output_config.format json_schema.
	m := marshalParams(t, params)
	oc, ok := m["output_config"].(map[string]any)
	if !ok {
		t.Fatalf("output_config must be present on the stable params, got %v", m["output_config"])
	}
	format, ok := oc["format"].(map[string]any)
	if !ok {
		t.Fatalf("output_config.format must be a json_schema config, got %v", oc["format"])
	}
	if format["schema"] == nil {
		t.Errorf("output_config.format.schema must carry the decoded schema")
	}
}

// TestBuildMessageParams_NoNative_WhenCapsNotStrict pins AC-11b (fallback): a
// central caps strict=false (e.g. a legacy id not flipped) leaves OutputConfig the
// ZERO value even with a schema — the schema still flows to the loop.
func TestBuildMessageParams_NoNative_WhenCapsNotStrict(t *testing.T) {
	req := llm.Request{
		Model:        "claude-3-5-sonnet-20241022",
		Messages:     []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Strict:       true,
	}
	params, err := buildMessageParams(req, 4096, llm.Capabilities{SupportsTools: true, SupportsJSONSchemaStrict: false})
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	if params.OutputConfig.Format.Schema != nil {
		t.Errorf("OutputConfig must be zero-valued when central caps strict is false (legacy/unflipped id)")
	}
}

// TestBuildMessageParams_NoNative_WhenNoSchema pins that an empty schema never sets
// OutputConfig regardless of caps.
func TestBuildMessageParams_NoNative_WhenNoSchema(t *testing.T) {
	req := llm.Request{
		Model:    "claude-opus-4-0",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}}},
	}
	params, err := buildMessageParams(req, 4096, strictCapsAnthropic())
	if err != nil {
		t.Fatalf("buildMessageParams: %v", err)
	}
	if params.OutputConfig.Format.Schema != nil {
		t.Errorf("OutputConfig must be zero-valued when OutputSchema is empty")
	}
}
