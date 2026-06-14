// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// Feature S (structured output) — TDD red, AC-7 / AC-8 / TASKS T-7.
//
// The MCP run tool is a thin shell over the REAL igrpc.Server. v1 adds two
// optional run-tool arguments:
//   - AC-7: the advertised run-tool inputSchema lists output_schema (type object)
//     and strict (type boolean); neither is in required.
//   - AC-8: calling run with an output_schema OBJECT marshals it to bytes onto the
//     RunSpec the shared Runner receives, and sets Strict; a NON-object
//     output_schema is a JSON-RPC InvalidParams (-32602) and no run starts.
//
// RED until tools.go advertises the two properties + runArgs gains the fields, and
// run.go toolRun marshals/validates them onto genproto.RunRequest (T-7), atop the
// proto field + RunSpec field (T-2 + T-3). Mirrors tools_test.go / run_test.go.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestRunTool_AdvertisesOutputSchemaAndStrict pins AC-7: tools/list shows the run
// tool's inputSchema with optional output_schema (object) + strict (boolean),
// neither in the required list.
func TestRunTool_AdvertisesOutputSchemaAndStrict(t *testing.T) {
	h := devHarness(t)
	env, _ := h.doRPC(t, "", "tools/list", map[string]any{})
	require.Nil(t, env.Error)

	var res struct {
		Tools []struct {
			Name        string         `json:"name"`
			InputSchema map[string]any `json:"inputSchema"`
		} `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(env.Result, &res))

	var runSchema map[string]any
	for _, tool := range res.Tools {
		if tool.Name == "run" {
			runSchema = tool.InputSchema
		}
	}
	require.NotNil(t, runSchema, "the run tool must be advertised")

	props, ok := runSchema["properties"].(map[string]any)
	require.True(t, ok, "run inputSchema must have properties")

	osProp, ok := props["output_schema"].(map[string]any)
	require.True(t, ok, "run inputSchema must advertise output_schema")
	assert.Equal(t, "object", osProp["type"], "output_schema must be type object")

	strictProp, ok := props["strict"].(map[string]any)
	require.True(t, ok, "run inputSchema must advertise strict")
	assert.Equal(t, "boolean", strictProp["type"], "strict must be type boolean")

	// Neither new field may be required (only session_id is required).
	if req, ok := runSchema["required"].([]any); ok {
		for _, r := range req {
			assert.NotEqual(t, "output_schema", r, "output_schema must NOT be required")
			assert.NotEqual(t, "strict", r, "strict must NOT be required")
		}
	}
}

// TestRunTool_OutputSchemaObjectReachesRunSpec pins AC-8 (happy path): calling run
// with an output_schema OBJECT + strict marshals the object to bytes onto
// RunSpec.OutputSchema and sets RunSpec.Strict.
func TestRunTool_OutputSchemaObjectReachesRunSpec(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-so", 1))
	h.runner.fn = completesWith(h, domain.Success, "ok")

	schema := map[string]any{
		"type":     "object",
		"required": []any{"answer"},
		"properties": map[string]any{
			"answer": map[string]any{"type": "string"},
		},
	}
	env, resp := h.callTool(t, "", "run", map[string]any{
		"session_id":    "r-so",
		"text":          "hi",
		"output_schema": schema,
		"strict":        true,
	})
	require.Nil(t, env.Error, "a valid object output_schema must not error")
	_ = resp

	require.Equal(t, 1, h.runner.specCount(), "run must drive the shared Runner once")
	spec := h.runner.specs[0]
	require.NotEmpty(t, spec.OutputSchema, "output_schema object must be marshaled to RunSpec.OutputSchema bytes")
	assert.JSONEq(t, `{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`,
		string(spec.OutputSchema), "the marshaled schema bytes must equal the supplied object")
	assert.True(t, spec.Strict, "strict:true must reach RunSpec.Strict")
}

// TestRunTool_NonObjectOutputSchema_32602 pins AC-8 (error path): a non-object
// output_schema (here a JSON number) is a JSON-RPC InvalidParams (-32602) and the
// shared Runner is never invoked (no run starts).
func TestRunTool_NonObjectOutputSchema_32602(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-bad", 1))

	env, _ := h.callTool(t, "", "run", map[string]any{
		"session_id":    "r-bad",
		"text":          "hi",
		"output_schema": 42, // not a JSON object
	})
	require.NotNil(t, env.Error, "a non-object output_schema must be a JSON-RPC error, not a result")
	assert.Equal(t, -32602, env.Error.Code, "non-object output_schema must be InvalidParams (-32602)")
	assert.Equal(t, 0, h.runner.specCount(), "no run must start on an invalid output_schema")
}
