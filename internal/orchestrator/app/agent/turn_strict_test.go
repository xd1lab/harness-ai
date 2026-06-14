// SPDX-License-Identifier: Apache-2.0

package agent_test

// Feature S (structured output) — TDD red, AC-5 / TASKS T-4 + T-5.
//
// These tests pin that the new additive loop-side field Config.Strict reaches the
// frozen llm.Request contract via turn.go buildRequest, alongside the already-wired
// Config.OutputSchema. They are RED until:
//   - T-4 adds `Strict bool` to agent.Config (loop.go), and
//   - T-5 sets `Strict: l.cfg.Strict` on the llm.Request in turn.go buildRequest.
// Until then this file does not COMPILE (agent.Config has no field Strict) — that
// compile failure is the red state for the new field; once the field exists the
// behavioral assertions take over.
//
// buildRequest is unexported, so the assertion is made through the real Loop.Run
// against the FakeModelGateway, which captures every llm.Request it received
// (apptest.ModelGatewayCall.Req). This mirrors the existing structured-output loop
// tests (loop_test.go TestRun_StructuredOutput*).

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// firstStreamReq returns the llm.Request the loop sent on its first Stream call.
func firstStreamReq(t *testing.T, h *harness) llm.Request {
	t.Helper()
	for _, c := range h.model.Calls() {
		if c.Method == "Stream" {
			return c.Req
		}
	}
	t.Fatalf("no Stream call was recorded on the fake gateway")
	return llm.Request{}
}

// TestBuildRequest_CarriesStrictAndOutputSchema pins AC-5: when Config.Strict is
// true and Config.OutputSchema is set, the llm.Request the loop builds carries
// BOTH Strict==true and the schema bytes (so strict reaches the gateway via the
// frozen llm.Request contract, exactly like OutputSchema already does).
func TestBuildRequest_CarriesStrictAndOutputSchema(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig()
	schema := json.RawMessage(`{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`)
	cfg.OutputSchema = schema
	cfg.Strict = true // RED: field does not exist until T-4.
	// A schema-valid response so the run terminates success on the first attempt;
	// the assertion is about the OUTBOUND request, not the outcome.
	h.model.AddStream(textStream(`{"answer":"42"}`), nil)

	_, err := h.run(t, cfg, "sess-strict", "structured")
	require.NoError(t, err)

	req := firstStreamReq(t, h)
	assert.True(t, req.Strict, "buildRequest must set llm.Request.Strict = Config.Strict")
	assert.JSONEq(t, string(schema), string(req.OutputSchema),
		"buildRequest must continue to carry the OutputSchema bytes")
}

// TestBuildRequest_StrictDefaultsFalse pins the negative: an unset Config.Strict
// leaves llm.Request.Strict false (backward compatible — existing clients that
// never set strict see no change), and an absent schema leaves OutputSchema empty.
func TestBuildRequest_StrictDefaultsFalse(t *testing.T) {
	h := newHarness(t)
	cfg := defaultConfig() // Strict zero value, no OutputSchema.
	h.model.AddStream(textStream("free-form answer"), nil)

	_, err := h.run(t, cfg, "sess-nostrict", "freeform")
	require.NoError(t, err)

	req := firstStreamReq(t, h)
	assert.False(t, req.Strict, "Strict must default false when Config.Strict is unset")
	assert.Empty(t, req.OutputSchema, "OutputSchema must stay empty when no schema is configured")
}
