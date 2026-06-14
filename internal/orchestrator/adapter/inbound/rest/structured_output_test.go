// SPDX-License-Identifier: Apache-2.0

package rest_test

// Feature S (structured output) — TDD red, AC-6 / TASKS T-6.
//
// The REST facade is a thin shell over the REAL igrpc.Server: a POST
// /v1/sessions/{id}/run with an output_schema (JSON object) + strict must assemble
// a genproto.RunRequest whose OutputSchema bytes / Strict are set, which the shared
// server maps onto the RunSpec the fake Runner captures. These are RED until:
//   - runBody gains OutputSchema json.RawMessage + Strict bool (T-6),
//   - the run handler copies them onto genproto.RunRequest (T-6), and
//   - the proto field + RunSpec field exist (T-2 + T-3).
//
// Mirrors the existing spec-capture idiom (rest_test.go
// TestRun_SSEStreamsFramesAndResult: h.runner.specs[0]).

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// TestRun_OutputSchemaAndStrictReachRunSpec pins AC-6: a run body carrying an
// inline JSON-object output_schema + strict:true sets RunSpec.OutputSchema (the raw
// JSON bytes) and RunSpec.Strict==true on the spec the shared Runner receives.
func TestRun_OutputSchemaAndStrictReachRunSpec(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s-so", TenantID: igrpc.DevTenantID, HeadSeq: 1})
	h.runner.fn = func(_ context.Context, _ igrpc.RunSpec) (igrpc.RunOutcome, error) {
		return igrpc.RunOutcome{Reason: domain.Success, FinalText: "ok", NumTurns: 1}, nil
	}

	schema := `{"type":"object","required":["answer"],"properties":{"answer":{"type":"string"}}}`
	body := `{"text":"hi","output_schema":` + schema + `,"strict":true}`
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s-so/run", "", body)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	drain(t, resp)

	require.Len(t, h.runner.specs, 1)
	assert.JSONEq(t, schema, string(h.runner.specs[0].OutputSchema),
		"the inline output_schema JSON must reach RunSpec.OutputSchema as raw bytes")
	assert.True(t, h.runner.specs[0].Strict, "strict:true must reach RunSpec.Strict")
}

// TestRun_NoSchemaIsFreeForm pins backward compatibility (AC-14 at REST): a body
// without the fields leaves the spec free-form (empty schema, strict false).
func TestRun_NoSchemaIsFreeForm(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s-free", TenantID: igrpc.DevTenantID, HeadSeq: 1})
	h.runner.fn = func(_ context.Context, _ igrpc.RunSpec) (igrpc.RunOutcome, error) {
		return igrpc.RunOutcome{Reason: domain.Success}, nil
	}

	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s-free/run", "", `{"text":"hi"}`)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	drain(t, resp)

	require.Len(t, h.runner.specs, 1)
	assert.Empty(t, h.runner.specs[0].OutputSchema, "absent output_schema must be free-form")
	assert.False(t, h.runner.specs[0].Strict, "absent strict must be false")
}

// TestRun_MalformedOutputSchemaIs400 pins the fail-closed-early boundary (T-6): a
// non-object / malformed output_schema is rejected at the facade with HTTP 400
// before any run starts (the Runner is never invoked).
func TestRun_MalformedOutputSchemaIs400(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s-bad", TenantID: igrpc.DevTenantID, HeadSeq: 1})

	// A JSON array is valid JSON but NOT a JSON Schema object → reject.
	body := `{"text":"hi","output_schema":[1,2,3]}`
	resp := h.doJSON(t, http.MethodPost, "/v1/sessions/s-bad/run", "", body)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"a non-object output_schema must be rejected with HTTP 400 before the run starts")
	assert.Empty(t, h.runner.specs, "no run must start on a malformed output_schema")
}

// drain reads the SSE/HTTP body to EOF so the run exchange completes before the
// test inspects the captured spec (reuses the package's copyAll helper).
func drain(t *testing.T, resp *http.Response) {
	t.Helper()
	var b strings.Builder
	_, _ = copyAll(&b, resp.Body)
}
