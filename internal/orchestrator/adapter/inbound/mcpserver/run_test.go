// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// The run streaming tool + the concurrent-approval model (T-13..T-19) — the
// load-bearing correctness milestone of MCP Server mode:
//
//   - AC-8:  run completes normally (progressToken present) → status:"completed"
//   - AC-12: progressToken ABSENT → single application/json (no SSE) [FIX-5]
//   - AC-12: progressToken round-trip (string AND number), strictly increasing
//   - AC-9:  run hitting the ask gate emits an in-band approval
//            notifications/progress frame, call stays OPEN (no terminal result)
//   - AC-10: a CONCURRENT control approve (separate connection) resolves the
//            REAL gate and the open run leg then reaches status:"completed"
//   - AC-11: after_seq reconnect (durable cursor)
//   - run error cases: missing session_id → -32602; unknown session → 404
//
// The concurrent-approval ACs use the REAL *approval.Gate (only it makes Resolve
// actually unblock Request). No fixed sleeps: readSSEUntil blocks on the live
// stream until the predicate matches (R-4).

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// completesWith returns a runner fn that appends one assistant message then
// terminates with the given reason and final text.
func completesWith(h *harness, reason domain.TerminationReason, finalText string) func(context.Context, igrpc.RunSpec) (igrpc.RunOutcome, error) {
	return func(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error) {
		if _, err := h.store.Append(ctx, spec.SessionID, 0, 0, "r1", app.AppendInput{Event: assistantEvent(finalText)}); err != nil {
			return igrpc.RunOutcome{}, err
		}
		return igrpc.RunOutcome{Reason: reason, FinalText: finalText, NumTurns: 1}, nil
	}
}

// ---------------------------------------------------------------------------
// T-14 — run completes normally (AC-8) + run error cases
// ---------------------------------------------------------------------------

// TestRun_CompletesNormally pins AC-8: a progressToken-present run that
// completes (no ask gate) ends with a CallToolResult isError:false,
// structuredContent.status=="completed", non-empty final_text, terminal
// after_seq.
func TestRun_CompletesNormally(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-ok", 1))
	h.runner.fn = completesWith(h, domain.Success, "hello from the loop")

	resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-ok", "text": "hi"}, "tok-1")
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/event-stream")

	// Read until the final JSON-RPC response (a result with structuredContent).
	final, _, ok := readSSEUntil(t, resp, 3*time.Second, isFinalResult)
	require.True(t, ok, "must observe a final CallToolResult on the run leg")

	cr := callResultFromSSE(t, final)
	assert.False(t, cr.IsError)
	assert.Equal(t, "completed", cr.StructuredContent["status"])
	assert.Equal(t, "hello from the loop", cr.StructuredContent["final_text"])
	require.Contains(t, cr.StructuredContent, "after_seq")

	// The run reached the shared Runner exactly once (it carried the user turn
	// onto the verified tenant) — mirrors rest_test.go's spec assertion.
	assert.Equal(t, 1, h.runner.specCount(), "run must drive the shared Runner once")
}

// TestRun_ProgressTokenAbsent_SingleJSON pins AC-12 / [FIX-5]: with no
// progressToken the response is a single application/json CallToolResult (NO
// text/event-stream, no progress framing).
func TestRun_ProgressTokenAbsent_SingleJSON(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-nojson", 1))
	h.runner.fn = completesWith(h, domain.Success, "done")

	// progressToken nil → single JSON. Use the plain callTool path with an
	// application/json Accept.
	env, resp := h.callTool(t, "", "run", map[string]any{"session_id": "r-nojson", "text": "hi"})
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json",
		"absent progressToken must be a single application/json response, NOT text/event-stream")
	assert.NotContains(t, resp.Header.Get("Content-Type"), "event-stream")
	cr := decodeCallResult(t, env)
	assert.False(t, cr.IsError)
	assert.Equal(t, "completed", cr.StructuredContent["status"])
}

// TestRun_MissingSessionID_32602 pins the pre-output edge error.
func TestRun_MissingSessionID_32602(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "run", map[string]any{"text": "hi"})
	require.NotNil(t, env.Error)
	assert.Equal(t, -32602, env.Error.Code)
}

// TestRun_UnknownSession_NotFound pins a pre-stream failure → JSON-RPC error,
// HTTP 404 (no SSE preamble committed).
func TestRun_UnknownSession_NotFound(t *testing.T) {
	h := devHarness(t)
	resp := h.rawRPC(t, "", buildRequest(t, 1, "tools/call",
		map[string]any{"name": "run", "arguments": map[string]any{"session_id": "nope", "text": "hi"}}), nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json", "pre-stream failures are JSON errors")
}

// ---------------------------------------------------------------------------
// T-15 — synthesized completed result shape (non-success subtype)
// ---------------------------------------------------------------------------

// TestRunResult_ShapeMatchesOutputSchema pins T-15: a non-success run carries
// the subtype token and status=="completed" with all declared output fields.
func TestRunResult_ShapeMatchesOutputSchema(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-sub", 1))
	h.runner.fn = completesWith(h, domain.ErrorMaxTurns, "")

	resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-sub", "text": "hi"}, "tok-2")
	defer func() { _ = resp.Body.Close() }()
	final, _, ok := readSSEUntil(t, resp, 3*time.Second, isFinalResult)
	require.True(t, ok)
	cr := callResultFromSSE(t, final)
	assert.Equal(t, "completed", cr.StructuredContent["status"])
	assert.Contains(t, jsonString(t, cr.StructuredContent["subtype"]), "TERMINATION_SUBTYPE_ERROR_MAX_TURNS")
	for _, field := range []string{"status", "session_id", "subtype", "final_text", "num_turns", "cost_usd", "after_seq"} {
		assert.Contains(t, cr.StructuredContent, field, "completed result must carry %q", field)
	}
}

// ---------------------------------------------------------------------------
// T-12-progress — progressToken round-trip (AC-12)
// ---------------------------------------------------------------------------

// TestRun_ProgressTokenRoundTrip pins AC-12: a progressToken that is a JSON
// string AND one that is a number both round-trip unchanged in the
// notifications/progress frames, progress strictly increases, and ≥1 progress
// frame precedes the final result.
func TestRun_ProgressTokenRoundTrip(t *testing.T) {
	for _, tok := range []any{"string-token", float64(7)} {
		tok := tok
		h := devHarness(t)
		h.store.seed(seededOwned("r-pt", 1))
		h.runner.fn = func(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error) {
			if _, err := h.store.Append(ctx, spec.SessionID, 0, 0, "r1",
				app.AppendInput{Event: assistantEvent("delta one")},
				app.AppendInput{Event: assistantEvent("delta two")},
			); err != nil {
				return igrpc.RunOutcome{}, err
			}
			return igrpc.RunOutcome{Reason: domain.Success, FinalText: "delta two", NumTurns: 1}, nil
		}

		resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-pt", "text": "hi"}, tok)
		// Read every frame up to and including the final result.
		_, all, ok := readSSEUntil(t, resp, 3*time.Second, isFinalResult)
		_ = resp.Body.Close()
		require.True(t, ok, "must reach the final result for token %v", tok)

		var progresses []float64
		var sawProgressBeforeResult bool
		var sawResult bool
		for _, f := range all {
			if isFinalResult(f) {
				sawResult = true
				continue
			}
			method, params := decodeProgress(t, f)
			if method != "notifications/progress" {
				continue
			}
			if !sawResult {
				sawProgressBeforeResult = true
			}
			// The token must round-trip verbatim.
			wantTok, err := json.Marshal(tok)
			require.NoError(t, err)
			assert.JSONEq(t, string(wantTok), string(params.ProgressToken), "progressToken must round-trip for %v", tok)
			progresses = append(progresses, params.Progress)
		}
		require.NotEmpty(t, progresses, "at least one progress frame for token %v", tok)
		assert.True(t, sawProgressBeforeResult, "a progress frame must precede the final result")
		for i := 1; i < len(progresses); i++ {
			assert.Greater(t, progresses[i], progresses[i-1], "progress must strictly increase")
		}
	}
}

// ---------------------------------------------------------------------------
// T-16 / T-17 — in-band approval + concurrent control (AC-9, AC-10)
// ---------------------------------------------------------------------------

// blockOnGateThenComplete returns a runner fn that calls the REAL gate's Request
// (which blocks until a concurrent control resolves it AND triggers the in-band
// approval frame via SubscribeApprovals), then — on approval — appends a final
// assistant message and returns Success; on denial it returns Refusal.
func blockOnGateThenComplete(h *harness, callID string) func(context.Context, igrpc.RunSpec) (igrpc.RunOutcome, error) {
	return func(ctx context.Context, spec igrpc.RunSpec) (igrpc.RunOutcome, error) {
		res, err := h.real.Request(ctx, app.ApprovalRequest{
			SessionID: spec.SessionID,
			CallID:    callID,
			ToolName:  "bash",
			Reason:    "mutating tool requires approval",
			Args:      map[string]any{"cmd": "rm -rf /tmp/x"},
		})
		if err != nil {
			return igrpc.RunOutcome{}, err
		}
		if res != domain.AskAllowed {
			return igrpc.RunOutcome{Reason: domain.Refusal}, nil
		}
		if _, aerr := h.store.Append(ctx, spec.SessionID, 0, 0, "r2", app.AppendInput{Event: assistantEvent("approved + done")}); aerr != nil {
			return igrpc.RunOutcome{}, aerr
		}
		return igrpc.RunOutcome{Reason: domain.Success, FinalText: "approved + done", NumTurns: 1}, nil
	}
}

// TestRun_ApprovalEmitsInBandProgress pins AC-9: a run that hits the ask gate
// emits an in-band approval notifications/progress frame carrying a non-empty
// call_id/tool_name/reason and an after_seq, while the run call has NOT yet
// returned a terminal result (the loop is still blocked at the gate).
func TestRun_ApprovalEmitsInBandProgress(t *testing.T) {
	h := devRealGateHarness(t)
	h.store.seed(seededOwned("r-appr", 1))
	h.runner.fn = blockOnGateThenComplete(h, "call-approval-1")

	resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-appr", "text": "do risky thing"}, "tok-appr")
	defer func() { _ = resp.Body.Close() }()

	frame, all, ok := readSSEUntil(t, resp, 3*time.Second, isApprovalProgress)
	require.True(t, ok, "must observe an in-band approval progress frame")

	_, params := decodeProgress(t, frame)
	assert.NotEmpty(t, params.CallID, "approval frame must carry call_id")
	assert.NotEmpty(t, params.ToolName, "approval frame must carry tool_name")
	assert.NotEmpty(t, params.Reason, "approval frame must carry reason")
	assert.NotZero(t, params.AfterSeq, "approval frame must carry an after_seq cursor")

	// The run call has NOT returned a terminal result yet (loop is blocked).
	for _, f := range all {
		assert.False(t, isFinalResult(f), "no terminal result must precede the approval (the call stays open, blocked at the gate)")
	}
}

// TestRun_ConcurrentApproveCompletes pins AC-10: while the run leg is open and
// blocked at the gate, a CONCURRENT control approve on a SEPARATE connection
// resolves the REAL gate (proving the loop was NOT aborted by the open call),
// and the run leg then reaches status:"completed".
func TestRun_ConcurrentApproveCompletes(t *testing.T) {
	h := devRealGateHarness(t)
	h.store.seed(seededOwned("r-cc", 1))
	const callID = "call-cc-1"
	h.runner.fn = blockOnGateThenComplete(h, callID)

	resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-cc", "text": "do risky thing"}, "tok-cc")
	defer func() { _ = resp.Body.Close() }()

	// 1) read until the in-band approval frame (this proves the loop is blocked).
	approval, _, ok := readSSEUntil(t, resp, 3*time.Second, isApprovalProgress)
	require.True(t, ok, "must observe the approval frame before approving")
	_, ap := decodeProgress(t, approval)
	require.Equal(t, callID, ap.CallID)

	// 2) CONCURRENTLY (separate connection) approve via the control tool.
	env, ctlResp := h.callTool(t, "", "control", map[string]any{
		"session_id": "r-cc", "action": "approve", "call_id": callID,
	})
	require.Equal(t, http.StatusOK, ctlResp.StatusCode)
	cr := decodeCallResult(t, env)
	require.Contains(t, cr.StructuredContent, "head_seq", "control approve must return head_seq (the gate Resolve succeeded → entry was still pending → loop NOT aborted)")

	// 3) the open run leg then reaches a terminal completed result.
	final, _, ok := readSSEUntil(t, resp, 3*time.Second, isFinalResult)
	require.True(t, ok, "the open run leg must complete after the concurrent approve")
	rcr := callResultFromSSE(t, final)
	assert.False(t, rcr.IsError)
	assert.Equal(t, "completed", rcr.StructuredContent["status"])
}

// ---------------------------------------------------------------------------
// T-18 — after_seq reconnect (AC-11)
// ---------------------------------------------------------------------------

// TestRun_AfterSeqReconnect pins AC-11: re-calling run with the prior after_seq
// (and empty text) drives the subscription from that cursor (only seq >
// after_seq replays) and reaches a terminal completed result.
func TestRun_AfterSeqReconnect(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("r-resume", 7))
	h.runner.fn = completesWith(h, domain.Success, "resumed")

	resp := h.openRunLeg(t, "", map[string]any{"session_id": "r-resume", "after_seq": 7}, "tok-rs")
	defer func() { _ = resp.Body.Close() }()
	_, _, ok := readSSEUntil(t, resp, 3*time.Second, isFinalResult)
	require.True(t, ok, "the resumed run must complete")

	from, present := h.store.firstSubscribedFrom()
	require.True(t, present, "the run must have subscribed")
	assert.Equal(t, int64(7), from, "after_seq must drive the resume cursor")
}

// ---------------------------------------------------------------------------
// SSE frame classifiers / decoders
// ---------------------------------------------------------------------------

// isFinalResult reports whether an SSE frame's data is a JSON-RPC RESPONSE
// (carrying a CallToolResult in result), i.e. the terminal frame of the run leg.
func isFinalResult(f sseFrame) bool {
	if f.data == "" {
		return false
	}
	var msg struct {
		Result *struct {
			StructuredContent map[string]any `json:"structuredContent"`
		} `json:"result"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal([]byte(f.data), &msg); err != nil {
		return false
	}
	return msg.Method == "" && msg.Result != nil
}

// isApprovalProgress reports whether an SSE frame is the in-band approval
// notifications/progress (it carries a non-empty call_id in params).
func isApprovalProgress(f sseFrame) bool {
	if f.data == "" {
		return false
	}
	var msg struct {
		Method string `json:"method"`
		Params struct {
			CallID string `json:"call_id"`
		} `json:"params"`
	}
	if err := json.Unmarshal([]byte(f.data), &msg); err != nil {
		return false
	}
	return msg.Method == "notifications/progress" && msg.Params.CallID != ""
}

// callResultFromSSE decodes the CallToolResult carried in a final-result SSE
// frame's JSON-RPC response.
func callResultFromSSE(t *testing.T, f sseFrame) callResult {
	t.Helper()
	require.Contains(t, f.data, "result", "final frame must be a JSON-RPC response")
	var msg struct {
		Result callResult `json:"result"`
	}
	require.NoError(t, json.Unmarshal([]byte(f.data), &msg), "final frame data: %s", f.data)
	return msg.Result
}

// guard against an accidental unused import of strings if the file evolves.
var _ = strings.TrimSpace
