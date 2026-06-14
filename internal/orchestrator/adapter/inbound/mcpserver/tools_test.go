// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// Unary tool-call tests (T-7..T-12): tools/call dispatch + the 4 non-streaming
// tools (create_session, get_session, control, fork) onto the shared
// igrpc.Server methods, plus the auth/origin/405 conformance ACs.
//
//   - AC-16 tail: unknown tool name → -32602
//   - AC-4 / AC-5: create_session plan-mode / bypass-rejected
//   - AC-6: get_session projection
//   - AC-7: fork creates child
//   - AC-10b: control no-pending approve/deny → FailedPrecondition → HTTP 400
//   - AC-13/14/15: auth modes + ownership
//   - AC-17: GET/DELETE → 405 + Allow: POST
//   - AC-18: Origin guard

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// seededForeign returns a session owned by a different tenant than the dev
// principal (for the ownership/PermissionDenied ACs).
func seededForeign(id string) domain.Session {
	return domain.Session{ID: id, TenantID: "99999999-9999-4999-8999-999999999999"}
}

// seededOwned returns a session owned by the dev tenant.
func seededOwned(id string, headSeq int64) domain.Session {
	return domain.Session{ID: id, TenantID: igrpc.DevTenantID, HeadSeq: headSeq}
}

// ---------------------------------------------------------------------------
// T-7 — tools/call dispatch (AC-16 tail)
// ---------------------------------------------------------------------------

// TestToolsCall_UnknownTool_32602 pins AC-16 tail: an unknown tool name is a
// JSON-RPC InvalidParams (-32602), not a CallToolResult.
func TestToolsCall_UnknownTool_32602(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "no_such_tool", map[string]any{})
	require.NotNil(t, env.Error)
	assert.Equal(t, -32602, env.Error.Code)
}

// ---------------------------------------------------------------------------
// T-8 — create_session (AC-4, AC-5)
// ---------------------------------------------------------------------------

// TestCreateSession_PlanMode pins AC-4: mode:"plan" → isError:false, non-empty
// structuredContent.session_id, and the shared CreateSession recorded the PLAN
// mode (proving the call reached igrpc.Server.CreateSession).
func TestCreateSession_PlanMode(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "create_session", map[string]any{"mode": "plan"})
	cr := decodeCallResult(t, env)
	assert.False(t, cr.IsError)
	sid, _ := cr.StructuredContent["session_id"].(string)
	require.NotEmpty(t, sid, "create_session must return a session_id")

	require.Equal(t, 1, h.store.createdModesLen())
	assert.Equal(t, domain.ModePlan, h.store.firstCreatedMode(), "must reach CreateSession with the plan mode")
}

// TestCreateSession_BypassRejected pins AC-5: mode:"bypass" → JSON-RPC
// InvalidParams (-32602) from the shared operator-only guard, NOT a
// CallToolResult.
func TestCreateSession_BypassRejected(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "create_session", map[string]any{"mode": "bypass"})
	require.NotNil(t, env.Error, "client-set bypass must be a JSON-RPC error, not a result")
	assert.Equal(t, -32602, env.Error.Code)
}

// TestCreateSession_UnknownMode_32602 pins the edge-side strict mode parse.
func TestCreateSession_UnknownMode_32602(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "create_session", map[string]any{"mode": "yolo"})
	require.NotNil(t, env.Error)
	assert.Equal(t, -32602, env.Error.Code)
}

// ---------------------------------------------------------------------------
// T-9 — get_session (AC-6)
// ---------------------------------------------------------------------------

// TestGetSession_ReturnsProjection pins AC-6: an owned session's
// structuredContent decodes to a Session with the expected sessionId/status/
// mode (token substrings, not exact bytes).
func TestGetSession_ReturnsProjection(t *testing.T) {
	h := devHarness(t)
	h.store.seed(domain.Session{ID: "s-get", TenantID: igrpc.DevTenantID, HeadSeq: 3, Mode: domain.ModePlan, Status: domain.StatusActive})

	env, _ := h.callTool(t, "", "get_session", map[string]any{"session_id": "s-get"})
	cr := decodeCallResult(t, env)
	assert.False(t, cr.IsError)
	sess, _ := cr.StructuredContent["session"].(map[string]any)
	require.NotNil(t, sess, "get_session must carry the session projection")
	assert.Equal(t, "s-get", sess["sessionId"])
	// protojson lowerCamel enum tokens.
	assert.Contains(t, jsonString(t, sess["status"]), "ACTIVE")
	assert.Contains(t, jsonString(t, sess["mode"]), "PLAN")
}

// TestGetSession_MissingID_32602 pins the missing-arg edge error.
func TestGetSession_MissingID_32602(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "get_session", map[string]any{})
	require.NotNil(t, env.Error)
	assert.Equal(t, -32602, env.Error.Code)
}

// ---------------------------------------------------------------------------
// T-10 — control (AC-10b + wiring)
// ---------------------------------------------------------------------------

// TestControl_ApproveResolvesGate pins the approve wiring against the STUB gate
// (mirrors REST): structuredContent.head_seq present and the gate recorded the
// resolve.
func TestControl_ApproveResolvesGate(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("s-ctl", 4))

	env, _ := h.callTool(t, "", "control", map[string]any{"session_id": "s-ctl", "action": "approve", "call_id": "call-9"})
	cr := decodeCallResult(t, env)
	assert.False(t, cr.IsError)
	require.Contains(t, cr.StructuredContent, "head_seq")

	resolves := h.gate.snapshot()
	require.Len(t, resolves, 1)
	assert.Equal(t, resolveCall{"s-ctl", "call-9", domain.AskAllowed}, resolves[0])
}

// TestControl_UnknownAction_32602 pins strict action parsing.
func TestControl_UnknownAction_32602(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("s-ctl2", 1))
	env, _ := h.callTool(t, "", "control", map[string]any{"action": "explode"})
	require.NotNil(t, env.Error)
	assert.Equal(t, -32602, env.Error.Code)
}

// TestControl_NoPendingApproval_400 pins AC-10b: against the REAL gate with no
// pending entry, approve returns FailedPrecondition → -32001 +
// data.grpc_code=="FailedPrecondition" + HTTP 400 (explicitly NOT 409).
func TestControl_NoPendingApproval_400(t *testing.T) {
	h := devRealGateHarness(t)
	h.store.seed(seededOwned("s-nop", 2))

	resp := h.rawRPC(t, "", buildRequest(t, 1, "tools/call", map[string]any{
		"name":      "control",
		"arguments": map[string]any{"session_id": "s-nop", "action": "approve", "call_id": "stale-call"},
	}), nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "no-pending FailedPrecondition must be HTTP 400, NOT 409")

	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.NotNil(t, env.Error)
	assert.Equal(t, -32001, env.Error.Code)
	var data struct {
		GRPCCode string `json:"grpc_code"`
	}
	require.NoError(t, json.Unmarshal(env.Error.Data, &data))
	assert.Equal(t, "FailedPrecondition", data.GRPCCode)
}

// ---------------------------------------------------------------------------
// T-11 — fork (AC-7)
// ---------------------------------------------------------------------------

// TestFork_CreatesChild pins AC-7: an owned parent yields a distinct child
// session_id and the store recorded the at_seq (ParentID/ForkedFromSeq).
func TestFork_CreatesChild(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededOwned("s-fork", 9))

	env, _ := h.callTool(t, "", "fork", map[string]any{"session_id": "s-fork", "at_seq": 5})
	cr := decodeCallResult(t, env)
	child, _ := cr.StructuredContent["session_id"].(string)
	require.NotEmpty(t, child)
	assert.NotEqual(t, "s-fork", child, "child must be distinct from parent")

	got, err := h.store.LoadSession(reqCtx(), child)
	require.NoError(t, err)
	assert.Equal(t, "s-fork", got.ParentID)
	assert.Equal(t, int64(5), got.ForkedFromSeq)
}

// ---------------------------------------------------------------------------
// T-12 — auth modes + ownership + Origin + 405 conformance
// ---------------------------------------------------------------------------

// TestAuth_ProductionRequiresBearer pins AC-13: in production, a tools/call with
// no/garbage bearer → HTTP 401 + WWW-Authenticate: Bearer + JSON-RPC error; the
// shared method is never reached (the store stays untouched).
func TestAuth_ProductionRequiresBearer(t *testing.T) {
	h := prodHarness(t)

	resp := h.rawRPC(t, "", buildRequest(t, 1, "tools/call",
		map[string]any{"name": "create_session", "arguments": map[string]any{}}), nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("WWW-Authenticate"), "Bearer")
	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.NotNil(t, env.Error)
	assert.Equal(t, 0, h.store.createdModesLen(), "the shared method must never be reached without a valid bearer")
}

// TestAuth_DevModeNoBearer pins AC-15: dev-insecure → a tools/call create_session
// with no bearer succeeds under DevTenantID.
func TestAuth_DevModeNoBearer(t *testing.T) {
	h := devHarness(t)
	env, _ := h.callTool(t, "", "create_session", map[string]any{})
	cr := decodeCallResult(t, env)
	sid, _ := cr.StructuredContent["session_id"].(string)
	require.NotEmpty(t, sid)
	sess, err := h.store.LoadSession(reqCtx(), sid)
	require.NoError(t, err)
	assert.Equal(t, igrpc.DevTenantID, sess.TenantID)
}

// TestOwnership_ForeignTenantDenied pins AC-14: get_session/run/control/fork
// against a foreign-tenant session → -32001 + HTTP 403 (PermissionDenied).
func TestOwnership_ForeignTenantDenied(t *testing.T) {
	h := prodHarness(t)
	h.store.seed(seededForeign("alien"))
	tenant := "33333333-3333-4333-8333-333333333333" // caller tenant != alien's
	token := hs256Token(t, tenant)

	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"get_session", map[string]any{"session_id": "alien"}},
		{"control", map[string]any{"session_id": "alien", "action": "interrupt"}},
		{"fork", map[string]any{"session_id": "alien", "at_seq": 1}},
		{"run", map[string]any{"session_id": "alien", "text": "x"}},
	} {
		resp := h.rawRPC(t, token, buildRequest(t, 1, "tools/call",
			map[string]any{"name": tc.tool, "arguments": tc.args}), nil)
		assert.Equal(t, http.StatusForbidden, resp.StatusCode, "%s on a foreign session must be 403", tc.tool)
		var env rpcEnvelope
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
		_ = resp.Body.Close()
		require.NotNil(t, env.Error, "%s must be a JSON-RPC error", tc.tool)
		assert.Equal(t, -32001, env.Error.Code, "%s must be the server-defined -32001", tc.tool)
	}
}

// TestGetDelete_405WithAllow pins AC-17: GET /mcp and DELETE /mcp each → 405
// with Allow: POST (deferred features signaled conformantly, not 404).
func TestGetDelete_405WithAllow(t *testing.T) {
	h := devHarness(t)
	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req, err := http.NewRequest(method, h.srv.URL+"/mcp", nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
		assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode, "%s /mcp must be 405", method)
		assert.Equal(t, "POST", resp.Header.Get("Allow"), "%s /mcp must advertise Allow: POST", method)
	}
}

// TestOrigin_PresentNotAllowed_403 pins AC-18: a present, non-allowlisted Origin
// is rejected with HTTP 403 (empty allowlist + present Origin → reject).
func TestOrigin_PresentNotAllowed_403(t *testing.T) {
	h := devHarness(t) // empty allowlist
	resp := h.rawRPC(t, "", buildRequest(t, 1, "initialize", map[string]any{}),
		map[string]string{"Origin": "https://evil.example"})
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "a present, non-allowlisted Origin must be 403")
}

// TestOrigin_AbsentAllowed pins AC-18: absent Origin proceeds (non-browser
// clients unaffected).
func TestOrigin_AbsentAllowed(t *testing.T) {
	h := devHarness(t)
	resp := h.rawRPC(t, "", buildRequest(t, 1, "initialize", map[string]any{}), nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "an absent Origin must proceed")
}

// TestOrigin_PresentAllowed pins the allowlisted-origin allow path.
func TestOrigin_PresentAllowed(t *testing.T) {
	h := newHarness(t, igrpc.AuthConfig{DevInsecure: true}, []string{"https://app.example"})
	resp := h.rawRPC(t, "", buildRequest(t, 1, "initialize", map[string]any{}),
		map[string]string{"Origin": "https://app.example"})
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "an allowlisted Origin must proceed")
}
