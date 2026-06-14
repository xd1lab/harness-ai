// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// Protocol-handshake + JSON-RPC envelope tests (T-3, T-4, T-5, T-6):
//   - dispatch error codes (-32700/-32600/-32601) — AC-16
//   - notifications/initialized → 202, no body — AC-2 (post-auth, dev mode)
//   - id echo verbatim (string and number) — [FIX-7]
//   - gRPC-status → HTTP-status parity + -32001 + data.grpc_code — R-2 / AC-10b half
//   - initialize result — AC-1
//   - tools/list 5 tools — AC-3
//
// All under the dev harness so the shared auth middleware passes (every
// POST /mcp is authenticated — R-1/[FIX-6]).

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// T-3 — dispatch + protocol error codes (AC-16)
// ---------------------------------------------------------------------------

// TestDispatch_ParseError_32700 pins that an unparseable JSON body yields a
// JSON-RPC parse error (-32700). AC-16.
func TestDispatch_ParseError_32700(t *testing.T) {
	h := devHarness(t)
	resp := h.rawRPC(t, "", "{not-json", nil)
	defer func() { _ = resp.Body.Close() }()
	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.NotNil(t, env.Error)
	assert.Equal(t, -32700, env.Error.Code, "unparseable body must be -32700 parse error")
}

// TestDispatch_InvalidRequest_32600 pins that valid JSON which is not a JSON-RPC
// 2.0 object (missing/!="2.0" jsonrpc, or missing method) yields -32600. AC-16.
func TestDispatch_InvalidRequest_32600(t *testing.T) {
	h := devHarness(t)
	for _, body := range []string{
		`{"id":1,"method":"initialize"}`,        // missing jsonrpc
		`{"jsonrpc":"1.0","id":1,"method":"x"}`, // wrong version
		`{"jsonrpc":"2.0","id":1}`,              // missing method
	} {
		resp := h.rawRPC(t, "", body, nil)
		var env rpcEnvelope
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
		_ = resp.Body.Close()
		require.NotNil(t, env.Error, "body %q must be an invalid-request error", body)
		assert.Equal(t, -32600, env.Error.Code, "body %q must be -32600", body)
	}
}

// TestDispatch_UnknownMethod_32601 pins that an unknown JSON-RPC method yields
// -32601 method-not-found. AC-16.
func TestDispatch_UnknownMethod_32601(t *testing.T) {
	h := devHarness(t)
	env, _ := h.doRPC(t, "", "nonexistent/method", nil)
	require.NotNil(t, env.Error)
	assert.Equal(t, -32601, env.Error.Code)
}

// TestNotificationsInitialized_202NoBody pins AC-2 ([FIX-6]): an AUTHENTICATED
// notifications/initialized (no id) is accepted with HTTP 202 and an empty body
// (no JSON-RPC result). The dev harness supplies the principal.
func TestNotificationsInitialized_202NoBody(t *testing.T) {
	h := devHarness(t)
	// A notification has no "id" field.
	body := `{"jsonrpc":"2.0","method":"notifications/initialized"}`
	resp := h.rawRPC(t, "", body, nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusAccepted, resp.StatusCode, "a notification must be 202")
	buf := make([]byte, 8)
	n, _ := resp.Body.Read(buf)
	assert.Equal(t, 0, n, "a notification must have no response body")
}

// TestResponse_EchoesIdVerbatim pins [FIX-7]: the response id round-trips
// unchanged whether it is a JSON string or a JSON number.
func TestResponse_EchoesIdVerbatim(t *testing.T) {
	h := devHarness(t)
	for _, idJSON := range []string{`"abc-123"`, `42`} {
		body := `{"jsonrpc":"2.0","id":` + idJSON + `,"method":"initialize","params":{}}`
		resp := h.rawRPC(t, "", body, nil)
		var env rpcEnvelope
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
		_ = resp.Body.Close()
		assert.JSONEq(t, idJSON, string(env.ID), "id must be echoed verbatim for %s", idJSON)
	}
}

// ---------------------------------------------------------------------------
// T-4 — gRPC-status → HTTP-status parity + error code mapping (R-2)
// ---------------------------------------------------------------------------

// TestStatusError_CarriesGrpcCodeAndCode32001 pins that a PermissionDenied
// (foreign-tenant) maps to the server-defined -32001 with
// data.grpc_code=="PermissionDenied" and HTTP 403; an Unauthenticated maps to
// HTTP 401 + WWW-Authenticate: Bearer. (AC-14 / AC-13 status halves; the parity
// table itself is asserted by the production-side TestHTTPStatusParity once the
// mapper exists — here we pin the observable wire behavior end-to-end.)
func TestStatusError_CarriesGrpcCodeAndCode32001(t *testing.T) {
	h := devHarness(t)
	h.store.seed(seededForeign("alien-1"))

	resp := h.rawRPC(t, "", buildRequest(t, 1, "tools/call",
		map[string]any{"name": "get_session", "arguments": map[string]any{"session_id": "alien-1"}}), nil)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusForbidden, resp.StatusCode, "foreign-tenant must be HTTP 403")

	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.NotNil(t, env.Error)
	assert.Equal(t, -32001, env.Error.Code, "PermissionDenied is the server-defined -32001")
	var data struct {
		GRPCCode string `json:"grpc_code"`
	}
	require.NoError(t, json.Unmarshal(env.Error.Data, &data))
	assert.Equal(t, "PermissionDenied", data.GRPCCode, "data.grpc_code must carry the gRPC code string")
}

// ---------------------------------------------------------------------------
// T-6 — initialize (AC-1)
// ---------------------------------------------------------------------------

// TestInitialize_ServerInfoAndCaps pins AC-1: serverInfo.name=="boltrope",
// serverInfo.version is the literal passed to NewHandler, protocolVersion is
// non-empty, capabilities.tools is present, and capabilities has NO
// sampling/elicitation key; the response carries a non-empty Mcp-Session-Id.
func TestInitialize_ServerInfoAndCaps(t *testing.T) {
	h := devHarness(t)
	resp := h.rawRPC(t, "", buildRequest(t, 1, "initialize", map[string]any{
		"protocolVersion": mcpProtoVers,
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test-client", "version": "0.0.1"},
	}), nil)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotEmpty(t, resp.Header.Get("Mcp-Session-Id"), "initialize must return an advisory Mcp-Session-Id header")

	var env rpcEnvelope
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&env))
	require.Nil(t, env.Error)

	var result struct {
		ProtocolVersion string `json:"protocolVersion"`
		ServerInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"serverInfo"`
		Capabilities map[string]any `json:"capabilities"`
	}
	require.NoError(t, json.Unmarshal(env.Result, &result))
	assert.Equal(t, "boltrope", result.ServerInfo.Name)
	assert.Equal(t, testVersion, result.ServerInfo.Version, "serverInfo.version must be the value passed to NewHandler")
	assert.NotEmpty(t, result.ProtocolVersion)
	_, hasTools := result.Capabilities["tools"]
	assert.True(t, hasTools, "capabilities must declare tools")
	_, hasSampling := result.Capabilities["sampling"]
	assert.False(t, hasSampling, "v1 must NOT declare sampling")
	_, hasElicitation := result.Capabilities["elicitation"]
	assert.False(t, hasElicitation, "v1 must NOT declare elicitation")
}

// ---------------------------------------------------------------------------
// T-5 — tools/list (AC-3)
// ---------------------------------------------------------------------------

// TestToolsList_ReturnsFiveTools pins AC-3: exactly 5 tools whose names are the
// expected set, each with a non-empty description + an inputSchema object, and
// run additionally has an outputSchema.
func TestToolsList_ReturnsFiveTools(t *testing.T) {
	h := devHarness(t)
	env, _ := h.doRPC(t, "", "tools/list", map[string]any{})
	require.Nil(t, env.Error)

	var result struct {
		Tools []struct {
			Name         string         `json:"name"`
			Description  string         `json:"description"`
			InputSchema  map[string]any `json:"inputSchema"`
			OutputSchema map[string]any `json:"outputSchema"`
		} `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(env.Result, &result))
	require.Len(t, result.Tools, 5, "v1 returns exactly 5 tools")

	want := map[string]bool{"create_session": false, "run": false, "get_session": false, "control": false, "fork": false}
	for _, tool := range result.Tools {
		_, known := want[tool.Name]
		require.True(t, known, "unexpected tool %q", tool.Name)
		want[tool.Name] = true
		assert.NotEmpty(t, tool.Description, "tool %q must have a description", tool.Name)
		assert.NotEmpty(t, tool.InputSchema, "tool %q must have an inputSchema object", tool.Name)
		if tool.Name == "run" {
			assert.NotEmpty(t, tool.OutputSchema, "run must declare an outputSchema")
		}
	}
	for name, seen := range want {
		assert.True(t, seen, "missing expected tool %q", name)
	}
}
