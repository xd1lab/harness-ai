// SPDX-License-Identifier: Apache-2.0

package mcpserver_test

// T-20 — composed-routes no-collision guard (AC-20, R-9). Registers BOTH the
// 2-arg rest.NewHandler and the NET-NEW 4-arg mcpserver.NewHandler on one
// http.ServeMux (the way wiring.go composes them into a single HTTPRoutes
// closure) and asserts no panic + both /mcp and /v1/sessions resolve. This
// exercises the [FIX-1] shape difference (rest takes srv,auth; mcpserver adds
// version + allowedOrigins) side by side.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/mcpserver"
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/rest"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
)

// TestComposedRoutes_NoCollision pins that the REST and MCP route registrars
// compose onto one ServeMux without collision and both paths resolve.
func TestComposedRoutes_NoCollision(t *testing.T) {
	store := newFakeStore()
	srv := igrpc.NewServer(store, &fakeGate{}, &fakeRunner{}, ids.System{}, igrpc.Config{})
	auth, err := igrpc.NewAuthenticator(igrpc.AuthConfig{DevInsecure: true})
	require.NoError(t, err)

	mux := http.NewServeMux()
	require.NotPanics(t, func() {
		// The exact composition wiring.go performs: REST first, MCP second.
		rest.NewHandler(srv, auth).Routes(mux)                   // 2-arg
		mcpserver.NewHandler(srv, auth, "test", nil).Routes(mux) // NET-NEW 4-arg
	})

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	// /mcp resolves (POST initialize → 200, not 404).
	initResp, err := http.Post(ts.URL+"/mcp", "application/json",
		stringReader(buildRequest(t, 1, "initialize", map[string]any{})))
	require.NoError(t, err)
	_ = initResp.Body.Close()
	assert.NotEqual(t, http.StatusNotFound, initResp.StatusCode, "/mcp must resolve on the composed mux")

	// /v1/sessions resolves (POST → not 404; the REST route is registered).
	restResp, err := http.Post(ts.URL+"/v1/sessions", "application/json", stringReader(`{}`))
	require.NoError(t, err)
	_ = restResp.Body.Close()
	assert.NotEqual(t, http.StatusNotFound, restResp.StatusCode, "/v1/sessions must resolve on the composed mux")
}
