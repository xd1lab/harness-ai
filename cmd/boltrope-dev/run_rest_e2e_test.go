// SPDX-License-Identifier: Apache-2.0

package main

// T-7 (AC-8 bonus plane) — RED. The REST/SSE facade must reach the same terminal
// Success through the SAME *igrpc.Server, so the two data planes cannot drift. With
// NO Authorization header (dev-insecure synthetic principal), POST /v1/sessions →
// POST /v1/sessions/{id}/run {"text":"hello"} must produce a text/event-stream
// ending in event: result / TERMINATION_SUBTYPE_SUCCESS. It references
// newServer()/serveOpts/serveResult (the T-11 factory), which do not exist yet →
// RED.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDevServer_REST_E2E_StubReachesSuccess asserts AC-8 (bonus): the REST/SSE
// plane on the dev binary's loopback HTTP listener drives the real loop against the
// stub to a terminal Success with no auth header.
func TestDevServer_REST_E2E_StubReachesSuccess(t *testing.T) {
	srv, err := newServer(serveOpts{ // newServer / serveOpts do not exist yet (RED)
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, c := context.WithTimeout(context.Background(), 3*time.Second)
		defer c()
		_ = srv.Shutdown(ctx)
	})

	base := "http://" + srv.HTTPAddr
	httpClient := &http.Client{Timeout: 8 * time.Second}

	// Create a session (no Authorization header — dev-insecure injects the dev
	// principal). Mode omitted → server default.
	csResp, err := httpClient.Post(base+"/v1/sessions", "application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	defer func() { _ = csResp.Body.Close() }()
	require.Equal(t, http.StatusOK, csResp.StatusCode)

	// The shared rest.NewHandler emits canonical protojson, which uses the proto3
	// JSON lowerCamelCase field name ("sessionId") — the SAME shape the production
	// rest_test.go and mcpserver tests assert. Decode that name (a snake_case tag
	// would silently miss it and read empty); the assertion below is unchanged.
	var cs struct {
		SessionID string `json:"sessionId"`
	}
	require.NoError(t, json.NewDecoder(csResp.Body).Decode(&cs))
	require.NotEmpty(t, cs.SessionID, "REST CreateSession must return a session_id")

	// Run over SSE.
	runResp, err := httpClient.Post(base+"/v1/sessions/"+cs.SessionID+"/run", "application/json",
		strings.NewReader(`{"text":"hello"}`))
	require.NoError(t, err)
	defer func() { _ = runResp.Body.Close() }()
	require.Equal(t, http.StatusOK, runResp.StatusCode)
	assert.Contains(t, runResp.Header.Get("Content-Type"), "text/event-stream")

	bodyBytes, err := io.ReadAll(runResp.Body)
	require.NoError(t, err)
	body := string(bodyBytes)

	assert.Contains(t, body, "event: result", "the SSE stream must end with the terminal result frame")
	assert.Contains(t, body, "TERMINATION_SUBTYPE_SUCCESS", "the terminal subtype must be Success against the stub")
}
