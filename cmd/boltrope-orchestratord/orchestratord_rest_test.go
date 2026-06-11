// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRun_MountsRESTFacade is the daemon-level smoke for the REST facade: the
// orchestrator's HTTP listener must serve the /v1 routes (next to /livez)
// with the dev principal injected. The fake Postgres DSN cannot serve a real
// session, so the route is proven by a typed JSON error (404 with the
// facade's error envelope) rather than by a 200 — which is exactly what
// distinguishes "route mounted and authenticated" from "route absent" (a bare
// ServeMux 404 has no JSON body).
func TestRun_MountsRESTFacade(t *testing.T) {
	t.Setenv("BOLTROPE_DEV_INSECURE", "1")

	// Reserve a concrete HTTP port so the test can dial it (the daemon offers no
	// bound-address introspection). Close-then-reuse is mildly racy but local.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	httpAddr := lis.Addr().String()
	_ = lis.Close()

	cfg := baseConfig(t)
	cfg.Server.HTTPAddr = httpAddr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, &strings.Builder{}) }()

	// Wait for the HTTP listener to come up (bounded), capturing the response
	// inside the probe so every body is closed where it is read.
	url := fmt.Sprintf("http://%s/v1/sessions/does-not-exist", httpAddr)
	var (
		statusCode  int
		contentType string
		body        map[string]string
	)
	require.Eventually(t, func() bool {
		r, gerr := http.Get(url) //nolint:gosec // G107: local loopback test URL
		if gerr != nil {
			return false
		}
		defer func() { _ = r.Body.Close() }()
		statusCode = r.StatusCode
		contentType = r.Header.Get("Content-Type")
		return json.NewDecoder(r.Body).Decode(&body) == nil
	}, 5*time.Second, 100*time.Millisecond, "REST facade did not come up on %s", httpAddr)

	assert.Equal(t, http.StatusNotFound, statusCode, "an unknown session must map to 404 through the facade")
	assert.Contains(t, contentType, "application/json")
	assert.Equal(t, "NotFound", body["code"], "the facade's typed error envelope proves the route + auth middleware ran")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
