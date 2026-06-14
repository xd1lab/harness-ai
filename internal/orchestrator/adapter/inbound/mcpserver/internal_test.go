// SPDX-License-Identifier: Apache-2.0

package mcpserver

// White-box tests for the package-internal seams that cannot be exercised from
// the external test package:
//
//   - T-2  (R-2b): bearer extraction parity with rest.httpBearer.
//   - T-4  (R-2):  gRPC-code → HTTP-status parity with rest.httpStatus, pinning
//                  FailedPrecondition→400 and Aborted/AlreadyExists→409 (the v2
//                  fix and AC-10b's HTTP half).
//   - T-13:        the mcpSSEStream shim wraps a text frame as a
//                  notifications/progress with a strictly-increasing progress and
//                  the echoed progressToken (string AND number); the preamble is
//                  lazy (no bytes/headers before the first Send); and the
//                  compile-time assertion that the shim satisfies the full
//                  OrchestratorService_RunServer interface ([FIX-2]).
//
// These are RED until the production symbols (httpBearer, httpStatus,
// mcpSSEStream, newMCPSSEStream / its constructor, the progress envelope) exist.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
)

// ---------------------------------------------------------------------------
// T-2 (R-2b) — bearer extraction parity
// ---------------------------------------------------------------------------

// TestBearerExtraction pins that the replicated bearer-extraction helper matches
// rest.httpBearer's documented behavior (the rest symbol is unexported and
// cannot be imported, so the switch is replicated here and pinned by this test).
func TestBearerExtraction(t *testing.T) {
	cases := []struct {
		header string
		want   string
	}{
		{"Bearer x", "x"},
		{"bearer  x ", "x"},
		{"Basic y", ""},
		{"", ""},
		{"Bearer", ""},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		if tc.header != "" {
			r.Header.Set("Authorization", tc.header)
		}
		got := httpBearer(r)
		assert.Equalf(t, tc.want, got, "httpBearer(%q)", tc.header)
	}
}

// ---------------------------------------------------------------------------
// T-4 (R-2) — httpStatus parity (drift guard)
// ---------------------------------------------------------------------------

// TestHTTPStatusParity pins the replicated gRPC-code → HTTP-status mapping
// against the documented rest.httpStatus values. It EXPLICITLY pins
// FailedPrecondition→400 and Aborted/AlreadyExists→409 (the v2 fix; AC-10b).
func TestHTTPStatusParity(t *testing.T) {
	cases := map[codes.Code]int{
		codes.OK:                 http.StatusOK,
		codes.Canceled:           499,
		codes.InvalidArgument:    http.StatusBadRequest,
		codes.FailedPrecondition: http.StatusBadRequest, // v2 fix: NOT 409
		codes.OutOfRange:         http.StatusBadRequest,
		codes.DeadlineExceeded:   http.StatusGatewayTimeout,
		codes.NotFound:           http.StatusNotFound,
		codes.AlreadyExists:      http.StatusConflict,
		codes.Aborted:            http.StatusConflict,
		codes.PermissionDenied:   http.StatusForbidden,
		codes.ResourceExhausted:  http.StatusTooManyRequests,
		codes.Unimplemented:      http.StatusNotImplemented,
		codes.Unavailable:        http.StatusServiceUnavailable,
		codes.Unauthenticated:    http.StatusUnauthorized,
		codes.Internal:           http.StatusInternalServerError,
	}
	for code, want := range cases {
		assert.Equalf(t, want, httpStatus(code), "httpStatus(%s)", code)
	}
}

// ---------------------------------------------------------------------------
// T-13 — the mcpSSEStream shim (progress envelopes + lazy preamble)
// ---------------------------------------------------------------------------

// textFrame builds a text_delta RunEvent at the given seq.
func textFrame(seq int64, text string) *genproto.RunEvent {
	return &genproto.RunEvent{
		Seq: seq,
		Payload: &genproto.RunEvent_TextDelta{
			TextDelta: &genproto.TextDelta{Text: text},
		},
	}
}

// TestSSEShim_LazyPreamble pins that no bytes/headers are written until the
// first Send (so a pre-stream error can still set a real HTTP status).
func TestSSEShim_LazyPreamble(t *testing.T) {
	rec := httptest.NewRecorder()
	s := newMCPSSEStream(context.Background(), rec, json.RawMessage(`"tok"`))
	require.False(t, s.started(), "no preamble before the first Send")
	assert.Empty(t, rec.Header().Get("Content-Type"), "no Content-Type before the first Send")
	assert.Equal(t, 0, rec.Body.Len(), "no body before the first Send")

	require.NoError(t, s.Send(textFrame(2, "hello")))
	assert.True(t, s.started(), "the first Send commits the SSE preamble")
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
}

// TestSSEShim_WrapsFramesAsProgress pins that a text_delta with a progressToken
// is wrapped as a notifications/progress with a strictly-increasing progress and
// the echoed token (string-id case AND number-id case).
func TestSSEShim_WrapsFramesAsProgress(t *testing.T) {
	for _, tokRaw := range []string{`"string-token"`, `9`} {
		rec := httptest.NewRecorder()
		s := newMCPSSEStream(context.Background(), rec, json.RawMessage(tokRaw))
		require.NoError(t, s.Send(textFrame(2, "alpha")))
		require.NoError(t, s.Send(textFrame(3, "beta")))

		progresses := parseProgressFrames(t, rec.Body.String())
		require.GreaterOrEqual(t, len(progresses), 2, "two text frames → ≥2 progress notifications for %s", tokRaw)
		for _, p := range progresses {
			assert.JSONEq(t, tokRaw, string(p.ProgressToken), "progressToken must round-trip for %s", tokRaw)
		}
		for i := 1; i < len(progresses); i++ {
			assert.Greater(t, progresses[i].Progress, progresses[i-1].Progress, "progress must strictly increase")
		}
	}
}

// progressNote is the decoded params of a notifications/progress frame.
type progressNote struct {
	ProgressToken json.RawMessage `json:"progressToken"`
	Progress      float64         `json:"progress"`
	Message       string          `json:"message"`
}

// parseProgressFrames extracts every notifications/progress params block from an
// SSE body.
func parseProgressFrames(t *testing.T, body string) []progressNote {
	t.Helper()
	var out []progressNote
	for _, block := range strings.Split(body, "\n\n") {
		for _, line := range strings.Split(block, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(line[len("data:"):])
			var msg struct {
				Method string       `json:"method"`
				Params progressNote `json:"params"`
			}
			if err := json.Unmarshal([]byte(data), &msg); err != nil {
				continue
			}
			if msg.Method == "notifications/progress" {
				out = append(out, msg.Params)
			}
		}
	}
	return out
}

// TestSSEShim_SatisfiesRunServer pins [FIX-2]: the shim implements the FULL
// OrchestratorService_RunServer interface (the compile-time assertion in the
// production file guards this; this test exercises the inert methods so they are
// covered and a regression in their signatures is caught).
func TestSSEShim_SatisfiesRunServer(t *testing.T) {
	var _ genproto.OrchestratorService_RunServer = (*mcpSSEStream)(nil)
	rec := httptest.NewRecorder()
	s := newMCPSSEStream(context.Background(), rec, nil)
	assert.NotNil(t, s.Context())
	assert.NoError(t, s.SetHeader(nil))
	assert.NoError(t, s.SendHeader(nil))
	s.SetTrailer(nil)
	assert.Error(t, s.RecvMsg(nil), "RecvMsg is unsupported on a server-stream facade")
	// SendMsg with a non-RunEvent is an error.
	assert.Error(t, s.SendMsg("not a frame"))
}
