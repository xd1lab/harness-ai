// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"google.golang.org/grpc/metadata"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
)

// toolRun maps a tools/call of "run" onto the shared streaming igrpc.Server.Run,
// adapting the gRPC server-stream to an MCP leg. The model is call-stays-open
// (per the DECISIONS.md amendment / ADR-0022): when the client sends a
// _meta.progressToken the response is a text/event-stream leg carrying
// notifications/progress (including the in-band approval) then the final JSON-RPC
// response; an approval pauses the loop on the LIVE request context while a
// concurrent control call resolves the gate. When no progressToken is sent the
// response is a single application/json CallToolResult (no SSE, no in-band
// approval — §6 item 4).
//
// Failures raised BEFORE the first frame (missing session_id, ownership, unknown
// session, in-flight cap) surface as a JSON-RPC error with a real HTTP status;
// failures after the SSE preamble committed surface as a terminal SSE error
// frame (the HTTP status is already 200) — the same before/after split as REST.
func (h *Handler) toolRun(w http.ResponseWriter, r *http.Request, req *rpcRequest, p toolCallParams) {
	var args runArgs
	if !decodeArgs(w, req, p.Arguments, &args) {
		return
	}
	if args.SessionID == "" {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "session_id is required", nil))
		return
	}

	runReq := &genproto.RunRequest{SessionId: args.SessionID, AfterSeq: args.AfterSeq, Strict: args.Strict}
	if args.Text != "" {
		runReq.Message = userTextMessage(args.Text)
	}
	// A non-object output_schema is a JSON-RPC InvalidParams (-32602) BEFORE any run
	// starts (fail-closed-early); the loop only accepts a JSON Schema object.
	if schema, ok := objectSchemaBytes(args.OutputSchema); ok {
		runReq.OutputSchema = schema
	} else if len(args.OutputSchema) > 0 {
		writeJSON(w, http.StatusOK, errorResponse(req.ID, codeInvalidParams, "output_schema must be a JSON object", nil))
		return
	}

	if len(p.Meta.ProgressToken) > 0 {
		h.runStreaming(w, r, req, args.SessionID, runReq, p.Meta.ProgressToken)
		return
	}
	h.runBuffered(w, r, req, args.SessionID, runReq)
}

// runStreaming drives the run on a text/event-stream leg, pushing progress and
// the terminal result as SSE events (AC-8/AC-9/AC-10/AC-12). A pre-first-frame
// error is a JSON-RPC error with the mapped HTTP status; a mid-flight break is a
// terminal SSE error frame.
func (h *Handler) runStreaming(w http.ResponseWriter, r *http.Request, req *rpcRequest, sessionID string, runReq *genproto.RunRequest, token []byte) {
	stream := newMCPSSEStream(r.Context(), w, token).withRun(req.ID, sessionID)
	err := h.grpc.Run(runReq, stream)
	switch {
	case err == nil:
	case !stream.started():
		// No SSE committed yet → answer with a JSON-RPC error + real HTTP status.
		h.writeToolStatusError(w, req, err)
	case !errors.Is(err, r.Context().Err()) || r.Context().Err() == nil:
		// The stream broke mid-flight for a non-disconnect reason: emit a terminal
		// SSE error so the client sees a typed end (the HTTP status is already 200).
		stream.sendError(err)
	}
}

// runBuffered drives the run with no progress subscription and returns a single
// application/json CallToolResult (AC-12 token-absent path). It captures only the
// terminal result via a buffering stream; a pre-result error is a JSON-RPC error.
// (In this model a token-less run cannot receive an in-band approval — documented
// in §6 item 4 — so it is intended for runs that complete without an ask gate.)
func (h *Handler) runBuffered(w http.ResponseWriter, r *http.Request, req *rpcRequest, sessionID string, runReq *genproto.RunRequest) {
	buf := newBufferStream(r.Context(), sessionID)
	err := h.grpc.Run(runReq, buf)
	if err != nil {
		h.writeToolStatusError(w, req, err)
		return
	}
	result, ok := buf.result()
	if !ok {
		h.writeToolStatusError(w, req, fmt.Errorf("run produced no terminal result"))
		return
	}
	writeJSON(w, http.StatusOK, resultResponse(req.ID, result))
}

// bufferStream is a non-streaming OrchestratorService_RunServer shim: it discards
// incremental frames and captures the terminal RunResult so runBuffered can emit
// it as a single JSON response. It satisfies the generated stream interface with
// the same inert methods as mcpSSEStream.
type bufferStream struct {
	ctx       context.Context
	sessionID string

	mu        sync.Mutex
	completed *callToolResult
}

// newBufferStream wraps the request ctx for a token-less run.
func newBufferStream(ctx context.Context, sessionID string) *bufferStream {
	return &bufferStream{ctx: ctx, sessionID: sessionID}
}

// result returns the captured terminal CallToolResult, if any.
func (b *bufferStream) result() (callToolResult, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.completed == nil {
		return callToolResult{}, false
	}
	return *b.completed, true
}

// Send captures the terminal result frame and discards every incremental frame.
func (b *bufferStream) Send(frame *genproto.RunEvent) error {
	if res := frame.GetResult(); res != nil {
		b.mu.Lock()
		cr := completedCallResult(b.sessionID, frame.GetSeq(), res)
		b.completed = &cr
		b.mu.Unlock()
	}
	return nil
}

// Context returns the request context.
func (b *bufferStream) Context() context.Context { return b.ctx }

// SetHeader is a no-op.
func (b *bufferStream) SetHeader(metadata.MD) error { return nil }

// SendHeader is a no-op.
func (b *bufferStream) SendHeader(metadata.MD) error { return nil }

// SetTrailer is a no-op.
func (b *bufferStream) SetTrailer(metadata.MD) {}

// SendMsg delegates to Send for *RunEvent.
func (b *bufferStream) SendMsg(m any) error {
	frame, ok := m.(*genproto.RunEvent)
	if !ok {
		return fmt.Errorf("mcpserver: SendMsg expects *RunEvent, got %T", m)
	}
	return b.Send(frame)
}

// RecvMsg is unsupported (server-stream).
func (b *bufferStream) RecvMsg(any) error {
	return errors.New("mcpserver: RecvMsg is not supported on a server-stream facade")
}

// Compile-time assertion: the buffering shim satisfies the generated stream.
var _ genproto.OrchestratorService_RunServer = (*bufferStream)(nil)

// userTextMessage wraps a plain user string as the proto Message the Run RPC
// expects (one text part, role user) — mirrors rest.userTextMessage.
func userTextMessage(text string) *genproto.Message {
	return &genproto.Message{
		Role: genproto.Role_ROLE_USER,
		Content: []*genproto.ContentPart{
			{Part: &genproto.ContentPart_Text{Text: &genproto.TextPart{Text: text}}},
		},
	}
}

// errInvalidMode is the edge-side strict-mode parse error (kept tiny so parseMode
// reads cleanly and the message is uniform).
func errInvalidMode(s string) error {
	return fmt.Errorf("unknown permission mode %q (want default|acceptEdits|plan)", s)
}
