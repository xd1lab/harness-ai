// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
)

// mcpSSEStream adapts an http.ResponseWriter to the gRPC server-streaming
// interface the shared Run handler writes to
// ([genproto.OrchestratorService_RunServer]), re-wrapping each incremental
// RunEvent as an MCP notifications/progress notification on a text/event-stream
// leg. It is the MCP analog of rest.sseStream: same lazy-preamble + flush
// pattern, but the payload is the MCP progress envelope rather than a raw
// protojson RunEvent.
//
// Each SSE event is:
//
//	id: <durable seq>            ← the underlying RunEvent.seq (durable cursor)
//	event: <payload case>        ← progress | result
//	data: <JSON-RPC notification or response>
//
// Non-terminal frames (text/thinking/tool deltas, the approval request) become
// notifications/progress; the terminal RunEvent_Result becomes the final
// JSON-RPC response carrying the synthesized CallToolResult, after which the run
// call ends. The approval request rides in-band on this same leg (the call stays
// OPEN, per the run/approval decision amendment) so a concurrent control call can
// resolve the gate while the loop keeps blocking on the live request context.
//
// The preamble (status 200 + text/event-stream headers) is written lazily on the
// FIRST Send, so an error raised before any frame (ownership, unknown session,
// in-flight cap) can still surface as a plain JSON-RPC error with a real HTTP
// status (the before-first-frame vs after-first-frame split, mirroring rest).
type mcpSSEStream struct {
	ctx context.Context
	w   http.ResponseWriter
	rc  *http.ResponseController
	// reqID is the JSON-RPC id of the tools/call run request, echoed on the
	// terminal response.
	reqID json.RawMessage
	// progressToken is the client's _meta.progressToken (raw JSON: string or
	// number), echoed verbatim on every progress notification. Nil means the
	// client did not subscribe to progress, so non-terminal frames are dropped.
	progressToken json.RawMessage
	// sessionID is the target session, set by the run handler so the synthesized
	// terminal result carries it.
	sessionID string

	mu       sync.Mutex
	began    bool
	progress float64 // strictly-increasing counter for notifications/progress
	sendErr  error
	// finished is set once the terminal result has been written, so a stray
	// post-terminal frame is ignored.
	finished bool
}

// newMCPSSEStream wraps w for the request-scoped ctx, echoing progressToken on
// progress notifications. reqID is set by the handler before the shared Run is
// invoked (newMCPSSEStream's signature keeps it optional so the internal shim
// tests can construct one with just a token).
func newMCPSSEStream(ctx context.Context, w http.ResponseWriter, progressToken json.RawMessage) *mcpSSEStream {
	return &mcpSSEStream{
		ctx:           ctx,
		w:             w,
		rc:            http.NewResponseController(w),
		progressToken: progressToken,
	}
}

// withRun records the JSON-RPC request id and target session id so the terminal
// response echoes the id and the synthesized result carries the session. It
// returns the stream for fluent construction.
func (s *mcpSSEStream) withRun(id json.RawMessage, sessionID string) *mcpSSEStream {
	s.reqID = id
	s.sessionID = sessionID
	return s
}

// started reports whether the SSE preamble has been committed (after which the
// HTTP status can no longer change).
func (s *mcpSSEStream) started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.began
}

// Send writes one RunEvent frame. A terminal result becomes the final JSON-RPC
// response (and ends the leg); every other frame becomes a notifications/progress
// (dropped when the client did not supply a progressToken). It satisfies the
// generated stream's Send and is safe for the relay's serialized use.
func (s *mcpSSEStream) Send(frame *genproto.RunEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	if s.finished {
		return nil
	}

	if res := frame.GetResult(); res != nil {
		return s.writeFinalResultLocked(frame.GetSeq(), res)
	}
	if len(s.progressToken) == 0 {
		// No progress subscription: incremental frames are not delivered. The
		// terminal result still rides the leg (or, for a token-less call, the
		// handler buffers it as a single application/json response).
		return nil
	}
	return s.writeProgressLocked(frame)
}

// writeProgressLocked emits a notifications/progress for a non-terminal frame.
// Caller holds s.mu.
func (s *mcpSSEStream) writeProgressLocked(frame *genproto.RunEvent) error {
	if err := s.beginLocked(); err != nil {
		return err
	}
	s.progress++ // strictly increasing per spec
	params := progressParamsFor(s.progressToken, s.progress, frame)
	note := rpcNotification{JSONRPC: jsonRPCVersion, Method: "notifications/progress", Params: params}
	return s.writeEventLocked(frame.GetSeq(), progressEventName(frame), note)
}

// writeFinalResultLocked emits the terminal JSON-RPC response carrying the
// synthesized completed CallToolResult and marks the leg finished. Caller holds
// s.mu.
func (s *mcpSSEStream) writeFinalResultLocked(seq int64, res *genproto.RunResult) error {
	if err := s.beginLocked(); err != nil {
		return err
	}
	result := completedCallResult(s.sessionID, seq, res)
	resp := resultResponse(s.reqID, result)
	if err := s.writeEventLocked(seq, "result", resp); err != nil {
		return err
	}
	s.finished = true
	return nil
}

// beginLocked writes the SSE preamble on the first frame. Caller holds s.mu.
func (s *mcpSSEStream) beginLocked() error {
	if s.began {
		return nil
	}
	h := s.w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
	s.w.WriteHeader(http.StatusOK)
	s.began = true
	return nil
}

// writeEventLocked marshals payload and writes one SSE event (id/event/data),
// then flushes. Caller holds s.mu.
func (s *mcpSSEStream) writeEventLocked(seq int64, event string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		s.sendErr = fmt.Errorf("mcpserver: encode SSE payload: %w", err)
		return s.sendErr
	}
	if _, err := fmt.Fprintf(s.w, "id: %d\nevent: %s\ndata: %s\n\n", seq, event, data); err != nil {
		s.sendErr = err
		return err
	}
	if err := s.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.sendErr = err
		return err
	}
	return nil
}

// sendError emits a terminal SSE error event for a failure that occurred AFTER
// the stream already began (the HTTP status is committed; this is the SSE analog
// of a broken gRPC stream carrying a status), mirroring rest.sseStream.sendError.
func (s *mcpSSEStream) sendError(err error) {
	st := status.Convert(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.began || s.sendErr != nil || s.finished {
		return
	}
	_, _ = fmt.Fprintf(s.w, "event: error\ndata: {\"code\":%q,\"error\":%q}\n\n", st.Code().String(), st.Message())
	_ = s.rc.Flush()
}

// ---- grpc.ServerStream surface --------------------------------------------------
//
// The shared Run handler only calls Send and Context; the rest of the
// grpc.ServerStream interface is satisfied with inert implementations (there is
// no gRPC transport underneath), exactly like rest.sseStream.

// Context returns the HTTP request context (cancellation = client disconnect).
func (s *mcpSSEStream) Context() context.Context { return s.ctx }

// SetHeader is a no-op (SSE has no gRPC header frame).
func (s *mcpSSEStream) SetHeader(metadata.MD) error { return nil }

// SendHeader is a no-op (SSE has no gRPC header frame).
func (s *mcpSSEStream) SendHeader(metadata.MD) error { return nil }

// SetTrailer is a no-op (SSE has no gRPC trailer frame).
func (s *mcpSSEStream) SetTrailer(metadata.MD) {}

// SendMsg delegates to Send for *RunEvent and rejects anything else.
func (s *mcpSSEStream) SendMsg(m any) error {
	frame, ok := m.(*genproto.RunEvent)
	if !ok {
		return fmt.Errorf("mcpserver: SendMsg expects *RunEvent, got %T", m)
	}
	return s.Send(frame)
}

// RecvMsg is unsupported: Run is a server-stream (no client messages).
func (s *mcpSSEStream) RecvMsg(any) error {
	return errors.New("mcpserver: RecvMsg is not supported on a server-stream facade")
}

// Compile-time assertion: the shim satisfies the generated stream interface
// ([FIX-2]).
var _ genproto.OrchestratorService_RunServer = (*mcpSSEStream)(nil)
