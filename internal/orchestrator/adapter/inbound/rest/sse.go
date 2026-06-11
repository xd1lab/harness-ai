// SPDX-License-Identifier: Apache-2.0

package rest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
)

// sseStream adapts an http.ResponseWriter to the gRPC server-streaming
// interface the shared Run handler writes to
// ([genproto.OrchestratorService_RunServer]). Each frame becomes one SSE event:
//
//	id: <durable seq>            ← the client's Last-Event-ID resume cursor
//	event: <payload case>        ← text_delta | thinking_delta | tool_progress |
//	                               approval_request | result | error
//	data: <protojson RunEvent>
//
// The SSE preamble (status 200 + text/event-stream headers) is written lazily
// on the FIRST frame, so an error raised before any frame (auth, ownership,
// unknown session) can still surface as a plain JSON error with a real HTTP
// status. Writes flush immediately — SSE is only live if the bytes leave.
type sseStream struct {
	ctx context.Context
	w   http.ResponseWriter
	rc  *http.ResponseController

	mu      sync.Mutex
	began   bool
	sendErr error
}

// newSSEStream wraps w for the request-scoped ctx.
func newSSEStream(ctx context.Context, w http.ResponseWriter) *sseStream {
	return &sseStream{ctx: ctx, w: w, rc: http.NewResponseController(w)}
}

// started reports whether the SSE preamble has been committed (after which the
// HTTP status can no longer change).
func (s *sseStream) started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.began
}

// Send writes one frame as an SSE event. It satisfies the generated stream's
// Send and is safe for the relay's serialized use.
func (s *sseStream) Send(frame *genproto.RunEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	if !s.began {
		h := s.w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("Connection", "keep-alive")
		h.Set("X-Accel-Buffering", "no") // disable proxy buffering (nginx)
		s.w.WriteHeader(http.StatusOK)
		s.began = true
	}

	data, err := protojson.Marshal(frame)
	if err != nil {
		s.sendErr = fmt.Errorf("rest: encode frame: %w", err)
		return s.sendErr
	}
	if _, err := fmt.Fprintf(s.w, "id: %d\nevent: %s\ndata: %s\n\n", frame.GetSeq(), eventName(frame), data); err != nil {
		s.sendErr = err
		return err
	}
	if err := s.rc.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.sendErr = err
		return err
	}
	return nil
}

// sendError emits a terminal `event: error` frame for a failure after the
// stream already began (the HTTP status is committed; this is the SSE analog
// of a broken gRPC stream carrying a status).
func (s *sseStream) sendError(err error) {
	st := status.Convert(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.began || s.sendErr != nil {
		return
	}
	_, _ = fmt.Fprintf(s.w, "event: error\ndata: {\"code\":%q,\"error\":%q}\n\n", st.Code().String(), st.Message())
	_ = s.rc.Flush()
}

// eventName names the SSE event after the frame's payload case.
func eventName(f *genproto.RunEvent) string {
	switch f.GetPayload().(type) {
	case *genproto.RunEvent_TextDelta:
		return "text_delta"
	case *genproto.RunEvent_ThinkingDelta:
		return "thinking_delta"
	case *genproto.RunEvent_ToolProgress:
		return "tool_progress"
	case *genproto.RunEvent_ApprovalRequest:
		return "approval_request"
	case *genproto.RunEvent_Result:
		return "result"
	default:
		return "event"
	}
}

// ---- grpc.ServerStream surface --------------------------------------------------
//
// The shared Run handler only calls Send and Context; the rest of the
// grpc.ServerStream interface is satisfied with inert implementations (there
// is no gRPC transport underneath).

// Context returns the HTTP request context (cancellation = client disconnect).
func (s *sseStream) Context() context.Context { return s.ctx }

// SetHeader is a no-op (SSE has no gRPC header frame).
func (s *sseStream) SetHeader(metadata.MD) error { return nil }

// SendHeader is a no-op (SSE has no gRPC header frame).
func (s *sseStream) SendHeader(metadata.MD) error { return nil }

// SetTrailer is a no-op (SSE has no gRPC trailer frame).
func (s *sseStream) SetTrailer(metadata.MD) {}

// SendMsg delegates to Send for *RunEvent and rejects anything else.
func (s *sseStream) SendMsg(m any) error {
	frame, ok := m.(*genproto.RunEvent)
	if !ok {
		return fmt.Errorf("rest: SendMsg expects *RunEvent, got %T", m)
	}
	return s.Send(frame)
}

// RecvMsg is unsupported: Run is a server-stream (no client messages).
func (s *sseStream) RecvMsg(any) error {
	return errors.New("rest: RecvMsg is not supported on a server-stream facade")
}

// Compile-time assertion: the shim satisfies the generated stream interface.
var _ genproto.OrchestratorService_RunServer = (*sseStream)(nil)
