package grpcx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/boltrope/boltrope/internal/platform/grpcx"
)

// fakeServerStream is a minimal grpc.ServerStream: the stream interceptors only
// ever read Context() from it, so the remaining methods are inert. Driving the
// interceptors directly (instead of through a live server) keeps these tests
// deterministic and lets them assert on the exact log records and panic values.
type fakeServerStream struct{ ctx context.Context }

func (*fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (*fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (*fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) Context() context.Context   { return f.ctx }
func (*fakeServerStream) SendMsg(any) error            { return nil }
func (*fakeServerStream) RecvMsg(any) error            { return nil }

// streamInfo is the RPC descriptor handed to every stream interceptor under test.
func streamInfo() *grpc.StreamServerInfo {
	return &grpc.StreamServerInfo{FullMethod: streamMethod, IsServerStream: true, IsClientStream: true}
}

// TestStreamLoggingInterceptor_LogsOutcome asserts one structured access-log
// record per handled stream: INFO with the resolved OK status on success, ERROR
// carrying the error text on failure, and in both cases the method and a
// duration attribute (the FR-OBS-01 access log, streaming side).
func TestStreamLoggingInterceptor_LogsOutcome(t *testing.T) {
	tests := []struct {
		name       string
		handlerErr error
		wantLevel  string
		wantMsg    string
		wantStatus string
	}{
		{
			name:       "success logs INFO with OK",
			handlerErr: nil,
			wantLevel:  "INFO",
			wantMsg:    "grpc request handled",
			wantStatus: codes.OK.String(),
		},
		{
			name:       "failure logs ERROR with the status code and error",
			handlerErr: status.Error(codes.NotFound, "missing thing"),
			wantLevel:  "ERROR",
			wantMsg:    "grpc request failed",
			wantStatus: codes.NotFound.String(),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))

			err := grpcx.StreamLoggingInterceptor(logger)(
				nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
				func(any, grpc.ServerStream) error { return tc.handlerErr },
			)
			// The interceptor must pass the handler's outcome through untouched.
			assert.Equal(t, tc.handlerErr, err)

			var rec map[string]any
			require.NoError(t, json.Unmarshal([]byte(strings.SplitN(buf.String(), "\n", 2)[0]), &rec))
			assert.Equal(t, tc.wantLevel, rec["level"])
			assert.Equal(t, tc.wantMsg, rec["msg"])
			assert.Equal(t, streamMethod, rec["rpc.method"])
			assert.Equal(t, tc.wantStatus, rec["rpc.status"])
			assert.Contains(t, rec, "rpc.duration")
			if tc.handlerErr != nil {
				assert.Contains(t, rec["error"], "missing thing")
			}
		})
	}
}

// TestStreamLoggingInterceptor_NilLoggerIsSafe asserts a nil logger degrades to
// "no access log" rather than a panic, and the handler outcome still flows.
func TestStreamLoggingInterceptor_NilLoggerIsSafe(t *testing.T) {
	wantErr := status.Error(codes.Aborted, "boom")
	err := grpcx.StreamLoggingInterceptor(nil)(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { return wantErr },
	)
	assert.Equal(t, wantErr, err)
}

// TestStreamRecoveryInterceptor_ConvertsPanic asserts a panicking stream
// handler is converted into an opaque codes.Internal error so the process keeps
// serving and the panic detail is logged, never leaked to the caller.
func TestStreamRecoveryInterceptor_ConvertsPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	err := grpcx.StreamRecoveryInterceptor(logger)(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { panic("stream-boom") },
	)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	assert.NotContains(t, err.Error(), "stream-boom", "the panic detail must not leak to the caller")

	// The incident must be captured: panic value, method, and a stack trace.
	out := buf.String()
	assert.Contains(t, out, "stream-boom")
	assert.Contains(t, out, streamMethod)
	assert.Contains(t, out, "stack")
}

// TestStreamRecoveryInterceptor_PassesThroughNonPanicError asserts an ordinary
// handler error is NOT rewritten to Internal and nothing is logged: recovery
// only intervenes for panics.
func TestStreamRecoveryInterceptor_PassesThroughNonPanicError(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	wantErr := status.Error(codes.NotFound, "missing")
	err := grpcx.StreamRecoveryInterceptor(logger)(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { return wantErr },
	)
	assert.Equal(t, wantErr, err)
	assert.Empty(t, buf.String(), "no panic, no recovery log")
}

// TestStreamRecoveryInterceptor_NilLoggerStillRecovers asserts the recovery
// guarantee holds even with no logger wired: the panic is still swallowed into
// codes.Internal (losing the log is acceptable; crashing the server is not).
func TestStreamRecoveryInterceptor_NilLoggerStillRecovers(t *testing.T) {
	err := grpcx.StreamRecoveryInterceptor(nil)(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error { panic("unlogged-boom") },
	)
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
}

// TestChainStreamInterceptors_OrderAndPassthrough asserts the chain runs
// interceptors in declaration order (first outermost), reaches the handler
// last, and threads the srv and stream values through unchanged.
func TestChainStreamInterceptors_OrderAndPassthrough(t *testing.T) {
	var order []string
	probe := func(label string) grpc.StreamServerInterceptor {
		return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			order = append(order, label)
			return handler(srv, ss)
		}
	}

	type srvSentinel struct{ name string }
	sentinel := &srvSentinel{name: "svc"}
	stream := &fakeServerStream{ctx: context.Background()}

	var gotSrv any
	var gotStream grpc.ServerStream
	chain := grpcx.ChainStreamInterceptors(probe("a"), probe("b"), probe("c"))
	err := chain(sentinel, stream, streamInfo(), func(srv any, ss grpc.ServerStream) error {
		order = append(order, "handler")
		gotSrv, gotStream = srv, ss
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "handler"}, order)
	assert.Same(t, sentinel, gotSrv, "srv must be threaded through the chain unchanged")
	assert.Same(t, stream, gotStream, "the stream must be threaded through the chain unchanged")
}

// TestChainStreamInterceptors_EmptyChainCallsHandler asserts the zero-element
// chain degenerates to a direct handler call.
func TestChainStreamInterceptors_EmptyChainCallsHandler(t *testing.T) {
	called := false
	err := grpcx.ChainStreamInterceptors()(
		nil, &fakeServerStream{ctx: context.Background()}, streamInfo(),
		func(any, grpc.ServerStream) error {
			called = true
			return nil
		},
	)
	require.NoError(t, err)
	assert.True(t, called)
}

// TestChainStreamInterceptors_ShortCircuit asserts an interceptor that rejects
// without invoking its next handler stops the chain: later interceptors and
// the handler never run, and its error is what the caller sees (this is how
// the RBAC interceptor blocks denied streams).
func TestChainStreamInterceptors_ShortCircuit(t *testing.T) {
	errDenied := errors.New("denied")
	var afterRan, handlerRan bool

	chain := grpcx.ChainStreamInterceptors(
		func(any, grpc.ServerStream, *grpc.StreamServerInfo, grpc.StreamHandler) error {
			return errDenied // reject without calling the next handler
		},
		func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			afterRan = true
			return handler(srv, ss)
		},
	)
	err := chain(nil, &fakeServerStream{ctx: context.Background()}, streamInfo(), func(any, grpc.ServerStream) error {
		handlerRan = true
		return nil
	})
	require.ErrorIs(t, err, errDenied)
	assert.False(t, afterRan, "interceptors after the rejection must not run")
	assert.False(t, handlerRan, "the handler must not run after a rejection")
}
