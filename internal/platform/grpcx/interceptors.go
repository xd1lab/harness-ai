package grpcx

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// ServerStatsHandler returns the OpenTelemetry gRPC stats handler for the server
// side. Installed via grpc.StatsHandler, it (a) creates a server span per RPC and
// (b) extracts the incoming W3C trace context from gRPC metadata so it becomes
// the active span context for the handler — this is the trace-propagation seam of
// FR-OBS-01 (trace context propagates via gRPC metadata). It uses the globally
// configured TracerProvider and propagator set by
// [github.com/xd1lab/harness-ai/internal/platform/obs.SetupTracing].
//
// Stats handlers are the current, non-deprecated OTel gRPC instrumentation path
// (the older otelgrpc.UnaryServerInterceptor is deprecated); using it keeps span
// context extraction ahead of the application interceptor chain.
func ServerStatsHandler(opts ...otelgrpc.Option) stats.Handler {
	return otelgrpc.NewServerHandler(opts...)
}

// ClientStatsHandler returns the OpenTelemetry gRPC stats handler for the client
// side. Installed via grpc.WithStatsHandler, it creates a client span per RPC and
// injects the active W3C trace context into outgoing gRPC metadata so the callee
// can continue the trace (FR-OBS-01 propagation).
func ClientStatsHandler(opts ...otelgrpc.Option) stats.Handler {
	return otelgrpc.NewClientHandler(opts...)
}

// UnaryLoggingInterceptor returns a unary server interceptor that logs each
// handled RPC as a structured slog record: the method, the resolved gRPC status
// code, and the handling latency. The record is emitted with the RPC context so
// the obs trace handler injects trace_id/span_id, correlating the log line to the
// active span (FR-OBS-01, NFR-OBS-02). Errors log at ERROR; successes at INFO.
func UnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logRPC(ctx, logger, info.FullMethod, err, time.Since(start))
		return resp, err
	}
}

// StreamLoggingInterceptor is the streaming counterpart of
// [UnaryLoggingInterceptor]: it logs the stream's method, terminal status code,
// and duration with trace correlation.
func StreamLoggingInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		logRPC(ss.Context(), logger, info.FullMethod, err, time.Since(start))
		return err
	}
}

// logRPC emits one structured access-log record for a handled RPC. It is shared
// by the unary and stream logging interceptors. The context is passed through so
// the obs handler can attach trace_id/span_id from the active span.
func logRPC(ctx context.Context, logger *slog.Logger, method string, err error, dur time.Duration) {
	if logger == nil {
		return
	}
	code := status.Code(err)
	attrs := []any{
		slog.String("rpc.method", method),
		slog.String("rpc.status", code.String()),
		slog.Duration("rpc.duration", dur),
	}
	if err != nil {
		logger.LogAttrs(ctx, slog.LevelError, "grpc request failed",
			append(toAttrs(attrs), slog.String("error", err.Error()))...)
		return
	}
	logger.LogAttrs(ctx, slog.LevelInfo, "grpc request handled", toAttrs(attrs)...)
}

// toAttrs narrows a []any of slog.Attr values to []slog.Attr for LogAttrs. Every
// element produced by logRPC is already a slog.Attr, so the assertion never fails
// in practice; a non-Attr element is dropped rather than panicking.
func toAttrs(in []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(in))
	for _, v := range in {
		if a, ok := v.(slog.Attr); ok {
			out = append(out, a)
		}
	}
	return out
}

// UnaryRecoveryInterceptor returns a unary server interceptor that recovers from
// a panic in a downstream handler or interceptor, converting it into a
// codes.Internal status error instead of crashing the server process. The panic
// value and stack are logged at ERROR (with trace correlation) so the incident is
// captured. The recovered error never leaks the panic detail to the caller.
func UnaryRecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverPanic(ctx, logger, info.FullMethod, r)
			}
		}()
		return handler(ctx, req)
	}
}

// StreamRecoveryInterceptor is the streaming counterpart of
// [UnaryRecoveryInterceptor].
func StreamRecoveryInterceptor(logger *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = recoverPanic(ss.Context(), logger, info.FullMethod, r)
			}
		}()
		return handler(srv, ss)
	}
}

// recoverPanic logs a recovered panic with its stack and returns the opaque
// Internal status surfaced to the caller. It is shared by both recovery
// interceptors.
func recoverPanic(ctx context.Context, logger *slog.Logger, method string, r any) error {
	if logger != nil {
		logger.LogAttrs(ctx, slog.LevelError, "grpc handler panic recovered",
			slog.String("rpc.method", method),
			slog.Any("panic", r),
			slog.String("stack", string(debug.Stack())),
		)
	}
	return status.Error(codes.Internal, "internal server error")
}

// ChainUnaryInterceptors composes unary server interceptors into one, invoked in
// the given order (the first wraps the outermost). It is a thin alias over
// grpc.ChainUnaryInterceptor's semantics, exposed so callers can build a chain
// value to pass to grpc.UnaryInterceptor or compose further in tests.
func ChainUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			chained = wrapUnary(interceptors[i], info, chained)
		}
		return chained(ctx, req)
	}
}

// wrapUnary binds one interceptor and the RPC info to the next handler in a chain.
func wrapUnary(interceptor grpc.UnaryServerInterceptor, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (any, error) {
		return interceptor(ctx, req, info, next)
	}
}

// ChainStreamInterceptors composes stream server interceptors into one, invoked
// in the given order (the first wraps the outermost).
func ChainStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			chained = wrapStream(interceptors[i], info, chained)
		}
		return chained(srv, ss)
	}
}

// wrapStream binds one interceptor and the stream info to the next handler.
func wrapStream(interceptor grpc.StreamServerInterceptor, info *grpc.StreamServerInfo, next grpc.StreamHandler) grpc.StreamHandler {
	return func(srv any, ss grpc.ServerStream) error {
		return interceptor(srv, ss, info, next)
	}
}
