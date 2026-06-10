package grpcx_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/boltrope/boltrope/internal/platform/grpcx"
	"github.com/boltrope/boltrope/internal/platform/grpcx/grpcxtest"
	"github.com/boltrope/boltrope/internal/platform/obs"
)

const (
	// testMethod is the fully-qualified RPC method the harness server registers.
	testMethod = "/boltrope.test.v1.Echo/Unary"
	// streamMethod is the streaming RPC the harness server registers.
	streamMethod = "/boltrope.test.v1.Echo/Stream"
)

// recordingHandler is a unary handler that records the order in which the
// interceptor chain reached it and echoes back the active trace_id so the test
// can assert cross-call propagation.
func echoTraceHandler(ctx context.Context, _ any) (any, error) {
	sc := trace.SpanContextFromContext(ctx)
	return sc.TraceID().String(), nil
}

// buildMTLSServerAndClient stands up a bufconn gRPC server with creds built from
// serverID and returns a dialed client connection authenticated as callerID.
// Both identities are minted under the same in-process trust domain so the mTLS
// handshake succeeds; the caller controls which SPIFFE ID the peer presents so
// RBAC can be exercised.
func buildMTLSServerAndClient(
	t *testing.T,
	serverID, callerID spiffeid.ID,
	srvOpts []grpc.ServerOption,
	unaryHandler grpc.UnaryHandler,
	streamHandler grpc.StreamHandler,
) *grpc.ClientConn {
	t.Helper()

	ca := grpcxtest.NewCA(t, testTrustDomain)
	serverCreds := credentials.NewTLS(ca.ServerTLSConfig(t, serverID))
	clientCreds := credentials.NewTLS(ca.ClientTLSConfig(t, callerID, serverID))

	lis := bufconn.Listen(1 << 20)
	opts := append([]grpc.ServerOption{grpc.Creds(serverCreds)}, srvOpts...)
	srv := grpc.NewServer(opts...)

	// Register a tiny unknown-service handler so we can drive arbitrary method
	// names through the interceptor chain without generated stubs.
	desc := &grpc.ServiceDesc{
		ServiceName: "boltrope.test.v1.Echo",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Unary",
			Handler: func(_ any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptyMsg)
				if err := dec(in); err != nil {
					return nil, err
				}
				info := &grpc.UnaryServerInfo{FullMethod: testMethod}
				if interceptor == nil {
					return unaryHandler(ctx, in)
				}
				return interceptor(ctx, in, info, unaryHandler)
			},
		}},
		Streams: []grpc.StreamDesc{{
			StreamName:    "Stream",
			ServerStreams: true,
			ClientStreams: true,
			Handler: func(_ any, stream grpc.ServerStream) error {
				return streamHandler(nil, stream)
			},
		}},
	}
	srv.RegisterService(desc, new(struct{}))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(clientCreds),
		// Install the OTel client stats handler so the active trace context is
		// injected into outgoing gRPC metadata (W3C traceparent). The server
		// stats handler extracts it, making the server span a child of the
		// client span and proving cross-call propagation. Harmless for tests
		// that do not start a client span (the global provider is a no-op).
		grpc.WithStatsHandler(grpcx.ClientStatsHandler()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// emptyMsg is a trivial proto-less message; the codec is overridden below.
type emptyMsg struct{ Payload string }

// TestInterceptorChain_RunsInOrderAndPropagatesTrace wires the full server chain
// (otel stats handler + logging + recovery + RBAC) and asserts (a) the
// interceptors run in the documented order and (b) W3C trace context set on the
// client side is propagated to the server handler across the bufconn call
// (FR-OBS-01 AC-2 propagation seam).
func TestInterceptorChain_RunsInOrderAndPropagatesTrace(t *testing.T) {
	// Install a real tracer provider + propagator so the otel stats handler
	// extracts the incoming trace context. Restore globals after the test.
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	tp := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})

	var order []string
	var mu sync.Mutex
	record := func(name string) {
		mu.Lock()
		order = append(order, name)
		mu.Unlock()
	}

	// Use the production obs logger so trace_id/span_id are injected into the
	// access-log record from the active (propagated) span — that injection is
	// what makes the log line correlate to the trace (FR-OBS-01, NFR-OBS-02).
	var buf bytes.Buffer
	logger := obs.NewLogger(&buf, slog.LevelInfo)

	serverID := mustID(t, "/ns/default/sa/orchestrator")
	callerID := mustID(t, "/ns/default/sa/orchestrator")

	// RBAC allows the caller for testMethod so the chain reaches the handler.
	policy := grpcx.RBACPolicy{testMethod: {callerID}}

	// Probe interceptors interleaved with the real ones let us assert ordering:
	// otel (stats handler, observed indirectly via trace propagation) → logging
	// → recovery → rbac → handler. We wrap each real concern's position with a
	// probe by composing the chain via ChainUnaryInterceptors.
	chain := grpcx.ChainUnaryInterceptors(
		probeUnary(record, "logging-before", grpcx.UnaryLoggingInterceptor(logger)),
		probeUnary(record, "recovery-before", grpcx.UnaryRecoveryInterceptor(logger)),
		probeUnary(record, "rbac-before", grpcx.UnaryRBACInterceptor(policy)),
	)

	handler := func(ctx context.Context, req any) (any, error) {
		record("handler")
		return echoTraceHandler(ctx, req)
	}

	conn := buildMTLSServerAndClient(t, serverID, callerID,
		[]grpc.ServerOption{
			grpc.StatsHandler(grpcx.ServerStatsHandler()),
			grpc.UnaryInterceptor(chain),
		},
		handler, nil)

	// Start a client span so there is a trace context to propagate.
	ctx, span := tp.Tracer("test").Start(context.Background(), "client-call")
	wantTrace := span.SpanContext().TraceID().String()
	defer span.End()

	var got string
	err := conn.Invoke(ctx, testMethod, &emptyMsg{Payload: "hi"}, &got, grpc.ForceCodec(rawStringCodec{}))
	require.NoError(t, err)

	// (a) order: each probe fires in chain order, then the handler.
	assert.Equal(t, []string{"logging-before", "recovery-before", "rbac-before", "handler"}, order)

	// (b) trace propagation: the handler observed the SAME trace id the client
	// started with, proving the otel stats handler extracted it from gRPC
	// metadata (W3C traceparent) across the call.
	assert.Equal(t, wantTrace, got, "trace context did not propagate to the server handler")
	assert.NotEqual(t, trace.TraceID{}.String(), got)

	// The logging interceptor must have emitted a structured record carrying the
	// propagated trace_id (trace correlation, FR-OBS-01).
	logOut := buf.String()
	require.NotEmpty(t, logOut)
	assert.Contains(t, logOut, wantTrace, "logging interceptor must correlate the log to the propagated trace")
	assert.Contains(t, logOut, testMethod)
}

// TestUnaryRBACInterceptor_RejectsDisallowedCaller is the architecture §8.1 verb
// gate: a caller whose SPIFFE ID is not on the per-RPC allowlist is rejected with
// PermissionDenied before the handler runs; an allowed caller passes.
func TestUnaryRBACInterceptor_RejectsDisallowedCaller(t *testing.T) {
	serverID := mustID(t, "/ns/default/sa/toolruntime")
	allowedCaller := mustID(t, "/ns/default/sa/orchestrator")
	disallowedCaller := mustID(t, "/ns/default/sa/modelgateway")

	// Only orchestrator may call testMethod (deny-by-default for everyone else).
	policy := grpcx.RBACPolicy{testMethod: {allowedCaller}}

	handlerCalled := false
	handler := func(_ context.Context, _ any) (any, error) {
		handlerCalled = true
		return "ok", nil
	}

	t.Run("disallowed caller -> PermissionDenied, handler not reached", func(t *testing.T) {
		handlerCalled = false
		conn := buildMTLSServerAndClient(t, serverID, disallowedCaller,
			[]grpc.ServerOption{grpc.UnaryInterceptor(grpcx.UnaryRBACInterceptor(policy))},
			handler, nil)

		var out string
		err := conn.Invoke(context.Background(), testMethod, &emptyMsg{}, &out, grpc.ForceCodec(rawStringCodec{}))
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
		assert.False(t, handlerCalled, "handler must not run for a denied caller")
	})

	t.Run("allowed caller -> handler reached", func(t *testing.T) {
		handlerCalled = false
		conn := buildMTLSServerAndClient(t, serverID, allowedCaller,
			[]grpc.ServerOption{grpc.UnaryInterceptor(grpcx.UnaryRBACInterceptor(policy))},
			handler, nil)

		var out string
		err := conn.Invoke(context.Background(), testMethod, &emptyMsg{}, &out, grpc.ForceCodec(rawStringCodec{}))
		require.NoError(t, err)
		assert.True(t, handlerCalled)
	})

	t.Run("method with no allowlist entry denies even a known caller", func(t *testing.T) {
		handlerCalled = false
		conn := buildMTLSServerAndClient(t, serverID, allowedCaller,
			[]grpc.ServerOption{grpc.UnaryInterceptor(grpcx.UnaryRBACInterceptor(grpcx.RBACPolicy{}))},
			handler, nil)

		var out string
		err := conn.Invoke(context.Background(), testMethod, &emptyMsg{}, &out, grpc.ForceCodec(rawStringCodec{}))
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err), "deny-by-default: unlisted method is denied")
		assert.False(t, handlerCalled)
	})
}

// TestUnaryRecoveryInterceptor_TranslatesPanic asserts a panicking handler does
// not crash the server: the recovery interceptor converts it into an Internal
// status and logs the panic.
func TestUnaryRecoveryInterceptor_TranslatesPanic(t *testing.T) {
	serverID := mustID(t, "/ns/default/sa/orchestrator")
	callerID := mustID(t, "/ns/default/sa/orchestrator")

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	panicking := func(_ context.Context, _ any) (any, error) {
		panic("boom")
	}

	conn := buildMTLSServerAndClient(t, serverID, callerID,
		[]grpc.ServerOption{grpc.UnaryInterceptor(grpcx.UnaryRecoveryInterceptor(logger))},
		panicking, nil)

	var out string
	err := conn.Invoke(context.Background(), testMethod, &emptyMsg{}, &out, grpc.ForceCodec(rawStringCodec{}))
	require.Error(t, err)
	assert.Equal(t, codes.Internal, status.Code(err))
	// The panic value must have been logged (at ERROR).
	assert.Contains(t, buf.String(), "boom")
}

// TestStreamRBACInterceptor_RejectsDisallowedCaller mirrors the unary RBAC test
// for streaming RPCs.
func TestStreamRBACInterceptor_RejectsDisallowedCaller(t *testing.T) {
	serverID := mustID(t, "/ns/default/sa/toolruntime")
	allowed := mustID(t, "/ns/default/sa/orchestrator")
	disallowed := mustID(t, "/ns/default/sa/modelgateway")

	policy := grpcx.RBACPolicy{streamMethod: {allowed}}

	reached := false
	streamHandler := func(_ any, _ grpc.ServerStream) error {
		reached = true
		return nil
	}

	conn := buildMTLSServerAndClient(t, serverID, disallowed,
		[]grpc.ServerOption{grpc.StreamInterceptor(grpcx.StreamRBACInterceptor(policy))},
		nil, streamHandler)

	stream, err := conn.NewStream(context.Background(), &grpc.StreamDesc{StreamName: "Stream", ServerStreams: true, ClientStreams: true}, streamMethod, grpc.ForceCodec(rawStringCodec{}))
	require.NoError(t, err)
	// The denial surfaces on the first Recv.
	err = stream.RecvMsg(new(emptyMsg))
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
	assert.False(t, reached, "stream handler must not run for a denied caller")
}

// probeUnary wraps a real interceptor, recording a label just before it runs so
// the chain order is observable in tests.
func probeUnary(record func(string), label string, inner grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		record(label)
		return inner(ctx, req, info, handler)
	}
}

// init registers the raw-string codec so the bufconn server can decode the
// harness's non-proto messages. ForceCodec on the client sets the matching
// content-subtype, and the server looks the codec up in this global registry;
// without registration the server falls back to the proto codec and rejects the
// non-proto request. Registration is process-global but harmless: it only
// activates for the "raw-string" content-subtype these tests use.
func init() { encoding.RegisterCodec(rawStringCodec{}) }

// rawStringCodec is a minimal gRPC codec that ships our emptyMsg.Payload as the
// request body and decodes a bare string response, so the harness needs no
// generated protobuf types.
type rawStringCodec struct{}

func (rawStringCodec) Name() string { return "raw-string" }

func (rawStringCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case *emptyMsg:
		return []byte(m.Payload), nil
	case *string:
		return []byte(*m), nil
	case string:
		return []byte(m), nil
	default:
		return nil, status.Errorf(codes.Internal, "raw-string codec: cannot marshal %T", v)
	}
}

func (rawStringCodec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case *emptyMsg:
		m.Payload = string(data)
		return nil
	case *string:
		*m = string(data)
		return nil
	default:
		return status.Errorf(codes.Internal, "raw-string codec: cannot unmarshal into %T", v)
	}
}

// ensure the codec satisfies the (encoding.Codec) shape used by ForceCodec.
var _ interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
} = rawStringCodec{}
