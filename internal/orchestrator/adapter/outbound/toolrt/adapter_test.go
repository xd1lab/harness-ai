// Package toolrt tests — adapter integration tests using bufconn.
//
// A fake gen.ToolRuntimeServiceServer streams scripted ExecuteToolEvent
// sequences and returns scripted ListToolsResponse values; the adapter's
// ExecuteTool and ListTools methods are exercised against it, asserting
// correct gen<->app mapping with no real network or sandbox.
package toolrt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

const bufSize = 1 << 20 // 1 MiB

// testTenant is the verified tenant the adapter requires on ctx (the edge-auth
// interceptor supplies it in production via db.WithTenant). The tool-runtime scopes
// its dedup ledger by tenant, so ExecuteTool fails closed without one.
const testTenant = "tenantA"

// tctx returns a context carrying testTenant, mirroring the verified tenant the
// orchestrator's edge-auth interceptor places on every request context.
func tctx() context.Context { return db.WithTenant(context.Background(), testTenant) }

// fakeServer is a scripted gen.ToolRuntimeServiceServer.
type fakeServer struct {
	genproto.UnimplementedToolRuntimeServiceServer

	// executeEvents is the sequence of ExecuteToolEvents to stream for each
	// ExecuteTool call (one slice per call, in order of calls).
	executeEvents [][]*genproto.ExecuteToolEvent
	execIdx       int
	executeErr    error // if non-nil, returned instead of streaming

	// listToolsResp is returned by ListTools.
	listToolsResp *genproto.ListToolsResponse
	listToolsErr  error
}

func (s *fakeServer) ExecuteTool(
	_ *genproto.ExecuteToolRequest,
	stream grpc.ServerStreamingServer[genproto.ExecuteToolEvent],
) error {
	if s.executeErr != nil {
		return s.executeErr
	}
	if s.execIdx >= len(s.executeEvents) {
		return status.Error(codes.Internal, "fakeServer: ExecuteTool queue exhausted")
	}
	evts := s.executeEvents[s.execIdx]
	s.execIdx++
	for _, ev := range evts {
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	return nil
}

func (s *fakeServer) ListTools(
	_ context.Context,
	_ *genproto.ListToolsRequest,
) (*genproto.ListToolsResponse, error) {
	return s.listToolsResp, s.listToolsErr
}

// newTestAdapter spins up a bufconn gRPC server backed by srv and returns a
// connected *Adapter. The returned cleanup func stops the server.
func newTestAdapter(t *testing.T, srv *fakeServer) (*Adapter, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterToolRuntimeServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	adapter := NewAdapter(genproto.NewToolRuntimeServiceClient(conn))
	cleanup := func() {
		_ = conn.Close()
		grpcSrv.Stop()
	}
	return adapter, cleanup
}

// ---- ExecuteTool: progress then terminal result -----------------------------

// TestExecuteTool_ProgressThenResult verifies that ExecuteTool opens a
// ToolStream that delivers mapped ToolProgress events followed by the mapped
// TerminalToolResult, then returns io.EOF.
func TestExecuteTool_ProgressThenResult(t *testing.T) {
	srv := &fakeServer{
		executeEvents: [][]*genproto.ExecuteToolEvent{
			{
				{Event: &genproto.ExecuteToolEvent_Progress{
					Progress: &genproto.ToolProgress{
						Message:     "running",
						StdoutChunk: []byte("partial output"),
					},
				}},
				{Event: &genproto.ExecuteToolEvent_TerminalResult{
					TerminalResult: &genproto.TerminalToolResult{
						Result: &genproto.ToolResult{
							CallId:  "call-1",
							Content: "done output",
							IsError: false,
						},
						Truncated: false,
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	exec := app.ToolExecution{
		SessionID: "sess-1",
		Call: llm.ToolCall{
			ID:   "call-1",
			Name: "read_file",
			Args: map[string]any{"path": "/tmp/x"},
		},
		IdempotencyKey: "idem-key-1",
	}

	stream, err := adapter.ExecuteTool(tctx(), exec)
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	defer func() { _ = stream.Close() }()

	// Event 1: progress.
	ev1, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv 1: %v", err)
	}
	if ev1.Progress == nil {
		t.Fatalf("event 1: want Progress, got %+v", ev1)
	}
	// The adapter maps Message + StdoutChunk into Output (concatenated).
	if ev1.Progress.Output == "" {
		t.Errorf("event 1 Progress.Output is empty; want non-empty from message/stdout")
	}

	// Event 2: terminal result.
	ev2, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv 2: %v", err)
	}
	if ev2.Result == nil {
		t.Fatalf("event 2: want Result, got %+v", ev2)
	}
	if ev2.Result.Content != "done output" {
		t.Errorf("Result.Content = %q, want %q", ev2.Result.Content, "done output")
	}
	if ev2.Result.IsError {
		t.Error("Result.IsError should be false")
	}
	if ev2.Result.Truncated {
		t.Error("Result.Truncated should be false")
	}

	// After terminal result: io.EOF.
	_, err = stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after terminal result: want io.EOF, got %v", err)
	}
}

// TestExecuteTool_ErrorResult verifies that a terminal result with is_error=true
// maps to ToolResult.IsError=true (not a gRPC fault).
func TestExecuteTool_ErrorResult(t *testing.T) {
	srv := &fakeServer{
		executeEvents: [][]*genproto.ExecuteToolEvent{
			{
				{Event: &genproto.ExecuteToolEvent_TerminalResult{
					TerminalResult: &genproto.TerminalToolResult{
						Result: &genproto.ToolResult{
							CallId:  "call-err",
							Content: "command not found",
							IsError: true,
						},
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	stream, err := adapter.ExecuteTool(tctx(), app.ToolExecution{
		SessionID:      "sess-2",
		Call:           llm.ToolCall{ID: "call-err", Name: "bash"},
		IdempotencyKey: "idem-2",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	defer func() { _ = stream.Close() }()

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Result == nil {
		t.Fatalf("want Result, got %+v", ev)
	}
	if !ev.Result.IsError {
		t.Error("IsError should be true")
	}
	if ev.Result.Content != "command not found" {
		t.Errorf("Content = %q", ev.Result.Content)
	}

	_, err = stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after terminal: want io.EOF, got %v", err)
	}
}

// TestExecuteTool_TruncatedBlobResult verifies that a terminal result with
// truncated=true and a blob_ref maps to ToolResult.Truncated=true and
// ToolResult.BlobRef is populated.
func TestExecuteTool_TruncatedBlobResult(t *testing.T) {
	srv := &fakeServer{
		executeEvents: [][]*genproto.ExecuteToolEvent{
			{
				{Event: &genproto.ExecuteToolEvent_TerminalResult{
					TerminalResult: &genproto.TerminalToolResult{
						Result: &genproto.ToolResult{
							CallId:  "call-blob",
							Content: "[truncated — full output in blob]",
							IsError: false,
						},
						Truncated: true,
						BlobRef: &genproto.BlobRef{
							Ref:       "sha256:abc123",
							MediaType: "text/plain",
							SizeBytes: 65536,
						},
					},
				}},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	stream, err := adapter.ExecuteTool(tctx(), app.ToolExecution{
		SessionID:      "sess-3",
		Call:           llm.ToolCall{ID: "call-blob", Name: "bash"},
		IdempotencyKey: "idem-3",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	defer func() { _ = stream.Close() }()

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.Result == nil {
		t.Fatalf("want Result, got %+v", ev)
	}
	if !ev.Result.Truncated {
		t.Error("Truncated should be true")
	}
	if ev.Result.BlobRef != "sha256:abc123" {
		t.Errorf("BlobRef = %q, want %q", ev.Result.BlobRef, "sha256:abc123")
	}

	_, err = stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Errorf("after terminal: want io.EOF, got %v", err)
	}
}

// TestExecuteTool_CtxCancelPropagates verifies that cancelling the context
// while a slow stream is in-flight causes the ToolStream.Recv to return a
// non-nil error (the gRPC layer cancels the RPC). We achieve this by never
// sending a terminal result, then cancelling the context.
func TestExecuteTool_CtxCancelPropagates(t *testing.T) {
	// The fake server blocks on ctx.Done — simulated by never sending events.
	// We exercise the cancel path by cancelling the context before calling.
	ctx, cancel := context.WithCancel(tctx())
	cancel() // pre-cancel so the RPC fails immediately

	srv := &fakeServer{
		executeEvents: [][]*genproto.ExecuteToolEvent{{}}, // empty stream
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	// With an already-cancelled context, ExecuteTool should either fail to
	// open the stream or the first Recv should return a context error.
	stream, openErr := adapter.ExecuteTool(ctx, app.ToolExecution{
		SessionID:      "sess-cancel",
		Call:           llm.ToolCall{ID: "c", Name: "bash"},
		IdempotencyKey: "idem-cancel",
	})
	if openErr != nil {
		// acceptable: opening the stream itself may fail
		return
	}
	defer func() { _ = stream.Close() }()

	_, recvErr := stream.Recv()
	if recvErr == nil {
		t.Error("expected a context error from cancelled stream, got nil")
	}
}

// TestExecuteTool_MapsRequestFields verifies that the adapter passes
// SessionID and IdempotencyKey to the gRPC request. We do this by
// inspecting the request inside a custom fake server.
func TestExecuteTool_MapsRequestFields(t *testing.T) {
	argsJSON, _ := json.Marshal(map[string]any{"path": "/tmp/file.txt"})
	var gotReq *genproto.ExecuteToolRequest

	// Use a capturing fake server.
	srv := &capturingFakeServer{
		onExecute: func(req *genproto.ExecuteToolRequest, stream grpc.ServerStreamingServer[genproto.ExecuteToolEvent]) error {
			gotReq = req
			// Send a minimal terminal result.
			return stream.Send(&genproto.ExecuteToolEvent{
				Event: &genproto.ExecuteToolEvent_TerminalResult{
					TerminalResult: &genproto.TerminalToolResult{
						Result: &genproto.ToolResult{CallId: req.GetCall().GetId(), Content: "ok"},
					},
				},
			})
		},
	}

	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterToolRuntimeServiceServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.Stop()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	adapter := NewAdapter(genproto.NewToolRuntimeServiceClient(conn))
	exec := app.ToolExecution{
		SessionID: "my-session",
		Call: llm.ToolCall{
			ID:   "call-42",
			Name: "write_file",
			Args: map[string]any{"path": "/tmp/file.txt"},
		},
		IdempotencyKey: "idem-42",
	}

	stream, err := adapter.ExecuteTool(tctx(), exec)
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	// Drain the stream.
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
	}
	_ = stream.Close()

	if gotReq == nil {
		t.Fatal("server did not receive request")
	}
	if gotReq.GetSessionId() != "my-session" {
		t.Errorf("session_id = %q, want %q", gotReq.GetSessionId(), "my-session")
	}
	if gotReq.GetTenantId() != testTenant {
		t.Errorf("tenant_id = %q, want %q (the verified tenant from ctx must be carried for the tool-runtime dedup ledger)", gotReq.GetTenantId(), testTenant)
	}
	if gotReq.GetIdempotencyKey() != "idem-42" {
		t.Errorf("idempotency_key = %q, want %q", gotReq.GetIdempotencyKey(), "idem-42")
	}
	if gotReq.GetCall().GetId() != "call-42" {
		t.Errorf("call.id = %q, want %q", gotReq.GetCall().GetId(), "call-42")
	}
	if gotReq.GetCall().GetName() != "write_file" {
		t.Errorf("call.name = %q, want %q", gotReq.GetCall().GetName(), "write_file")
	}
	// Verify args_json is a valid JSON encoding of the supplied args map.
	var gotArgs map[string]any
	if err := json.Unmarshal([]byte(gotReq.GetCall().GetArgsJson()), &gotArgs); err != nil {
		t.Fatalf("args_json not valid JSON: %v — raw: %s", err, gotReq.GetCall().GetArgsJson())
	}
	_ = argsJSON // used to silence "declared but not used" if marshaling above is skipped
	if gotArgs["path"] != "/tmp/file.txt" {
		t.Errorf("args[path] = %v, want /tmp/file.txt", gotArgs["path"])
	}
}

// capturingFakeServer is a ToolRuntimeServiceServer that calls a user-supplied
// function for ExecuteTool so tests can capture the incoming request.
type capturingFakeServer struct {
	genproto.UnimplementedToolRuntimeServiceServer
	onExecute func(*genproto.ExecuteToolRequest, grpc.ServerStreamingServer[genproto.ExecuteToolEvent]) error
}

func (s *capturingFakeServer) ExecuteTool(
	req *genproto.ExecuteToolRequest,
	stream grpc.ServerStreamingServer[genproto.ExecuteToolEvent],
) error {
	return s.onExecute(req, stream)
}

// ---- ListTools tests --------------------------------------------------------

// TestListTools_MapsDescriptors verifies that ListTools maps every ToolSpec
// field — name, description, json_schema (as []byte), SideEffect,
// EgressClass — to the corresponding app.ToolDescriptor fields.
func TestListTools_MapsDescriptors(t *testing.T) {
	schema := `{"type":"object","properties":{"path":{"type":"string"}}}`
	srv := &fakeServer{
		listToolsResp: &genproto.ListToolsResponse{
			Tools: []*genproto.ToolSpec{
				{
					Name:        "read_file",
					Description: "Read a file",
					JsonSchema:  schema,
					SideEffect:  genproto.SideEffect_SIDE_EFFECT_READ_ONLY,
					EgressClass: genproto.EgressClass_EGRESS_CLASS_NONE,
				},
				{
					Name:        "bash",
					Description: "Run a shell command",
					JsonSchema:  schema,
					SideEffect:  genproto.SideEffect_SIDE_EFFECT_MUTATING,
					EgressClass: genproto.EgressClass_EGRESS_CLASS_NONE,
				},
				{
					Name:        "webfetch",
					Description: "Fetch a URL",
					JsonSchema:  schema,
					SideEffect:  genproto.SideEffect_SIDE_EFFECT_MUTATING,
					EgressClass: genproto.EgressClass_EGRESS_CLASS_EXTERNAL,
				},
				{
					Name:        "mcp_tool",
					Description: "An MCP tool",
					JsonSchema:  schema,
					// UNSPECIFIED → fail-safe mutating + external
					SideEffect:  genproto.SideEffect_SIDE_EFFECT_UNSPECIFIED,
					EgressClass: genproto.EgressClass_EGRESS_CLASS_UNSPECIFIED,
				},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	descs, err := adapter.ListTools(context.Background(), "sess-list")
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(descs) != 4 {
		t.Fatalf("len(descs) = %d, want 4", len(descs))
	}

	// read_file — ReadOnly, None.
	d := descs[0]
	if d.Name != "read_file" {
		t.Errorf("[0] Name = %q, want read_file", d.Name)
	}
	if d.Description != "Read a file" {
		t.Errorf("[0] Description = %q", d.Description)
	}
	if string(d.JSONSchema) != schema {
		t.Errorf("[0] JSONSchema = %s, want %s", d.JSONSchema, schema)
	}
	if d.SideEffect != domain.SideEffectReadOnly {
		t.Errorf("[0] SideEffect = %q, want %q", d.SideEffect, domain.SideEffectReadOnly)
	}
	if d.EgressClass != domain.EgressClassNone {
		t.Errorf("[0] EgressClass = %q, want %q", d.EgressClass, domain.EgressClassNone)
	}

	// bash — Mutating, None.
	d = descs[1]
	if d.SideEffect != domain.SideEffectMutating {
		t.Errorf("[1] SideEffect = %q, want %q", d.SideEffect, domain.SideEffectMutating)
	}
	if d.EgressClass != domain.EgressClassNone {
		t.Errorf("[1] EgressClass = %q, want %q", d.EgressClass, domain.EgressClassNone)
	}

	// webfetch — Mutating, External.
	d = descs[2]
	if d.SideEffect != domain.SideEffectMutating {
		t.Errorf("[2] SideEffect = %q, want Mutating", d.SideEffect)
	}
	if d.EgressClass != domain.EgressClassExternal {
		t.Errorf("[2] EgressClass = %q, want External", d.EgressClass)
	}

	// mcp_tool — UNSPECIFIED → fail-safe Mutating + External.
	d = descs[3]
	if d.SideEffect != domain.SideEffectMutating {
		t.Errorf("[3] SideEffect = %q, want Mutating (fail-safe from UNSPECIFIED)", d.SideEffect)
	}
	if d.EgressClass != domain.EgressClassExternal {
		t.Errorf("[3] EgressClass = %q, want External (fail-safe from UNSPECIFIED)", d.EgressClass)
	}
}

// TestListTools_InternalEgressClass verifies that EGRESS_CLASS_INTERNAL maps
// correctly to EgressClassInternal.
func TestListTools_InternalEgressClass(t *testing.T) {
	schema := `{"type":"object"}`
	srv := &fakeServer{
		listToolsResp: &genproto.ListToolsResponse{
			Tools: []*genproto.ToolSpec{
				{
					Name:        "internal_tool",
					Description: "Internal tool",
					JsonSchema:  schema,
					SideEffect:  genproto.SideEffect_SIDE_EFFECT_READ_ONLY,
					EgressClass: genproto.EgressClass_EGRESS_CLASS_INTERNAL,
				},
			},
		},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	descs, err := adapter.ListTools(context.Background(), "sess-internal")
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(descs) != 1 {
		t.Fatalf("len = %d, want 1", len(descs))
	}
	if descs[0].EgressClass != domain.EgressClassInternal {
		t.Errorf("EgressClass = %q, want %q", descs[0].EgressClass, domain.EgressClassInternal)
	}
}

// TestListTools_Empty verifies that an empty ToolSpec list returns an empty
// []app.ToolDescriptor (never nil is a concern but both are acceptable here).
func TestListTools_Empty(t *testing.T) {
	srv := &fakeServer{
		listToolsResp: &genproto.ListToolsResponse{Tools: nil},
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	descs, err := adapter.ListTools(context.Background(), "sess-empty")
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	if len(descs) != 0 {
		t.Errorf("len = %d, want 0", len(descs))
	}
}

// TestListTools_RPCError verifies that a gRPC UNAVAILABLE error surfaces as a
// non-nil error from ListTools.
func TestListTools_RPCError(t *testing.T) {
	srv := &fakeServer{
		listToolsErr: status.Error(codes.Unavailable, "tool-runtime down"),
	}
	adapter, cleanup := newTestAdapter(t, srv)
	defer cleanup()

	_, err := adapter.ListTools(context.Background(), "sess-err")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---- Compile-time interface assertion ---------------------------------------

// TestInterfaceCompliance is a compile-time assertion that *Adapter satisfies
// app.ToolRuntimePort.
func TestInterfaceCompliance(t *testing.T) {
	var _ app.ToolRuntimePort = (*Adapter)(nil)
	t.Log("Adapter satisfies ToolRuntimePort: compile-time assertion OK")
}
