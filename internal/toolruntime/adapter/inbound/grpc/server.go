// Package grpc implements the tool-runtime inbound gRPC server: the generated
// boltrope.v1.ToolRuntimeServiceServer (T-TR-08). It is a thin transport adapter
// that maps gen ⇄ the tool-runtime app/domain types at the edge and delegates to
// the injected use-cases — the ExecuteTool service and the tool registry. It
// holds no execution, sandbox, dedup, egress, or blob knowledge of its own; all
// of that lives behind the injected collaborators (architecture §5.3, §12.4).
//
// ExecuteTool is a server stream of zero or more ToolProgress events followed by
// exactly one TerminalToolResult. A tool that errors (unknown tool, schema
// violation, egress denial, or a tool that ran but failed) is reported on that
// terminal result with is_error=true — NEVER as a gRPC fault (FR-TOOL-01); a gRPC
// status is reserved for genuine infrastructure failures. Client cancellation
// flows through the request context into the use-case and, via the bound
// Workspace, into a real in-sandbox process-group kill (architecture §9.3).
//
// ListTools is unary and returns the merged native + lazily-loaded, approved MCP
// tool set (FR-EXT-01 AC-3).
package grpc

import (
	"context"
	"encoding/json"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/toolruntime/app/execute"
	"github.com/boltrope/boltrope/internal/toolruntime/domain"
)

// executor is the consumer-defined port the transport needs for ExecuteTool: the
// subset of the ExecuteTool use-case. [*execute.Service] satisfies it. Declaring
// it here (rather than importing the concrete type) keeps the transport decoupled
// and independently testable.
type executor interface {
	// Execute runs one tool call, streaming interim progress through em and
	// returning the terminal result. A nil error with an error Observation denotes
	// a tool-level failure; a non-nil error denotes an infrastructure failure.
	Execute(ctx context.Context, req execute.Request, em execute.Emitter) (execute.Result, error)
}

// lister is the consumer-defined port the transport needs for ListTools: the
// registry's List. The tool registry satisfies it.
type lister interface {
	// List returns the specs of all currently available (native + approved/loaded
	// MCP) tools.
	List(ctx context.Context) ([]domain.ToolSpec, error)
}

// Server implements [genproto.ToolRuntimeServiceServer] over an [executor] and a
// [lister]. Construct one with [NewServer].
type Server struct {
	genproto.UnimplementedToolRuntimeServiceServer
	exec executor
	reg  lister
}

// Compile-time assertion that *Server implements the generated server interface.
var _ genproto.ToolRuntimeServiceServer = (*Server)(nil)

// NewServer returns a *Server backed by the ExecuteTool use-case exec and the
// tool registry reg. exec is typically a [*execute.Service]; reg is typically the
// tool-runtime [github.com/boltrope/boltrope/internal/toolruntime/adapter/registry.Registry].
func NewServer(exec executor, reg lister) *Server {
	return &Server{exec: exec, reg: reg}
}

// ExecuteTool runs one tool call inside the session's sandbox and server-streams
// zero or more ToolProgress events followed by exactly one TerminalToolResult.
// Tool-level failures are reported on the terminal result (is_error=true), not as
// a gRPC fault (FR-TOOL-01); an infrastructure failure from the use-case is
// mapped to a gRPC status. Cancellation flows from the stream context into the
// use-case and the sandbox kill (architecture §9.3).
func (s *Server) ExecuteTool(req *genproto.ExecuteToolRequest, stream genproto.ToolRuntimeService_ExecuteToolServer) error {
	ctx := stream.Context()
	em := &streamEmitter{stream: stream}

	res, err := s.exec.Execute(ctx, toExecuteRequest(req), em)
	if err != nil {
		// Infrastructure failure (e.g. dedup/blob unreachable, or the client
		// stream went away). Honor a cancelled context as a context error so the
		// client sees CANCELED/DEADLINE_EXCEEDED rather than INTERNAL.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return status.Error(codes.Internal, err.Error())
	}

	// Terminal result: always exactly one, always last.
	term := &genproto.ExecuteToolEvent{
		Event: &genproto.ExecuteToolEvent_TerminalResult{
			TerminalResult: toTerminalResult(req.GetCall().GetId(), res),
		},
	}
	return stream.Send(term)
}

// ListTools returns the merged native + approved MCP tool specs, mapped to the
// wire ToolSpec (FR-EXT-01 AC-3).
func (s *Server) ListTools(ctx context.Context, _ *genproto.ListToolsRequest) (*genproto.ListToolsResponse, error) {
	specs, err := s.reg.List(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := &genproto.ListToolsResponse{Tools: make([]*genproto.ToolSpec, 0, len(specs))}
	for _, spec := range specs {
		out.Tools = append(out.Tools, toGenToolSpec(spec))
	}
	return out, nil
}

// streamEmitter adapts the ExecuteTool server stream to the use-case's
// [execute.Emitter]: each interim progress event is sent as an
// ExecuteToolEvent_Progress. A send error aborts the use-case (it propagates back
// as the emitter's error).
type streamEmitter struct {
	stream genproto.ToolRuntimeService_ExecuteToolServer
}

// Progress sends one interim progress event on the wire.
func (e *streamEmitter) Progress(_ context.Context, p execute.Progress) error {
	return e.stream.Send(&genproto.ExecuteToolEvent{
		Event: &genproto.ExecuteToolEvent_Progress{
			Progress: &genproto.ToolProgress{
				Message:     p.Message,
				StdoutChunk: p.StdoutChunk,
			},
		},
	})
}

// ---------------------------------------------------------------------------
// gen ⇄ domain mapping (the transport edge)
// ---------------------------------------------------------------------------

// toExecuteRequest maps a gen.ExecuteToolRequest to an execute.Request, parsing
// the call's args_json into a map. A nil request yields a zero execute.Request.
func toExecuteRequest(req *genproto.ExecuteToolRequest) execute.Request {
	if req == nil {
		return execute.Request{}
	}
	call := req.GetCall()
	return execute.Request{
		TenantID:       req.GetTenantId(),
		SessionID:      req.GetSessionId(),
		CallID:         call.GetId(),
		ToolName:       call.GetName(),
		Args:           parseArgs(call.GetArgsJson()),
		IdempotencyKey: req.GetIdempotencyKey(),
		Timeout:        millisToDuration(req.GetTimeoutMs()),
	}
}

// millisToDuration converts a non-negative millisecond count to a
// time.Duration. A zero or negative value yields 0 (no per-call deadline).
func millisToDuration(ms int64) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

// parseArgs decodes a JSON-object args string into a map[string]any. An empty
// string yields nil; a malformed one yields nil so the registry's schema
// validation (which rejects a missing required field on nil/empty) produces the
// error observation rather than this edge faulting.
func parseArgs(s string) map[string]any {
	if s == "" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil
	}
	return m
}

// toTerminalResult maps the use-case result to a gen.TerminalToolResult,
// carrying the call id, content, is_error, and (when offloaded) the truncated
// flag + blob_ref. The tenant is implied by the request; the wire BlobRef carries
// only the per-tenant key + authoritative size (architecture §6.4).
func toTerminalResult(callID string, res execute.Result) *genproto.TerminalToolResult {
	obs := res.Observation
	tr := &genproto.TerminalToolResult{
		Result: &genproto.ToolResult{
			CallId:  callID,
			Content: obs.Content,
			IsError: obs.IsError,
		},
		Truncated: obs.Truncated,
	}
	if obs.BlobRef != "" {
		tr.BlobRef = &genproto.BlobRef{
			Ref:       obs.BlobRef,
			MediaType: "text/plain; charset=utf-8",
			SizeBytes: res.BlobSizeBytes,
		}
	}
	return tr
}

// toGenToolSpec maps a domain.ToolSpec to a gen.ToolSpec, including the
// SideEffect/EgressClass classifications and the JSON Schema carried verbatim as
// a string.
func toGenToolSpec(spec domain.ToolSpec) *genproto.ToolSpec {
	return &genproto.ToolSpec{
		Name:        spec.Name,
		Description: spec.Description,
		JsonSchema:  string(spec.JSONSchema),
		SideEffect:  toGenSideEffect(spec.SideEffect),
		EgressClass: toGenEgressClass(spec.EgressClass),
	}
}

// toGenSideEffect maps the domain SideEffect to the wire enum. The unset/unknown
// value maps to MUTATING (fail-safe; architecture §9.2).
func toGenSideEffect(se domain.SideEffect) genproto.SideEffect {
	switch se {
	case domain.SideEffectReadOnly:
		return genproto.SideEffect_SIDE_EFFECT_READ_ONLY
	case domain.SideEffectMutating:
		return genproto.SideEffect_SIDE_EFFECT_MUTATING
	default:
		// Fail-safe: an unannotated tool is treated as mutating (§9.2).
		return genproto.SideEffect_SIDE_EFFECT_MUTATING
	}
}

// toGenEgressClass maps the domain EgressClass to the wire enum. The unset value
// maps to EXTERNAL (fail-safe maximally-gated class; architecture §8.4).
func toGenEgressClass(ec domain.EgressClass) genproto.EgressClass {
	switch ec {
	case domain.EgressClassNone:
		return genproto.EgressClass_EGRESS_CLASS_NONE
	case domain.EgressClassInternal:
		return genproto.EgressClass_EGRESS_CLASS_INTERNAL
	case domain.EgressClassExternal:
		return genproto.EgressClass_EGRESS_CLASS_EXTERNAL
	default:
		// Fail-safe: an unannotated tool is treated as external (§8.4).
		return genproto.EgressClass_EGRESS_CLASS_EXTERNAL
	}
}
