// Package toolrt is the orchestrator's outbound adapter for the tool-runtime
// gRPC service. It implements [app.ToolRuntimePort] over the generated
// [genproto.ToolRuntimeServiceClient], mapping gen/ wire types to and from the
// orchestrator's app-layer types ([app.ToolExecution], [app.ToolStream],
// [app.ToolDescriptor]) at this transport edge only (architecture §5.1,
// §12.3; ADR-0013, ADR-0014).
//
// ExecuteTool opens the tool-runtime's server-streaming ExecuteTool RPC and
// wraps it in a [toolStream] that yields [app.ToolEvent]s by mapping
// progress/terminal-result proto events to the app types. Context
// cancellation is propagated into the RPC (which the runtime turns into a
// cgroup/PID-namespace sandbox kill; architecture §9.3).
//
// ListTools calls the tool-runtime's unary ListTools RPC and maps every
// returned [genproto.ToolSpec] to an [app.ToolDescriptor], applying the
// fail-safe defaults for unspecified SideEffect and EgressClass values
// (ADR-0013; ADR-0014).
package toolrt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

// Compile-time assertion: *Adapter must satisfy app.ToolRuntimePort.
var _ app.ToolRuntimePort = (*Adapter)(nil)

// Adapter implements [app.ToolRuntimePort] over the generated
// [genproto.ToolRuntimeServiceClient]. It is the sole place in the
// orchestrator that imports gen/ for the tool-runtime boundary; all
// gen↔app mapping is confined here (architecture §12.3).
type Adapter struct {
	client genproto.ToolRuntimeServiceClient
}

// NewAdapter returns an Adapter backed by the given ToolRuntimeServiceClient.
func NewAdapter(client genproto.ToolRuntimeServiceClient) *Adapter {
	return &Adapter{client: client}
}

// ---- ExecuteTool ------------------------------------------------------------

// ExecuteTool dispatches one tool call to the tool-runtime service and returns
// a [app.ToolStream] that yields zero or more [app.ToolProgress] events
// followed by exactly one [app.ToolResult]. Cancelling ctx propagates into the
// RPC and the runtime kills the in-sandbox process tree (architecture §9.3).
//
// The request maps [app.ToolExecution] to [genproto.ExecuteToolRequest]:
//   - the VERIFIED tenant from ctx → tenant_id. It is REQUIRED, not optional:
//     the tool-runtime scopes its dedup ledger by (tenant_id, session_id,
//     idempotency_key) (ADR-0011) and rejects an empty tenant, so omitting it
//     fails the very first tool execution. mTLS authenticates the calling SERVICE
//     (the orchestrator), not the tenant; the tenant is the one the edge-auth
//     interceptor placed on ctx via db.WithTenant — the SAME source the event-store
//     append reads — so tool execution and the event log agree on the owner. A ctx
//     with no verified tenant fails closed here with a clear error rather than
//     surfacing the tool-runtime's opaque dedup rejection.
//   - SessionID → session_id
//   - Call.ID / Name / Args → call.id / call.name / call.args_json
//   - IdempotencyKey → idempotency_key
//
// On failure to open the stream a non-nil error is returned; errors surfaced
// during streaming are returned from [toolStream.Recv].
func (a *Adapter) ExecuteTool(ctx context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	tenant, err := db.TenantFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("toolrt: execute %q: %w", exec.Call.Name, err)
	}
	argsJSON, err := json.Marshal(exec.Call.Args)
	if err != nil {
		return nil, fmt.Errorf("toolrt: marshal args: %w", err)
	}
	req := &genproto.ExecuteToolRequest{
		TenantId:       tenant,
		SessionId:      exec.SessionID,
		IdempotencyKey: exec.IdempotencyKey,
		Call: &genproto.ToolCall{
			Id:       exec.Call.ID,
			Name:     exec.Call.Name,
			ArgsJson: string(argsJSON),
		},
	}
	grpcStream, err := a.client.ExecuteTool(ctx, req)
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return &toolStream{stream: grpcStream}, nil
}

// ---- ListTools --------------------------------------------------------------

// ListTools queries the tool-runtime for all currently registered tools
// (native + human-approved MCP) and maps each [genproto.ToolSpec] to an
// [app.ToolDescriptor]. Unspecified SideEffect/EgressClass values are mapped
// to their fail-safe defaults (Mutating / External; ADR-0013, ADR-0014).
func (a *Adapter) ListTools(ctx context.Context, sessionID string) ([]app.ToolDescriptor, error) {
	resp, err := a.client.ListTools(ctx, &genproto.ListToolsRequest{
		SessionId: sessionID,
	})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	specs := resp.GetTools()
	descs := make([]app.ToolDescriptor, 0, len(specs))
	for _, spec := range specs {
		descs = append(descs, mapToolSpec(spec))
	}
	return descs, nil
}

// mapToolSpec maps a single [genproto.ToolSpec] to an [app.ToolDescriptor].
// The JSON Schema is carried as []byte (the wire value is a JSON string).
// SideEffect and EgressClass UNSPECIFIED values are treated as the fail-safe
// Mutating / External defaults (ADR-0013 §"Fail-safe defaults"; ADR-0014).
func mapToolSpec(s *genproto.ToolSpec) app.ToolDescriptor {
	return app.ToolDescriptor{
		Name:        s.GetName(),
		Description: s.GetDescription(),
		JSONSchema:  []byte(s.GetJsonSchema()),
		SideEffect:  normalizeSideEffect(s.GetSideEffect()),
		EgressClass: normalizeEgressClass(s.GetEgressClass()),
	}
}

// normalizeSideEffect maps the gen SideEffect enum to the orchestrator-domain
// SideEffect string. UNSPECIFIED maps to the fail-safe Mutating (ADR-0014).
func normalizeSideEffect(se genproto.SideEffect) domain.SideEffect {
	switch se {
	case genproto.SideEffect_SIDE_EFFECT_READ_ONLY:
		return domain.SideEffectReadOnly
	case genproto.SideEffect_SIDE_EFFECT_MUTATING:
		return domain.SideEffectMutating
	default:
		// SIDE_EFFECT_UNSPECIFIED and any future unknown value → fail-safe
		// Mutating: the orchestrator will serialize the tool call and will not
		// auto-retry it at the RPC layer (ADR-0014; architecture §9.2).
		return domain.SideEffectMutating
	}
}

// normalizeEgressClass maps the gen EgressClass enum to the
// orchestrator-domain EgressClass string. UNSPECIFIED maps to the fail-safe
// External (ADR-0013; architecture §8.4).
func normalizeEgressClass(ec genproto.EgressClass) domain.EgressClass {
	switch ec {
	case genproto.EgressClass_EGRESS_CLASS_NONE:
		return domain.EgressClassNone
	case genproto.EgressClass_EGRESS_CLASS_INTERNAL:
		return domain.EgressClassInternal
	case genproto.EgressClass_EGRESS_CLASS_EXTERNAL:
		return domain.EgressClassExternal
	default:
		// EGRESS_CLASS_UNSPECIFIED and any future unknown value → fail-safe
		// External: the egress broker and taint/ask gate must be applied
		// (ADR-0013; architecture §8.4).
		return domain.EgressClassExternal
	}
}

// ---- toolStream -------------------------------------------------------------

// toolStream wraps a genproto.ToolRuntimeService_ExecuteToolClient (a
// grpc.ServerStreamingClient[genproto.ExecuteToolEvent]) and implements
// [app.ToolStream]. Recv maps each incoming proto event to a normalized
// [app.ToolEvent]; the terminal result sets done=true so the next Recv
// returns io.EOF.
type toolStream struct {
	stream genproto.ToolRuntimeService_ExecuteToolClient
	done   bool
}

// Recv returns the next [app.ToolEvent]. It returns [io.EOF] after the
// terminal result event, or a non-nil error on failure (context cancellation,
// network error, etc.). Not safe for concurrent use.
func (s *toolStream) Recv() (app.ToolEvent, error) {
	if s.done {
		return app.ToolEvent{}, io.EOF
	}
	protoEv, err := s.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			s.done = true
			return app.ToolEvent{}, io.EOF
		}
		return app.ToolEvent{}, mapGRPCError(err)
	}
	return mapExecuteToolEvent(protoEv, &s.done), nil
}

// Close marks the stream as done so subsequent Recv calls return io.EOF
// immediately. Abandoning a stream is safe: the gRPC runtime cancels the
// underlying HTTP/2 stream when the context is cancelled (architecture §9.3).
func (s *toolStream) Close() error {
	s.done = true
	return nil
}

// mapExecuteToolEvent converts one [genproto.ExecuteToolEvent] to an
// [app.ToolEvent]. When the event is a terminal result, done is set to true
// so the caller knows the stream is finished.
func mapExecuteToolEvent(ev *genproto.ExecuteToolEvent, done *bool) app.ToolEvent {
	if ev == nil {
		return app.ToolEvent{}
	}
	switch v := ev.GetEvent().(type) {
	case *genproto.ExecuteToolEvent_Progress:
		if v == nil || v.Progress == nil {
			return app.ToolEvent{}
		}
		return app.ToolEvent{
			Progress: mapToolProgress(v.Progress),
		}

	case *genproto.ExecuteToolEvent_TerminalResult:
		if v == nil || v.TerminalResult == nil {
			return app.ToolEvent{}
		}
		*done = true
		return app.ToolEvent{
			Result: mapTerminalToolResult(v.TerminalResult),
		}

	default:
		return app.ToolEvent{}
	}
}

// mapToolProgress maps a [genproto.ToolProgress] to an [app.ToolProgress].
// The proto carries a human-readable message plus a raw stdout_chunk; the app
// type carries a single Output string. Both fields are concatenated (message
// first, then stdout bytes) so the loop can relay them to the client as a
// single partial-output chunk.
func mapToolProgress(p *genproto.ToolProgress) *app.ToolProgress {
	if p == nil {
		return &app.ToolProgress{}
	}
	output := p.GetMessage()
	if chunk := p.GetStdoutChunk(); len(chunk) > 0 {
		if output != "" {
			output += "\n"
		}
		output += string(chunk)
	}
	return &app.ToolProgress{Output: output}
}

// mapTerminalToolResult maps a [genproto.TerminalToolResult] to an
// [app.ToolResult]. The BlobRef.Ref field is used as the app BlobRef string
// (the orchestrator does not need the media_type or size_bytes at this
// boundary; those are available if needed via the blob store directly).
func mapTerminalToolResult(r *genproto.TerminalToolResult) *app.ToolResult {
	if r == nil {
		return &app.ToolResult{}
	}
	result := r.GetResult()
	var content string
	var isError bool
	if result != nil {
		content = result.GetContent()
		isError = result.GetIsError()
	}
	blobRef := ""
	if br := r.GetBlobRef(); br != nil {
		blobRef = br.GetRef()
	}
	return &app.ToolResult{
		Content:   content,
		IsError:   isError,
		Truncated: r.GetTruncated(),
		BlobRef:   blobRef,
	}
}

// ---- error mapping ----------------------------------------------------------

// mapGRPCError converts a gRPC status error to a wrapped error with the gRPC
// code and message. Non-status errors are wrapped as-is. The loop sees a
// plain error; callers that need to distinguish codes may use
// [google.golang.org/grpc/status.FromError].
func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("toolrt: %w", err)
	}
	switch st.Code() {
	case codes.Canceled, codes.DeadlineExceeded:
		// Propagate context errors so callers can use errors.Is(err, context.Canceled).
		return fmt.Errorf("toolrt: rpc %s: %s: %w", st.Code(), st.Message(), err)
	default:
		return fmt.Errorf("toolrt: rpc %s: %s", st.Code(), st.Message())
	}
}
