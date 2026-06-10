package modelgw

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Compile-time assertion: *Adapter must satisfy app.ModelGatewayPort.
var _ app.ModelGatewayPort = (*Adapter)(nil)

// Adapter implements [app.ModelGatewayPort] over the generated
// [genproto.ModelGatewayServiceClient]. It is a thin transport-edge adapter
// whose only job is to map gen/ wire types ↔ llm kernel types; all
// provider-specific normalization, retry, and stop-reason handling live in the
// gateway service on the other side of the gRPC boundary (architecture §4.3,
// §12.3; ADR-0004, ADR-0016).
//
// Generate drives the gateway's streaming Generate RPC and assembles the
// result into a single *llm.Response. Stream wraps the same RPC as a lazy
// llm.StreamReader. CountTokens and Capabilities are unary RPCs.
//
// gRPC status codes are mapped to *llm.ProviderError at this boundary so the
// loop never sees a raw *status.Status (architecture §4.4).
type Adapter struct {
	client genproto.ModelGatewayServiceClient
}

// NewAdapter returns an Adapter backed by the given ModelGatewayServiceClient.
func NewAdapter(client genproto.ModelGatewayServiceClient) *Adapter {
	return &Adapter{client: client}
}

// ---- Stream -----------------------------------------------------------------

// Stream opens the gateway's Generate RPC and returns a [llm.StreamReader]
// that yields normalized [llm.StreamEvent]s as they arrive. The mapping from
// gen.StreamEvent → llm.StreamEvent is done by normalizeEvent inside the
// reader's Recv method, so the loop drives the stream directly without
// buffering (architecture §4.3).
//
// On a failure to open the stream a [*llm.ProviderError] is returned; errors
// surfaced during streaming appear from StreamReader.Recv.
func (a *Adapter) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	grpcStream, err := a.client.Generate(ctx, &genproto.GenerateRequest{
		Params: toGenerationParams(req),
	})
	if err != nil {
		return nil, mapGRPCError(err)
	}
	return &streamReader{stream: grpcStream}, nil
}

// ---- Generate ---------------------------------------------------------------

// Generate drives the gateway's streaming Generate RPC to completion and
// assembles the result into a *llm.Response. Text deltas are concatenated into
// a single TextPart; ToolCallDeltas are accumulated by CallID into ToolCall
// ContentParts; the terminal Done fields (StopReason, Usage, ProviderRaw)
// populate the Response.
//
// On [llm.Pause] the Response.ProviderRaw carries the continuation blob the
// loop must echo back on the next Generate call (architecture §4.3, §11.1).
func (a *Adapter) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	reader, err := a.Stream(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }()
	return assembleResponse(reader)
}

// assembleResponse consumes a StreamReader and builds a *llm.Response.
// It is separate from Generate so it can be tested independently if needed.
func assembleResponse(reader llm.StreamReader) (*llm.Response, error) {
	var (
		textBuf   string                        // accumulates TextDelta fragments
		callAccs  = make(map[string]*callAccum) // keyed by CallID
		callOrder []string                      // CallIDs in first-seen order
		done      *llm.Done
	)

	for {
		ev, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}

		switch {
		case ev.TextDelta != nil:
			textBuf += ev.TextDelta.Text

		case ev.ThinkingDelta != nil:
			// Thinking deltas are not accumulated into the Response content
			// in this assembler — the gateway already collected signatures into
			// Done.ProviderRaw. The loop relays ThinkingDelta events live
			// before they arrive here, so we discard them in the assembler.

		case ev.ToolCallDelta != nil:
			tcd := ev.ToolCallDelta
			acc, exists := callAccs[tcd.CallID]
			if !exists {
				acc = &callAccum{id: tcd.CallID, name: tcd.Name}
				callAccs[tcd.CallID] = acc
				callOrder = append(callOrder, tcd.CallID)
			}
			if tcd.Name != "" && acc.name == "" {
				acc.name = tcd.Name
			}
			// For simplicity all arg fragments are appended; path-addressed
			// accumulation is the gateway's job (architecture §11.2).
			acc.argsFragments = append(acc.argsFragments, tcd.ArgsFragment...)

		case ev.Done != nil:
			done = ev.Done
		}
	}

	if done == nil {
		return nil, fmt.Errorf("modelgw: stream ended without a Done event")
	}

	// Assemble content parts.
	var content []llm.ContentPart
	if textBuf != "" {
		content = append(content, llm.ContentPart{
			Text: &llm.TextPart{Text: textBuf},
		})
	}
	for _, id := range callOrder {
		acc := callAccs[id]
		args := parseArgs(acc.argsFragments)
		content = append(content, llm.ContentPart{
			ToolCall: &llm.ToolCall{
				ID:   acc.id,
				Name: acc.name,
				Args: args,
			},
		})
	}

	return &llm.Response{
		Content:       content,
		StopReason:    done.StopReason,
		RawStopReason: done.RawStopReason,
		Usage:         done.Usage,
		ProviderRaw:   done.ProviderRaw,
	}, nil
}

// callAccum accumulates ToolCallDelta fragments for a single call.
type callAccum struct {
	id            string
	name          string
	argsFragments []byte // concatenated raw JSON argument bytes
}

// parseArgs attempts to JSON-unmarshal the accumulated argument bytes into a
// map[string]any. On failure it returns nil — the loop will surface the raw
// fragment via ProviderRaw if needed.
func parseArgs(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

// ---- CountTokens ------------------------------------------------------------

// CountTokens returns the input token count for req from the gateway.
// When the model does not support token counting the gateway returns
// gRPC UNIMPLEMENTED, which this adapter maps to
// *llm.ProviderError{Kind: llm.ErrUnsupported} (architecture §11.6).
func (a *Adapter) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	resp, err := a.client.CountTokens(ctx, &genproto.CountTokensRequest{
		Params: toGenerationParams(req),
	})
	if err != nil {
		return 0, mapGRPCError(err)
	}
	return int(resp.GetInputTokens()), nil
}

// ---- Capabilities -----------------------------------------------------------

// Capabilities returns the [llm.Capabilities] for the given model from the
// gateway. The model id is the lookup key because capability variability is
// per-(endpoint, model) (architecture §11.4).
func (a *Adapter) Capabilities(ctx context.Context, model string) (llm.Capabilities, error) {
	resp, err := a.client.GetCapabilities(ctx, &genproto.GetCapabilitiesRequest{
		Model: model,
	})
	if err != nil {
		return llm.Capabilities{}, mapGRPCError(err)
	}
	if resp == nil {
		return llm.Capabilities{}, nil
	}
	return normalizeCapabilities(resp.GetCapabilities()), nil
}

// ---- streamReader -----------------------------------------------------------

// streamReader wraps a grpc.ServerStreamingClient[genproto.StreamEvent] and
// implements [llm.StreamReader]. Recv maps each incoming proto event to a
// normalized [llm.StreamEvent]; errors from the gRPC stream (including the
// final io.EOF) are passed through unchanged after mapping gRPC status codes
// to *llm.ProviderError where appropriate.
type streamReader struct {
	stream genproto.ModelGatewayService_GenerateClient
	done   bool
}

// Recv returns the next normalized [llm.StreamEvent]. It returns [io.EOF]
// after the terminal Done event has been delivered (consistent with
// [llm.StreamReader] semantics). Mid-stream gRPC errors are mapped to
// [*llm.ProviderError].
func (r *streamReader) Recv() (llm.StreamEvent, error) {
	if r.done {
		return llm.StreamEvent{}, io.EOF
	}
	protoEv, err := r.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.done = true
			return llm.StreamEvent{}, io.EOF
		}
		return llm.StreamEvent{}, mapGRPCError(err)
	}
	ev := normalizeEvent(protoEv)
	if ev.Done != nil {
		// Mark that next Recv should return io.EOF.
		r.done = true
	}
	return ev, nil
}

// Close discards any buffered stream data. It is safe to call after Recv
// returns io.EOF or an error, and may be called to abandon a stream early.
func (r *streamReader) Close() error {
	// grpc.ServerStreamingClient has no explicit Close; cancelling the context
	// (held by the caller) is the standard way to abort. We mark done here so
	// any subsequent Recv returns EOF immediately.
	r.done = true
	return nil
}

// ---- error mapping ----------------------------------------------------------

// mapGRPCError converts a gRPC status error to a *llm.ProviderError.
// Non-status errors are wrapped with ErrServer. The mapping follows the
// retryability semantics in architecture §4.4:
//
//   - UNAVAILABLE / DEADLINE_EXCEEDED → ErrServer (retryable)
//   - RESOURCE_EXHAUSTED              → ErrRateLimited (retryable)
//   - UNIMPLEMENTED                   → ErrUnsupported (not retryable)
//   - UNAUTHENTICATED / PERMISSION_DENIED → ErrAuth (not retryable)
//   - INVALID_ARGUMENT / FAILED_PRECONDITION → ErrInvalidRequest (not retryable)
//   - everything else                 → ErrServer (retryable)
func mapGRPCError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
	}
	var kind llm.ErrorKind
	switch st.Code() {
	case codes.ResourceExhausted:
		kind = llm.ErrRateLimited
	case codes.Unauthenticated, codes.PermissionDenied:
		kind = llm.ErrAuth
	case codes.InvalidArgument, codes.FailedPrecondition:
		kind = llm.ErrInvalidRequest
	case codes.Unimplemented:
		kind = llm.ErrUnsupported
	case codes.DeadlineExceeded:
		kind = llm.ErrTimeout
	default:
		// UNAVAILABLE, INTERNAL, UNKNOWN, etc. → retryable server error
		kind = llm.ErrServer
	}
	return &llm.ProviderError{Kind: kind, Raw: fmt.Errorf("rpc %s: %s", st.Code(), st.Message())}
}
