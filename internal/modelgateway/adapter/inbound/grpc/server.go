// Package grpc implements the model-gateway inbound gRPC server: the generated
// boltrope.v1.ModelGatewayServiceServer (T-MGW-09). It is a thin transport
// adapter that maps gen ⇄ llm at the edge and delegates to the model-gateway
// use-case ([app.Service]): Generate server-streams normalized StreamEvents,
// CountTokens is unary and capability-gated, GetCapabilities is unary and forces
// provider-native server-side tools off (architecture §4.3, §8.12, §11; ADR-0004,
// ADR-0016).
//
// Providers are INJECTED into the use-case; this server holds no provider
// knowledge. All provider-specific normalization, the harness retry policy, and
// cost-on-Done live in the use-case and the provider adapters behind it.
package grpc

import (
	"context"
	"errors"
	"io"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// gatewayService is the consumer-defined port this server depends on: the subset
// of the model-gateway use-case the transport needs. [app.Service] satisfies it.
// Declaring it here (rather than importing *app.Service directly) keeps the
// transport decoupled and independently testable.
type gatewayService interface {
	// Stream runs a streaming generation and returns a normalized reader. On a
	// failure to open the stream it returns a [*llm.ProviderError].
	Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error)
	// CountTokens returns the input token count; it returns a [*llm.ProviderError]
	// with kind [llm.ErrUnsupported] when the model does not support counting.
	CountTokens(ctx context.Context, req llm.Request) (int, error)
	// Capabilities returns the resolved capabilities for the model on this
	// gateway's endpoint.
	Capabilities(ctx context.Context, model string) (llm.Capabilities, error)
}

// Server implements [genproto.ModelGatewayServiceServer] over a [gatewayService].
// Construct one with [NewServer].
type Server struct {
	genproto.UnimplementedModelGatewayServiceServer
	svc gatewayService
}

// Compile-time assertion that *Server implements the generated server interface.
var _ genproto.ModelGatewayServiceServer = (*Server)(nil)

// NewServer returns a *Server backed by the given gateway use-case. svc is
// typically a *app.Service wrapping a retry-decorated provider.
func NewServer(svc gatewayService) *Server {
	return &Server{svc: svc}
}

// Generate runs a single generation and server-streams normalized StreamEvents,
// terminated by a StreamEvent carrying Done. It opens the use-case stream,
// mapping a failure-to-open to a gRPC status, then relays each [llm.StreamEvent]
// to the wire as a gen.StreamEvent. Mid-stream failures from the reader are
// mapped to a gRPC status and end the stream (architecture §4.3).
func (s *Server) Generate(req *genproto.GenerateRequest, stream genproto.ModelGatewayService_GenerateServer) error {
	ctx := stream.Context()
	reader, err := s.svc.Stream(ctx, toLLMRequest(req.GetParams()))
	if err != nil {
		return statusFromError(err)
	}
	defer func() { _ = reader.Close() }()

	for {
		// Honor client cancellation / deadline promptly between events.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		ev, recvErr := reader.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				return nil
			}
			return statusFromError(recvErr)
		}
		genEv := toGenStreamEvent(ev)
		if genEv == nil {
			// A zero/empty event carries nothing to relay; skip it.
			continue
		}
		if sendErr := stream.Send(genEv); sendErr != nil {
			return sendErr
		}
	}
}

// CountTokens returns the input token count for the request under its target
// model. When the model does not support token counting the use-case returns a
// [*llm.ProviderError] with kind [llm.ErrUnsupported], mapped here to gRPC
// UNIMPLEMENTED (architecture §11.6).
func (s *Server) CountTokens(ctx context.Context, req *genproto.CountTokensRequest) (*genproto.CountTokensResponse, error) {
	n, err := s.svc.CountTokens(ctx, toLLMRequest(req.GetParams()))
	if err != nil {
		return nil, statusFromError(err)
	}
	return &genproto.CountTokensResponse{InputTokens: int64(n)}, nil
}

// GetCapabilities returns the resolved capabilities for the requested model on
// this gateway's endpoint. provider-native server-side tools are forced off in
// the mapping (architecture §8.12).
func (s *Server) GetCapabilities(ctx context.Context, req *genproto.GetCapabilitiesRequest) (*genproto.GetCapabilitiesResponse, error) {
	caps, err := s.svc.Capabilities(ctx, req.GetModel())
	if err != nil {
		return nil, statusFromError(err)
	}
	return &genproto.GetCapabilitiesResponse{Capabilities: toGenCapabilities(caps)}, nil
}
