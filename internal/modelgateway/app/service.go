// Package app holds the model-gateway use-case (T-MGW-09): the stateless
// gateway service that fronts a configured [llm.Provider]. It selects the
// injected provider (already wrapped by the harness retry decorator —
// internal/modelgateway/app/retry), resolves per-(endpoint, model) capabilities
// via internal/modelgateway/app/capabilities, and computes the per-turn USD cost
// via internal/platform/pricing from the authoritative [llm.Usage] on the
// terminal Done stream event (architecture §4.3, §11.4, §11.6; ADR-0004,
// ADR-0016).
//
// The Service is provider-agnostic: it speaks only the normalized [llm] kernel
// types. All provider-specific wire-format mapping, stream normalization, and
// error classification live in the provider adapters behind the [llm.Provider]
// it is given. The gRPC inbound server
// (internal/modelgateway/adapter/inbound/grpc) maps gen ⇄ llm at the transport
// edge and delegates to this Service.
//
// # Cost on Done
//
// Cost is computed in the gateway, not the orchestrator (architecture §11.6).
// The Service wraps the provider's [llm.StreamReader] so that exactly when the
// terminal Done event passes through, it computes cost from Done.Usage and the
// configured pricing function and reports a [CostReport] to the [CostSink]. Cost
// is best-effort observability: an unknown-model pricing error is carried on the
// CostReport.Err, never surfaced as a stream failure.
package app

import (
	"context"
	"errors"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// CapabilityResolver resolves the [llm.Capabilities] for an (endpoint, model)
// pair. It is the consumer-defined port the Service depends on; the model-gateway
// capabilities registry (internal/modelgateway/app/capabilities) satisfies it.
type CapabilityResolver interface {
	// Resolve returns the capabilities for the given endpoint and model,
	// applying the registry's override precedence. It does not error: an
	// unknown (endpoint, model) resolves to a conservative all-false default.
	Resolve(endpoint, model string) llm.Capabilities
}

// CostFunc computes the USD cost of a single turn from the model id and its
// normalized usage. It mirrors [pricing.Cost]; an unknown model must return a
// typed error rather than a best-guess zero (architecture §11.6).
type CostFunc func(model string, u llm.Usage) (float64, error)

// CostReport is the result of computing a turn's cost on the terminal Done
// event. It is handed to the [CostSink] for observability (the OTel `chat` span
// `gen_ai.usage.*` attributes and the cost-rollup projection consume it).
type CostReport struct {
	// Model is the request's target model id.
	Model string
	// Usage is the authoritative normalized usage from the Done event.
	Usage llm.Usage
	// CostUSD is the computed cost in USD. It is zero when Err is non-nil.
	CostUSD float64
	// Err is non-nil when the cost could not be computed (e.g. the model is
	// absent from the pricing table — a [*pricing.UnknownModelError]). Cost is
	// best-effort: a non-nil Err never fails the generation stream.
	Err error
}

// CostSink receives a [CostReport] when a generation completes (its Done event
// is observed). Implementations record it (e.g. as an OTel span attribute or a
// metric). The default no-op sink is used when none is configured.
type CostSink interface {
	// Record reports the computed cost for one completed turn. It must not block
	// the stream; implementations should be cheap and non-failing.
	Record(ctx context.Context, r CostReport)
}

// nopCostSink discards all reports. It is the default when Config.CostSink is
// nil so the Service never needs a nil check.
type nopCostSink struct{}

func (nopCostSink) Record(context.Context, CostReport) {}

// Config configures a [Service]. Provider, Capabilities, and Cost are required;
// CostSink defaults to a no-op when nil.
type Config struct {
	// Provider is the selected, already-retry-decorated provider the gateway
	// fronts. It must be non-nil. Wrap the raw provider adapter with
	// internal/modelgateway/app/retry before passing it here.
	Provider llm.Provider
	// Endpoint is the logical endpoint name used as the capability-resolution
	// key (e.g. "anthropic", or a self-hosted endpoint id). It must be non-empty.
	Endpoint string
	// Capabilities resolves per-(endpoint, model) capability flags. It must be
	// non-nil.
	Capabilities CapabilityResolver
	// Cost computes per-turn USD cost from usage. It must be non-nil; pass
	// pricing.Cost (optionally wrapped with a config-driven overlay).
	Cost CostFunc
	// CostSink receives the computed cost on each completed turn. When nil a
	// no-op sink is used.
	CostSink CostSink
}

// Service is the model-gateway use-case. Construct one with [NewService]. It is
// safe for concurrent use when its collaborators are.
type Service struct {
	provider llm.Provider
	endpoint string
	caps     CapabilityResolver
	cost     CostFunc
	sink     CostSink
}

// NewService validates cfg and returns a *Service. It returns a non-nil error
// when a required collaborator (Provider, Capabilities, Cost) is missing or
// Endpoint is empty.
func NewService(cfg Config) (*Service, error) {
	if cfg.Provider == nil {
		return nil, errors.New("modelgateway/app: Config.Provider must not be nil")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("modelgateway/app: Config.Endpoint must not be empty")
	}
	if cfg.Capabilities == nil {
		return nil, errors.New("modelgateway/app: Config.Capabilities must not be nil")
	}
	if cfg.Cost == nil {
		return nil, errors.New("modelgateway/app: Config.Cost must not be nil")
	}
	sink := cfg.CostSink
	if sink == nil {
		sink = nopCostSink{}
	}
	return &Service{
		provider: cfg.Provider,
		endpoint: cfg.Endpoint,
		caps:     cfg.Capabilities,
		cost:     cfg.Cost,
		sink:     sink,
	}, nil
}

// Stream runs a streaming generation and returns a [llm.StreamReader] of
// normalized events. The reader is wrapped so that, when the terminal Done event
// passes through, the Service computes the turn cost from Done.Usage and the
// configured pricing function and reports it to the [CostSink] exactly once.
//
// On a failure to open the stream a [*llm.ProviderError] (already classified by
// the provider adapter, retried by the decorator) is returned; mid-stream
// failures surface from the returned reader's Recv.
func (s *Service) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	reader, err := s.provider.Stream(ctx, req)
	if err != nil {
		return nil, err
	}
	return &costingReader{
		ctx:   ctx,
		inner: reader,
		model: req.Model,
		cost:  s.cost,
		sink:  s.sink,
	}, nil
}

// CountTokens returns the input token count for req under its target model. It
// delegates to the provider, which is capability-gated: when the model does not
// support token counting the provider returns a [*llm.ProviderError] with kind
// [llm.ErrUnsupported], propagated unchanged so the gRPC edge can map it to
// UNIMPLEMENTED (architecture §11.6).
func (s *Service) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	return s.provider.CountTokens(ctx, req)
}

// Capabilities returns the resolved [llm.Capabilities] for model on this
// gateway's configured endpoint. Resolution is the gateway's job via the
// injected registry (a static per-model table overridable per endpoint), NOT a
// provider round-trip — capability variability is per-(endpoint, model)
// (architecture §11.4).
func (s *Service) Capabilities(_ context.Context, model string) (llm.Capabilities, error) {
	return s.caps.Resolve(s.endpoint, model), nil
}

// costingReader wraps a [llm.StreamReader] and reports cost to the sink when the
// terminal Done event is observed. It forwards every event unchanged.
type costingReader struct {
	ctx      context.Context //nolint:containedctx // carried only to pass to the non-blocking CostSink on Done
	inner    llm.StreamReader
	model    string
	cost     CostFunc
	sink     CostSink
	reported bool
}

// Recv returns the next event from the inner reader. When the event is the
// terminal Done, it computes and reports the turn cost exactly once before
// returning the event to the caller.
func (r *costingReader) Recv() (llm.StreamEvent, error) {
	ev, err := r.inner.Recv()
	if err != nil {
		return ev, err
	}
	if ev.Done != nil && !r.reported {
		r.reported = true
		report := computeReport(r.model, ev.Done.Usage, r.cost)
		r.sink.Record(r.ctx, report)
	}
	return ev, nil
}

// Close releases the inner reader's resources.
func (r *costingReader) Close() error { return r.inner.Close() }

// computeReport builds a CostReport from a model id, usage, and a cost function.
// It is isolated as a package-level helper so the cost-on-Done logic is uniform
// and directly testable.
func computeReport(model string, u llm.Usage, cost CostFunc) CostReport {
	c, err := cost(model, u)
	if err != nil {
		return CostReport{Model: model, Usage: u, CostUSD: 0, Err: err}
	}
	return CostReport{Model: model, Usage: u, CostUSD: c}
}
