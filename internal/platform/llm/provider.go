package llm

import (
	"context"
	"time"
)

// Provider is the core provider-agnostic abstraction the agent loop talks to. One
// adapter implements Provider per provider family (Anthropic, Gemini, OpenAI
// Responses); self-hosted / OpenAI-compatible servers reuse the OpenAI
// Chat-Completions adapter pointed at a configurable base URL. Adding a provider is
// one adapter; the loop is untouched (ADR-0004).
//
// All provider-specific behavior — wire-format mapping, stream normalization,
// stop-reason and usage normalization, capability resolution, and error
// classification — lives behind this interface in the model-gateway adapters, so
// every method returns the normalized types defined in this package and a failure
// is always a [*ProviderError].
type Provider interface {
	// Generate runs a single non-streaming generation and returns the aggregated
	// normalized [Response]. On a [Pause] stop reason, the response's ProviderRaw
	// carries the continuation state to echo back via [Request.ProviderRaw]. On
	// failure it returns a [*ProviderError].
	Generate(ctx context.Context, req Request) (*Response, error)

	// Stream runs a streaming generation and returns a [StreamReader] of
	// normalized [StreamEvent]s terminated by a [Done] event. The caller drives
	// it with Recv/Close. On failure to start the stream it returns a
	// [*ProviderError]; mid-stream failures surface from [StreamReader.Recv].
	Stream(ctx context.Context, req Request) (StreamReader, error)

	// CountTokens returns the input token count for req under its target model.
	// It is capability-gated: when [Capabilities.SupportsTokenCounting] is false
	// for the model, it returns a [*ProviderError] with kind [ErrUnsupported].
	// It is never used for billing on providers that report authoritative usage
	// (architecture §11.6).
	CountTokens(ctx context.Context, req Request) (int, error)

	// Capabilities returns the [Capabilities] for the given model on this
	// provider's endpoint. The model id is an input because capability
	// variability is per-(endpoint, model) (architecture §11.4).
	Capabilities(ctx context.Context, model string) (Capabilities, error)
}

// Clock abstracts the passage of time for the model-gateway's retry policy so that
// backoff schedules are deterministic under test (an injected clock and jitter
// source replace direct time.Sleep — ADR-0016; architecture §4.4, §5.2). It lives
// in this kernel package because it is part of the contract the gateway's retry
// behavior is written and tested against.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After returns a channel that delivers the current time after at least d has
	// elapsed, analogous to [time.After].
	After(d time.Duration) <-chan time.Time
}

// SystemClock is the real [Clock] backed by the standard library. It is the one
// concrete implementation in this otherwise contract-only package, provided so
// production wiring need not redefine a trivial adapter; tests inject a fake clock
// instead.
type SystemClock struct{}

// Now returns the current wall-clock time via [time.Now].
func (SystemClock) Now() time.Time { return time.Now() }

// After returns [time.After](d).
func (SystemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// Ensure SystemClock satisfies Clock at compile time.
var _ Clock = SystemClock{}
