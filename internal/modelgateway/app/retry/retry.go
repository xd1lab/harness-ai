// Package retry provides a [llm.Provider] decorator that adds the harness
// retry policy defined in ADR-0004.
//
// # Retry policy
//
// Retries are attempted ONLY for [*llm.ProviderError] values whose
// [llm.ProviderError.Retryable] method returns true, i.e. the transient kinds
//   - [llm.ErrRateLimited]  (HTTP 429)
//   - [llm.ErrOverloaded]   (HTTP 529)
//   - [llm.ErrServer]       (HTTP 5xx)
//   - [llm.ErrTimeout]      (transport deadline / provider-side timeout)
//
// Non-transient kinds ([llm.ErrInvalidRequest], [llm.ErrAuth],
// [llm.ErrUnsupported]) and any error that is not a [*llm.ProviderError] are
// propagated immediately with no retry.
//
// # Wait schedule
//
//  1. If [llm.ProviderError.RetryAfter] > 0 the decorator waits exactly that
//     long (honoring the provider hint first).
//
//  2. Otherwise, exponential backoff with FULL JITTER is applied:
//
//     cap  = min(Config.MaxDelay, Config.BaseDelay * 2^attempt)
//     wait = rand(0, cap)          // uniform in [0, cap)
//
// Both the clock and the random source are injected so backoff schedules are
// fully deterministic under test (ADR-0016; architecture §5.2).
//
// # Stream behavior
//
// [Provider.Stream] is retried only when [llm.Provider.Stream] itself returns
// an error (i.e. the stream failed to open, BEFORE the first event is
// delivered).  Once a [llm.StreamReader] has been returned to the caller,
// mid-stream errors from [llm.StreamReader.Recv] are NOT retried — the caller
// receives them directly.
//
// # Cancellation
//
// Context cancellation (or deadline expiry) during a wait causes the decorator
// to return the context error wrapped so [errors.Is](err, ctx.Err()) is true.
package retry

import (
	"context"
	"errors"
	"time"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Rand is the randomness port injected into the retry policy.  The single
// method Float64 returns a pseudo-random float64 in [0.0, 1.0).  Callers
// supply a seeded *[math/rand/v2.Rand] (or any other implementation) at
// wiring time; the determinism rule (ADR-0015) forbids direct construction of
// rand.* inside this package.
type Rand interface {
	// Float64 returns a pseudo-random float64 in [0.0, 1.0).
	Float64() float64
}

// Config holds the tunable parameters of the retry policy.  All fields have
// sensible production defaults but must be supplied explicitly so callers make
// a deliberate choice.
type Config struct {
	// MaxAttempts is the maximum total number of calls (first attempt plus
	// retries).  A value of 1 disables retries.  Zero is treated as 1.
	MaxAttempts int

	// BaseDelay is the base duration for exponential backoff.  It is the
	// minimum cap for attempt 0 (before 2^attempt scaling).
	BaseDelay time.Duration

	// MaxDelay is the ceiling on the computed backoff cap before jitter.
	// Jitter is applied on top of the cap, so actual waits are always ≤
	// MaxDelay (exclusive).
	MaxDelay time.Duration
}

// maxAttempts returns the effective attempt limit (at least 1).
func (c Config) maxAttempts() int {
	if c.MaxAttempts < 1 {
		return 1
	}
	return c.MaxAttempts
}

// Provider wraps an inner [llm.Provider] and adds the harness retry policy.
// Obtain one via [New].
type Provider struct {
	inner llm.Provider
	cfg   Config
	clock llm.Clock
	rng   Rand
}

// Compile-time assertion that *Provider satisfies llm.Provider.
var _ llm.Provider = (*Provider)(nil)

// New returns a *Provider that wraps inner with the harness retry policy.
// clock and rng are injected for deterministic test control; in production
// pass [llm.SystemClock]{} and a randomly-seeded *rand.Rand from the wiring
// layer (outside this package).
func New(inner llm.Provider, cfg Config, clock llm.Clock, rng Rand) *Provider {
	return &Provider{inner: inner, cfg: cfg, clock: clock, rng: rng}
}

// ---------------------------------------------------------------------------
// llm.Provider implementation
// ---------------------------------------------------------------------------

// Generate runs a single non-streaming generation, retrying on transient
// [*llm.ProviderError]s up to Config.MaxAttempts times.
func (p *Provider) Generate(ctx context.Context, req llm.Request) (*llm.Response, error) {
	var lastErr error
	for attempt := 0; attempt < p.cfg.maxAttempts(); attempt++ {
		if attempt > 0 {
			if err := p.wait(ctx, lastErr, attempt-1); err != nil {
				return nil, err
			}
		}
		resp, err := p.inner.Generate(ctx, req)
		if err == nil {
			return resp, nil
		}
		if !isRetryable(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// Stream opens a streaming generation, retrying on transient errors that
// occur BEFORE the first event is delivered (i.e. errors returned by
// [llm.Provider.Stream] itself).  Once a [llm.StreamReader] has been returned
// to the caller, mid-stream errors from [llm.StreamReader.Recv] are NOT
// retried.
func (p *Provider) Stream(ctx context.Context, req llm.Request) (llm.StreamReader, error) {
	var lastErr error
	for attempt := 0; attempt < p.cfg.maxAttempts(); attempt++ {
		if attempt > 0 {
			if err := p.wait(ctx, lastErr, attempt-1); err != nil {
				return nil, err
			}
		}
		reader, err := p.inner.Stream(ctx, req)
		if err == nil {
			return reader, nil
		}
		if !isRetryable(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

// CountTokens returns the input token count for req, retrying on transient
// [*llm.ProviderError]s.
func (p *Provider) CountTokens(ctx context.Context, req llm.Request) (int, error) {
	var lastErr error
	for attempt := 0; attempt < p.cfg.maxAttempts(); attempt++ {
		if attempt > 0 {
			if err := p.wait(ctx, lastErr, attempt-1); err != nil {
				return 0, err
			}
		}
		n, err := p.inner.CountTokens(ctx, req)
		if err == nil {
			return n, nil
		}
		if !isRetryable(err) {
			return 0, err
		}
		lastErr = err
	}
	return 0, lastErr
}

// Capabilities returns the capabilities for the given model on this provider's
// endpoint.  Capability lookups are cheap and idempotent; transient errors are
// retried.
func (p *Provider) Capabilities(ctx context.Context, model string) (llm.Capabilities, error) {
	var lastErr error
	for attempt := 0; attempt < p.cfg.maxAttempts(); attempt++ {
		if attempt > 0 {
			if err := p.wait(ctx, lastErr, attempt-1); err != nil {
				return llm.Capabilities{}, err
			}
		}
		caps, err := p.inner.Capabilities(ctx, model)
		if err == nil {
			return caps, nil
		}
		if !isRetryable(err) {
			return llm.Capabilities{}, err
		}
		lastErr = err
	}
	return llm.Capabilities{}, lastErr
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// isRetryable reports whether err should trigger a retry.  Only
// *llm.ProviderError values with Retryable() == true qualify; all other errors
// (including non-ProviderError and non-retryable kinds) are returned
// immediately.
func isRetryable(err error) bool {
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		return false
	}
	return pe.Retryable()
}

// wait blocks until the computed delay elapses, the context is done, or the
// attempt budget is exceeded.  prevErr is the error from the previous attempt
// and is inspected for a RetryAfter hint.  attempt is zero-indexed from 0
// (i.e. the wait BEFORE the second call is attempt=0).
func (p *Provider) wait(ctx context.Context, prevErr error, attempt int) error {
	d := p.delay(prevErr, attempt)
	ch := p.clock.After(d)
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// delay computes the duration to wait before the next attempt.
//
//  1. If prevErr is a *llm.ProviderError with RetryAfter > 0, that value is
//     used directly (provider hint takes priority).
//
//  2. Otherwise, full-jitter exponential backoff:
//
//     cap  = min(MaxDelay, BaseDelay * 2^attempt)
//     wait = rng.Float64() * cap     — uniformly distributed in [0, cap)
func (p *Provider) delay(prevErr error, attempt int) time.Duration {
	// Honor provider-supplied RetryAfter first.
	var pe *llm.ProviderError
	if errors.As(prevErr, &pe) && pe.RetryAfter > 0 {
		return pe.RetryAfter
	}

	// Full-jitter exponential backoff.
	base := p.cfg.BaseDelay
	if base <= 0 {
		base = time.Second
	}
	maxDelay := p.cfg.MaxDelay
	if maxDelay <= 0 {
		maxDelay = 30 * time.Second
	}

	// cap = min(MaxDelay, BaseDelay * 2^attempt)
	// Use a shift; clamp at 62 to avoid int64 overflow.
	shift := attempt
	if shift > 62 {
		shift = 62
	}
	capDuration := time.Duration(int64(base) << shift) //nolint:gosec // shift is bounded to 62
	if capDuration > maxDelay || capDuration <= 0 /* overflow guard */ {
		capDuration = maxDelay
	}

	// wait = rand(0, cap) — full jitter.
	return time.Duration(p.rng.Float64() * float64(capDuration))
}
