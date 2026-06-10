package llm

import (
	"fmt"
	"time"
)

// ErrorKind is the normalized classification of a provider failure. Adapters map
// each provider's error surface (HTTP status, error type/code) onto one of these
// kinds so the single harness-level retry policy can key on the kind without
// provider-specific branching (ADR-0004; architecture §4.4).
type ErrorKind string

const (
	// ErrRateLimited indicates the request was rate limited (e.g. HTTP 429).
	// [ProviderError.RetryAfter] carries the provider's hint when present;
	// retry honors it, then applies exponential backoff with full jitter.
	ErrRateLimited ErrorKind = "rate_limited"
	// ErrInvalidRequest indicates a client/request error (e.g. HTTP 4xx other
	// than auth/rate-limit). It is NOT retryable.
	ErrInvalidRequest ErrorKind = "invalid_request"
	// ErrAuth indicates an authentication or authorization failure (e.g. HTTP
	// 401/403). It is NOT retryable.
	ErrAuth ErrorKind = "auth"
	// ErrOverloaded indicates the provider is temporarily overloaded (e.g. HTTP
	// 529). It is retryable with backoff.
	ErrOverloaded ErrorKind = "overloaded"
	// ErrServer indicates a provider-side server error (e.g. HTTP 5xx). It is
	// retryable with backoff.
	ErrServer ErrorKind = "server"
	// ErrTimeout indicates the request timed out (transport deadline or
	// provider-side timeout). It is retryable with backoff.
	ErrTimeout ErrorKind = "timeout"
	// ErrUnsupported indicates the requested operation or feature is not
	// supported for this (endpoint, model) — for example [Provider.CountTokens]
	// when [Capabilities.SupportsTokenCounting] is false. It is NOT retryable.
	ErrUnsupported ErrorKind = "unsupported"
)

// ProviderError is the single normalized error type that every [Provider] method
// returns on failure. The orchestrator and the harness-level retry policy inspect
// only the normalized Kind (and RetryAfter); the original provider error is
// preserved in Raw for logging and diagnostics (ADR-0004; ADR-0016).
//
// Use [errors.As] to recover a *ProviderError from a returned error.
type ProviderError struct {
	// Kind is the normalized classification of the failure.
	Kind ErrorKind
	// RetryAfter is the provider-suggested minimum wait before retrying, when the
	// provider supplied one (typically with [ErrRateLimited] or [ErrOverloaded]).
	// Zero means no hint was given; the retry policy then relies on its backoff
	// schedule alone.
	RetryAfter time.Duration
	// Raw is the underlying provider error, preserved verbatim for logging and
	// debugging. It may be nil.
	Raw error
}

// Error implements the error interface, summarizing the normalized kind and the
// wrapped provider error.
func (e *ProviderError) Error() string {
	if e.Raw != nil {
		return fmt.Sprintf("llm: %s: %v", e.Kind, e.Raw)
	}
	return fmt.Sprintf("llm: %s", e.Kind)
}

// Unwrap returns the wrapped provider error so [errors.Is] / [errors.As] can
// inspect the cause.
func (e *ProviderError) Unwrap() error { return e.Raw }

// Retryable reports whether a failure of this kind is eligible for the
// harness-level retry policy. Only transient kinds ([ErrRateLimited],
// [ErrOverloaded], [ErrServer], [ErrTimeout]) are retryable; client, auth, and
// unsupported errors are not.
func (e *ProviderError) Retryable() bool {
	switch e.Kind {
	case ErrRateLimited, ErrOverloaded, ErrServer, ErrTimeout:
		return true
	default:
		return false
	}
}
