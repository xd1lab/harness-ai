package gemini

import (
	"context"
	"errors"
	"net/http"

	"google.golang.org/genai"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// normalizeError maps a google.golang.org/genai failure onto the harness's single
// normalized error type, *llm.ProviderError, so the gateway's retry policy keys on the
// normalized Kind without any provider-specific branching (ADR-0004; architecture §4.4).
//
// The mapping is:
//   - context deadline / cancellation -> ErrTimeout (transient transport kind);
//   - genai.APIError by HTTP status code -> 429 ErrRateLimited, 401/403 ErrAuth,
//     other 4xx ErrInvalidRequest, 529 ErrOverloaded, other 5xx ErrServer;
//   - any other (transport/unknown) error -> ErrServer (retryable), preserving the
//     original error in Raw for diagnostics.
//
// A nil error returns nil. The original error is always preserved in
// [llm.ProviderError.Raw].
func normalizeError(err error) error {
	if err == nil {
		return nil
	}

	// Context errors take precedence: a cancelled or timed-out turn is a transport
	// timeout from the provider's perspective, classified as transient.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}

	// genai returns APIError by value (not pointer); errors.As against a value target
	// recovers it whether returned directly or wrapped with %w.
	var apiErr genai.APIError
	if errors.As(err, &apiErr) {
		return &llm.ProviderError{Kind: kindForStatus(apiErr.Code), Raw: err}
	}

	// Unknown/transport error: treat as a retryable server error rather than guessing
	// a non-retryable kind, so a transient network blip is retried, not surfaced as a
	// hard failure.
	return &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
}

// kindForStatus classifies an HTTP status code into a normalized [llm.ErrorKind].
func kindForStatus(code int) llm.ErrorKind {
	switch code {
	case http.StatusTooManyRequests: // 429
		return llm.ErrRateLimited
	case http.StatusUnauthorized, http.StatusForbidden: // 401, 403
		return llm.ErrAuth
	case statusOverloaded: // 529 (provider-specific overload)
		return llm.ErrOverloaded
	}
	switch {
	case code >= 400 && code < 500:
		return llm.ErrInvalidRequest
	case code >= 500:
		return llm.ErrServer
	default:
		// Codes below 400 should not arrive as errors; classify defensively as server.
		return llm.ErrServer
	}
}

// statusOverloaded is the non-standard 529 "overloaded" status some providers return;
// it is retryable with backoff (mirrors [llm.ErrOverloaded]).
const statusOverloaded = 529
