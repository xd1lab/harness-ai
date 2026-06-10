package anthropic

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// mapError normalizes any error returned by the Anthropic SDK into a
// [*llm.ProviderError] (architecture §4.4; ADR-0004). The mapping keys on the
// HTTP status of an [sdk.Error] (the SDK's API-error type):
//
//   - 429 -> [llm.ErrRateLimited] (Retry-After parsed from the response header)
//   - 529 -> [llm.ErrOverloaded]
//   - 5xx -> [llm.ErrServer]
//   - 401 / 403 -> [llm.ErrAuth]
//   - other 4xx (incl. 400) -> [llm.ErrInvalidRequest]
//
// A context cancellation/deadline or a transport timeout that is not an
// [sdk.Error] maps to [llm.ErrTimeout]; anything else falls back to
// [llm.ErrServer] (retryable) so a transient transport blip is retried rather
// than surfaced as a permanent failure. The original error is always preserved
// in [llm.ProviderError.Raw]. A nil error returns nil.
func mapError(err error) error {
	if err == nil {
		return nil
	}

	// Context deadline/cancellation is classified before unwrapping the SDK
	// error: a deadline is a timeout (retryable), an explicit cancel propagates
	// as a timeout too so the loop's retry policy can decide.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}

	var apiErr *sdk.Error
	if errors.As(err, &apiErr) {
		return mapAPIError(apiErr)
	}

	// A non-API transport error: classify timeouts, otherwise treat as a
	// retryable server-side blip.
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}
	return &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
}

// mapAPIError maps an [sdk.Error] (an Anthropic HTTP API error) by status code.
func mapAPIError(apiErr *sdk.Error) *llm.ProviderError {
	pe := &llm.ProviderError{Raw: apiErr}
	switch {
	case apiErr.StatusCode == http.StatusTooManyRequests: // 429
		pe.Kind = llm.ErrRateLimited
		pe.RetryAfter = retryAfter(apiErr.Response)
	case apiErr.StatusCode == 529: // overloaded
		pe.Kind = llm.ErrOverloaded
		pe.RetryAfter = retryAfter(apiErr.Response)
	case apiErr.StatusCode == http.StatusUnauthorized, apiErr.StatusCode == http.StatusForbidden: // 401/403
		pe.Kind = llm.ErrAuth
	case apiErr.StatusCode >= 500: // 5xx (other than 529)
		pe.Kind = llm.ErrServer
	case apiErr.StatusCode >= 400: // other 4xx incl. 400
		pe.Kind = llm.ErrInvalidRequest
	default:
		// A non-error status reaching here is unexpected; treat as retryable.
		pe.Kind = llm.ErrServer
	}
	return pe
}

// retryAfter extracts the provider's Retry-After hint from an HTTP response.
// Per RFC 7231 the value is either delta-seconds (an integer) or an HTTP-date;
// both forms are honored. A missing, empty, malformed, or non-positive value
// yields zero, signaling the retry policy to rely on its own backoff schedule.
func retryAfter(resp *http.Response) time.Duration {
	if resp == nil {
		return 0
	}
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}
