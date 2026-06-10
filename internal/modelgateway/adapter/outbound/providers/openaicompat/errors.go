package openaicompat

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// normalizeError maps any error returned by the OpenAI SDK onto the normalized
// [*llm.ProviderError] the gateway's retry policy keys on. HTTP status codes are
// classified per ADR-0004 / architecture §4.4: 429 → rate-limited (honoring
// Retry-After), 401/403 → auth, other 4xx → invalid request, 5xx → server, and
// transport timeouts/cancellations → timeout. The original SDK error is preserved
// in [llm.ProviderError.Raw].
//
// It returns nil for a nil input so callers can wrap unconditionally.
func normalizeError(err error) error {
	if err == nil {
		return nil
	}

	// Context cancellation/deadline surfaces as a timeout-kind provider error so
	// the loop treats it as transient/abortable rather than a client bug.
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}

	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		return &llm.ProviderError{
			Kind:       kindForStatus(apiErr.StatusCode, apiErr.Type),
			RetryAfter: retryAfter(apiErr),
			Raw:        err,
		}
	}

	// Unclassifiable transport error: treat as a retryable server error so a
	// flaky connection to a self-hosted endpoint does not immediately fail the
	// turn, while still surfacing the cause in Raw.
	return &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
}

// kindForStatus classifies an HTTP status code (and optional provider error type)
// into a normalized [llm.ErrorKind].
func kindForStatus(status int, errType string) llm.ErrorKind {
	switch {
	case status == http.StatusTooManyRequests:
		return llm.ErrRateLimited
	case status == http.StatusUnauthorized, status == http.StatusForbidden:
		return llm.ErrAuth
	case status == http.StatusRequestTimeout, status == http.StatusGatewayTimeout:
		return llm.ErrTimeout
	case status >= 500:
		return llm.ErrServer
	case status >= 400:
		return llm.ErrInvalidRequest
	default:
		// No HTTP status (status == 0) but an API error type was decoded; default
		// to a non-retryable invalid-request unless the type hints otherwise.
		if errType == "" {
			return llm.ErrServer
		}
		return llm.ErrInvalidRequest
	}
}

// retryAfter extracts a Retry-After hint from the API error's response headers, if
// present. It supports the delta-seconds form; an absent or unparseable header
// yields zero (the retry policy then relies on its own backoff schedule).
func retryAfter(apiErr *openai.Error) time.Duration {
	if apiErr == nil || apiErr.Response == nil {
		return 0
	}
	v := apiErr.Response.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

// newInvalidRequest wraps a local request-construction failure as a non-retryable
// [llm.ErrInvalidRequest] provider error.
func newInvalidRequest(err error) error {
	return &llm.ProviderError{Kind: llm.ErrInvalidRequest, Raw: err}
}

// newUnsupported builds an [llm.ErrUnsupported] provider error for capability-gated
// operations (e.g. token counting on an endpoint without a tokenizer).
func newUnsupported(err error) error {
	return &llm.ProviderError{Kind: llm.ErrUnsupported, Raw: err}
}
