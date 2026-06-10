package openai

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
// [*llm.ProviderError] the gateway's retry policy keys on, classifying HTTP status
// codes per ADR-0004 / architecture §4.4 and preserving the original error in
// [llm.ProviderError.Raw]. It returns nil for a nil input.
func normalizeError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &llm.ProviderError{Kind: llm.ErrTimeout, Raw: err}
	}

	var apiErr *openai.Error
	if errors.As(err, &apiErr) {
		return &llm.ProviderError{
			Kind:       kindForStatus(apiErr.StatusCode),
			RetryAfter: retryAfter(apiErr),
			Raw:        err,
		}
	}
	return &llm.ProviderError{Kind: llm.ErrServer, Raw: err}
}

// kindForStatus classifies an HTTP status code into a normalized [llm.ErrorKind].
func kindForStatus(status int) llm.ErrorKind {
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
		return llm.ErrServer
	}
}

// retryAfter extracts a Retry-After hint (delta-seconds or HTTP-date) from the API
// error's response headers; absent or unparseable yields zero.
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
// operations.
func newUnsupported(err error) error {
	return &llm.ProviderError{Kind: llm.ErrUnsupported, Raw: err}
}
