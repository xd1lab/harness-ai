package anthropic

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	sdk "github.com/anthropics/anthropic-sdk-go"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// newAPIError builds a synthetic *sdk.Error with the given status and optional
// Retry-After header, populating Request/Response so the SDK error's own
// Error()/methods are safe to call.
func newAPIError(status int, retryAfter string) *sdk.Error {
	hdr := http.Header{}
	if retryAfter != "" {
		hdr.Set("Retry-After", retryAfter)
	}
	req, _ := http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages", nil)
	return &sdk.Error{
		StatusCode: status,
		Request:    req,
		Response:   &http.Response{StatusCode: status, Header: hdr},
	}
}

// TestMapError_StatusCodes is the error-classification table (architecture §4.4):
// each HTTP status maps to the correct normalized kind and retryability.
func TestMapError_StatusCodes(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantKind  llm.ErrorKind
		wantRetry bool
	}{
		{"rate_limited_429", 429, llm.ErrRateLimited, true},
		{"overloaded_529", 529, llm.ErrOverloaded, true},
		{"server_500", 500, llm.ErrServer, true},
		{"server_502", 502, llm.ErrServer, true},
		{"server_503", 503, llm.ErrServer, true},
		{"server_504", 504, llm.ErrServer, true},
		{"auth_401", 401, llm.ErrAuth, false},
		{"auth_403", 403, llm.ErrAuth, false},
		{"invalid_400", 400, llm.ErrInvalidRequest, false},
		{"invalid_404", 404, llm.ErrInvalidRequest, false},
		{"invalid_422", 422, llm.ErrInvalidRequest, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mapError(newAPIError(tc.status, ""))
			var pe *llm.ProviderError
			if !errors.As(err, &pe) {
				t.Fatalf("mapError did not return *llm.ProviderError, got %T", err)
			}
			if pe.Kind != tc.wantKind {
				t.Errorf("kind = %q, want %q", pe.Kind, tc.wantKind)
			}
			if pe.Retryable() != tc.wantRetry {
				t.Errorf("Retryable() = %v, want %v", pe.Retryable(), tc.wantRetry)
			}
			if pe.Raw == nil {
				t.Error("Raw should preserve the underlying provider error")
			}
		})
	}
}

// TestMapError_RetryAfterSeconds asserts an integer Retry-After on a 429 is
// parsed into RetryAfter as a duration.
func TestMapError_RetryAfterSeconds(t *testing.T) {
	err := mapError(newAPIError(429, "5"))
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("not a ProviderError: %T", err)
	}
	if pe.Kind != llm.ErrRateLimited {
		t.Fatalf("kind = %q, want rate_limited", pe.Kind)
	}
	if pe.RetryAfter != 5*time.Second {
		t.Errorf("RetryAfter = %v, want 5s", pe.RetryAfter)
	}
}

// TestMapError_RetryAfterHTTPDate asserts an HTTP-date Retry-After yields a
// positive duration.
func TestMapError_RetryAfterHTTPDate(t *testing.T) {
	future := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	err := mapError(newAPIError(429, future))
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("not a ProviderError: %T", err)
	}
	if pe.RetryAfter <= 0 {
		t.Errorf("RetryAfter = %v, want > 0 for a future HTTP-date", pe.RetryAfter)
	}
}

// TestMapError_RetryAfterAbsentOrBad asserts a missing/garbage/past Retry-After
// yields zero (rely on backoff schedule).
func TestMapError_RetryAfterAbsentOrBad(t *testing.T) {
	for _, v := range []string{"", "not-a-number", "0", "-3"} {
		err := mapError(newAPIError(429, v))
		var pe *llm.ProviderError
		if !errors.As(err, &pe) {
			t.Fatalf("not a ProviderError: %T", err)
		}
		if pe.RetryAfter != 0 {
			t.Errorf("Retry-After %q -> RetryAfter %v, want 0", v, pe.RetryAfter)
		}
	}
}

// TestMapError_Timeout asserts context deadline/cancellation map to ErrTimeout.
func TestMapError_Timeout(t *testing.T) {
	for _, base := range []error{context.DeadlineExceeded, context.Canceled} {
		err := mapError(base)
		var pe *llm.ProviderError
		if !errors.As(err, &pe) {
			t.Fatalf("not a ProviderError: %T", err)
		}
		if pe.Kind != llm.ErrTimeout {
			t.Errorf("%v -> kind %q, want timeout", base, pe.Kind)
		}
	}
}

// TestMapError_Nil asserts a nil error maps to nil.
func TestMapError_Nil(t *testing.T) {
	if got := mapError(nil); got != nil {
		t.Errorf("mapError(nil) = %v, want nil", got)
	}
}

// TestMapError_UnknownTransport asserts a plain non-API error is treated as a
// retryable server-side failure (never silently dropped, never misclassified as
// a permanent error).
func TestMapError_UnknownTransport(t *testing.T) {
	err := mapError(errors.New("connection reset by peer"))
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("not a ProviderError: %T", err)
	}
	if pe.Kind != llm.ErrServer {
		t.Errorf("kind = %q, want server (retryable)", pe.Kind)
	}
}
