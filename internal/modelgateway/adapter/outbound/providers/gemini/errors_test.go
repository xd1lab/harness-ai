package gemini

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/genai"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// TestNormalizeError maps genai.APIError HTTP codes (and transport/context errors)
// onto the normalized *llm.ProviderError kinds the harness retry policy keys on.
func TestNormalizeError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		in       error
		wantKind llm.ErrorKind
	}{
		{"429 rate limited", genai.APIError{Code: 429, Message: "quota"}, llm.ErrRateLimited},
		{"400 invalid", genai.APIError{Code: 400, Message: "bad"}, llm.ErrInvalidRequest},
		{"401 auth", genai.APIError{Code: 401, Message: "key"}, llm.ErrAuth},
		{"403 auth", genai.APIError{Code: 403, Message: "perm"}, llm.ErrAuth},
		{"404 invalid", genai.APIError{Code: 404, Message: "model"}, llm.ErrInvalidRequest},
		{"500 server", genai.APIError{Code: 500, Message: "boom"}, llm.ErrServer},
		{"503 server", genai.APIError{Code: 503, Message: "down"}, llm.ErrServer},
		{"529 overloaded", genai.APIError{Code: 529, Message: "overloaded"}, llm.ErrOverloaded},
		{"wrapped api error", fmt.Errorf("call failed: %w", genai.APIError{Code: 500}), llm.ErrServer},
		{"deadline", context.DeadlineExceeded, llm.ErrTimeout},
		{"unknown -> server", errors.New("some transport glitch"), llm.ErrServer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := normalizeError(tc.in)
			var pe *llm.ProviderError
			if !errors.As(err, &pe) {
				t.Fatalf("normalizeError returned %T, want *llm.ProviderError", err)
			}
			if pe.Kind != tc.wantKind {
				t.Errorf("kind = %q, want %q", pe.Kind, tc.wantKind)
			}
			if pe.Raw == nil {
				t.Errorf("ProviderError.Raw is nil; the original error must be preserved")
			}
		})
	}
}

// TestNormalizeErrorNil asserts a nil error normalizes to nil (no spurious wrapping).
func TestNormalizeErrorNil(t *testing.T) {
	t.Parallel()
	if err := normalizeError(nil); err != nil {
		t.Errorf("normalizeError(nil) = %v, want nil", err)
	}
}

// TestNormalizeErrorContextCanceled maps a cancelled context to ErrTimeout (the
// transport deadline / cancellation kind the retry policy treats as transient).
func TestNormalizeErrorContextCanceled(t *testing.T) {
	t.Parallel()
	err := normalizeError(context.Canceled)
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *llm.ProviderError, got %T", err)
	}
	if pe.Kind != llm.ErrTimeout {
		t.Errorf("kind = %q, want %q", pe.Kind, llm.ErrTimeout)
	}
}
