package openai

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

func apiErrWithStatus(status int, retryAfter string) *openai.Error {
	e := &openai.Error{StatusCode: status}
	if retryAfter != "" {
		e.Response = &http.Response{Header: http.Header{"Retry-After": []string{retryAfter}}}
	}
	return e
}

func TestNormalizeError_StatusTable(t *testing.T) {
	cases := []struct {
		status int
		want   llm.ErrorKind
	}{
		{http.StatusTooManyRequests, llm.ErrRateLimited},
		{http.StatusUnauthorized, llm.ErrAuth},
		{http.StatusForbidden, llm.ErrAuth},
		{http.StatusBadRequest, llm.ErrInvalidRequest},
		{http.StatusInternalServerError, llm.ErrServer},
		{http.StatusBadGateway, llm.ErrServer},
		{http.StatusServiceUnavailable, llm.ErrServer},
		{http.StatusGatewayTimeout, llm.ErrTimeout},
	}
	for _, c := range cases {
		assertProviderErrorKind(t, normalizeError(apiErrWithStatus(c.status, "")), c.want)
	}
}

func TestNormalizeError_RetryAfter(t *testing.T) {
	err := normalizeError(apiErrWithStatus(http.StatusTooManyRequests, "7"))
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *llm.ProviderError, got %T", err)
	}
	if pe.RetryAfter != 7*time.Second {
		t.Fatalf("RetryAfter = %v, want 7s", pe.RetryAfter)
	}
}

func TestNormalizeError_ContextCanceled_IsTimeout(t *testing.T) {
	assertProviderErrorKind(t, normalizeError(context.Canceled), llm.ErrTimeout)
	assertProviderErrorKind(t, normalizeError(context.DeadlineExceeded), llm.ErrTimeout)
}

func TestNormalizeError_NoStatus_IsServer(t *testing.T) {
	// An API error decoded without an HTTP status (status 0) falls through to the
	// retryable server default.
	assertProviderErrorKind(t, normalizeError(&openai.Error{}), llm.ErrServer)
}

// TestRetryAfter_HeaderForms covers the full Retry-After parsing surface:
// delta-seconds, the HTTP-date form, and the defensive zero paths (missing error,
// missing response, absent/negative/garbage header values).
func TestRetryAfter_HeaderForms(t *testing.T) {
	if got := retryAfter(nil); got != 0 {
		t.Fatalf("retryAfter(nil) = %v, want 0", got)
	}
	if got := retryAfter(&openai.Error{StatusCode: http.StatusTooManyRequests}); got != 0 {
		t.Fatalf("retryAfter without Response = %v, want 0", got)
	}
	cases := []struct {
		name     string
		header   string
		want     time.Duration
		positive bool // assert > 0 instead of an exact value (HTTP-date depends on the wall clock)
	}{
		{"delta seconds", "5", 5 * time.Second, false},
		{"zero seconds", "0", 0, false},
		{"negative seconds rejected", "-3", 0, false},
		{"garbage", "soon-ish", 0, false},
		{"future http-date", "Mon, 01 Jan 2999 00:00:00 GMT", 0, true},
		{"past http-date", "Mon, 02 Jan 2006 15:04:05 GMT", 0, false},
	}
	for _, c := range cases {
		got := retryAfter(apiErrWithStatus(http.StatusTooManyRequests, c.header))
		if c.positive {
			if got <= 0 {
				t.Errorf("%s: retryAfter = %v, want > 0", c.name, got)
			}
			continue
		}
		if got != c.want {
			t.Errorf("%s: retryAfter = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestNormalizeError_NilIsNil(t *testing.T) {
	if normalizeError(nil) != nil {
		t.Fatalf("normalizeError(nil) must be nil")
	}
}

func TestCountTokens_Unsupported(t *testing.T) {
	p, err := New(Config{APIKey: "sk-test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = p.CountTokens(context.Background(), llm.Request{Model: "gpt-5"})
	assertProviderErrorKind(t, err, llm.ErrUnsupported)
}

func TestCapabilities_DefaultTokenCountingOff(t *testing.T) {
	p, err := New(Config{APIKey: "sk-test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caps, err := p.Capabilities(context.Background(), "gpt-5")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.SupportsTokenCounting {
		t.Fatalf("OpenAI Responses default must report SupportsTokenCounting=false")
	}
	if !caps.SupportsTools || !caps.SupportsSystemPrompt {
		t.Fatalf("default caps should support tools + system prompt: %#v", caps)
	}
}

func TestNew_ChatCompletionsModeBuildsDelegate(t *testing.T) {
	p, err := New(Config{APIKey: "sk-test", UseChatCompletions: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.chatImpl == nil {
		t.Fatalf("Chat-Completions mode must build the shared openaicompat delegate")
	}
}

func TestNew_ResponsesModeNoDelegate(t *testing.T) {
	p, err := New(Config{APIKey: "sk-test"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.chatImpl != nil {
		t.Fatalf("Responses mode must not build a Chat delegate")
	}
}

// assertProviderErrorKind asserts err is a *llm.ProviderError of the wanted kind.
func assertProviderErrorKind(t *testing.T, err error, want llm.ErrorKind) {
	t.Helper()
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *llm.ProviderError, got %T (%v)", err, err)
	}
	if pe.Kind != want {
		t.Fatalf("ProviderError.Kind = %q, want %q", pe.Kind, want)
	}
}
