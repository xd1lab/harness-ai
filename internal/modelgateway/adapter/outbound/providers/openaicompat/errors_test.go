package openaicompat

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// apiErrWithStatus builds a synthetic SDK API error with the given HTTP status and
// optional Retry-After header value.
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
		{http.StatusNotFound, llm.ErrInvalidRequest},
		{http.StatusRequestTimeout, llm.ErrTimeout},
		{http.StatusInternalServerError, llm.ErrServer},
		{http.StatusBadGateway, llm.ErrServer},
		{http.StatusServiceUnavailable, llm.ErrServer},
		{http.StatusGatewayTimeout, llm.ErrTimeout},
	}
	for _, c := range cases {
		err := normalizeError(apiErrWithStatus(c.status, ""))
		assertProviderErrorKind(t, err, c.want)
	}
}

func TestNormalizeError_RetryAfterSeconds(t *testing.T) {
	err := normalizeError(apiErrWithStatus(http.StatusTooManyRequests, "5"))
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *llm.ProviderError, got %T", err)
	}
	if pe.RetryAfter != 5*time.Second {
		t.Fatalf("RetryAfter = %v, want 5s", pe.RetryAfter)
	}
	if !pe.Retryable() {
		t.Fatalf("429 must be retryable")
	}
}

func TestNormalizeError_ContextCanceled_IsTimeout(t *testing.T) {
	assertProviderErrorKind(t, normalizeError(context.Canceled), llm.ErrTimeout)
	assertProviderErrorKind(t, normalizeError(context.DeadlineExceeded), llm.ErrTimeout)
}

func TestNormalizeError_UnknownTransport_IsServer(t *testing.T) {
	assertProviderErrorKind(t, normalizeError(errors.New("connection refused")), llm.ErrServer)
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

// TestKindForStatus_NoHTTPStatus covers the status==0 default branch: a decoded
// API error type hints at a client bug (non-retryable), no type at all is treated
// as a retryable server fault.
func TestKindForStatus_NoHTTPStatus(t *testing.T) {
	if got := kindForStatus(0, ""); got != llm.ErrServer {
		t.Fatalf("kindForStatus(0, \"\") = %q, want %q", got, llm.ErrServer)
	}
	if got := kindForStatus(0, "invalid_request_error"); got != llm.ErrInvalidRequest {
		t.Fatalf("kindForStatus(0, type) = %q, want %q", got, llm.ErrInvalidRequest)
	}
}

func TestNormalizeError_NilIsNil(t *testing.T) {
	if normalizeError(nil) != nil {
		t.Fatalf("normalizeError(nil) must be nil")
	}
}

func TestCountTokens_Unsupported(t *testing.T) {
	p, err := New(Config{BaseURL: "http://localhost:1234/v1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, err = p.CountTokens(context.Background(), llm.Request{Model: "x"})
	assertProviderErrorKind(t, err, llm.ErrUnsupported)
}

func TestNew_RequiresBaseURL(t *testing.T) {
	_, err := New(Config{})
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
}

func TestCapabilities_LMStudioProfile_NoStreamingToolCalls(t *testing.T) {
	p, err := New(Config{BaseURL: "http://localhost:1234/v1", Profile: LMStudioProfile()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caps, err := p.Capabilities(context.Background(), "any-model")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if caps.SupportsStreamingToolCalls {
		t.Fatalf("LM Studio must report SupportsStreamingToolCalls=false")
	}
	if caps.SupportsParallelToolCalls {
		t.Fatalf("LM Studio must report SupportsParallelToolCalls=false")
	}
	if caps.SupportsTokenCounting {
		t.Fatalf("OpenAI-compatible path must report SupportsTokenCounting=false")
	}
	if !caps.SupportsTools || !caps.SupportsSystemPrompt {
		t.Fatalf("LM Studio supports tools + system prompt")
	}
}

func TestCapabilities_GenericDefault(t *testing.T) {
	p, err := New(Config{BaseURL: "http://localhost:11434/v1"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caps, _ := p.Capabilities(context.Background(), "llama3")
	if !caps.SupportsStreamingToolCalls || !caps.SupportsParallelToolCalls {
		t.Fatalf("generic profile should support streaming + parallel tool calls: %#v", caps)
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
