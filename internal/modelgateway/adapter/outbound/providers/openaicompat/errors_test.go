package openaicompat

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"

	"github.com/boltrope/boltrope/internal/platform/llm"
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
