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
