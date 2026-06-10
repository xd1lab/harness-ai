package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3/option"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// These tests drive Generate/Stream through a real HTTP round-trip against an
// httptest server replaying canned Responses bodies, so the SDK transport, the SSE
// decoder, and the responsesStreamReader adapter are exercised together without
// any network or live endpoint.

// respCompletedJSON is a terminal response.completed event carrying the
// authoritative output (a message and a function_call) and usage, as the Responses
// API delivers it.
const respCompletedJSON = `{"type":"response.completed","sequence_number":9,"response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-test","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Hello world","annotations":[]}]},{"type":"function_call","id":"fc_1","call_id":"call_1","name":"get_weather","arguments":"{\"city\":\"Paris\"}","status":"completed"}],"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":4},"output_tokens":6,"output_tokens_details":{"reasoning_tokens":2},"total_tokens":16}}}`

// sseEvents renders typed Responses SSE events (event: line + data: line) into one
// body. The Responses stream ends at EOF with no [DONE] sentinel.
func sseEvents(events ...[2]string) string {
	var b strings.Builder
	for _, ev := range events {
		b.WriteString("event: ")
		b.WriteString(ev[0])
		b.WriteString("\ndata: ")
		b.WriteString(ev[1])
		b.WriteString("\n\n")
	}
	return b.String()
}

// newResponsesServer starts an httptest server replying to every request with the
// given body under the given content type. When gotBody is non-nil the decoded
// JSON request body is stored there for wire-shape assertions.
func newResponsesServer(t *testing.T, contentType, body string, gotBody *map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotBody != nil {
			b, err := io.ReadAll(r.Body)
			if err == nil {
				_ = json.Unmarshal(b, gotBody)
			}
		}
		w.Header().Set("Content-Type", contentType)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newResponsesProvider builds a Responses-surface Provider against the test server
// with SDK retries disabled, so error-path tests observe the first response
// deterministically instead of the SDK's backoff schedule.
func newResponsesProvider(t *testing.T, baseURL string) *Provider {
	t.Helper()
	p, err := New(Config{
		APIKey:  "sk-test",
		BaseURL: baseURL,
		Options: []option.RequestOption{option.WithMaxRetries(0)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

// userRequest is a minimal valid request for the fake endpoint.
func userRequest() llm.Request {
	return llm.Request{
		Model: "gpt-test",
		Messages: []llm.Message{
			{Role: llm.RoleUser, Content: []llm.ContentPart{{Text: &llm.TextPart{Text: "hi"}}}},
		},
	}
}

// recvAll drains the reader until io.EOF, failing the test on any other error.
func recvAll(t *testing.T, r llm.StreamReader) []llm.StreamEvent {
	t.Helper()
	var out []llm.StreamEvent
	for {
		ev, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		out = append(out, ev)
	}
}

// TestStream_EndToEnd_TextToolCallUsage replays the canonical Responses stream —
// lifecycle noise, split text deltas, item-scoped argument deltas, and the
// authoritative response.completed terminal — and asserts the reader emits the
// normalized sequence ending in a single Done, then sticky io.EOF.
func TestStream_EndToEnd_TextToolCallUsage(t *testing.T) {
	body := sseEvents(
		// Lifecycle and argument-delta events must pass through silently; the
		// authoritative tool call comes from the terminal Response.
		[2]string{"response.created", `{"type":"response.created","sequence_number":1,"response":{"id":"resp_1","object":"response","status":"in_progress","model":"gpt-test","output":[]}}`},
		[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","sequence_number":2,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"Hello "}`},
		[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","sequence_number":3,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"world"}`},
		[2]string{"response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","sequence_number":4,"item_id":"fc_1","output_index":1,"delta":"{\"city\":"}`},
		[2]string{"response.completed", respCompletedJSON},
	)
	var gotBody map[string]any
	srv := newResponsesServer(t, "text/event-stream", body, &gotBody)
	p := newResponsesProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	got := recvAll(t, reader)
	if len(got) != 4 {
		t.Fatalf("want 4 events (2 text + tool call + Done), got %d: %s", len(got), dump(got))
	}
	if got[0].TextDelta == nil || got[0].TextDelta.Text != "Hello " {
		t.Fatalf("event 0 wrong: %s", dump(got[:1]))
	}
	if got[1].TextDelta == nil || got[1].TextDelta.Text != "world" {
		t.Fatalf("event 1 wrong: %s", dump(got[1:2]))
	}
	tc := got[2].ToolCallDelta
	if tc == nil || tc.CallID != "call_1" || tc.Name != "get_weather" {
		t.Fatalf("event 2 must be the complete tool call, got %s", dump(got[2:3]))
	}
	var args map[string]any
	if err := json.Unmarshal(tc.ArgsFragment, &args); err != nil || args["city"] != "Paris" {
		t.Fatalf("tool call args wrong: %q (%v)", string(tc.ArgsFragment), err)
	}
	done := got[3].Done
	if done == nil || done.StopReason != llm.StopToolUse || done.RawStopReason != "completed" {
		t.Fatalf("terminal event wrong: %s", dump(got[3:4]))
	}
	wantUsage := llm.Usage{InputTokens: 6, OutputTokens: 6, CacheReadTokens: 4, ReasoningTokens: 2}
	if done.Usage != wantUsage {
		t.Fatalf("usage wrong:\n got %#v\nwant %#v", done.Usage, wantUsage)
	}

	// Stateless Item-passing: the streaming request must also pin store=false.
	if gotBody["store"] != false {
		t.Fatalf("request body must set store=false, got %v", gotBody["store"])
	}

	// EOF is sticky and Close is idempotent.
	if _, err := reader.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after exhaustion must keep returning io.EOF, got %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close must be safe: %v", err)
	}
}

// TestStream_MalformedEvent asserts an undecodable SSE payload is normalized to a
// retryable server-kind ProviderError that stays sticky on subsequent Recv calls.
func TestStream_MalformedEvent(t *testing.T) {
	body := sseEvents(
		[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","delta":"Hi"}`},
		[2]string{"response.output_text.delta", `{"type":"response.output_`},
	)
	srv := newResponsesServer(t, "text/event-stream", body, nil)
	p := newResponsesProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if ev, err := reader.Recv(); err != nil || ev.TextDelta == nil || ev.TextDelta.Text != "Hi" {
		t.Fatalf("first Recv must deliver the text delta, got (%#v, %v)", ev, err)
	}
	_, err = reader.Recv()
	assertProviderErrorKind(t, err, llm.ErrServer)
	_, err = reader.Recv()
	assertProviderErrorKind(t, err, llm.ErrServer) // terminal error is sticky
}

// TestStream_TruncatedWithoutTerminal documents the Responses-surface contract for
// a stream that ends cleanly before any terminal event: Recv returns io.EOF with
// NO Done — unlike the chat surface there is no synthesized terminal, because only
// the response.completed/incomplete/failed payload is authoritative.
func TestStream_TruncatedWithoutTerminal(t *testing.T) {
	body := sseEvents(
		[2]string{"response.output_text.delta", `{"type":"response.output_text.delta","delta":"partial"}`},
	)
	srv := newResponsesServer(t, "text/event-stream", body, nil)
	p := newResponsesProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	got := recvAll(t, reader)
	if len(got) != 1 || got[0].TextDelta == nil {
		t.Fatalf("want only the text delta, got %s", dump(got))
	}
	for _, ev := range got {
		if ev.Done != nil {
			t.Fatalf("truncated Responses stream must not synthesize a Done: %s", dump(got))
		}
	}
}

// TestStream_HTTPError asserts a non-2xx handshake is classified before any reader
// is returned (here 401 -> auth, a non-retryable kind).
func TestStream_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"bad key","type":"invalid_api_key"}}`)
	}))
	t.Cleanup(srv.Close)
	p := newResponsesProvider(t, srv.URL)

	_, err := p.Stream(context.Background(), userRequest())
	assertProviderErrorKind(t, err, llm.ErrAuth)
}

// TestGenerate_EndToEnd drives the non-streaming Responses path through HTTP and
// asserts the assembled normalized response plus the stateless wire shape.
func TestGenerate_EndToEnd(t *testing.T) {
	// The unary endpoint returns the bare Response object (the "response" payload
	// of the terminal stream event).
	respJSON := `{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-test","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Hello world","annotations":[]}]},{"type":"function_call","id":"fc_1","call_id":"call_1","name":"get_weather","arguments":"{\"city\":\"Paris\"}","status":"completed"}],"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":4},"output_tokens":6,"output_tokens_details":{"reasoning_tokens":2},"total_tokens":16}}`
	var gotPath string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, err := io.ReadAll(r.Body)
		if err == nil {
			_ = json.Unmarshal(b, &gotBody)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, respJSON)
	}))
	t.Cleanup(srv.Close)
	p := newResponsesProvider(t, srv.URL)

	resp, err := p.Generate(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if gotPath != "/responses" {
		t.Fatalf("request path = %q, want /responses", gotPath)
	}
	// Stateless Item-passing: never persist server-side conversation state.
	if gotBody["store"] != false {
		t.Fatalf("request body must set store=false, got %v", gotBody["store"])
	}
	if len(resp.Content) != 2 {
		t.Fatalf("want text + tool call, got %d parts: %#v", len(resp.Content), resp.Content)
	}
	if resp.Content[0].Text == nil || resp.Content[0].Text.Text != "Hello world" {
		t.Fatalf("first part must be the text, got %#v", resp.Content[0])
	}
	tc := resp.Content[1].ToolCall
	if tc == nil || tc.ID != "call_1" || tc.Name != "get_weather" || tc.Args["city"] != "Paris" {
		t.Fatalf("second part must be the parsed tool call, got %#v", resp.Content[1])
	}
	if resp.StopReason != llm.StopToolUse || resp.RawStopReason != "completed" {
		t.Fatalf("stop reason wrong: %q (%q)", resp.StopReason, resp.RawStopReason)
	}
	wantUsage := llm.Usage{InputTokens: 6, OutputTokens: 6, CacheReadTokens: 4, ReasoningTokens: 2}
	if resp.Usage != wantUsage {
		t.Fatalf("usage wrong:\n got %#v\nwant %#v", resp.Usage, wantUsage)
	}
	var st continuationState
	if err := json.Unmarshal(resp.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if st.Surface != surfaceResponses || len(st.Items) != 2 {
		t.Fatalf("continuation blob wrong: %#v", st)
	}
}

// TestGenerate_HTTPError_RetryAfterHonored asserts a 429 maps to rate-limited with
// the Retry-After hint carried through from the real response headers.
func TestGenerate_HTTPError_RetryAfterHonored(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"slow down","type":"rate_limit_error"}}`)
	}))
	t.Cleanup(srv.Close)
	p := newResponsesProvider(t, srv.URL)

	_, err := p.Generate(context.Background(), userRequest())
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("want *llm.ProviderError, got %T (%v)", err, err)
	}
	if pe.Kind != llm.ErrRateLimited {
		t.Fatalf("Kind = %q, want %q", pe.Kind, llm.ErrRateLimited)
	}
	if pe.RetryAfter != 3*time.Second {
		t.Fatalf("RetryAfter = %v, want 3s", pe.RetryAfter)
	}
}

// TestResponsesStreamReader_CloseWithoutStream covers the defensive nil-stream
// guard: Close on a zero reader must be a no-op, not a panic.
func TestResponsesStreamReader_CloseWithoutStream(t *testing.T) {
	r := &responsesStreamReader{}
	if err := r.Close(); err != nil {
		t.Fatalf("Close on nil stream must return nil, got %v", err)
	}
}

// TestChatCompletionsMode_DelegatesBothPaths asserts the UseChatCompletions
// sub-flag routes Generate AND Stream through the shared openaicompat adapter:
// requests hit /chat/completions (not /responses) and come back normalized.
func TestChatCompletionsMode_DelegatesBothPaths(t *testing.T) {
	var gotPaths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPaths = append(gotPaths, r.URL.Path)
		var body map[string]any
		b, err := io.ReadAll(r.Body)
		if err == nil {
			_ = json.Unmarshal(b, &body)
		}
		if body["stream"] == true {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"pong\"},\"finish_reason\":null}]}\n\ndata: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"gpt-test\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"gpt-test","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	}))
	t.Cleanup(srv.Close)

	p, err := New(Config{
		APIKey:             "sk-test",
		BaseURL:            srv.URL,
		UseChatCompletions: true,
		Options:            []option.RequestOption{option.WithMaxRetries(0)},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	resp, err := p.Generate(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if len(resp.Content) != 1 || resp.Content[0].Text == nil || resp.Content[0].Text.Text != "pong" {
		t.Fatalf("delegated Generate content wrong: %#v", resp.Content)
	}

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()
	got := recvAll(t, reader)
	if len(got) != 2 || got[0].TextDelta == nil || got[1].Done == nil || got[1].Done.StopReason != llm.StopEnd {
		t.Fatalf("delegated Stream events wrong: %s", dump(got))
	}

	for _, path := range gotPaths {
		if path != "/chat/completions" {
			t.Fatalf("Chat-Completions mode must hit /chat/completions, got %v", gotPaths)
		}
	}
}

// TestBadRequest_FailsBeforeHTTP asserts a request-construction failure (bad tool
// schema) is rejected locally as invalid_request on both paths without ever
// reaching the endpoint — local bugs must not consume provider quota or retries.
func TestBadRequest_FailsBeforeHTTP(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	t.Cleanup(srv.Close)
	p := newResponsesProvider(t, srv.URL)

	bad := userRequest()
	bad.Tools = []llm.ToolDef{{Name: "bad", JSONSchema: json.RawMessage(`{nope`)}}

	_, err := p.Generate(context.Background(), bad)
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
	_, err = p.Stream(context.Background(), bad)
	assertProviderErrorKind(t, err, llm.ErrInvalidRequest)
	if called {
		t.Fatalf("invalid request must be rejected before any HTTP call")
	}
}
