package openaicompat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// httptest server replaying canned Chat Completions bodies, so the SDK transport,
// the SSE decoder, and the chatStreamReader adapter are exercised together without
// any network or live endpoint.

// sseData renders the given JSON payloads as a data-only SSE body, one event per
// payload, exactly as Chat Completions emits them.
func sseData(payloads ...string) string {
	var b strings.Builder
	for _, p := range payloads {
		b.WriteString("data: ")
		b.WriteString(p)
		b.WriteString("\n\n")
	}
	return b.String()
}

// newSSEServer starts an httptest server replying to every request with the given
// pre-rendered SSE body. When gotBody is non-nil the decoded JSON request body is
// stored there so tests can assert the wire shape the adapter produced.
func newSSEServer(t *testing.T, body string, gotBody *map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotBody != nil {
			b, err := io.ReadAll(r.Body)
			if err == nil {
				_ = json.Unmarshal(b, gotBody)
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// newCompatProvider builds a Provider against the test server with SDK retries
// disabled, so error-path tests observe the first response deterministically
// instead of the SDK's backoff schedule.
func newCompatProvider(t *testing.T, baseURL string) *Provider {
	t.Helper()
	p, err := New(Config{
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
		Model: "m-test",
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

// TestStream_EndToEnd_TextToolCallUsage replays the canonical streamed shape —
// split text deltas, a tool call fragmented across chunks, a finish_reason chunk,
// the usage-only trailer, and [DONE] — and asserts the reader emits the normalized
// sequence ending in a single Done, then sticky io.EOF.
func TestStream_EndToEnd_TextToolCallUsage(t *testing.T) {
	body := sseData(
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":"{\"ci"}}]},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"ty\":\"Paris\"}"}}]},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[],"usage":{"prompt_tokens":100,"completion_tokens":40,"total_tokens":140,"prompt_tokens_details":{"cached_tokens":30},"completion_tokens_details":{"reasoning_tokens":12}}}`,
		`[DONE]`,
	)
	var gotBody map[string]any
	srv := newSSEServer(t, body, &gotBody)
	p := newCompatProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	got := recvAll(t, reader)
	if len(got) != 4 {
		t.Fatalf("want 4 events (2 text + tool call + Done), got %d: %s", len(got), dump(got))
	}
	if got[0].TextDelta == nil || got[0].TextDelta.Text != "Hel" {
		t.Fatalf("event 0 wrong: %s", dump(got[:1]))
	}
	if got[1].TextDelta == nil || got[1].TextDelta.Text != "lo" {
		t.Fatalf("event 1 wrong: %s", dump(got[1:2]))
	}
	tc := got[2].ToolCallDelta
	if tc == nil || tc.CallID != "call_1" || tc.Name != "get_weather" {
		t.Fatalf("event 2 must be the assembled tool call, got %s", dump(got[2:3]))
	}
	var args map[string]any
	if err := json.Unmarshal(tc.ArgsFragment, &args); err != nil || args["city"] != "Paris" {
		t.Fatalf("assembled args wrong: %q (%v)", string(tc.ArgsFragment), err)
	}
	done := got[3].Done
	if done == nil || done.StopReason != llm.StopToolUse || done.RawStopReason != "tool_calls" {
		t.Fatalf("terminal event wrong: %s", dump(got[3:4]))
	}
	wantUsage := llm.Usage{InputTokens: 70, OutputTokens: 40, CacheReadTokens: 30, ReasoningTokens: 12}
	if done.Usage != wantUsage {
		t.Fatalf("usage wrong:\n got %#v\nwant %#v", done.Usage, wantUsage)
	}

	// The adapter must request streaming with usage on the trailing chunk.
	if gotBody["stream"] != true {
		t.Fatalf("request body must set stream=true, got %v", gotBody["stream"])
	}
	so, ok := gotBody["stream_options"].(map[string]any)
	if !ok || so["include_usage"] != true {
		t.Fatalf("request body must set stream_options.include_usage=true, got %#v", gotBody["stream_options"])
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

// TestStream_TruncatedStream_SynthesizesDone models a server that drops the stream
// cleanly without a finish_reason or [DONE] (e.g. an aborted self-hosted server):
// the chat surface must still flush its terminal tail so the orchestrator sees
// exactly one Done rather than a bare EOF.
func TestStream_TruncatedStream_SynthesizesDone(t *testing.T) {
	body := sseData(
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`,
	)
	srv := newSSEServer(t, body, nil)
	p := newCompatProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	got := recvAll(t, reader)
	if len(got) != 2 || got[0].TextDelta == nil || got[1].Done == nil {
		t.Fatalf("want [TextDelta Done], got %s", dump(got))
	}
	// No finish_reason was reported; the normalizer treats that as a normal end.
	if got[1].Done.StopReason != llm.StopEnd || got[1].Done.RawStopReason != "" {
		t.Fatalf("truncated stream Done wrong: %#v", got[1].Done)
	}
}

// TestStream_MidStreamErrorEvent asserts an in-band error event (the {"error":...}
// payload some servers emit mid-stream) surfaces from Recv as a retryable
// server-kind ProviderError, and that the error is sticky on subsequent calls.
func TestStream_MidStreamErrorEvent(t *testing.T) {
	body := sseData(
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`,
		`{"error":{"message":"backend exploded","type":"server_error"}}`,
	)
	srv := newSSEServer(t, body, nil)
	p := newCompatProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	ev, err := reader.Recv()
	if err != nil || ev.TextDelta == nil || ev.TextDelta.Text != "Hi" {
		t.Fatalf("first Recv must deliver the text delta, got (%#v, %v)", ev, err)
	}
	_, err = reader.Recv()
	assertProviderErrorKind(t, err, llm.ErrServer)
	_, err = reader.Recv()
	assertProviderErrorKind(t, err, llm.ErrServer) // terminal error is sticky
}

// TestStream_MalformedChunk asserts an undecodable SSE payload (truncated JSON) is
// normalized to a server-kind ProviderError rather than panicking or hanging.
func TestStream_MalformedChunk(t *testing.T) {
	body := sseData(
		`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"m-test","choices":[{"index":0,"delta":{"content":"Hi"},"finish_reason":null}]}`,
		`{"id":"c1","object":"chat.completion.chu`,
	)
	srv := newSSEServer(t, body, nil)
	p := newCompatProvider(t, srv.URL)

	reader, err := p.Stream(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = reader.Close() }()

	if ev, err := reader.Recv(); err != nil || ev.TextDelta == nil {
		t.Fatalf("first Recv must deliver the text delta, got (%#v, %v)", ev, err)
	}
	_, err = reader.Recv()
	assertProviderErrorKind(t, err, llm.ErrServer)
}

// TestStream_AbruptConnectionClose hijacks the connection and closes it without
// terminating the chunked body, modeling a crashed server mid-stream. The
// transport error must surface as a retryable server-kind ProviderError.
func TestStream_AbruptConnectionClose(t *testing.T) {
	payload := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m-test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hi\"},\"finish_reason\":null}]}\n\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("test server does not support hijacking")
			return
		}
		conn, buf, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		// One valid chunk, then close without the terminal 0-length chunk so the
		// client observes an unexpected EOF mid-body.
		_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(payload), payload)
		_ = buf.Flush()
	}))
	t.Cleanup(srv.Close)
	p := newCompatProvider(t, srv.URL)

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
	p := newCompatProvider(t, srv.URL)

	_, err := p.Stream(context.Background(), userRequest())
	assertProviderErrorKind(t, err, llm.ErrAuth)
}

// TestGenerate_EndToEnd drives the non-streaming path through HTTP and asserts the
// assembled normalized response: ordered content, mapped stop reason, usage, and
// the stateless continuation blob.
func TestGenerate_EndToEnd(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"m-test","choices":[{"index":0,"message":{"role":"assistant","content":"All done","tool_calls":[{"id":"call_9","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"go\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`)
	}))
	t.Cleanup(srv.Close)
	p := newCompatProvider(t, srv.URL)

	resp, err := p.Generate(context.Background(), userRequest())
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if gotPath != "/chat/completions" {
		t.Fatalf("request path = %q, want /chat/completions", gotPath)
	}
	if len(resp.Content) != 2 {
		t.Fatalf("want text + tool call, got %d parts: %#v", len(resp.Content), resp.Content)
	}
	if resp.Content[0].Text == nil || resp.Content[0].Text.Text != "All done" {
		t.Fatalf("first part must be the text, got %#v", resp.Content[0])
	}
	tc := resp.Content[1].ToolCall
	if tc == nil || tc.ID != "call_9" || tc.Name != "lookup" || tc.Args["q"] != "go" {
		t.Fatalf("second part must be the parsed tool call, got %#v", resp.Content[1])
	}
	if resp.StopReason != llm.StopToolUse || resp.RawStopReason != "tool_calls" {
		t.Fatalf("stop reason wrong: %q (%q)", resp.StopReason, resp.RawStopReason)
	}
	if (resp.Usage != llm.Usage{InputTokens: 10, OutputTokens: 5}) {
		t.Fatalf("usage wrong: %#v", resp.Usage)
	}
	var st continuationState
	if err := json.Unmarshal(resp.ProviderRaw, &st); err != nil {
		t.Fatalf("ProviderRaw must be valid continuationState JSON: %v", err)
	}
	if st.Surface != surfaceChat || st.Model != "m-test" || st.Text != "All done" {
		t.Fatalf("continuation blob wrong: %#v", st)
	}
	if len(st.ToolCalls) != 1 || st.ToolCalls[0].Args != `{"q":"go"}` {
		t.Fatalf("continuation tool calls wrong: %#v", st.ToolCalls)
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
	p := newCompatProvider(t, srv.URL)

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

// TestBadRequest_FailsBeforeHTTP asserts a request-construction failure (bad tool
// schema) is rejected locally as invalid_request on both paths without ever
// reaching the endpoint — local bugs must not consume provider quota or retries.
func TestBadRequest_FailsBeforeHTTP(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}))
	t.Cleanup(srv.Close)
	p := newCompatProvider(t, srv.URL)

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

// TestChatStreamReader_CloseWithoutStream covers the defensive nil-stream guard:
// Close on a zero reader must be a no-op, not a panic.
func TestChatStreamReader_CloseWithoutStream(t *testing.T) {
	r := &chatStreamReader{}
	if err := r.Close(); err != nil {
		t.Fatalf("Close on nil stream must return nil, got %v", err)
	}
}
