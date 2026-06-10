package anthropic

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	sdk "github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// sseStream builds a *ssestream.Stream over a synthetic text/event-stream body,
// mirroring the SDK's own decode path so the streamReader is exercised
// end-to-end with no network. withRequest controls whether the response carries a
// *http.Request (needed for the SDK to construct rich API errors on error events).
func sseStream(body string, withRequest bool) *ssestream.Stream[sdk.MessageStreamEventUnion] {
	resp := &http.Response{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	if withRequest {
		req, _ := http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages", nil)
		resp.Request = req
	}
	return ssestream.NewStream[sdk.MessageStreamEventUnion](ssestream.NewDecoder(resp), nil)
}

// sseEvent formats one SSE frame.
func sseEvent(typ, data string) string {
	return "event: " + typ + "\ndata: " + data + "\n\n"
}

// drain reads a streamReader to completion, returning the events (excluding the
// trailing io.EOF) and the terminating error (nil if it ended on io.EOF).
func drain(t *testing.T, r llm.StreamReader) ([]llm.StreamEvent, error) {
	t.Helper()
	var out []llm.StreamEvent
	for {
		ev, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

// TestStreamReader_EndToEnd drives a full synthetic SSE sequence (including a
// ping that must be skipped) through the streamReader and asserts the normalized
// events and terminal Done.
func TestStreamReader_EndToEnd(t *testing.T) {
	body := strings.Join([]string{
		sseEvent("message_start", `{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":3,"output_tokens":1}}}`),
		sseEvent("ping", `{"type":"ping"}`),
		sseEvent("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`),
		sseEvent("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}`),
		sseEvent("content_block_stop", `{"type":"content_block_stop","index":0}`),
		sseEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"input_tokens":3,"output_tokens":2}}`),
		sseEvent("message_stop", `{"type":"message_stop"}`),
	}, "")

	r := newStreamReader(sseStream(body, true))
	defer func() { _ = r.Close() }()
	events, err := drain(t, r)
	if err != nil {
		t.Fatalf("drain: unexpected error %v", err)
	}

	var text string
	var done *llm.Done
	for _, ev := range events {
		switch {
		case ev.TextDelta != nil:
			text += ev.TextDelta.Text
		case ev.Done != nil:
			done = ev.Done
		}
	}
	if text != "Hi" {
		t.Errorf("text = %q, want Hi", text)
	}
	if done == nil || done.StopReason != llm.StopEnd {
		t.Fatalf("done = %+v, want StopEnd", done)
	}
}

// TestStreamReader_ErrorEvent asserts a mid-stream SSE error event surfaces from
// Recv as a *llm.ProviderError (here a 529 overloaded -> ErrOverloaded).
func TestStreamReader_ErrorEvent(t *testing.T) {
	// The richErrorDecoder uses the response status code for classification, so
	// the synthetic response carries 529.
	resp := &http.Response{
		StatusCode: 529,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
	}
	req, _ := http.NewRequest(http.MethodPost, "https://api.anthropic.com/v1/messages", nil)
	resp.Request = req
	body := sseEvent("message_start", `{"type":"message_start","message":{"id":"m","type":"message","role":"assistant","model":"claude-opus-4-8","content":[],"stop_reason":null,"usage":{"input_tokens":1,"output_tokens":1}}}`) +
		sseEvent("error", `{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}`)
	resp.Body = io.NopCloser(strings.NewReader(body))
	stream := ssestream.NewStream[sdk.MessageStreamEventUnion](ssestream.NewDecoder(resp), nil)

	r := newStreamReader(stream)
	defer func() { _ = r.Close() }()
	_, err := drain(t, r)
	if err == nil {
		t.Fatal("expected an error from the SSE error event")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %T, want *llm.ProviderError", err)
	}
	if pe.Kind != llm.ErrOverloaded {
		t.Errorf("kind = %q, want overloaded", pe.Kind)
	}
}

// TestResponseFromMessage aggregates a synthetic non-streaming Message (text +
// tool_use) into the normalized Response and asserts content, stop reason, usage,
// and the tool-call args, plus the provider-raw continuation blob.
func TestResponseFromMessage(t *testing.T) {
	raw := `{
      "id":"msg_x","type":"message","role":"assistant","model":"claude-opus-4-8",
      "stop_reason":"tool_use",
      "usage":{"input_tokens":12,"output_tokens":7,"cache_read_input_tokens":4,"cache_creation_input_tokens":2,"output_tokens_details":{"thinking_tokens":3}},
      "content":[
        {"type":"text","text":"Let me check."},
        {"type":"tool_use","id":"toolu_z","name":"get_weather","input":{"city":"Paris"}}
      ]
    }`
	var msg sdk.Message
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal message: %v", err)
	}
	resp, err := responseFromMessage(&msg)
	if err != nil {
		t.Fatalf("responseFromMessage: %v", err)
	}
	if resp.StopReason != llm.StopToolUse {
		t.Errorf("stop reason = %q, want tool_use", resp.StopReason)
	}
	if resp.RawStopReason != "tool_use" {
		t.Errorf("raw stop reason = %q, want tool_use", resp.RawStopReason)
	}
	if resp.Usage.InputTokens != 12 || resp.Usage.OutputTokens != 7 ||
		resp.Usage.CacheReadTokens != 4 || resp.Usage.CacheWriteTokens != 2 ||
		resp.Usage.ReasoningTokens != 3 {
		t.Errorf("usage = %+v, want input=12 output=7 cacheRead=4 cacheWrite=2 reasoning=3", resp.Usage)
	}
	if len(resp.Content) != 2 {
		t.Fatalf("content parts = %d, want 2", len(resp.Content))
	}
	if resp.Content[0].Text == nil || resp.Content[0].Text.Text != "Let me check." {
		t.Errorf("content[0] = %+v, want text 'Let me check.'", resp.Content[0])
	}
	tc := resp.Content[1].ToolCall
	if tc == nil {
		t.Fatalf("content[1] is not a tool call: %+v", resp.Content[1])
	}
	if tc.ID != "toolu_z" || tc.Name != "get_weather" || tc.Args["city"] != "Paris" {
		t.Errorf("tool call = %+v, want id=toolu_z name=get_weather city=Paris", tc)
	}
	if len(resp.ProviderRaw) == 0 {
		t.Error("ProviderRaw should carry the assistant continuation blob")
	}
}

// TestCapabilities asserts the per-model capability table resolves distinct
// MaxOutputTokens and the broadly-supported flags.
func TestCapabilities(t *testing.T) {
	p := New(WithAPIKey("test-key"))
	opus, _ := p.Capabilities(t.Context(), "claude-opus-4-8")
	if opus.MaxOutputTokens != 128_000 {
		t.Errorf("opus MaxOutputTokens = %d, want 128000", opus.MaxOutputTokens)
	}
	if !opus.SupportsTools || !opus.SupportsStreamingToolCalls || !opus.SupportsTokenCounting || !opus.SupportsThinking {
		t.Errorf("opus caps missing expected flags: %+v", opus)
	}
	haiku35, _ := p.Capabilities(t.Context(), "claude-3-5-haiku-20241022")
	if haiku35.MaxOutputTokens != 8_192 {
		t.Errorf("haiku-3.5 MaxOutputTokens = %d, want 8192", haiku35.MaxOutputTokens)
	}
	// Unknown model falls back to defaults (not zero), so the loop neither blocks
	// nor over-promises.
	unknown, _ := p.Capabilities(t.Context(), "claude-future-99")
	if unknown.MaxOutputTokens == 0 || !unknown.SupportsTools {
		t.Errorf("unknown model caps = %+v, want non-zero default with tools", unknown)
	}
}

// TestProvider_RequestBuildErrors asserts that a request that fails to build
// (a content part with no variant set) is reported as ErrInvalidRequest from
// Generate/Stream/CountTokens before any network call is attempted.
func TestProvider_RequestBuildErrors(t *testing.T) {
	p := New(WithAPIKey("k"))
	bad := llm.Request{
		Model:    "claude-opus-4-8",
		Messages: []llm.Message{{Role: llm.RoleUser, Content: []llm.ContentPart{{}}}}, // empty content part
	}
	ctx := t.Context()

	if _, err := p.Generate(ctx, bad); !isInvalidRequest(err) {
		t.Errorf("Generate error = %v, want ErrInvalidRequest", err)
	}
	if _, err := p.Stream(ctx, bad); !isInvalidRequest(err) {
		t.Errorf("Stream error = %v, want ErrInvalidRequest", err)
	}
	if _, err := p.CountTokens(ctx, bad); !isInvalidRequest(err) {
		t.Errorf("CountTokens error = %v, want ErrInvalidRequest", err)
	}
}

func isInvalidRequest(err error) bool {
	var pe *llm.ProviderError
	return errors.As(err, &pe) && pe.Kind == llm.ErrInvalidRequest
}

// TestProvider_ContractAndConstruction asserts New builds a usable Provider that
// satisfies llm.Provider and that the default max-tokens is applied.
func TestProvider_ContractAndConstruction(t *testing.T) {
	var _ llm.Provider = New(WithAPIKey("k"), WithBaseURL("https://example.test"), WithDefaultMaxTokens(2048))
	p := New(WithDefaultMaxTokens(2048))
	if p.defaultMaxToken != 2048 {
		t.Errorf("defaultMaxToken = %d, want 2048", p.defaultMaxToken)
	}
	// A zero or negative override is ignored (keeps the package default).
	p2 := New(WithDefaultMaxTokens(0))
	if p2.defaultMaxToken != defaultMaxOutputTokens {
		t.Errorf("defaultMaxToken with 0 override = %d, want package default %d", p2.defaultMaxToken, defaultMaxOutputTokens)
	}
}
