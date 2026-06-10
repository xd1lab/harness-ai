package llmtest_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/llm"
	"github.com/boltrope/boltrope/internal/platform/llm/llmtest"
)

// ---------------------------------------------------------------------------
// FakeStreamReader
// ---------------------------------------------------------------------------

// TestFakeStreamReader_YieldsEventsThenEOF is the primary contract test for
// the scripted reader: it must replay all events in order, then return io.EOF.
func TestFakeStreamReader_YieldsEventsThenEOF(t *testing.T) {
	events := []llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "hello"}},
		{ThinkingDelta: &llm.ThinkingDelta{Text: "thinking"}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}
	r := llmtest.NewFakeStreamReader(events...)

	ev0, err := r.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev0.TextDelta)
	assert.Equal(t, "hello", ev0.TextDelta.Text)

	ev1, err := r.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev1.ThinkingDelta)
	assert.Equal(t, "thinking", ev1.ThinkingDelta.Text)

	ev2, err := r.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev2.Done)
	assert.Equal(t, llm.StopEnd, ev2.Done.StopReason)

	// After Done, next Recv MUST return io.EOF.
	_, err = r.Recv()
	assert.Equal(t, io.EOF, err, "expected io.EOF after scripted events exhausted")
}

// TestFakeStreamReader_EmptyYieldsEOFImmediately: an empty reader returns EOF on
// the first Recv.
func TestFakeStreamReader_EmptyYieldsEOFImmediately(t *testing.T) {
	r := llmtest.NewFakeStreamReader()
	_, err := r.Recv()
	assert.Equal(t, io.EOF, err)
}

// TestFakeStreamReader_PosTracking: Pos() tracks consumed events.
func TestFakeStreamReader_PosTracking(t *testing.T) {
	r := llmtest.NewFakeStreamReader(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "a"}},
		llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
	)
	assert.Equal(t, 0, r.Pos())
	r.Recv() //nolint:errcheck // advances the reader; only the Pos() cursor is under test
	assert.Equal(t, 1, r.Pos())
}

// TestFakeStreamReader_ToolCallDelta: verifies ToolCallDelta events are
// replayed correctly (exercising the assembler test scenario).
func TestFakeStreamReader_ToolCallDelta(t *testing.T) {
	events := []llm.StreamEvent{
		{ToolCallDelta: &llm.ToolCallDelta{CallID: "c1", Name: "bash"}},
		{Done: &llm.Done{StopReason: llm.StopToolUse}},
	}
	r := llmtest.NewFakeStreamReader(events...)

	ev0, err := r.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev0.ToolCallDelta)
	assert.Equal(t, "c1", ev0.ToolCallDelta.CallID)
	assert.Equal(t, "bash", ev0.ToolCallDelta.Name)

	ev1, err := r.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev1.Done)
	assert.Equal(t, llm.StopToolUse, ev1.Done.StopReason)

	_, err = r.Recv()
	assert.Equal(t, io.EOF, err)
}

// ---------------------------------------------------------------------------
// FakeProvider
// ---------------------------------------------------------------------------

// TestFakeProvider_Generate: scripted response is returned in order.
func TestFakeProvider_Generate(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddGenerateText("world")

	resp, err := fp.Generate(context.Background(), llm.Request{Model: "m1"})
	require.NoError(t, err)
	require.Len(t, resp.Content, 1)
	assert.Equal(t, "world", resp.Content[0].Text.Text)
	assert.Equal(t, llm.StopEnd, resp.StopReason)
	assert.Equal(t, 1, fp.GenerateCalls())
}

// TestFakeProvider_GenerateError: error entries propagate.
func TestFakeProvider_GenerateError(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	provErr := &llm.ProviderError{Kind: llm.ErrServer}
	fp.AddGenerate(nil, provErr)

	_, err := fp.Generate(context.Background(), llm.Request{})
	require.Error(t, err)
	var pe *llm.ProviderError
	require.ErrorAs(t, err, &pe)
	assert.Equal(t, llm.ErrServer, pe.Kind)
}

// TestFakeProvider_Stream: scripted stream events are relayed through.
func TestFakeProvider_Stream(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStreamEvents(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "hi"}},
	)

	sr, err := fp.Stream(context.Background(), llm.Request{Model: "m"})
	require.NoError(t, err)
	defer func() { _ = sr.Close() }()

	ev, err := sr.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev.TextDelta)
	assert.Equal(t, "hi", ev.TextDelta.Text)

	ev2, err := sr.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev2.Done) // auto-appended Done
	assert.Equal(t, llm.StopEnd, ev2.Done.StopReason)

	_, err = sr.Recv()
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 1, fp.StreamCalls())
}

// TestFakeProvider_StreamError: a queued stream error is returned without a reader.
func TestFakeProvider_StreamError(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrRateLimited})

	_, err := fp.Stream(context.Background(), llm.Request{})
	require.Error(t, err)
}

// TestFakeProvider_CountTokens: scripted token counts returned.
func TestFakeProvider_CountTokens(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddTokenCount(42, nil)

	n, err := fp.CountTokens(context.Background(), llm.Request{})
	require.NoError(t, err)
	assert.Equal(t, 42, n)
}

// TestFakeProvider_Capabilities: returns zero capabilities when queue empty.
func TestFakeProvider_Capabilities(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	caps, err := fp.Capabilities(context.Background(), "any-model")
	require.NoError(t, err)
	assert.False(t, caps.SupportsTools) // zero value
}

// TestFakeProvider_QueueExhaustedPanic: exceeding the queue panics (default).
func TestFakeProvider_QueueExhaustedPanic(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddGenerateText("first")
	fp.Generate(context.Background(), llm.Request{}) //nolint:errcheck // drains the single scripted entry; the panic on the NEXT call is the assertion
	assert.Panics(t, func() {
		fp.Generate(context.Background(), llm.Request{}) //nolint:errcheck // must panic on the exhausted queue — assert.Panics checks that, not the returns
	})
}

// TestFakeProvider_RecordedRequests: requests are captured for assertion.
func TestFakeProvider_RecordedRequests(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddGenerateText("ok")
	req := llm.Request{Model: "claude-test", System: "be helpful"}
	fp.Generate(context.Background(), req) //nolint:errcheck // only the captured RecordedRequests entry is under test, not the response
	require.Len(t, fp.RecordedRequests, 1)
	assert.Equal(t, "claude-test", fp.RecordedRequests[0].Model)
}
