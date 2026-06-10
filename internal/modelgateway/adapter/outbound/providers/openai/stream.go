package openai

import (
	"io"

	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/openai/openai-go/v3/responses"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// responsesStreamReader adapts an OpenAI Responses typed-event stream
// ([ssestream.Stream] of [responses.ResponseStreamEventUnion]) to an
// [llm.StreamReader]. It pulls events from the SDK stream, runs each through the
// [Normalizer], buffers the resulting [llm.StreamEvent] values, and emits them one
// per Recv. The Responses surface delivers an explicit terminal event
// (response.completed / .incomplete / .failed / error), so the Normalizer emits the
// single [llm.Done] there; once the SDK stream is exhausted the reader returns
// [io.EOF]. A mid-stream SDK error is normalized to a [*llm.ProviderError].
type responsesStreamReader struct {
	stream *ssestream.Stream[responses.ResponseStreamEventUnion]
	norm   *Normalizer

	buf  []llm.StreamEvent
	done bool
	err  error
}

// newResponsesStreamReader wraps an SDK Responses stream as an [llm.StreamReader].
func newResponsesStreamReader(stream *ssestream.Stream[responses.ResponseStreamEventUnion]) *responsesStreamReader {
	return &responsesStreamReader{stream: stream, norm: NewNormalizer()}
}

// Recv returns the next normalized [llm.StreamEvent], or [io.EOF] once the stream is
// exhausted after the terminal [llm.Done]. It is not safe for concurrent use.
func (r *responsesStreamReader) Recv() (llm.StreamEvent, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return llm.StreamEvent{}, r.err
		}
		if r.done {
			return llm.StreamEvent{}, io.EOF
		}
		r.fill()
	}
	ev := r.buf[0]
	r.buf = r.buf[1:]
	return ev, nil
}

// fill advances the SDK stream by one event and appends produced normalized events
// to the buffer, or records terminal/error state when the stream ends.
func (r *responsesStreamReader) fill() {
	if r.stream.Next() {
		ev := r.stream.Current()
		r.buf = append(r.buf, r.norm.Next(ev)...)
		return
	}
	if err := r.stream.Err(); err != nil {
		r.err = normalizeError(err)
		return
	}
	r.done = true
}

// Close releases the underlying SSE stream. It is safe to call multiple times.
func (r *responsesStreamReader) Close() error {
	if r.stream == nil {
		return nil
	}
	return r.stream.Close()
}

// Ensure responsesStreamReader satisfies llm.StreamReader at compile time.
var _ llm.StreamReader = (*responsesStreamReader)(nil)
