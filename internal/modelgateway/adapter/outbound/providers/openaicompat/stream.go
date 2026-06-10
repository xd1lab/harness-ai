package openaicompat

import (
	"io"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/ssestream"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// chatStreamReader adapts an OpenAI Chat Completions SSE stream
// ([ssestream.Stream] of [openai.ChatCompletionChunk]) to an [llm.StreamReader]. It
// pulls chunks from the SDK stream, runs each through the [Normalizer], buffers the
// resulting [llm.StreamEvent] values, and emits them one per Recv. When the SDK
// stream is exhausted it flushes the normalizer's terminal events (buffered tool
// calls + the single [llm.Done]); after those it returns [io.EOF]. A mid-stream SDK
// error is normalized to a [*llm.ProviderError] and returned from Recv.
type chatStreamReader struct {
	stream *ssestream.Stream[openai.ChatCompletionChunk]
	norm   *Normalizer

	buf      []llm.StreamEvent // pending normalized events not yet returned
	finished bool              // SDK stream drained and Finish() flushed
	err      error             // sticky terminal error (non-EOF)
}

// newChatStreamReader wraps an SDK chat stream as an [llm.StreamReader].
func newChatStreamReader(stream *ssestream.Stream[openai.ChatCompletionChunk]) *chatStreamReader {
	return &chatStreamReader{stream: stream, norm: NewNormalizer()}
}

// Recv returns the next normalized [llm.StreamEvent], or [io.EOF] once the terminal
// [llm.Done] has been delivered. It is not safe for concurrent use.
func (r *chatStreamReader) Recv() (llm.StreamEvent, error) {
	for len(r.buf) == 0 {
		if r.err != nil {
			return llm.StreamEvent{}, r.err
		}
		if r.finished {
			return llm.StreamEvent{}, io.EOF
		}
		if !r.fill() {
			// fill set either err, finished, or appended to buf.
			continue
		}
	}
	ev := r.buf[0]
	r.buf = r.buf[1:]
	return ev, nil
}

// fill advances the SDK stream by one chunk (or flushes the terminal tail) and
// appends any produced events to the buffer. It returns false to signal the caller
// to re-check loop conditions (buffer/err/finished); it never blocks beyond a
// single underlying Next.
func (r *chatStreamReader) fill() bool {
	if r.stream.Next() {
		chunk := r.stream.Current()
		r.buf = append(r.buf, r.norm.Next(chunk)...)
		return false
	}
	// Stream ended: surface a transport/decode error if any, else flush terminal
	// events exactly once.
	if err := r.stream.Err(); err != nil {
		r.err = normalizeError(err)
		return false
	}
	r.buf = append(r.buf, r.norm.Finish()...)
	r.finished = true
	return false
}

// Close releases the underlying SSE stream. It is safe to call multiple times and
// after Recv has returned an error or io.EOF.
func (r *chatStreamReader) Close() error {
	if r.stream == nil {
		return nil
	}
	return r.stream.Close()
}

// Ensure chatStreamReader satisfies llm.StreamReader at compile time.
var _ llm.StreamReader = (*chatStreamReader)(nil)
