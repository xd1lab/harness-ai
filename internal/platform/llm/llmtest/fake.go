// Package llmtest provides scripted fakes for [llm.Provider] and
// [llm.StreamReader] used by the agent-loop unit tests and the eval harness
// (T-FND-02, T-LOOP-01, T-EVAL-01). Both types replay a deterministic
// programmer-supplied sequence of responses and stream events so tests are
// fast, network-free, and fully reproducible.
//
// # FakeStreamReader
//
// FakeStreamReader replays a scripted []StreamEvent sequence. The final item in
// the sequence should be a Done event; Recv returns [io.EOF] immediately after
// that Done is delivered. Each Recv call returns the next event; after the
// sequence is exhausted it returns io.EOF.
//
//	reader := llmtest.NewFakeStreamReader(
//	    llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "Hello"}},
//	    llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}},
//	)
//	ev, err := reader.Recv() // TextDelta
//	ev, err = reader.Recv()  // Done
//	ev, err = reader.Recv()  // io.EOF
//
// # FakeProvider
//
// FakeProvider returns scripted responses and stream readers. Each call to
// Generate or Stream consumes the next entry in the scripted queue. If the queue
// is exhausted and NoMoreError is set, that error is returned; otherwise the
// call panics.
package llmtest

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// Compile-time assertions.
var (
	_ llm.Provider     = (*FakeProvider)(nil)
	_ llm.StreamReader = (*FakeStreamReader)(nil)
)

// ----------------------- FakeStreamReader ----------------------------------

// FakeStreamReader replays a scripted sequence of [llm.StreamEvent]s. Recv
// returns events in order, then io.EOF after the last event has been delivered.
// Close is a no-op. FakeStreamReader is NOT safe for concurrent use.
type FakeStreamReader struct {
	events []llm.StreamEvent
	pos    int
	closed bool
}

// NewFakeStreamReader returns a FakeStreamReader that replays the given events.
// Typically the last event should have a non-nil Done field.
func NewFakeStreamReader(events ...llm.StreamEvent) *FakeStreamReader {
	return &FakeStreamReader{events: events}
}

// Recv returns the next event in the scripted sequence. It returns [io.EOF]
// after the last event has been returned (even if that event had a non-Done
// payload, EOF is returned on the subsequent call — the reader is exhausted).
func (r *FakeStreamReader) Recv() (llm.StreamEvent, error) {
	if r.pos >= len(r.events) {
		return llm.StreamEvent{}, io.EOF
	}
	ev := r.events[r.pos]
	r.pos++
	// After yielding the last event, subsequent Recv returns EOF.
	return ev, nil
}

// Close is a no-op for the fake reader.
func (r *FakeStreamReader) Close() error {
	r.closed = true
	return nil
}

// Pos returns the number of events that have been consumed.
func (r *FakeStreamReader) Pos() int { return r.pos }

// ----------------------- ScriptedGenerate / ScriptedStream ------------------

// ScriptedGenerate is one scripted entry for a FakeProvider.Generate call.
type ScriptedGenerate struct {
	// Response is returned when Err is nil.
	Response *llm.Response
	// Err, when non-nil, is returned instead of Response.
	Err error
}

// ScriptedStream is one scripted entry for a FakeProvider.Stream call.
type ScriptedStream struct {
	// Events are the stream events the returned FakeStreamReader will replay.
	// Typically the last event has a non-nil Done.
	Events []llm.StreamEvent
	// Err, when non-nil, is returned immediately instead of a reader.
	Err error
}

// ScriptedCapabilities is one scripted entry for FakeProvider.Capabilities.
type ScriptedCapabilities struct {
	Caps llm.Capabilities
	Err  error
}

// ----------------------- FakeProvider --------------------------------------

// FakeProvider is a scriptable [llm.Provider] for tests. Calls to Generate,
// Stream, CountTokens, and Capabilities consume the next entry from their
// respective queues. If a queue is exhausted and the call is made, it panics
// with a helpful message unless you set NoMoreError, in which case that error
// is returned instead.
//
// All call counts are recorded and retrievable.
type FakeProvider struct {
	generates   []ScriptedGenerate
	genIdx      atomic.Int64
	streams     []ScriptedStream
	streamIdx   atomic.Int64
	tokenCounts []int
	tokenErrs   []error
	tokenIdx    atomic.Int64
	caps        []ScriptedCapabilities
	capsIdx     atomic.Int64
	// NoMoreError is returned when any scripted queue is exhausted. When nil,
	// exhaustion panics.
	NoMoreError error

	// call counters (read with Load)
	generateCalls     atomic.Int64
	streamCalls       atomic.Int64
	countTokensCalls  atomic.Int64
	capabilitiesCalls atomic.Int64

	// RecordedRequests captures every Request passed to Generate or Stream so
	// tests can assert request shapes.
	RecordedRequests []llm.Request
}

// NewFakeProvider returns a FakeProvider with no scripted entries. Queue up
// entries via AddGenerate, AddStream, AddTokenCount, AddCapabilities.
func NewFakeProvider() *FakeProvider {
	return &FakeProvider{}
}

// AddGenerate appends one scripted Generate entry to the queue.
func (f *FakeProvider) AddGenerate(resp *llm.Response, err error) {
	f.generates = append(f.generates, ScriptedGenerate{Response: resp, Err: err})
}

// AddGenerateText is a convenience helper that adds a Generate entry that
// returns a text-only response with StopEnd.
func (f *FakeProvider) AddGenerateText(text string) {
	f.AddGenerate(&llm.Response{
		Content:    []llm.ContentPart{{Text: &llm.TextPart{Text: text}}},
		StopReason: llm.StopEnd,
	}, nil)
}

// AddStream appends one scripted Stream entry to the queue.
func (f *FakeProvider) AddStream(events []llm.StreamEvent, err error) {
	f.streams = append(f.streams, ScriptedStream{Events: events, Err: err})
}

// AddStreamEvents is a convenience helper: each event becomes part of the
// stream, with a Done(StopEnd) appended automatically if the last event is not
// already a Done.
func (f *FakeProvider) AddStreamEvents(events ...llm.StreamEvent) {
	if len(events) == 0 || events[len(events)-1].Done == nil {
		events = append(events, llm.StreamEvent{Done: &llm.Done{StopReason: llm.StopEnd}})
	}
	f.AddStream(events, nil)
}

// AddTokenCount appends one scripted CountTokens result.
func (f *FakeProvider) AddTokenCount(count int, err error) {
	f.tokenCounts = append(f.tokenCounts, count)
	f.tokenErrs = append(f.tokenErrs, err)
}

// AddCapabilities appends one scripted Capabilities result.
func (f *FakeProvider) AddCapabilities(caps llm.Capabilities, err error) {
	f.caps = append(f.caps, ScriptedCapabilities{Caps: caps, Err: err})
}

// Generate returns the next scripted response, or panics/returns NoMoreError.
func (f *FakeProvider) Generate(_ context.Context, req llm.Request) (*llm.Response, error) {
	f.generateCalls.Add(1)
	f.RecordedRequests = append(f.RecordedRequests, req)
	idx := int(f.genIdx.Add(1) - 1)
	if idx >= len(f.generates) {
		if f.NoMoreError != nil {
			return nil, f.NoMoreError
		}
		panic(fmt.Sprintf("llmtest.FakeProvider: Generate queue exhausted (call %d, have %d entries)", idx+1, len(f.generates)))
	}
	s := f.generates[idx]
	return s.Response, s.Err
}

// Stream returns the next scripted StreamReader, or panics/returns NoMoreError.
func (f *FakeProvider) Stream(_ context.Context, req llm.Request) (llm.StreamReader, error) {
	f.streamCalls.Add(1)
	f.RecordedRequests = append(f.RecordedRequests, req)
	idx := int(f.streamIdx.Add(1) - 1)
	if idx >= len(f.streams) {
		if f.NoMoreError != nil {
			return nil, f.NoMoreError
		}
		panic(fmt.Sprintf("llmtest.FakeProvider: Stream queue exhausted (call %d, have %d entries)", idx+1, len(f.streams)))
	}
	s := f.streams[idx]
	if s.Err != nil {
		return nil, s.Err
	}
	return NewFakeStreamReader(s.Events...), nil
}

// CountTokens returns the next scripted token count.
func (f *FakeProvider) CountTokens(_ context.Context, _ llm.Request) (int, error) {
	f.countTokensCalls.Add(1)
	idx := int(f.tokenIdx.Add(1) - 1)
	if idx >= len(f.tokenCounts) {
		if f.NoMoreError != nil {
			return 0, f.NoMoreError
		}
		panic(fmt.Sprintf("llmtest.FakeProvider: CountTokens queue exhausted (call %d)", idx+1))
	}
	return f.tokenCounts[idx], f.tokenErrs[idx]
}

// Capabilities returns the next scripted capabilities entry.
func (f *FakeProvider) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	f.capabilitiesCalls.Add(1)
	idx := int(f.capsIdx.Add(1) - 1)
	if idx >= len(f.caps) {
		if f.NoMoreError != nil {
			return llm.Capabilities{}, f.NoMoreError
		}
		// Return a zero-value Capabilities rather than panic when no caps are scripted.
		return llm.Capabilities{}, nil
	}
	s := f.caps[idx]
	return s.Caps, s.Err
}

// GenerateCalls returns the number of times Generate has been called.
func (f *FakeProvider) GenerateCalls() int { return int(f.generateCalls.Load()) }

// StreamCalls returns the number of times Stream has been called.
func (f *FakeProvider) StreamCalls() int { return int(f.streamCalls.Load()) }

// CountTokensCalls returns the number of times CountTokens has been called.
func (f *FakeProvider) CountTokensCalls() int { return int(f.countTokensCalls.Load()) }

// CapabilitiesCalls returns the number of times Capabilities has been called.
func (f *FakeProvider) CapabilitiesCalls() int { return int(f.capabilitiesCalls.Load()) }
