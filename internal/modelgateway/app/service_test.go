package app_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/modelgateway/app"
	"github.com/xd1lab/harness-ai/internal/modelgateway/app/capabilities"
	"github.com/xd1lab/harness-ai/internal/modelgateway/app/retry"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/internal/platform/llm/llmtest"
	"github.com/xd1lab/harness-ai/internal/platform/pricing"
)

// fakeClock is a deterministic llm.Clock whose After channel fires immediately,
// so retry waits do not actually sleep under test.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.now = c.now.Add(d)
	ch <- c.now
	return ch
}

// zeroRand is a retry.Rand that always returns 0 (deterministic, no jitter).
type zeroRand struct{}

func (zeroRand) Float64() float64 { return 0 }

// recordingCostSink captures cost reports made on Done.
type recordingCostSink struct {
	calls []app.CostReport
}

func (s *recordingCostSink) Record(_ context.Context, r app.CostReport) {
	s.calls = append(s.calls, r)
}

// newService wires a Service around a fake provider with a deterministic retry
// decorator, the built-in capabilities registry, and the pricing cost function.
func newService(t *testing.T, fp *llmtest.FakeProvider, sink app.CostSink) *app.Service {
	t.Helper()
	retrying := retry.New(fp, retry.Config{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Second}, &fakeClock{}, zeroRand{})
	reg := capabilities.NewRegistry(nil)
	svc, err := app.NewService(app.Config{
		Provider:     retrying,
		Endpoint:     "anthropic",
		Capabilities: reg,
		Cost:         pricing.Cost,
		CostSink:     sink,
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

// drain reads a StreamReader to completion, collecting events.
func drain(t *testing.T, r llm.StreamReader) []llm.StreamEvent {
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

// TestStream_ComputesCostOnDone asserts the Service computes the turn cost from
// the Done usage and the pricing table, reporting it to the CostSink exactly
// once when the Done event passes through the stream.
func TestStream_ComputesCostOnDone(t *testing.T) {
	const model = "claude-3-5-sonnet-20241022"
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "hi"}},
		{Done: &llm.Done{
			StopReason: llm.StopEnd,
			Usage:      llm.Usage{InputTokens: 1000, OutputTokens: 500},
		}},
	}, nil)

	sink := &recordingCostSink{}
	svc := newService(t, fp, sink)

	r, err := svc.Stream(context.Background(), llm.Request{Model: model})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()

	events := drain(t, r)
	// The full event sequence is preserved through the wrapper.
	if len(events) != 2 || events[0].TextDelta == nil || events[1].Done == nil {
		t.Fatalf("events = %+v, want [TextDelta, Done]", events)
	}

	if len(sink.calls) != 1 {
		t.Fatalf("CostSink called %d times, want 1", len(sink.calls))
	}
	got := sink.calls[0]
	if got.Model != model {
		t.Errorf("report Model = %q, want %q", got.Model, model)
	}
	want, err := pricing.Cost(model, llm.Usage{InputTokens: 1000, OutputTokens: 500})
	if err != nil {
		t.Fatalf("pricing.Cost: %v", err)
	}
	if got.CostUSD != want {
		t.Errorf("CostUSD = %v, want %v", got.CostUSD, want)
	}
	if got.Usage.InputTokens != 1000 || got.Usage.OutputTokens != 500 {
		t.Errorf("report Usage = %+v", got.Usage)
	}
	if got.Err != nil {
		t.Errorf("report Err = %v, want nil", got.Err)
	}
}

// TestStream_UnknownModelCostError asserts that when the model is absent from
// the pricing table the Service still streams successfully but reports the
// typed UnknownModelError on the CostReport (cost is best-effort observability,
// never a stream failure).
func TestStream_UnknownModelCostError(t *testing.T) {
	const model = "no-such-model"
	fp := llmtest.NewFakeProvider()
	fp.AddStream([]llm.StreamEvent{
		{Done: &llm.Done{StopReason: llm.StopEnd, Usage: llm.Usage{InputTokens: 10}}},
	}, nil)

	sink := &recordingCostSink{}
	svc := newService(t, fp, sink)

	r, err := svc.Stream(context.Background(), llm.Request{Model: model})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()
	_ = drain(t, r)

	if len(sink.calls) != 1 {
		t.Fatalf("CostSink called %d times, want 1", len(sink.calls))
	}
	var ume *pricing.UnknownModelError
	if !errors.As(sink.calls[0].Err, &ume) {
		t.Fatalf("report Err = %v, want *pricing.UnknownModelError", sink.calls[0].Err)
	}
	if sink.calls[0].CostUSD != 0 {
		t.Errorf("CostUSD = %v, want 0 on unknown model", sink.calls[0].CostUSD)
	}
}

// TestStream_RetriesThenSucceeds asserts the retry decorator wrapping the
// provider re-opens the stream after transient errors and the Service streams
// the eventual successful sequence (errors-then-success).
func TestStream_RetriesThenSucceeds(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	// Two transient failures, then a successful stream.
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrOverloaded})
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrServer})
	fp.AddStream([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "ok"}},
		{Done: &llm.Done{StopReason: llm.StopEnd, Usage: llm.Usage{InputTokens: 1}}},
	}, nil)

	sink := &recordingCostSink{}
	svc := newService(t, fp, sink)

	r, err := svc.Stream(context.Background(), llm.Request{Model: "claude-3-haiku-20240307"})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	defer func() { _ = r.Close() }()
	events := drain(t, r)

	if fp.StreamCalls() != 3 {
		t.Errorf("provider Stream called %d times, want 3 (2 retries + success)", fp.StreamCalls())
	}
	if len(events) != 2 || events[0].TextDelta == nil || events[1].Done == nil {
		t.Fatalf("events = %+v", events)
	}
	if len(sink.calls) != 1 {
		t.Errorf("CostSink called %d times, want 1", len(sink.calls))
	}
}

// TestStream_NonRetryableErrorPropagates asserts a non-retryable provider error
// opening the stream is returned to the caller without retry.
func TestStream_NonRetryableErrorPropagates(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddStream(nil, &llm.ProviderError{Kind: llm.ErrInvalidRequest})

	sink := &recordingCostSink{}
	svc := newService(t, fp, sink)

	_, err := svc.Stream(context.Background(), llm.Request{Model: "claude-3-haiku-20240307"})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) || pe.Kind != llm.ErrInvalidRequest {
		t.Fatalf("err = %v, want ErrInvalidRequest", err)
	}
	if fp.StreamCalls() != 1 {
		t.Errorf("Stream called %d times, want 1 (no retry)", fp.StreamCalls())
	}
	if len(sink.calls) != 0 {
		t.Errorf("CostSink called %d times, want 0", len(sink.calls))
	}
}

// TestCountTokens_Delegates asserts CountTokens passes through to the provider.
func TestCountTokens_Delegates(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddTokenCount(123, nil)
	svc := newService(t, fp, &recordingCostSink{})

	n, err := svc.CountTokens(context.Background(), llm.Request{Model: "m"})
	if err != nil {
		t.Fatalf("CountTokens: %v", err)
	}
	if n != 123 {
		t.Errorf("CountTokens = %d, want 123", n)
	}
}

// TestCountTokens_UnsupportedPropagates asserts an ErrUnsupported from the
// provider is returned unchanged so the gRPC edge can map it to UNIMPLEMENTED.
func TestCountTokens_UnsupportedPropagates(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	fp.AddTokenCount(0, &llm.ProviderError{Kind: llm.ErrUnsupported})
	svc := newService(t, fp, &recordingCostSink{})

	_, err := svc.CountTokens(context.Background(), llm.Request{Model: "m"})
	var pe *llm.ProviderError
	if !errors.As(err, &pe) || pe.Kind != llm.ErrUnsupported {
		t.Fatalf("err = %v, want ErrUnsupported", err)
	}
}

// TestCapabilities_UsesRegistry asserts the Service resolves capabilities via
// the injected registry keyed on the configured endpoint and the requested
// model (not via the provider).
func TestCapabilities_UsesRegistry(t *testing.T) {
	fp := llmtest.NewFakeProvider()
	caps := llm.Capabilities{SupportsTools: true, MaxOutputTokens: 4242}
	reg := capabilities.NewRegistry(map[string]capabilities.EndpointOverride{
		"anthropic": {PerModel: map[string]*llm.Capabilities{"custom": &caps}},
	})
	svc, err := app.NewService(app.Config{
		Provider:     fp,
		Endpoint:     "anthropic",
		Capabilities: reg,
		Cost:         pricing.Cost,
		CostSink:     &recordingCostSink{},
	})
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	got, err := svc.Capabilities(context.Background(), "custom")
	if err != nil {
		t.Fatalf("Capabilities: %v", err)
	}
	if !got.SupportsTools || got.MaxOutputTokens != 4242 {
		t.Errorf("Capabilities = %+v, want from registry override", got)
	}
	// Provider.Capabilities must not be consulted — capability resolution is the
	// gateway's job via the registry.
	if fp.CapabilitiesCalls() != 0 {
		t.Errorf("provider Capabilities called %d times, want 0", fp.CapabilitiesCalls())
	}
}

// TestNewService_Validates asserts NewService rejects a nil Provider.
func TestNewService_Validates(t *testing.T) {
	_, err := app.NewService(app.Config{
		Endpoint:     "anthropic",
		Capabilities: capabilities.NewRegistry(nil),
		Cost:         pricing.Cost,
	})
	if err == nil {
		t.Fatal("expected error for nil Provider, got nil")
	}
}
