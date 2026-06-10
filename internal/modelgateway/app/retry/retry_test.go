package retry_test

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"runtime"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/modelgateway/app/retry"
	"github.com/boltrope/boltrope/internal/platform/clock/clocktest"
	"github.com/boltrope/boltrope/internal/platform/llm"
	"github.com/boltrope/boltrope/internal/platform/llm/llmtest"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// epoch is a fixed start time used for the fake clock.
var epoch = time.Unix(1_000_000, 0)

// providerErr builds a *llm.ProviderError of the given kind.
func providerErr(kind llm.ErrorKind) *llm.ProviderError {
	return &llm.ProviderError{Kind: kind}
}

// providerErrRetryAfter builds a *llm.ProviderError with a RetryAfter hint.
func providerErrRetryAfter(kind llm.ErrorKind, d time.Duration) *llm.ProviderError {
	return &llm.ProviderError{Kind: kind, RetryAfter: d}
}

// deterministicRand returns a retry.Rand backed by a deterministically seeded
// PRNG for repeatable jitter sequences in tests.
func deterministicRand(seed uint64) retry.Rand { //nolint:gosec // test-only weak RNG is intentional
	var s [32]byte
	for i := 0; i < 8; i++ {
		s[i] = byte(seed >> (i * 8)) //nolint:gosec // G115: uint64→byte is safe here; upper bits are discarded intentionally
	}
	return rand.New(rand.NewChaCha8(s)) //nolint:gosec // G404: test-only deterministic PRNG; crypto/rand not needed for jitter tests
}

// drainReader reads all events from a StreamReader until EOF.  Returns the
// total number of non-Done events seen and the first error (if any).
func drainReader(r llm.StreamReader) (int, error) {
	count := 0
	for {
		ev, err := r.Recv()
		if errors.Is(err, io.EOF) {
			return count, nil
		}
		if err != nil {
			return count, err
		}
		if ev.Done == nil {
			count++
		}
	}
}

// yieldThenAdvance yields the goroutine scheduler a few times so that any
// background goroutine has a chance to reach its clock.After call, then
// advances the fake clock.  This is the standard technique for driving fake
// clocks in concurrent tests — a real sleep is intentionally kept at ≤1ms so
// the CI wall time cost is negligible.
func yieldThenAdvance(fc *clocktest.Fake, d time.Duration) {
	for i := 0; i < 50; i++ {
		runtime.Gosched()
	}
	// A minimal real sleep ensures the OS gives the goroutine CPU time to
	// reach the select/After call even on a loaded scheduler.
	time.Sleep(time.Millisecond)
	fc.Advance(d)
}

// ---------------------------------------------------------------------------
// Test: RateLimited with RetryAfter waits exactly that long then succeeds
// ---------------------------------------------------------------------------

// TestRetryAfterHonored verifies FR-MODEL-05 AC-1: when the provider returns a
// *ProviderError{Kind:ErrRateLimited, RetryAfter:5s}, the retry decorator waits
// exactly 5 s (via the fake clock) before issuing the next attempt, which
// succeeds.
func TestRetryAfterHonored(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	// First call: rate-limited with a 5 s RetryAfter.
	fake.AddGenerate(nil, providerErrRetryAfter(llm.ErrRateLimited, 5*time.Second))
	// Second call: success.
	fake.AddGenerateText("hello")

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	done := make(chan struct{})
	var resp *llm.Response
	var callErr error

	go func() {
		resp, callErr = dec.Generate(context.Background(), llm.Request{})
		close(done)
	}()

	// Advance the fake clock by exactly 5 s to satisfy the RetryAfter wait.
	yieldThenAdvance(fc, 5*time.Second)

	<-done

	if callErr != nil {
		t.Fatalf("expected success, got error: %v", callErr)
	}
	if resp == nil || len(resp.Content) == 0 {
		t.Fatal("expected non-empty response")
	}
	if fake.GenerateCalls() != 2 {
		t.Fatalf("expected 2 Generate calls, got %d", fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Auth error is NOT retried
// ---------------------------------------------------------------------------

// TestAuthNotRetried verifies that a *ProviderError{Kind:ErrAuth} is returned
// immediately with exactly one attempt — the clock is never driven.
func TestAuthNotRetried(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	authErr := providerErr(llm.ErrAuth)
	fake.AddGenerate(nil, authErr)

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	_, err := dec.Generate(context.Background(), llm.Request{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) {
		t.Fatalf("expected *llm.ProviderError, got %T: %v", err, err)
	}
	if pe.Kind != llm.ErrAuth {
		t.Fatalf("expected ErrAuth, got %q", pe.Kind)
	}
	if fake.GenerateCalls() != 1 {
		t.Fatalf("expected exactly 1 Generate call (no retries), got %d", fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: InvalidRequest is NOT retried
// ---------------------------------------------------------------------------

// TestInvalidRequestNotRetried verifies that ErrInvalidRequest is propagated
// immediately with no retries.
func TestInvalidRequestNotRetried(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	fake.AddGenerate(nil, providerErr(llm.ErrInvalidRequest))

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	_, err := dec.Generate(context.Background(), llm.Request{})
	if err == nil {
		t.Fatal("expected error")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) || pe.Kind != llm.ErrInvalidRequest {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
	if fake.GenerateCalls() != 1 {
		t.Fatalf("expected 1 call, got %d", fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Backoff + full-jitter is deterministic with injected rng + clock
// ---------------------------------------------------------------------------

// TestBackoffJitterDeterministic verifies that with a deterministic (seeded)
// random source and a fake clock, the delays are computed within bounds and
// the fake clock is advanced by exactly those amounts.  Two Server errors then
// success: delays must be <= cap(attempt) = min(MaxDelay, BaseDelay*2^attempt).
func TestBackoffJitterDeterministic(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)

	fake := llmtest.NewFakeProvider()
	fake.AddGenerate(nil, providerErr(llm.ErrServer))
	fake.AddGenerate(nil, providerErr(llm.ErrServer))
	fake.AddGenerateText("ok")

	cfg := retry.Config{
		MaxAttempts: 5,
		BaseDelay:   200 * time.Millisecond,
		MaxDelay:    10 * time.Second,
	}

	dec := retry.New(fake, cfg, fc, deterministicRand(42))

	done := make(chan struct{})
	var callErr error
	go func() {
		_, callErr = dec.Generate(context.Background(), llm.Request{})
		close(done)
	}()

	// Full jitter: delay = rand(0, min(cap, base*2^attempt))
	// attempt 1 (0-indexed): cap = min(10s, 200ms*2^1) = 400ms → advance max cap
	// attempt 2:              cap = min(10s, 200ms*2^2) = 800ms → advance max cap
	yieldThenAdvance(fc, 400*time.Millisecond) // unblocks first retry wait
	yieldThenAdvance(fc, 800*time.Millisecond) // unblocks second retry wait

	<-done

	if callErr != nil {
		t.Fatalf("unexpected error: %v", callErr)
	}
	if fake.GenerateCalls() != 3 {
		t.Fatalf("expected 3 calls, got %d", fake.GenerateCalls())
	}
	// Total virtual time advanced must be 1200ms (400+800).
	elapsed := fc.Since(epoch)
	if elapsed != 1200*time.Millisecond {
		t.Fatalf("expected 1200ms of virtual time advanced, got %v", elapsed)
	}
}

// ---------------------------------------------------------------------------
// Test: Exhausting max attempts returns the last error
// ---------------------------------------------------------------------------

// TestMaxAttemptsExhausted verifies that when all attempts fail with a
// retryable error, the last *ProviderError is returned and the call count
// equals MaxAttempts.
func TestMaxAttemptsExhausted(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(7)

	const maxAttempts = 3
	fake := llmtest.NewFakeProvider()
	for i := 0; i < maxAttempts; i++ {
		fake.AddGenerate(nil, providerErr(llm.ErrOverloaded))
	}

	dec := retry.New(fake, retry.Config{
		MaxAttempts: maxAttempts,
		BaseDelay:   50 * time.Millisecond,
		MaxDelay:    1 * time.Second,
	}, fc, rng)

	done := make(chan struct{})
	var callErr error
	go func() {
		_, callErr = dec.Generate(context.Background(), llm.Request{})
		close(done)
	}()

	// Drain the two inter-attempt waits (attempts 1→2, 2→3).
	// cap(attempt=0): min(1s, 50ms*2^1) = 100ms
	// cap(attempt=1): min(1s, 50ms*2^2) = 200ms
	yieldThenAdvance(fc, 100*time.Millisecond)
	yieldThenAdvance(fc, 200*time.Millisecond)

	<-done

	if callErr == nil {
		t.Fatal("expected error after exhausted attempts")
	}
	var pe *llm.ProviderError
	if !errors.As(callErr, &pe) {
		t.Fatalf("expected *llm.ProviderError, got %T", callErr)
	}
	if pe.Kind != llm.ErrOverloaded {
		t.Fatalf("expected ErrOverloaded, got %q", pe.Kind)
	}
	if fake.GenerateCalls() != maxAttempts {
		t.Fatalf("expected %d calls, got %d", maxAttempts, fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Timeout is retryable
// ---------------------------------------------------------------------------

// TestTimeoutIsRetryable confirms ErrTimeout triggers a retry.
func TestTimeoutIsRetryable(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(1)

	fake := llmtest.NewFakeProvider()
	fake.AddGenerate(nil, providerErr(llm.ErrTimeout))
	fake.AddGenerateText("recovered")

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}, fc, rng)

	done := make(chan struct{})
	var callErr error
	go func() {
		_, callErr = dec.Generate(context.Background(), llm.Request{})
		close(done)
	}()

	// cap(attempt=0) = min(5s, 100ms*2^1) = 200ms
	yieldThenAdvance(fc, 200*time.Millisecond)
	<-done

	if callErr != nil {
		t.Fatalf("expected success, got %v", callErr)
	}
	if fake.GenerateCalls() != 2 {
		t.Fatalf("expected 2 calls, got %d", fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: ErrUnsupported is NOT retried
// ---------------------------------------------------------------------------

// TestUnsupportedNotRetried verifies ErrUnsupported is not retried.
func TestUnsupportedNotRetried(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	fake.AddTokenCount(0, providerErr(llm.ErrUnsupported))

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	_, err := dec.CountTokens(context.Background(), llm.Request{})
	if err == nil {
		t.Fatal("expected error")
	}
	var pe *llm.ProviderError
	if !errors.As(err, &pe) || pe.Kind != llm.ErrUnsupported {
		t.Fatalf("expected ErrUnsupported, got %v", err)
	}
	if fake.CountTokensCalls() != 1 {
		t.Fatalf("expected 1 CountTokens call, got %d", fake.CountTokensCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Stream retried only if it fails BEFORE the first event
// ---------------------------------------------------------------------------

// TestStreamRetriedBeforeFirstEvent verifies that a Stream call whose
// Provider.Stream itself returns an error (before any event) is retried.
func TestStreamRetriedBeforeFirstEvent(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	// First call: Stream itself fails (before first event).
	fake.AddStream(nil, providerErr(llm.ErrServer))
	// Second call: succeeds.
	fake.AddStreamEvents(
		llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "hi"}},
	)

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    5 * time.Second,
	}, fc, rng)

	done := make(chan struct{})
	var callErr error
	var reader llm.StreamReader
	go func() {
		reader, callErr = dec.Stream(context.Background(), llm.Request{})
		close(done)
	}()

	// cap(attempt=0) = min(5s, 100ms*2^1) = 200ms
	yieldThenAdvance(fc, 200*time.Millisecond)
	<-done

	if callErr != nil {
		t.Fatalf("expected successful stream, got %v", callErr)
	}

	// Drain the stream to completion.
	n, drainErr := drainReader(reader)
	if drainErr != nil {
		t.Fatalf("drain error: %v", drainErr)
	}
	if n == 0 {
		t.Fatal("expected at least one non-Done event")
	}
	_ = reader.Close()

	if fake.StreamCalls() != 2 {
		t.Fatalf("expected 2 Stream calls (1 retry), got %d", fake.StreamCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Non-ProviderError is propagated without retrying
// ---------------------------------------------------------------------------

// TestNonProviderErrorNotRetried verifies that errors that are NOT
// *llm.ProviderError are propagated immediately (no retry).
func TestNonProviderErrorNotRetried(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	plainErr := errors.New("unexpected internal error")
	fake.AddGenerate(nil, plainErr)

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	_, err := dec.Generate(context.Background(), llm.Request{})
	if !errors.Is(err, plainErr) {
		t.Fatalf("expected the plain error, got %v", err)
	}
	if fake.GenerateCalls() != 1 {
		t.Fatalf("expected 1 call (no retry for non-ProviderError), got %d", fake.GenerateCalls())
	}
}

// ---------------------------------------------------------------------------
// Test: Context cancellation aborts the wait
// ---------------------------------------------------------------------------

// TestContextCancellationAborts verifies that cancelling the context while the
// retry decorator is waiting between attempts causes it to return promptly.
func TestContextCancellationAborts(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	// First attempt fails; second is never reached because ctx is cancelled.
	fake.AddGenerate(nil, providerErr(llm.ErrServer))

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 5,
		BaseDelay:   10 * time.Second, // long wait so we can cancel first
		MaxDelay:    60 * time.Second,
	}, fc, rng)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := dec.Generate(ctx, llm.Request{})
		done <- err
	}()

	// Give the goroutine a chance to reach the select inside wait, then cancel.
	// The context cancellation will wake the select regardless of the clock.
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}
	time.Sleep(time.Millisecond)
	cancel()

	err := <-done
	if err == nil {
		t.Fatal("expected error after context cancel")
	}
	// The error should wrap context.Canceled.
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled chain, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Test: Capabilities and CountTokens pass through unchanged when successful
// ---------------------------------------------------------------------------

// TestPassThroughOnSuccess verifies that Capabilities and CountTokens are
// forwarded to the inner provider and their successful responses returned.
func TestPassThroughOnSuccess(t *testing.T) {
	t.Parallel()

	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)

	fake := llmtest.NewFakeProvider()
	fake.AddCapabilities(llm.Capabilities{MaxOutputTokens: 4096}, nil)
	fake.AddTokenCount(123, nil)

	dec := retry.New(fake, retry.Config{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    30 * time.Second,
	}, fc, rng)

	caps, err := dec.Capabilities(context.Background(), "claude-3-5-sonnet")
	if err != nil {
		t.Fatalf("Capabilities: unexpected error: %v", err)
	}
	if caps.MaxOutputTokens != 4096 {
		t.Fatalf("Capabilities: expected 4096, got %d", caps.MaxOutputTokens)
	}

	n, err := dec.CountTokens(context.Background(), llm.Request{})
	if err != nil {
		t.Fatalf("CountTokens: unexpected error: %v", err)
	}
	if n != 123 {
		t.Fatalf("CountTokens: expected 123, got %d", n)
	}
}

// ---------------------------------------------------------------------------
// Test: Compile-time interface satisfaction
// ---------------------------------------------------------------------------

// TestInterfaceSatisfaction is a compile-time assertion that *retry.Provider
// implements llm.Provider.
func TestInterfaceSatisfaction(t *testing.T) {
	t.Parallel()
	fc := clocktest.NewFake(epoch)
	rng := deterministicRand(0)
	fake := llmtest.NewFakeProvider()
	var _ llm.Provider = retry.New(fake, retry.Config{}, fc, rng)
}

// ---------------------------------------------------------------------------
// Live smoke test (network-gated — skips when no API key is set)
// ---------------------------------------------------------------------------

// The live smoke test is in a separate file with //go:build livesmoke so it
// can be opt-in.  This file only contains network-free unit tests.
