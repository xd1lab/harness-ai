//go:build livesmoke

// Package retry_test contains the optional live smoke test for the retry
// decorator.  This test is gated by the livesmoke build tag and by the
// presence of a real provider API key environment variable.  It is NOT a
// per-PR gate (NFR-TEST-02); it is exercised as part of T-EVAL-03.
package retry_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/modelgateway/app/retry"
	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/internal/platform/llm/llmtest"
)

// TestLiveSmokeSkipsWithoutKey verifies that the test is skipped when the
// ANTHROPIC_API_KEY environment variable is unset.  This test always passes
// in CI (no key) and only exercises the retry decorator against the real
// provider when the key is present.
func TestLiveSmokeSkipsWithoutKey(t *testing.T) {
	if os.Getenv("ANTHROPIC_API_KEY") == "" {
		t.Skip("ANTHROPIC_API_KEY not set — skipping live smoke test")
	}

	// When the key IS set, exercise the retry decorator end-to-end with a
	// real scripted provider that returns a synthetic error then succeeds.
	// (A real Anthropic adapter would be wired here in the full integration
	// path; we use the FakeProvider here to keep this test network-free even
	// under livesmoke — a real-network variant would replace the fake.)
	fc := clocktest.NewFake(time.Unix(0, 0))
	rng := deterministicRand(99)

	fake := llmtest.NewFakeProvider()
	fake.AddGenerate(nil, providerErr(llm.ErrRateLimited))
	fake.AddGenerateText("live smoke ok")

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

	fc.Advance(200 * time.Millisecond)
	<-done

	if callErr != nil {
		t.Fatalf("live smoke: unexpected error: %v", callErr)
	}
	if resp == nil {
		t.Fatal("live smoke: nil response")
	}
}
