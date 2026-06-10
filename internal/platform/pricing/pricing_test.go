package pricing_test

import (
	"errors"
	"testing"

	"github.com/xd1lab/harness-ai/internal/platform/llm"
	"github.com/xd1lab/harness-ai/internal/platform/pricing"
)

// TestCost_KnownModel_CacheReadWriteSplit verifies that Cost computes the
// correct USD amount for a known model whose Usage includes both cache-read and
// cache-write tokens in addition to standard input/output tokens.
//
// The expected value is derived from the rates documented in pricing.go.  If
// rates are updated the golden value here must be updated to match.
func TestCost_KnownModel_CacheReadWriteSplit(t *testing.T) {
	t.Parallel()

	// Use claude-sonnet-4-6 whose listed rates (per 1M tokens) are:
	//   input       = $3.00  →  $0.000003  per token
	//   output      = $15.00 →  $0.000015  per token
	//   cache-read  = $0.30  →  $0.0000003 per token
	//   cache-write = $3.75  →  $0.00000375 per token
	u := llm.Usage{
		InputTokens:      1_000_000, // $3.00
		OutputTokens:     100_000,   // $1.50
		CacheReadTokens:  500_000,   // $0.15
		CacheWriteTokens: 200_000,   // $0.75
		// ReasoningTokens is a subset of OutputTokens; not billed separately here.
		ReasoningTokens: 0,
	}
	// expected = 3.00 + 1.50 + 0.15 + 0.75 = 5.40
	const wantUSD = 5.40

	got, err := pricing.Cost("claude-sonnet-4-6", u)
	if err != nil {
		t.Fatalf("Cost: unexpected error: %v", err)
	}
	// Allow a tiny floating-point rounding tolerance.
	const epsilon = 1e-9
	if diff := got - wantUSD; diff > epsilon || diff < -epsilon {
		t.Errorf("Cost = %.10f, want %.10f (diff %.2e)", got, wantUSD, diff)
	}
}

// TestCost_UnknownModel_ReturnsTypedError verifies that Cost returns a
// *pricing.UnknownModelError (and never a silent zero/guess) when the model id
// is not in the table.
func TestCost_UnknownModel_ReturnsTypedError(t *testing.T) {
	t.Parallel()

	_, err := pricing.Cost("does-not-exist-v99", llm.Usage{InputTokens: 100})
	if err == nil {
		t.Fatal("Cost with unknown model: expected error, got nil")
	}

	var ume *pricing.UnknownModelError
	if !errors.As(err, &ume) {
		t.Fatalf("Cost with unknown model: expected *pricing.UnknownModelError, got %T: %v", err, err)
	}
	if ume.Model != "does-not-exist-v99" {
		t.Errorf("UnknownModelError.Model = %q, want %q", ume.Model, "does-not-exist-v99")
	}
}

// TestCost_ZeroUsage_KnownModel verifies that a known model with all-zero
// token counts returns $0.00 without error.
func TestCost_ZeroUsage_KnownModel(t *testing.T) {
	t.Parallel()

	got, err := pricing.Cost("gpt-5.4", llm.Usage{})
	if err != nil {
		t.Fatalf("Cost with zero usage: unexpected error: %v", err)
	}
	if got != 0.0 {
		t.Errorf("Cost with zero usage = %v, want 0", got)
	}
}

// TestCost_AllModelFamilies_NoError ensures that every model id seeded in the
// default table resolves without error when called with a non-zero Usage.
func TestCost_AllModelFamilies_NoError(t *testing.T) {
	t.Parallel()

	models := []string{
		// Anthropic — current + legacy-active Claude models
		"claude-fable-5",
		"claude-opus-4-8",
		"claude-opus-4-7",
		"claude-opus-4-6",
		"claude-opus-4-5",
		"claude-opus-4-1",
		"claude-sonnet-4-6",
		"claude-sonnet-4-5",
		"claude-haiku-4-5",
		"claude-haiku-4-5-20251001",
		// OpenAI — GPT-5.x
		"gpt-5.5",
		"gpt-5.4",
		"gpt-5.4-mini",
		"gpt-5.4-nano",
		// Google Gemini
		"gemini-3.5-flash",
		"gemini-3.1-pro-preview",
		"gemini-3.1-flash-lite",
		"gemini-2.5-pro",
		"gemini-2.5-flash",
		"gemini-2.5-flash-lite",
	}
	u := llm.Usage{InputTokens: 1, OutputTokens: 1}
	for _, m := range models {
		m := m
		t.Run(m, func(t *testing.T) {
			t.Parallel()
			_, err := pricing.Cost(m, u)
			if err != nil {
				t.Errorf("Cost(%q): unexpected error: %v", m, err)
			}
		})
	}
}
