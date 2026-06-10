// Package llm_test covers the kernel package's behavioral surface: the
// normalized ProviderError (Error/Unwrap/Retryable), the open StopReason set's
// terminality predicate, and the SystemClock adapter. Everything here is
// contract-level — no provider, no network.
package llm_test

import (
	"errors"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/platform/llm"
)

// TestProviderError_ErrorString pins the rendered message with and without a
// wrapped raw cause, since operators grep logs for the "llm: <kind>" prefix.
func TestProviderError_ErrorString(t *testing.T) {
	withRaw := &llm.ProviderError{Kind: llm.ErrServer, Raw: errors.New("boom")}
	if got, want := withRaw.Error(), "llm: server: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	bare := &llm.ProviderError{Kind: llm.ErrAuth}
	if got, want := bare.Error(), "llm: auth"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// TestProviderError_Unwrap asserts the raw provider error is recoverable via
// errors.Is/errors.As through the normalized wrapper (ADR-0004: normalize the
// kind, preserve the cause).
func TestProviderError_Unwrap(t *testing.T) {
	cause := errors.New("HTTP 529 upstream")
	pe := &llm.ProviderError{Kind: llm.ErrOverloaded, Raw: cause}

	if !errors.Is(pe, cause) {
		t.Error("errors.Is must reach the wrapped raw cause")
	}
	if pe.Unwrap() != cause { //nolint:errorlint // asserting Unwrap identity itself
		t.Error("Unwrap must return the raw cause verbatim")
	}

	var got *llm.ProviderError
	wrapped := errors.Join(errors.New("outer"), pe)
	if !errors.As(wrapped, &got) || got.Kind != llm.ErrOverloaded {
		t.Error("errors.As must recover the *ProviderError from a wrapping chain")
	}

	if (&llm.ProviderError{Kind: llm.ErrTimeout}).Unwrap() != nil {
		t.Error("Unwrap of an error with no raw cause must be nil")
	}
}

// TestProviderError_Retryable is the full kind→retryability table: only the
// four transient kinds are retryable; client/auth/unsupported and unknown
// kinds are not (the harness-level retry policy keys on exactly this).
func TestProviderError_Retryable(t *testing.T) {
	cases := []struct {
		kind llm.ErrorKind
		want bool
	}{
		{llm.ErrRateLimited, true},
		{llm.ErrOverloaded, true},
		{llm.ErrServer, true},
		{llm.ErrTimeout, true},
		{llm.ErrInvalidRequest, false},
		{llm.ErrAuth, false},
		{llm.ErrUnsupported, false},
		{llm.ErrorKind("future_unknown_kind"), false}, // fail-safe: unknown is not retryable
		{llm.ErrorKind(""), false},
	}
	for _, tc := range cases {
		pe := &llm.ProviderError{Kind: tc.kind}
		if got := pe.Retryable(); got != tc.want {
			t.Errorf("ProviderError{Kind:%q}.Retryable() = %v, want %v", tc.kind, got, tc.want)
		}
	}
}

// TestStopReason_IsTerminal asserts Pause is the SOLE non-terminal reason in
// the frozen contract; every other reason — including the open-set StopOther
// and unknown future strings — is terminal (architecture §11.3).
func TestStopReason_IsTerminal(t *testing.T) {
	cases := []struct {
		r    llm.StopReason
		want bool
	}{
		{llm.StopEnd, true},
		{llm.StopMaxTokens, true},
		{llm.StopToolUse, true},
		{llm.StopStopSequence, true},
		{llm.StopContentFilter, true},
		{llm.StopRefusal, true},
		{llm.StopContextWindowExceeded, true},
		{llm.StopOther, true},
		{llm.StopReason("provider_specific_novelty"), true}, // open set: unknown ≠ non-terminal
		{llm.Pause, false},
	}
	for _, tc := range cases {
		if got := tc.r.IsTerminal(); got != tc.want {
			t.Errorf("StopReason(%q).IsTerminal() = %v, want %v", tc.r, got, tc.want)
		}
	}
}

// TestSystemClock covers the one concrete implementation in the contract
// package: Now yields a live, monotonically non-decreasing time and After
// delivers once the duration elapses. (The production retry policy is tested
// against a fake clock; this only proves the real adapter is wired to the
// standard library.)
func TestSystemClock(t *testing.T) {
	var c llm.Clock = llm.SystemClock{}

	t1 := c.Now()
	if t1.IsZero() {
		t.Fatal("SystemClock.Now returned the zero time")
	}
	t2 := c.Now()
	if t2.Before(t1) {
		t.Errorf("SystemClock.Now went backwards: %v then %v", t1, t2)
	}

	select {
	case tick := <-c.After(time.Millisecond):
		if tick.IsZero() {
			t.Error("SystemClock.After delivered the zero time")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("SystemClock.After(1ms) did not deliver within 10s")
	}
}
