package clocktest_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
)

func TestFake_NowReturnsInitial(t *testing.T) {
	start := time.Unix(1_000_000, 0)
	fc := clocktest.NewFake(start)
	assert.Equal(t, start, fc.Now())
}

func TestFake_AdvanceMoves(t *testing.T) {
	start := time.Unix(0, 0)
	fc := clocktest.NewFake(start)
	fc.Advance(10 * time.Second)
	assert.Equal(t, start.Add(10*time.Second), fc.Now())
}

func TestFake_Since(t *testing.T) {
	start := time.Unix(100, 0)
	fc := clocktest.NewFake(start)
	fc.Advance(3 * time.Second)
	assert.Equal(t, 3*time.Second, fc.Since(start))
}

// TestFake_AfterDoesNotFireBeforeAdvance is the primary determinism requirement:
// After(5s) must NOT deliver until Advance(>=5s).
func TestFake_AfterDoesNotFireBeforeAdvance(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	ch := fc.After(5 * time.Second)

	// Advance by less than the deadline — must not fire.
	fc.Advance(4 * time.Second)
	select {
	case <-ch:
		t.Fatal("After(5s) fired after only 4s of virtual time")
	default:
		// good: not fired
	}

	// Advance to exactly the deadline — must fire now.
	fc.Advance(1 * time.Second)
	select {
	case got := <-ch:
		assert.Equal(t, time.Unix(5, 0), got)
	default:
		t.Fatal("After(5s) did not fire after exactly 5s of virtual time")
	}
}

// TestFake_AfterFiresPastDeadline: advancing past the deadline also fires.
func TestFake_AfterFiresPastDeadline(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	ch := fc.After(3 * time.Second)
	fc.Advance(10 * time.Second) // well past deadline
	select {
	case <-ch:
		// good
	default:
		t.Fatal("After(3s) did not fire after 10s advance")
	}
}

// TestFake_NewTimer_StopPreventsfire verifies Stop prevents delivery.
func TestFake_NewTimer_StopPreventsfire(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	tmr := fc.NewTimer(5 * time.Second)
	stopped := tmr.Stop()
	require.True(t, stopped, "Stop should return true for active timer")
	fc.Advance(10 * time.Second)
	select {
	case <-tmr.C():
		t.Fatal("stopped timer still fired")
	default:
		// good
	}
}

// TestFake_ZeroDurationFiresImmediately: After(0) or After(-1) fires without Advance.
func TestFake_ZeroDurationFiresImmediately(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	ch := fc.After(0)
	select {
	case <-ch:
		// good
	default:
		t.Fatal("After(0) did not fire immediately")
	}
}

// TestFake_MultipleTimers: multiple independent timers fire at the right times.
func TestFake_MultipleTimers(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	ch1 := fc.After(2 * time.Second)
	ch2 := fc.After(5 * time.Second)

	fc.Advance(2 * time.Second)
	select {
	case <-ch1:
	default:
		t.Fatal("ch1 (2s) did not fire after 2s")
	}
	select {
	case <-ch2:
		t.Fatal("ch2 (5s) fired too early")
	default:
	}

	fc.Advance(3 * time.Second)
	select {
	case <-ch2:
	default:
		t.Fatal("ch2 (5s) did not fire after 5s total")
	}
}

// TestFake_SetDirectly: Set moves to a specific time and fires timers.
func TestFake_SetDirectly(t *testing.T) {
	fc := clocktest.NewFake(time.Unix(0, 0))
	ch := fc.After(10 * time.Second)
	fc.Set(time.Unix(10, 0))
	select {
	case <-ch:
	default:
		t.Fatal("After(10s) did not fire after Set(10s)")
	}
}
