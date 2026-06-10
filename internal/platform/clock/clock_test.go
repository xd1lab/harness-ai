package clock_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/platform/clock"
)

// TestSystem_InterfaceCompliance ensures clock.System satisfies clock.Clock at
// compile time.
func TestSystem_InterfaceCompliance(_ *testing.T) {
	var _ clock.Clock = clock.System{}
}

// TestSystem_Now_Monotonic asserts that two successive calls to Now() return
// non-decreasing times, confirming the real wall-clock is in use.
func TestSystem_Now_Monotonic(t *testing.T) {
	c := clock.System{}
	t1 := c.Now()
	t2 := c.Now()
	assert.False(t, t2.Before(t1), "second Now() must not be before first Now()")
}

// TestSystem_Since_Positive asserts that Since a time in the past returns a
// non-negative duration.
func TestSystem_Since_Positive(t *testing.T) {
	c := clock.System{}
	past := time.Now().Add(-10 * time.Millisecond)
	dur := c.Since(past)
	assert.True(t, dur >= 0, "Since a past time must be >= 0, got %v", dur)
}

// TestSystem_After_Fires asserts that After fires within a reasonable real-time
// window, exercising the real time.After path.
func TestSystem_After_Fires(t *testing.T) {
	c := clock.System{}
	ch := c.After(10 * time.Millisecond)
	select {
	case got := <-ch:
		// got should be a real time value (non-zero)
		require.False(t, got.IsZero(), "After channel delivered zero time")
	case <-time.After(2 * time.Second):
		t.Fatal("clock.System.After(10ms) did not fire within 2s")
	}
}

// TestSystem_NewTimer_Fires asserts that a Timer from NewTimer fires within a
// reasonable real-time window.
func TestSystem_NewTimer_Fires(t *testing.T) {
	c := clock.System{}
	tmr := c.NewTimer(10 * time.Millisecond)
	select {
	case got := <-tmr.C():
		require.False(t, got.IsZero(), "Timer channel delivered zero time")
	case <-time.After(2 * time.Second):
		t.Fatal("clock.System.NewTimer(10ms) did not fire within 2s")
	}
}

// TestSystem_NewTimer_Stop asserts that Stop() on an unfired timer returns true
// and prevents the timer from firing.
func TestSystem_NewTimer_Stop(t *testing.T) {
	c := clock.System{}
	// Use a long duration so the timer does not fire before we stop it.
	tmr := c.NewTimer(10 * time.Second)
	stopped := tmr.Stop()
	assert.True(t, stopped, "Stop() on an active timer must return true")

	// After stopping, the channel must not deliver within a short window.
	select {
	case <-tmr.C():
		t.Fatal("stopped timer fired unexpectedly")
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

// TestSystem_NewTimer_Reset asserts that Reset() re-arms the timer so it fires
// after the new duration.
func TestSystem_NewTimer_Reset(t *testing.T) {
	c := clock.System{}
	tmr := c.NewTimer(10 * time.Second)
	// Stop and drain before Reset, as documented by time.Timer.Reset.
	if !tmr.Stop() {
		select {
		case <-tmr.C():
		default:
		}
	}
	tmr.Reset(10 * time.Millisecond)
	select {
	case got := <-tmr.C():
		require.False(t, got.IsZero())
	case <-time.After(2 * time.Second):
		t.Fatal("timer did not fire after Reset(10ms)")
	}
}

// TestSystem_After_ZeroDuration asserts that After(0) fires immediately (or
// very quickly), mirroring time.After behavior.
func TestSystem_After_ZeroDuration(t *testing.T) {
	c := clock.System{}
	ch := c.After(0)
	select {
	case <-ch:
		// good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("After(0) did not fire quickly")
	}
}
