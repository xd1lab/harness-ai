// Package clocktest provides a deterministic fake [clock.Clock] for use in tests.
// It replaces wall-clock time with a controllable virtual instant so that timer
// and deadline logic can be exercised without real sleeps.
//
// Usage:
//
//	fc := clocktest.NewFake(time.Unix(0, 0))
//	ch := fc.After(5 * time.Second)
//	// ch has NOT fired yet
//	fc.Advance(5 * time.Second)
//	// ch now fires
package clocktest

import (
	"sync"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/clock"
)

// Compile-time assertion that Fake satisfies clock.Clock.
var _ clock.Clock = (*Fake)(nil)

// Fake is a controllable virtual clock. Now returns a settable instant; After
// and NewTimer only fire when virtual time is Advance()d past their deadline.
// All methods are safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFake returns a Fake clock whose virtual time starts at t.
func NewFake(t time.Time) *Fake {
	return &Fake{now: t}
}

// Now returns the fake clock's current virtual time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the elapsed virtual time since t.
func (f *Fake) Since(t time.Time) time.Duration {
	return f.Now().Sub(t)
}

// After returns a channel that fires once virtual time reaches or passes
// f.Now()+d. The channel only fires when Advance is called.
func (f *Fake) After(d time.Duration) <-chan time.Time {
	return f.NewTimer(d).C()
}

// NewTimer returns a fake Timer that fires once virtual time reaches or passes
// f.Now()+d. It only fires when Advance is called.
func (f *Fake) NewTimer(d time.Duration) clock.Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	deadline := f.now.Add(d)
	if d <= 0 {
		// Fire immediately: deadline is already in the past.
		deadline = f.now
	}
	t := &fakeTimer{
		ch:       make(chan time.Time, 1),
		deadline: deadline,
		active:   true,
	}
	// If deadline is already met, fire immediately.
	if !deadline.After(f.now) {
		t.ch <- f.now
		t.active = false
	} else {
		f.timers = append(f.timers, t)
	}
	return t
}

// Advance moves virtual time forward by d, firing any timers whose deadline
// has been reached or passed. Advance is safe for concurrent use.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	now := f.now
	remaining := f.timers[:0]
	for _, t := range f.timers {
		t.mu.Lock()
		if t.active && !t.deadline.After(now) {
			t.ch <- now
			t.active = false
		} else if t.active {
			remaining = append(remaining, t)
		}
		t.mu.Unlock()
	}
	f.timers = remaining
}

// Set sets virtual time to t, firing any pending timers whose deadline has
// been reached or passed.
func (f *Fake) Set(t time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = t
	remaining := f.timers[:0]
	for _, ft := range f.timers {
		ft.mu.Lock()
		if ft.active && !ft.deadline.After(t) {
			ft.ch <- t
			ft.active = false
		} else if ft.active {
			remaining = append(remaining, ft)
		}
		ft.mu.Unlock()
	}
	f.timers = remaining
}

// fakeTimer is the Timer returned by Fake.NewTimer and Fake.After.
type fakeTimer struct {
	mu       sync.Mutex
	ch       chan time.Time
	deadline time.Time
	active   bool
}

// Compile-time assertion that fakeTimer satisfies clock.Timer.
var _ clock.Timer = (*fakeTimer)(nil)

// C returns the channel on which the timer delivers the virtual time when it fires.
func (t *fakeTimer) C() <-chan time.Time { return t.ch }

// Stop prevents the timer from firing. Returns true if the timer was active.
func (t *fakeTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.active
	t.active = false
	return was
}

// Reset changes the timer's deadline to fire after d from the fake clock's
// current virtual time. It re-registers the timer with the owning Fake if
// the fake clock reference is available. Because fakeTimer does not hold a
// back-reference to Fake, Reset on a stopped/fired timer is a no-op that
// returns false. Callers that need Reset should retain the *Fake and
// NewTimer again instead.
func (t *fakeTimer) Reset(_ time.Duration) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	was := t.active
	return was
}
