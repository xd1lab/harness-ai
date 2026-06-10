// Package clock defines the cross-service [Clock] port that abstracts the passage
// of time, and a trivial system implementation backed by the standard library.
//
// # Why a port
//
// The architecture mandates a cross-cutting determinism rule (architecture §5,
// "Cross-cutting determinism rule"): every component that sleeps, times out on its
// own, or expires state takes an injected Clock through its ports.go. No domain or
// app code calls time.Now, time.After, or time.NewTimer directly. This is enforced
// by a forbidigo/depguard rule. Injecting a Clock lets the agent loop's relay-stall
// deadline (architecture §9.4), the model-gateway's retry backoff schedule
// (architecture §4.4), lease TTL/heartbeat timing (architecture §9.6), and sandbox
// idle/absolute TTLs (architecture §10.6) all be asserted deterministically with a
// fake clock instead of real wall-clock sleeps.
//
// # Relationship to llm.Clock
//
// The platform Clock is the canonical, richer time port used throughout the
// services (it adds NewTimer and Since on top of Now/After). The narrower
// [github.com/boltrope/boltrope/internal/platform/llm.Clock] is a kernel-local
// contract the gateway's retry policy is written against; a Clock defined here
// satisfies it structurally (same Now/After signatures), so production wiring can
// pass one [Clock] everywhere.
//
// # Purity
//
// This package is contract-only except for the deliberately trivial [System]
// implementation, which is a thin pass-through to the standard library (a permitted
// "real Clock" system impl). It imports only the standard library.
package clock

import "time"

// Clock abstracts the passage of time so that time-dependent behavior is
// deterministic under test. Implementations must be safe for concurrent use by
// multiple goroutines.
type Clock interface {
	// Now returns the current time as observed by this clock. Production wiring
	// returns wall-clock time; a fake clock returns its controlled virtual time.
	Now() time.Time

	// Since returns the time elapsed since t, equivalent to Now().Sub(t). It is a
	// convenience for measuring durations against this clock's notion of now.
	Since(t time.Time) time.Duration

	// After returns a channel that delivers a single value after at least d has
	// elapsed on this clock, analogous to [time.After]. A non-positive d delivers
	// as soon as possible. Callers that may abandon the wait should prefer
	// [Clock.NewTimer] so the underlying resource can be released via
	// [Timer.Stop].
	After(d time.Duration) <-chan time.Time

	// NewTimer returns a [Timer] that fires once after at least d has elapsed on
	// this clock, analogous to [time.NewTimer]. Unlike After it can be stopped to
	// release resources, which matters for the many bounded deadlines (relay
	// stall, SIGTERM→SIGKILL grace, lease TTL) that are frequently cancelled
	// before they fire.
	NewTimer(d time.Duration) Timer
}

// Timer represents a single-shot timer obtained from [Clock.NewTimer]. It mirrors
// the relevant surface of [time.Timer] without exposing the concrete struct, so a
// fake clock can supply its own timer.
type Timer interface {
	// C is the channel on which the timer delivers the current time when it fires.
	// It delivers at most once for a single-shot timer.
	C() <-chan time.Time

	// Stop prevents the timer from firing. It reports whether the call stopped the
	// timer before it fired: true if the timer was active, false if it had already
	// fired or been stopped. As with [time.Timer.Stop], a caller that did not
	// observe a fire and receives false may need to drain C.
	Stop() bool

	// Reset changes the timer to fire after at least d has elapsed from now,
	// analogous to [time.Timer.Reset]. It reports whether the timer had been
	// active. Reset should be invoked only on a stopped or already-fired timer
	// whose channel has been drained.
	Reset(d time.Duration) bool
}

// System is the real [Clock] backed by the standard library's time package. It is
// the single concrete implementation in this otherwise contract-only package,
// provided so production wiring need not redefine a trivial adapter; tests inject a
// fake clock instead. The zero value is ready to use.
type System struct{}

// Now returns the current wall-clock time via [time.Now].
func (System) Now() time.Time { return time.Now() }

// Since returns the elapsed time since t via [time.Since].
func (System) Since(t time.Time) time.Duration { return time.Since(t) }

// After returns the channel from [time.After](d).
func (System) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer returns a [Timer] wrapping the standard library [time.NewTimer](d).
func (System) NewTimer(d time.Duration) Timer { return &systemTimer{t: time.NewTimer(d)} }

// systemTimer adapts [time.Timer] to the [Timer] interface.
type systemTimer struct {
	t *time.Timer
}

// C returns the underlying [time.Timer]'s channel.
func (s *systemTimer) C() <-chan time.Time { return s.t.C }

// Stop delegates to the underlying [time.Timer.Stop].
func (s *systemTimer) Stop() bool { return s.t.Stop() }

// Reset delegates to the underlying [time.Timer.Reset].
func (s *systemTimer) Reset(d time.Duration) bool { return s.t.Reset(d) }

// Compile-time assertions that System and its timer satisfy the ports.
var (
	_ Clock = System{}
	_ Timer = (*systemTimer)(nil)
)
