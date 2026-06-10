package runtime

import (
	"context"
	"strings"
	"time"
)

// This file implements the sandbox lifecycle manager (architecture §10.6): the
// idle/absolute TTL enforcement and the session-status reconciliation reaper that
// reclaims sandboxes whose session is finished/failed/abandoned. The max-live cap
// (backpressure) is enforced inline in [Runtime.Create].

// reapReason explains why the reaper reclaimed a sandbox (for logging/metrics by the
// caller; returned from [Runtime.reapOnce]).
type reapReason string

const (
	reapIdleTTL        reapReason = "idle_ttl"
	reapAbsoluteTTL    reapReason = "absolute_ttl"
	reapSessionEnded   reapReason = "session_ended"
	reapSessionMissing reapReason = "session_missing"
)

// reapEvent records one sandbox reclamation performed by [Runtime.reapOnce].
type reapEvent struct {
	SessionID string
	Reason    reapReason
}

// RunReaper runs the reconciliation loop until ctx is cancelled, sweeping every
// ReapInterval. It is the long-lived lifecycle manager goroutine; production wiring
// starts it once after [New]. Each sweep reaps idle/absolute-TTL-expired sandboxes
// and (when a [SessionStatusFunc] is configured) sandboxes whose session has ended.
// Unit tests call [Runtime.reapOnce] directly with a fake clock instead.
func (r *Runtime) RunReaper(ctx context.Context) {
	ticker := r.clk.NewTimer(r.cfg.ReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			r.reapOnce(ctx)
			ticker.Reset(r.cfg.ReapInterval)
		}
	}
}

// reapOnce performs a single reconciliation sweep and returns the reclamations it
// made. A sandbox is reaped when:
//
//   - its session status is finished/failed/abandoned (when a SessionStatusFunc is
//     configured), or its status lookup says the session no longer exists;
//   - it has been idle longer than IdleTTL; or
//   - its total lifetime exceeds AbsoluteTTL.
//
// A sandbox whose status cannot be classified ([SessionUnknown] or a lookup error)
// is RETAINED — the reaper never reclaims a sandbox it cannot positively classify as
// ended (fail-safe), falling back to TTL bounds. reapOnce is safe for concurrent use
// and is the unit-testable core of [Runtime.RunReaper].
func (r *Runtime) reapOnce(ctx context.Context) []reapEvent {
	now := r.clk.Now()

	// Snapshot the live set under lock; classify without holding the lock so a slow
	// status lookup does not block Create/Destroy.
	r.mu.Lock()
	type entry struct {
		id   string
		c    *container
		idle time.Time
		born time.Time
	}
	entries := make([]entry, 0, len(r.live))
	for id, c := range r.live {
		entries = append(entries, entry{id: id, c: c, idle: c.idleSince(), born: c.createdAt()})
	}
	r.mu.Unlock()

	var reaped []reapEvent
	for _, e := range entries {
		reason, reap := r.classify(ctx, e.id, now, e.idle, e.born)
		if !reap {
			continue
		}
		// Destroy is idempotent and removes the entry from r.live.
		_ = r.Destroy(ctx, e.id)
		reaped = append(reaped, reapEvent{SessionID: e.id, Reason: reason})
	}
	return reaped
}

// classify decides whether the sandbox for sessionID should be reaped now and why.
func (r *Runtime) classify(ctx context.Context, sessionID string, now, idle, born time.Time) (reapReason, bool) {
	// Session-status reconciliation takes precedence: an ended session is reclaimed
	// immediately regardless of TTL (architecture §10.6).
	if r.status != nil {
		st, err := r.status(ctx, sessionID)
		switch {
		case err != nil:
			// Cannot classify → retain and fall through to TTL checks (fail-safe).
		case st == SessionFinished || st == SessionFailed || st == SessionAbandoned:
			return reapSessionEnded, true
		}
	}

	// Absolute TTL: total lifetime cap.
	if r.cfg.AbsoluteTTL > 0 && now.Sub(born) >= r.cfg.AbsoluteTTL {
		return reapAbsoluteTTL, true
	}
	// Idle TTL: time since last Exec.
	if r.cfg.IdleTTL > 0 && now.Sub(idle) >= r.cfg.IdleTTL {
		return reapIdleTTL, true
	}
	return "", false
}

// ReconcileOrphans reaps containers this runtime manages on the host (by label)
// that are NOT tracked in the live set — i.e. orphaned by a prior process crash —
// when their session is finished/failed/abandoned or no longer exists. It is the
// crash-recovery half of the reaper (architecture §10.6): a fresh process lists the
// managed containers via `docker ps` and reclaims the dead ones, leaving live
// in-process sandboxes untouched. When no [SessionStatusFunc] is configured every
// untracked orphan is removed (no session authority to consult).
//
// It returns the session ids it reclaimed. Safe for concurrent use.
func (r *Runtime) ReconcileOrphans(ctx context.Context) ([]string, error) {
	res, err := r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: listManagedArgs()})
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	tracked := make(map[string]struct{}, len(r.live))
	for _, c := range r.live {
		tracked[c.name] = struct{}{}
	}
	r.mu.Unlock()

	var reclaimed []string
	for _, line := range strings.Split(strings.TrimSpace(string(res.Stdout)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, sessionID, _ := strings.Cut(line, "\t")
		name = strings.TrimSpace(name)
		sessionID = strings.TrimSpace(sessionID)
		if name == "" {
			continue
		}
		if _, live := tracked[name]; live {
			continue // owned by a live sandbox in this process
		}
		// Orphan: consult session status (if available). Reap when ended/missing or
		// when there is no status authority to say it is still active.
		if r.status != nil && sessionID != "" {
			st, sErr := r.status(ctx, sessionID)
			if sErr == nil && st == SessionActive {
				continue // a live session whose sandbox we simply do not track yet
			}
		}
		_, _ = r.runner.Run(ctx, cmdSpec{Name: r.cfg.DockerBin, Args: removeArgs(name)})
		reclaimed = append(reclaimed, sessionID)
	}
	return reclaimed, nil
}

// liveCount returns the number of currently live sandboxes (for tests/metrics).
func (r *Runtime) liveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.live)
}
