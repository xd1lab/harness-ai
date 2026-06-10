package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

func TestReaper_IdleTTL(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.IdleTTL = 10 * time.Minute
	cfg.AbsoluteTTL = time.Hour
	cfg.MaxLive = 8
	r, err := New(cfg, WithClock(fc), withRunner(fr))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := r.Create(ctx, "s1", app.EgressPolicy{}); err != nil {
		t.Fatal(err)
	}

	// Not yet idle.
	fc.Advance(9 * time.Minute)
	if ev := r.reapOnce(ctx); len(ev) != 0 {
		t.Fatalf("reaped too early: %v", ev)
	}
	// Cross the idle TTL.
	fc.Advance(2 * time.Minute)
	ev := r.reapOnce(ctx)
	if len(ev) != 1 || ev[0].Reason != reapIdleTTL || ev[0].SessionID != "s1" {
		t.Fatalf("idle reap = %+v, want one idle_ttl for s1", ev)
	}
	if r.liveCount() != 0 {
		t.Errorf("liveCount after idle reap = %d, want 0", r.liveCount())
	}
}

func TestReaper_IdleTTL_TouchedByExecResetsClock(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.IdleTTL = 10 * time.Minute
	cfg.AbsoluteTTL = time.Hour
	r, err := New(cfg, WithClock(fc), withRunner(fr))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, _ := r.Create(ctx, "s1", app.EgressPolicy{})

	fc.Advance(9 * time.Minute)
	// An Exec touches lastUsed, resetting the idle clock.
	if _, err := ws.Exec(ctx, app.ExecRequest{Cmd: []string{"true"}}); err != nil {
		t.Fatal(err)
	}
	fc.Advance(9 * time.Minute) // 18m total, but only 9m since the Exec
	if ev := r.reapOnce(ctx); len(ev) != 0 {
		t.Fatalf("reaped despite recent Exec: %v", ev)
	}
}

func TestReaper_AbsoluteTTL(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	// Idle TTL equals absolute here; we keep touching the sandbox so idle never
	// trips, leaving the absolute lifetime cap as the reason it is reaped.
	cfg.IdleTTL = 30 * time.Minute
	cfg.AbsoluteTTL = 30 * time.Minute
	r, err := New(cfg, WithClock(fc), withRunner(fr))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, _ := r.Create(ctx, "s1", app.EgressPolicy{})

	// Keep touching it so idle never trips; absolute TTL must still reap it.
	fc.Advance(20 * time.Minute)
	_, _ = ws.Exec(ctx, app.ExecRequest{Cmd: []string{"true"}})
	fc.Advance(11 * time.Minute) // 31m total lifetime
	_, _ = ws.Exec(ctx, app.ExecRequest{Cmd: []string{"true"}})

	ev := r.reapOnce(ctx)
	if len(ev) != 1 || ev[0].Reason != reapAbsoluteTTL {
		t.Fatalf("absolute reap = %+v, want one absolute_ttl", ev)
	}
}

func TestReaper_SessionStatusEnded(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.IdleTTL = time.Hour
	cfg.AbsoluteTTL = time.Hour

	status := map[string]SessionStatus{"s1": SessionActive, "s2": SessionFinished}
	r, err := New(cfg, WithClock(fc), withRunner(fr), WithSessionStatus(
		func(_ context.Context, id string) (SessionStatus, error) {
			return status[id], nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = r.Create(ctx, "s1", app.EgressPolicy{})
	_, _ = r.Create(ctx, "s2", app.EgressPolicy{})

	// No TTL elapsed, but s2's session is finished → reap s2, keep s1.
	ev := r.reapOnce(ctx)
	if len(ev) != 1 || ev[0].SessionID != "s2" || ev[0].Reason != reapSessionEnded {
		t.Fatalf("status reap = %+v, want one session_ended for s2", ev)
	}
	if _, err := r.Get(ctx, "s1"); err != nil {
		t.Errorf("active session s1 was reaped: %v", err)
	}
}

func TestReaper_UnknownStatus_RetainsFailSafe(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.IdleTTL = time.Hour
	cfg.AbsoluteTTL = time.Hour
	r, err := New(cfg, WithClock(fc), withRunner(fr), WithSessionStatus(
		func(_ context.Context, _ string) (SessionStatus, error) {
			return SessionUnknown, nil // cannot classify
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = r.Create(ctx, "s1", app.EgressPolicy{})
	// Unknown status + no TTL elapsed → retain (fail-safe: never reap what we can't
	// positively classify as ended).
	if ev := r.reapOnce(ctx); len(ev) != 0 {
		t.Fatalf("reaped on unknown status: %v", ev)
	}
}

func TestReaper_StatusErrorFallsBackToTTL(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.IdleTTL = 5 * time.Minute
	cfg.AbsoluteTTL = time.Hour
	r, err := New(cfg, WithClock(fc), withRunner(fr), WithSessionStatus(
		func(_ context.Context, _ string) (SessionStatus, error) {
			return SessionUnknown, context.DeadlineExceeded // lookup failed
		}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, _ = r.Create(ctx, "s1", app.EgressPolicy{})
	// Status lookup errors, but the idle TTL still applies as the fallback bound.
	fc.Advance(6 * time.Minute)
	ev := r.reapOnce(ctx)
	if len(ev) != 1 || ev[0].Reason != reapIdleTTL {
		t.Fatalf("expected idle_ttl fallback when status errors, got %+v", ev)
	}
}

func TestReconcileOrphans_ReapsUntrackedEndedContainers(t *testing.T) {
	fr := newFakeRunner()
	// docker ps reports three managed containers: s1 (tracked/live), s2 (orphan,
	// finished), s3 (orphan, still active).
	fr.on("ps", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		out := "boltrope-sbx-s1\ts1\nboltrope-sbx-s2\ts2\nboltrope-sbx-s3\ts3\n"
		return cmdResult{ExitCode: 0, Stdout: []byte(out)}, nil
	})
	status := map[string]SessionStatus{"s2": SessionFinished, "s3": SessionActive}
	fc := clocktest.NewFake(time.Unix(0, 0))
	cfg := DefaultConfig()
	cfg.MaxLive = 8
	r, err := New(cfg, WithClock(fc), withRunner(fr), WithSessionStatus(
		func(_ context.Context, id string) (SessionStatus, error) { return status[id], nil }))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	// s1 is live/tracked in this process. (Create itself issues a clean-workspace
	// pre-remove rm of s1's name, so we measure rm calls only AFTER this point.)
	if _, err := r.Create(ctx, "s1", app.EgressPolicy{}); err != nil {
		t.Fatal(err)
	}
	rmBefore := len(fr.callsFor("rm"))

	reclaimed, err := r.ReconcileOrphans(ctx)
	if err != nil {
		t.Fatalf("ReconcileOrphans: %v", err)
	}
	// Only s2 (untracked + finished) is reclaimed. s1 is live; s3 is still active.
	if len(reclaimed) != 1 || reclaimed[0] != "s2" {
		t.Fatalf("reclaimed = %v, want [s2]", reclaimed)
	}
	// Inspect only the rm calls ReconcileOrphans made.
	rmAfter := fr.callsFor("rm")[rmBefore:]
	var removedS2 bool
	for _, call := range rmAfter {
		switch call.Args[len(call.Args)-1] {
		case "boltrope-sbx-s2":
			removedS2 = true
		case "boltrope-sbx-s1":
			t.Error("ReconcileOrphans removed the live container s1")
		case "boltrope-sbx-s3":
			t.Error("ReconcileOrphans removed the still-active container s3")
		}
	}
	if !removedS2 {
		t.Error("ReconcileOrphans did not rm the orphaned finished container s2")
	}
}

func TestRunReaper_StopsOnContextCancel(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.RunReaper(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunReaper did not return on ctx cancel")
	}
}
