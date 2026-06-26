package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

func newTestRuntime(t *testing.T, fr *fakeRunner, fc *clocktest.Fake, opts ...Option) *Runtime {
	t.Helper()
	cfg := DefaultConfig()
	cfg.MaxLive = 2
	base := []Option{WithClock(fc), withRunner(fr)}
	r, err := New(cfg, append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func TestRuntime_CreateGetDestroy_Lifecycle(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	ctx := context.Background()

	ws, err := r.Create(ctx, "s1", app.EgressPolicy{SessionID: "s1"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if ws == nil {
		t.Fatal("Create returned nil workspace")
	}

	// Create issues docker create then docker start, in that order, with the
	// session-named container.
	creates := fr.callsFor("create")
	starts := fr.callsFor("start")
	if len(creates) != 1 || len(starts) != 1 {
		t.Fatalf("want 1 create + 1 start, got %d + %d", len(creates), len(starts))
	}
	if argValue(creates[0].Args, "--name") != "boltrope-sbx-s1" {
		t.Errorf("create name = %q", argValue(creates[0].Args, "--name"))
	}

	got, err := r.Get(ctx, "s1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != ws {
		t.Error("Get returned a different workspace instance")
	}

	if err := r.Destroy(ctx, "s1"); err != nil {
		t.Fatalf("Destroy: %v", err)
	}
	if _, err := r.Get(ctx, "s1"); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Errorf("Get after Destroy = %v, want ErrWorkspaceNotFound", err)
	}
	// Destroy issued a docker rm --force.
	if len(fr.callsFor("rm")) == 0 {
		t.Error("Destroy did not issue docker rm")
	}
}

// TestRuntime_Create_RollsBackWhenNotRunning pins the readiness gate: when the
// just-started container never reports running, Create must NOT hand back a
// workspace — it rolls the container back (docker rm) and returns an error. Using
// a cancelled ctx makes waitRunning's poll return promptly via ctx.Done rather
// than waiting the full timeout, keeping the test deterministic.
func TestRuntime_Create_RollsBackWhenNotRunning(t *testing.T) {
	fr := newFakeRunner()
	fr.on("inspect", func(context.Context, cmdSpec) (cmdResult, error) {
		return cmdResult{ExitCode: 0, Stdout: []byte("false\n")}, nil // never running
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := r.Create(ctx, "s1", app.EgressPolicy{SessionID: "s1"}); err == nil {
		t.Fatal("Create must fail when the container never reaches running")
	}
	if len(fr.callsFor("inspect")) == 0 {
		t.Error("Create did not inspect for running state before exec")
	}
	if len(fr.callsFor("rm")) == 0 {
		t.Error("Create did not roll back (docker rm) after the readiness wait failed")
	}
	if _, err := r.Get(ctx, "s1"); !errors.Is(err, ErrWorkspaceNotFound) {
		t.Error("a workspace that never ran must not be registered live")
	}
}

func TestRuntime_Destroy_Idempotent(t *testing.T) {
	fr := newFakeRunner()
	// docker rm of an absent container exits non-zero with "No such container".
	fr.on("rm", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		return cmdResult{ExitCode: 1, Stderr: []byte("Error: No such container: boltrope-sbx-ghost")}, nil
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	if err := r.Destroy(context.Background(), "ghost"); err != nil {
		t.Errorf("Destroy of absent workspace should be nil, got %v", err)
	}
}

func TestRuntime_MaxLiveBackpressure(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc) // MaxLive = 2
	ctx := context.Background()

	if _, err := r.Create(ctx, "s1", app.EgressPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Create(ctx, "s2", app.EgressPolicy{}); err != nil {
		t.Fatal(err)
	}
	// Third distinct session exceeds the cap → backpressure.
	_, err := r.Create(ctx, "s3", app.EgressPolicy{})
	if !errors.Is(err, ErrMaxLiveSandboxes) {
		t.Fatalf("Create over cap = %v, want ErrMaxLiveSandboxes", err)
	}
	// Freeing one makes room.
	if err := r.Destroy(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Create(ctx, "s3", app.EgressPolicy{}); err != nil {
		t.Errorf("Create after freeing a slot failed: %v", err)
	}
}

func TestRuntime_CleanWorkspaceResume_RecreatesFresh(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	ctx := context.Background()

	ws1, err := r.Create(ctx, "s1", app.EgressPolicy{SessionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	// Resume: a second Create for the same session must re-attach to a FRESH
	// workspace (no durable FS snapshot), tearing down the prior one.
	ws2, err := r.Create(ctx, "s1", app.EgressPolicy{SessionID: "s1"})
	if err != nil {
		t.Fatalf("resume Create: %v", err)
	}
	if ws1 == ws2 {
		t.Error("resume returned the same workspace; expected a fresh one")
	}
	// The stale workspace is no longer usable.
	if _, err := ws1.(*container).Exec(ctx, app.ExecRequest{Cmd: []string{"true"}}); err == nil {
		t.Error("stale workspace after resume should fail fast")
	}
	// Create also pre-removes any lingering container with the same name.
	if len(fr.callsFor("rm")) == 0 {
		t.Error("resume did not pre-remove a stale container")
	}
}

func TestRuntime_CreateRejectsEmptySession(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	if _, err := r.Create(context.Background(), "", app.EgressPolicy{}); err == nil {
		t.Error("Create with empty session id should error")
	}
}

func TestRuntime_CreateStartFailureRollsBack(t *testing.T) {
	fr := newFakeRunner()
	fr.on("start", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		return cmdResult{ExitCode: 1, Stderr: []byte("start boom")}, nil
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	if _, err := r.Create(context.Background(), "s1", app.EgressPolicy{}); err == nil {
		t.Fatal("expected Create to fail when start fails")
	}
	// A failed start must not leave a live workspace, and must roll back via rm.
	if r.liveCount() != 0 {
		t.Errorf("liveCount = %d after failed start, want 0", r.liveCount())
	}
}

func TestRuntime_Close_DestroysAll(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	r := newTestRuntime(t, fr, fc)
	ctx := context.Background()
	_, _ = r.Create(ctx, "s1", app.EgressPolicy{})
	_, _ = r.Create(ctx, "s2", app.EgressPolicy{})
	if err := r.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if r.liveCount() != 0 {
		t.Errorf("liveCount after Close = %d, want 0", r.liveCount())
	}
	// Create after Close is rejected.
	if _, err := r.Create(ctx, "s3", app.EgressPolicy{}); err == nil {
		t.Error("Create after Close should error")
	}
}
