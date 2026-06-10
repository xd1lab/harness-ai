package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/xd1lab/harness-ai/internal/platform/clock/clocktest"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// newTestContainer builds a container wired to a fake runner and fake clock, without
// going through Create (so Exec/Read/Write can be tested in isolation).
func newTestContainer(t *testing.T, fr *fakeRunner, fc *clocktest.Fake) *container {
	t.Helper()
	cfg := DefaultConfig().withDefaults()
	cfg.WallClock = 30 * time.Second
	cfg.KillGrace = 5 * time.Second
	now := fc.Now()
	return &container{
		name:      "boltrope-sbx-s1",
		sessionID: "s1",
		cfg:       cfg,
		runner:    fr,
		clk:       fc,
		policy:    app.EgressPolicy{SessionID: "s1"},
		lastUsed:  now,
		created:   now,
	}
}

func TestContainerExec_CleanRun_NoKill(t *testing.T) {
	fr := newFakeRunner()
	fr.on("exec", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		return cmdResult{ExitCode: 0, Stdout: []byte("hello\n")}, nil
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)

	res, err := c.Exec(context.Background(), app.ExecRequest{Cmd: []string{"echo", "hello"}})
	if err != nil {
		t.Fatalf("Exec error: %v", err)
	}
	if res.Killed {
		t.Errorf("clean run reported Killed")
	}
	if string(res.Stdout) != "hello\n" {
		t.Errorf("stdout = %q", res.Stdout)
	}
	// Exactly one exec call, started in its own process group, no docker kill.
	execs := fr.callsFor("exec")
	if len(execs) != 1 {
		t.Fatalf("exec calls = %d, want 1", len(execs))
	}
	if !execs[0].ProcessGroup {
		t.Errorf("exec spec must request ProcessGroup for group-kill wiring")
	}
	if got := fr.callsFor("kill"); len(got) != 0 {
		t.Errorf("clean run issued docker kill: %v", got)
	}
}

func TestContainerExec_CtxCancel_ReapsProcessTree(t *testing.T) {
	fr := newFakeRunner()
	// The exec blocks until its ctx is cancelled, then reports Killed — modeling the
	// real runner's host-side process-group kill.
	fr.on("exec", func(ctx context.Context, _ cmdSpec) (cmdResult, error) {
		<-ctx.Done()
		return cmdResult{Killed: true}, nil
	})
	// Signal when the SIGTERM reaper kill has been issued so the test can advance the
	// clock for the SIGKILL escalation.
	termIssued := make(chan struct{})
	var closed bool
	fr.on("kill", func(_ context.Context, spec cmdSpec) (cmdResult, error) {
		if hasArg(spec.Args, "--signal=TERM") && !closed {
			closed = true
			close(termIssued)
		}
		return cmdResult{ExitCode: 0}, nil
	})

	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)

	ctx, cancel := context.WithCancel(context.Background())
	resCh := make(chan app.ExecResult, 1)
	go func() {
		res, _ := c.Exec(ctx, app.ExecRequest{Cmd: []string{"sleep", "999"}})
		resCh <- res
	}()

	cancel() // deliver the interrupt

	// The reaper must issue SIGTERM, then SIGKILL after the grace window.
	select {
	case <-termIssued:
	case <-time.After(2 * time.Second):
		t.Fatal("SIGTERM reaper kill was not issued within 2s of cancellation")
	}
	fc.Advance(5 * time.Second) // fire the SIGKILL escalation timer

	select {
	case res := <-resCh:
		if !res.Killed {
			t.Errorf("Exec after cancel must report Killed=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Exec did not return after reaper completed")
	}

	sigs := fr.signalsFor()
	if !reflect.DeepEqual(sigs, []string{"TERM", "KILL"}) {
		t.Errorf("reaper signals = %v, want [TERM KILL] (SIGTERM→SIGKILL)", sigs)
	}
	// The kill targets the container's PID namespace (its name), reaping every
	// in-container descendant (architecture §9.3).
	for _, k := range fr.callsFor("kill") {
		if k.Args[len(k.Args)-1] != c.name {
			t.Errorf("kill target = %q, want container %q", k.Args[len(k.Args)-1], c.name)
		}
	}
}

func TestContainerExec_WallClockCap_Fires(t *testing.T) {
	fr := newFakeRunner()
	execStarted := make(chan struct{})
	var startOnce bool
	fr.on("exec", func(ctx context.Context, _ cmdSpec) (cmdResult, error) {
		if !startOnce {
			startOnce = true
			close(execStarted)
		}
		<-ctx.Done() // never finishes on its own
		return cmdResult{Killed: true}, nil
	})
	termIssued := make(chan struct{})
	var closed bool
	fr.on("kill", func(_ context.Context, spec cmdSpec) (cmdResult, error) {
		if hasArg(spec.Args, "--signal=TERM") && !closed {
			closed = true
			close(termIssued)
		}
		return cmdResult{ExitCode: 0}, nil
	})

	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)
	c.cfg.WallClock = 10 * time.Second

	resCh := make(chan app.ExecResult, 1)
	go func() {
		res, _ := c.Exec(context.Background(), app.ExecRequest{Cmd: []string{"sleep", "999"}})
		resCh <- res
	}()

	// Wait until Exec has registered the wall-clock timer (the exec handler is
	// entered after the timer is created), then fire the absolute wall-clock cap.
	<-execStarted
	fc.Advance(10 * time.Second)

	select {
	case <-termIssued:
	case <-time.After(2 * time.Second):
		t.Fatal("wall-clock cap did not trigger the reaper SIGTERM")
	}
	fc.Advance(5 * time.Second) // SIGKILL escalation

	select {
	case res := <-resCh:
		if !res.Killed {
			t.Errorf("Exec after wall-clock cap must report Killed=true")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Exec did not return after wall-clock reap")
	}
}

func TestContainerExec_RunnerError_Surfaced(t *testing.T) {
	fr := newFakeRunner()
	wantErr := errors.New("docker not found")
	fr.on("exec", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		return cmdResult{}, wantErr
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)

	_, err := c.Exec(context.Background(), app.ExecRequest{Cmd: []string{"true"}})
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("Exec error = %v, want wrap of %v", err, wantErr)
	}
}

func TestContainer_ReadWriteMkdir_Commands(t *testing.T) {
	fr := newFakeRunner()
	fr.on("exec", func(_ context.Context, spec cmdSpec) (cmdResult, error) {
		// `cat` returns file bytes on stdout.
		if hasArg(spec.Args, "cat") {
			return cmdResult{ExitCode: 0, Stdout: []byte("file-bytes")}, nil
		}
		return cmdResult{ExitCode: 0}, nil
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)
	ctx := context.Background()

	// Write feeds bytes over stdin to tee and creates parent dirs first.
	if err := c.Write(ctx, "sub/dir/file.txt", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	var sawMkdir, sawTee bool
	for _, call := range fr.callsFor("exec") {
		if hasArg(call.Args, "mkdir") && hasArg(call.Args, "/workspace/sub/dir") {
			sawMkdir = true
		}
		if hasArg(call.Args, "tee") && hasArg(call.Args, "/workspace/sub/dir/file.txt") {
			sawTee = true
			if string(call.Stdin) != "data" {
				t.Errorf("tee stdin = %q, want data (binary-safe write)", call.Stdin)
			}
		}
	}
	if !sawMkdir {
		t.Errorf("Write did not mkdir -p the parent dir")
	}
	if !sawTee {
		t.Errorf("Write did not pipe to tee")
	}

	// Read returns the cat stdout, resolving a relative path under the workspace.
	data, err := c.Read(ctx, "file.txt")
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(data) != "file-bytes" {
		t.Errorf("Read = %q, want file-bytes", data)
	}

	// Mkdir uses mkdir -p with the absolute workspace-rooted path.
	if err := c.Mkdir(ctx, "newdir"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	var sawNewdir bool
	for _, call := range fr.callsFor("exec") {
		if hasArg(call.Args, "mkdir") && hasArg(call.Args, "-p") && hasArg(call.Args, "/workspace/newdir") {
			sawNewdir = true
		}
	}
	if !sawNewdir {
		t.Errorf("Mkdir did not issue mkdir -p /workspace/newdir")
	}
}

func TestContainer_Read_NonZeroExitIsError(t *testing.T) {
	fr := newFakeRunner()
	fr.on("exec", func(_ context.Context, _ cmdSpec) (cmdResult, error) {
		return cmdResult{ExitCode: 1, Stderr: []byte("cat: no such file")}, nil
	})
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)
	if _, err := c.Read(context.Background(), "missing.txt"); err == nil {
		t.Error("expected error for missing file (non-zero cat exit)")
	}
}

func TestContainer_OpsAfterDestroy_FailFast(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)
	c.markDestroyed()
	if _, err := c.Exec(context.Background(), app.ExecRequest{Cmd: []string{"true"}}); err == nil {
		t.Error("Exec on destroyed workspace should error")
	}
	if _, err := c.Read(context.Background(), "x"); err == nil {
		t.Error("Read on destroyed workspace should error")
	}
	if err := c.Write(context.Background(), "x", nil); err == nil {
		t.Error("Write on destroyed workspace should error")
	}
}

func TestContainer_NetworkPolicy(t *testing.T) {
	fr := newFakeRunner()
	fc := clocktest.NewFake(time.Unix(0, 0))
	c := newTestContainer(t, fr, fc)
	c.policy = app.EgressPolicy{SessionID: "s1", AllowedHosts: []string{"a.example"}}
	p, err := c.NetworkPolicy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.SessionID != "s1" || len(p.AllowedHosts) != 1 {
		t.Errorf("NetworkPolicy = %+v", p)
	}
}
