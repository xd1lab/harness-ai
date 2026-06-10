package runtime

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestHelperProcess is a standard Go test-binary helper: when invoked with the
// GO_RUNTIME_HELPER env var set, it impersonates a child process whose behavior is
// selected by the var, then exits. It lets the execRunner be exercised against a
// REAL os/exec child cross-platform, with no Docker and no shell.
func TestHelperProcess(_ *testing.T) {
	mode := os.Getenv("GO_RUNTIME_HELPER")
	if mode == "" {
		return // ordinary test run; not the helper
	}
	switch mode {
	case "exit0":
		_, _ = fmt.Fprint(os.Stdout, "out")
		_, _ = fmt.Fprint(os.Stderr, "err")
		os.Exit(0)
	case "exit7":
		os.Exit(7)
	case "sleep":
		// Sleep long enough that the test must cancel it.
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		os.Exit(1)
	}
}

// runHelper runs this test binary as the helper child, selecting its behavior via
// the GO_RUNTIME_HELPER env var, through the given execRunner and context.
func runHelper(ctx context.Context, t *testing.T, r *execRunner, mode string) (cmdResult, error) {
	t.Helper()
	spec := cmdSpec{
		Name:         os.Args[0],
		Args:         []string{"-test.run=TestHelperProcess"},
		ProcessGroup: true,
	}
	// The child selects its behavior from this env var.
	t.Setenv("GO_RUNTIME_HELPER", mode)
	return r.Run(ctx, spec)
}

func TestExecRunner_ExitCodeZero(t *testing.T) {
	r := newExecRunner(time.Second)
	res, err := runHelper(context.Background(), t, r, "exit0")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.ExitCode != 0 || res.Killed {
		t.Errorf("res = %+v, want exit 0 not killed", res)
	}
	if string(res.Stdout) != "out" {
		t.Errorf("stdout = %q, want out", res.Stdout)
	}
	if string(res.Stderr) != "err" {
		t.Errorf("stderr = %q, want err", res.Stderr)
	}
}

func TestExecRunner_NonZeroExitIsNotRunnerError(t *testing.T) {
	r := newExecRunner(time.Second)
	res, err := runHelper(context.Background(), t, r, "exit7")
	if err != nil {
		t.Fatalf("non-zero exit must not be a runner error, got %v", err)
	}
	if res.ExitCode != 7 {
		t.Errorf("ExitCode = %d, want 7", res.ExitCode)
	}
	if res.Killed {
		t.Errorf("clean non-zero exit reported Killed")
	}
}

func TestExecRunner_CtxCancelKillsChild(t *testing.T) {
	r := newExecRunner(500 * time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())

	// Set the helper mode in the test goroutine (t.Setenv must not run off-goroutine),
	// then build the spec inline so the run can happen in a child goroutine.
	t.Setenv("GO_RUNTIME_HELPER", "sleep")
	spec := cmdSpec{Name: os.Args[0], Args: []string{"-test.run=TestHelperProcess"}, ProcessGroup: true}

	type out struct {
		res cmdResult
		err error
	}
	ch := make(chan out, 1)
	go func() {
		res, err := r.Run(ctx, spec)
		ch <- out{res, err}
	}()

	// Give the child a moment to start, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case o := <-ch:
		if !o.res.Killed {
			t.Errorf("cancelled child must report Killed=true, got %+v (err=%v)", o.res, o.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("execRunner did not return after cancelling a sleeping child within 5s")
	}
}
