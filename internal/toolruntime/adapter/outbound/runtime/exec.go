package runtime

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"sync/atomic"
	"time"
)

// cmdSpec describes one external command invocation for a [commandRunner]. It is
// the single unit of currency between [container]/[Runtime] and the process layer,
// so unit tests can assert the exact argv and behavior with a fake runner.
type cmdSpec struct {
	// Name is the executable to run (always the docker binary path in production).
	Name string
	// Args is the argument vector passed to Name (not shell-interpreted).
	Args []string
	// Stdin is optional standard input piped to the command.
	Stdin []byte
	// ProcessGroup requests that the command run in its own process group so the
	// runner can signal the WHOLE group (not just the leader) on cancellation.
	// This is set for `docker exec` so the exec client and any helper it spawns are
	// signaled together; the guaranteed reap of the in-container tree is the
	// separate `docker kill` issued against the container's PID namespace
	// (architecture §9.3).
	ProcessGroup bool
}

// cmdResult is the captured outcome of a [commandRunner.Run].
type cmdResult struct {
	// ExitCode is the process exit code. It is meaningful only when Killed is false.
	ExitCode int
	// Stdout is the captured standard output.
	Stdout []byte
	// Stderr is the captured standard error.
	Stderr []byte
	// Killed reports whether the process was terminated by the runner (e.g. on ctx
	// cancellation) rather than exiting on its own.
	Killed bool
}

// commandRunner runs external commands. The production implementation
// ([execRunner]) shells out via [os/exec]; unit tests inject a fake so the exact
// docker argv and the cancellation-to-kill wiring can be asserted without Docker.
//
// Run blocks until the command completes or is killed. On ctx cancellation it
// terminates the command's process group (SIGTERM→SIGKILL within the configured
// grace) and returns with [cmdResult.Killed] true rather than leaking the child.
type commandRunner interface {
	Run(ctx context.Context, spec cmdSpec) (cmdResult, error)
}

// errProcessGroupUnsupported is returned by setProcessGroup on a platform that does
// not support process-group signaling (e.g. the Windows dev host). The container
// then relies on the `docker kill` reaper, which is the guaranteed in-container reap
// regardless of host-side group support (architecture §9.3).
var errProcessGroupUnsupported = errors.New("runtime: process-group signaling unsupported on this platform")

// execRunner is the production [commandRunner] backed by [os/exec]. It starts the
// command in its own process group when requested and, on ctx cancellation, signals
// the group with SIGTERM then escalates to SIGKILL after killGrace, using
// [exec.Cmd.WaitDelay] as the hard backstop so a wedged client process can never
// block Run forever.
type execRunner struct {
	// killGrace is the SIGTERM→SIGKILL escalation window applied to the host-side
	// command process group on cancellation.
	killGrace time.Duration
}

// newExecRunner returns an execRunner with the given SIGTERM→SIGKILL grace window.
// A non-positive grace falls back to defaultKillGrace.
func newExecRunner(killGrace time.Duration) *execRunner {
	if killGrace <= 0 {
		killGrace = defaultKillGrace
	}
	return &execRunner{killGrace: killGrace}
}

// Run executes spec, capturing stdout/stderr. On ctx cancellation it terminates the
// host-side process group (SIGTERM→SIGKILL after killGrace) and returns Killed=true.
// A non-zero command exit is reported via [cmdResult.ExitCode] with a nil error (a
// normal command outcome, not a runner failure).
func (r *execRunner) Run(ctx context.Context, spec cmdSpec) (cmdResult, error) {
	// CommandContext wires ctx cancellation to process termination; we override
	// Cancel to target the whole process group and set WaitDelay so a process that
	// ignores the signal is force-killed and Wait always returns.
	cmd := exec.CommandContext(ctx, spec.Name, spec.Args...) //nolint:gosec // argv built by this package from validated config; never shell-interpreted.

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if len(spec.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(spec.Stdin)
	}

	var killed atomic.Bool
	groupSet := false
	if spec.ProcessGroup {
		if err := setProcessGroup(cmd); err == nil {
			groupSet = true
		} else if !errors.Is(err, errProcessGroupUnsupported) {
			return cmdResult{}, err
		}
	}

	cmd.Cancel = func() error {
		killed.Store(true)
		// Graceful SIGTERM to the group (or leader if group unavailable). The
		// WaitDelay backstop and the container's `docker kill` reaper guarantee
		// forward progress regardless of signal handling.
		return signalGroup(cmd, groupSet, false)
	}
	cmd.WaitDelay = r.killGrace

	if err := cmd.Start(); err != nil {
		return cmdResult{}, err
	}

	waitErr := cmd.Wait()

	res := cmdResult{
		Stdout: append([]byte(nil), stdout.Bytes()...),
		Stderr: append([]byte(nil), stderr.Bytes()...),
		Killed: killed.Load(),
	}
	if waitErr == nil {
		return res, nil
	}

	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		res.ExitCode = exitErr.ExitCode()
		return res, nil
	}
	// A non-exit error after cancellation is the expected kill path; otherwise it is
	// a genuine runner failure (failed spawn, I/O error).
	if res.Killed || ctx.Err() != nil {
		res.Killed = true
		return res, nil
	}
	return res, waitErr
}
