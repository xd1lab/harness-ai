// This file exercises the PRODUCTION OSCommandRunner against real OS
// subprocesses. To stay hermetic and portable (no shell, no fixtures on PATH),
// the subprocess is this very test binary re-executed in a helper mode
// selected via the BOLTROPE_HOOKS_HELPER_MODE environment variable — the
// standard Go helper-process pattern. No network, no Docker.
package hooks_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/hooks"
)

// TestMain intercepts helper-mode re-executions of the test binary. Without
// the mode variable it runs the package's tests normally.
func TestMain(m *testing.M) {
	mode := os.Getenv("BOLTROPE_HOOKS_HELPER_MODE")
	if mode == "" {
		os.Exit(m.Run())
	}
	helperMain(mode)
}

// helperMain is the subprocess body for each scripted hook behavior.
func helperMain(mode string) {
	switch mode {
	case "echo-stdin":
		// Copy stdin verbatim to stdout, exit 0.
		_, _ = io.Copy(os.Stdout, os.Stdin)
		os.Exit(0)
	case "block-json":
		fmt.Print(`{"continue":false,"reason":"helper says no"}`)
		os.Exit(0)
	case "garbage-stdout":
		fmt.Print("definitely-not-json{{")
		os.Exit(0)
	case "exit-3":
		fmt.Fprint(os.Stderr, "helper stderr reason")
		os.Exit(3)
	case "print-env":
		fmt.Print(os.Getenv("HOOK_TEST_VAR"))
		os.Exit(0)
	case "sleep":
		// Long enough that only a context kill ends it; the kill bounds the test.
		time.Sleep(30 * time.Second)
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper mode %q", mode)
		os.Exit(97)
	}
}

// helperInput builds a CommandInput that re-executes this test binary in the
// given helper mode.
func helperInput(t *testing.T, mode string, extraEnv ...string) hooks.CommandInput {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	env := append([]string{"BOLTROPE_HOOKS_HELPER_MODE=" + mode}, extraEnv...)
	return hooks.CommandInput{Command: exe, Env: env}
}

// TestOSCommandRunner_StdinStdoutRoundTrip asserts the runner pipes Stdin to
// the subprocess and captures its stdout verbatim with exit code 0.
func TestOSCommandRunner_StdinStdoutRoundTrip(t *testing.T) {
	r := hooks.NewOSCommandRunner()
	in := helperInput(t, "echo-stdin")
	in.Stdin = []byte(`{"event":"PreToolUse","tool_name":"bash"}`)

	res, err := r.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("ExitCode = %d, want 0 (stderr: %q)", res.ExitCode, res.Stderr)
	}
	if got := string(res.Stdout); got != string(in.Stdin) {
		t.Errorf("stdout = %q, want the stdin payload echoed back %q", got, in.Stdin)
	}
}

// TestOSCommandRunner_NonZeroExitCaptured asserts a non-zero exit is reported
// in ExitCode (NOT as an error) with stderr captured for the block reason.
func TestOSCommandRunner_NonZeroExitCaptured(t *testing.T) {
	r := hooks.NewOSCommandRunner()

	res, err := r.Run(context.Background(), helperInput(t, "exit-3"))
	if err != nil {
		t.Fatalf("Run: non-zero exit must not be an error, got: %v", err)
	}
	if res.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", res.ExitCode)
	}
	if !strings.Contains(res.Stderr, "helper stderr reason") {
		t.Errorf("Stderr = %q, want the helper's stderr output", res.Stderr)
	}
}

// TestOSCommandRunner_EnvOverridesInjected asserts CommandInput.Env entries are
// visible to the subprocess (appended after the inherited environment, so they
// win on duplicate keys).
func TestOSCommandRunner_EnvOverridesInjected(t *testing.T) {
	r := hooks.NewOSCommandRunner()

	res, err := r.Run(context.Background(), helperInput(t, "print-env", "HOOK_TEST_VAR=injected-value"))
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if got := string(res.Stdout); got != "injected-value" {
		t.Errorf("subprocess saw HOOK_TEST_VAR=%q, want %q", got, "injected-value")
	}
}

// TestOSCommandRunner_ContextCancellationKillsProcess asserts a context
// deadline kills a hung hook and Run returns the context error, not a
// CommandResult — the timeout path of the decision protocol.
func TestOSCommandRunner_ContextCancellationKillsProcess(t *testing.T) {
	r := hooks.NewOSCommandRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := r.Run(ctx, helperInput(t, "sleep"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded from a killed hook, got %v", err)
	}
	// Sanity bound: the kill must have ended the 30s sleeper early.
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("hook was not killed promptly: took %v", elapsed)
	}
}

// TestOSCommandRunner_MissingBinaryIsMechanismError asserts a non-existent
// executable is a mechanism error (non-nil error), distinct from a hook that
// ran and exited non-zero.
func TestOSCommandRunner_MissingBinaryIsMechanismError(t *testing.T) {
	r := hooks.NewOSCommandRunner()
	in := hooks.CommandInput{Command: filepath.Join(t.TempDir(), "no-such-hook-binary")}

	_, err := r.Run(context.Background(), in)
	if err == nil {
		t.Fatal("expected mechanism error for a missing binary, got nil")
	}
	if !strings.Contains(err.Error(), "exec") {
		t.Errorf("error should be wrapped as an exec failure, got %v", err)
	}
}

// TestRunnerWithOSCommandRunner_BlockJSON drives the FULL production stack —
// Runner over OSCommandRunner over a real subprocess — and asserts a clean
// JSON block decision propagates with its reason (FR-EXT-03 over a real exec).
func TestRunnerWithOSCommandRunner_BlockJSON(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cfg := hooks.Config{Hooks: []hooks.HookSpec{{
		Command: exe,
		Env:     []string{"BOLTROPE_HOOKS_HELPER_MODE=block-json"},
		Events:  []app.HookEvent{app.HookPreToolUse},
	}}}
	runner := hooks.NewRunner(cfg, hooks.NewOSCommandRunner())

	dec, err := runner.Run(context.Background(), preToolUseInput("sess-os1", "call-os1", "bash"))
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatal("expected Allow=false from the blocking subprocess hook")
	}
	if dec.Reason != "helper says no" {
		t.Errorf("Reason = %q, want %q", dec.Reason, "helper says no")
	}
}

// TestRunnerWithOSCommandRunner_MalformedJSONAllows asserts a zero-exit hook
// emitting malformed JSON on stdout is treated as "no opinion" → allow over
// the real subprocess path (forward-compatible decision protocol).
func TestRunnerWithOSCommandRunner_MalformedJSONAllows(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cfg := hooks.Config{Hooks: []hooks.HookSpec{{
		Command: exe,
		Env:     []string{"BOLTROPE_HOOKS_HELPER_MODE=garbage-stdout"},
		Events:  []app.HookEvent{app.HookPreToolUse},
	}}}
	runner := hooks.NewRunner(cfg, hooks.NewOSCommandRunner())

	dec, err := runner.Run(context.Background(), preToolUseInput("sess-os2", "call-os2", "read"))
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for malformed stdout, got false (reason %q)", dec.Reason)
	}
}
