// This file extends hooks_test.go with the parseResult edge cases, the
// catch-all event matcher, and the Runner mechanism-failure paths — all via the
// in-file fake CommandRunner, deterministic and subprocess-free.
package hooks_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/hooks"
)

// singleHookConfig returns a Config with one PreToolUse hook named cmd.
func singleHookConfig(cmd string) hooks.Config {
	return hooks.Config{Hooks: []hooks.HookSpec{
		{Command: cmd, Events: []app.HookEvent{app.HookPreToolUse}},
	}}
}

// TestParseResult_NonZeroExitEmptyStderrGetsGenericReason asserts that a hook
// exiting non-zero WITHOUT stderr output still blocks with a synthesized
// reason naming the command and exit code (a silent failure is never an allow).
func TestParseResult_NonZeroExitEmptyStderrGetsGenericReason(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addResult(2, nil, "")

	runner := hooks.NewRunner(singleHookConfig("silent-hook"), fake)
	dec, err := runner.Run(context.Background(), preToolUseInput("sess-pr1", "call-1", "bash"))
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatalf("expected Allow=false for non-zero exit, got Allow=true")
	}
	if !strings.Contains(dec.Reason, "silent-hook") || !strings.Contains(dec.Reason, "2") {
		t.Errorf("generic reason must name the command and exit code, got %q", dec.Reason)
	}
}

// TestParseResult_UnparseableStdoutAllows asserts that a zero-exit hook whose
// stdout is not JSON is treated as "no opinion" → allow (forward-compatible).
func TestParseResult_UnparseableStdoutAllows(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addResult(0, []byte("plain log line, not json"), "")

	runner := hooks.NewRunner(singleHookConfig("chatty-hook"), fake)
	dec, err := runner.Run(context.Background(), preToolUseInput("sess-pr2", "call-2", "read"))
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for unparseable stdout, got false (reason %q)", dec.Reason)
	}
}

// TestParseResult_WhitespaceOnlyStdoutAllows asserts whitespace-only stdout is
// trimmed to empty and treated as "no opinion" → allow.
func TestParseResult_WhitespaceOnlyStdoutAllows(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addResult(0, []byte("  \n\t "), "")

	runner := hooks.NewRunner(singleHookConfig("quiet-hook"), fake)
	dec, err := runner.Run(context.Background(), preToolUseInput("sess-pr3", "call-3", "read"))
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for whitespace-only stdout, got false (reason %q)", dec.Reason)
	}
}

// TestParseResult_BlockWithoutReasonGetsGenericReason asserts a clean
// {"continue":false} block with no reason field is given a synthesized reason
// naming the hook.
func TestParseResult_BlockWithoutReasonGetsGenericReason(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addResult(0, []byte(`{"continue":false}`), "")

	runner := hooks.NewRunner(singleHookConfig("terse-hook"), fake)
	dec, err := runner.Run(context.Background(), preToolUseInput("sess-pr4", "call-4", "bash"))
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatalf("expected Allow=false for continue=false, got Allow=true")
	}
	if !strings.Contains(dec.Reason, "terse-hook") {
		t.Errorf("synthesized reason must name the hook, got %q", dec.Reason)
	}
}

// TestCatchAllSpecMatchesEveryEvent asserts a HookSpec with an empty Events
// list runs for ALL lifecycle events.
func TestCatchAllSpecMatchesEveryEvent(t *testing.T) {
	fake := &fakeCommandRunner{}
	cfg := hooks.Config{Hooks: []hooks.HookSpec{{Command: "catch-all"}}} // no Events → catch-all
	runner := hooks.NewRunner(cfg, fake)

	inputs := []app.HookInput{
		preToolUseInput("sess-ca", "call-ca", "read"),
		postToolUseInput("sess-ca", "call-ca", "read", "ok"),
		stopInput("sess-ca"),
		preCompactInput("sess-ca"),
	}
	for _, in := range inputs {
		fake.addAllow()
		dec, err := runner.Run(context.Background(), in)
		if err != nil {
			t.Fatalf("Run(%s) returned unexpected error: %v", in.Event, err)
		}
		if !dec.Allow {
			t.Errorf("Run(%s): expected Allow=true, got false", in.Event)
		}
	}
	if len(fake.calls) != len(inputs) {
		t.Fatalf("catch-all hook must run for every event: expected %d calls, got %d", len(inputs), len(fake.calls))
	}
}

// erroringCommandRunner is a CommandRunner whose mechanism itself fails.
type erroringCommandRunner struct{ err error }

func (r erroringCommandRunner) Run(context.Context, hooks.CommandInput) (hooks.CommandResult, error) {
	return hooks.CommandResult{}, r.err
}

// TestRunnerMechanismErrorPropagates asserts a CommandRunner failure (e.g.
// binary not found) is returned as an error naming the hook, NOT converted
// into an allow or a clean block.
func TestRunnerMechanismErrorPropagates(t *testing.T) {
	boom := errors.New("binary not found")
	runner := hooks.NewRunner(singleHookConfig("broken-hook"), erroringCommandRunner{err: boom})

	_, err := runner.Run(context.Background(), preToolUseInput("sess-mech", "call-m", "bash"))
	if err == nil {
		t.Fatal("expected error from failing CommandRunner, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("expected wrapped cause to be retrievable via errors.Is, got %v", err)
	}
	if !strings.Contains(err.Error(), "broken-hook") {
		t.Errorf("error must name the failing hook command, got %v", err)
	}
}

// TestMarshalPayloadErrorSurfaces asserts that un-marshalable tool args (NaN
// cannot be encoded as JSON) fail the run with a payload-marshaling error
// before any hook is invoked.
func TestMarshalPayloadErrorSurfaces(t *testing.T) {
	fake := &fakeCommandRunner{}
	runner := hooks.NewRunner(singleHookConfig("never-runs"), fake)

	in := preToolUseInput("sess-nan", "call-nan", "calc")
	in.ToolArgs = map[string]any{"value": math.NaN()}

	_, err := runner.Run(context.Background(), in)
	if err == nil {
		t.Fatal("expected marshal error for NaN tool args, got nil")
	}
	if !strings.Contains(err.Error(), "marshal payload") {
		t.Errorf("expected payload-marshaling error, got %v", err)
	}
	if len(fake.calls) != 0 {
		t.Errorf("no hook may run when the payload cannot be built; got %d calls", len(fake.calls))
	}
}

// cancellingCommandRunner allows its first invocation but cancels the supplied
// cancel func during it, so the chain's per-hook cancellation re-check fires
// before the second hook.
type cancellingCommandRunner struct {
	cancel context.CancelFunc
	calls  int
}

func (r *cancellingCommandRunner) Run(context.Context, hooks.CommandInput) (hooks.CommandResult, error) {
	r.calls++
	r.cancel()
	return hooks.CommandResult{ExitCode: 0, Stdout: []byte(`{"continue":true}`)}, nil
}

// TestMidChainCancellationStopsRemainingHooks asserts that a context cancelled
// while hook 1 runs prevents hook 2 from being invoked (the per-hook re-check),
// returning the context error.
func TestMidChainCancellationStopsRemainingHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &cancellingCommandRunner{cancel: cancel}

	cfg := hooks.Config{Hooks: []hooks.HookSpec{
		{Command: "hook-1", Events: []app.HookEvent{app.HookPreToolUse}},
		{Command: "hook-2", Events: []app.HookEvent{app.HookPreToolUse}},
	}}
	runner := hooks.NewRunner(cfg, r)

	_, err := runner.Run(ctx, preToolUseInput("sess-midcancel", "call-mc", "bash"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if r.calls != 1 {
		t.Fatalf("expected exactly 1 hook invocation before cancellation stopped the chain, got %d", r.calls)
	}
}
