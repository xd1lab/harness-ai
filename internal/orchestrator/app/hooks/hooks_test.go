// Package hooks_test exercises the hooks.Runner implementation with a fake
// CommandRunner so no subprocess is ever spawned during tests. All assertions
// are deterministic (FR-EXT-03 AC-1/AC-2; T-LOOP-04).
package hooks_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/hooks"
)

// ---------------------------------------------------------------------------
// Fake CommandRunner
// ---------------------------------------------------------------------------

// fakeCommandRunner is a deterministic [hooks.CommandRunner] that records every
// invocation and returns pre-scripted (exitCode, stdout, stderr) responses in
// queue order.
type fakeCommandRunner struct {
	calls   []hooks.CommandInput
	results []fakeResult
	idx     int
}

type fakeResult struct {
	exitCode int
	stdout   []byte
	stderr   string
}

// addResult enqueues one scripted result for the next Run call.
func (f *fakeCommandRunner) addResult(exitCode int, stdout []byte, stderr string) {
	f.results = append(f.results, fakeResult{exitCode: exitCode, stdout: stdout, stderr: stderr})
}

// addAllow enqueues a result that encodes {"continue":true} on stdout.
func (f *fakeCommandRunner) addAllow() {
	out, _ := json.Marshal(map[string]any{"continue": true})
	f.addResult(0, out, "")
}

// addBlock enqueues a result that encodes {"continue":false,"reason":"blocked by hook"} on stdout.
func (f *fakeCommandRunner) addBlock(reason string) {
	out, _ := json.Marshal(map[string]any{"continue": false, "reason": reason})
	f.addResult(0, out, "")
}

// Run satisfies [hooks.CommandRunner].
func (f *fakeCommandRunner) Run(_ context.Context, in hooks.CommandInput) (hooks.CommandResult, error) {
	f.calls = append(f.calls, in)
	if f.idx >= len(f.results) {
		// Default: allow (no results queued).
		out, _ := json.Marshal(map[string]any{"continue": true})
		return hooks.CommandResult{ExitCode: 0, Stdout: out}, nil
	}
	r := f.results[f.idx]
	f.idx++
	return hooks.CommandResult{ExitCode: r.exitCode, Stdout: r.stdout, Stderr: r.stderr}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// preToolUseInput builds a minimal HookInput for a PreToolUse event.
func preToolUseInput(sessionID, callID, toolName string) app.HookInput {
	return app.HookInput{
		Event:     app.HookPreToolUse,
		SessionID: sessionID,
		TurnID:    "turn-1",
		CallID:    callID,
		ToolName:  toolName,
		ToolArgs:  map[string]any{"path": "/tmp/test.txt"},
	}
}

// postToolUseInput builds a minimal HookInput for a PostToolUse event.
func postToolUseInput(sessionID, callID, toolName, result string) app.HookInput {
	return app.HookInput{
		Event:      app.HookPostToolUse,
		SessionID:  sessionID,
		TurnID:     "turn-1",
		CallID:     callID,
		ToolName:   toolName,
		ToolResult: result,
	}
}

// stopInput builds a minimal HookInput for a Stop event.
func stopInput(sessionID string) app.HookInput {
	return app.HookInput{
		Event:     app.HookStop,
		SessionID: sessionID,
		TurnID:    "turn-1",
	}
}

// preCompactInput builds a minimal HookInput for a PreCompact event.
func preCompactInput(sessionID string) app.HookInput {
	return app.HookInput{
		Event:     app.HookPreCompact,
		SessionID: sessionID,
		TurnID:    "turn-1",
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestPreToolUse_BlockYieldsHookBlocked asserts that a PreToolUse hook
// returning block yields HookDecision{Allow:false, Reason:"hook_blocked"}.
// This is FR-EXT-03 AC-1: the deterministic block path via fake CommandRunner.
func TestPreToolUse_BlockYieldsHookBlocked(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addBlock("hook_blocked")

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "check-tool", Args: []string{"--mode", "pre"}, Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preToolUseInput("sess-1", "call-1", "bash")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatalf("expected Allow=false (blocked), got Allow=true")
	}
	if dec.Reason != "hook_blocked" {
		t.Fatalf("expected Reason=%q, got %q", "hook_blocked", dec.Reason)
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 CommandRunner call, got %d", len(fake.calls))
	}
}

// TestPassingChain asserts that when all hooks in a chain return allow=true the
// aggregate decision is Allow=true.
func TestPassingChain(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addAllow()
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "hook-a", Args: nil, Events: []app.HookEvent{app.HookPreToolUse}},
			{Command: "hook-b", Args: nil, Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preToolUseInput("sess-2", "call-2", "read")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true (passing chain), got Allow=false (reason: %q)", dec.Reason)
	}
	if len(fake.calls) != 2 {
		t.Fatalf("expected 2 CommandRunner calls (all hooks ran), got %d", len(fake.calls))
	}
}

// TestPostToolUse_HookReceivesPayload verifies that PostToolUse hooks run for
// their event and that the CommandRunner receives a payload containing the tool
// name and result (FR-EXT-03 AC-2).
func TestPostToolUse_HookReceivesPayload(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "post-hook", Events: []app.HookEvent{app.HookPostToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := postToolUseInput("sess-3", "call-3", "edit", "file written ok")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for PostToolUse, got false")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 CommandRunner call, got %d", len(fake.calls))
	}
	// The payload fed to the subprocess must contain the tool name and result.
	call := fake.calls[0]
	if call.Command != "post-hook" {
		t.Fatalf("unexpected command %q", call.Command)
	}
	var payload map[string]any
	if err := json.Unmarshal(call.Stdin, &payload); err != nil {
		t.Fatalf("failed to unmarshal CommandInput.Stdin: %v", err)
	}
	if payload["tool_name"] != "edit" {
		t.Fatalf("expected tool_name=edit in payload, got %v", payload["tool_name"])
	}
	if payload["tool_result"] != "file written ok" {
		t.Fatalf("expected tool_result=file written ok in payload, got %v", payload["tool_result"])
	}
}

// TestStopHook_Runs verifies that a Stop-event hook runs and that its Allow
// decision is returned.
func TestStopHook_Runs(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "stop-hook", Events: []app.HookEvent{app.HookStop}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := stopInput("sess-4")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for Stop hook, got false")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 CommandRunner call, got %d", len(fake.calls))
	}
}

// TestPreCompactHook_Runs verifies that a PreCompact-event hook runs and
// returns its decision.
func TestPreCompactHook_Runs(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "compact-hook", Events: []app.HookEvent{app.HookPreCompact}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preCompactInput("sess-5")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true for PreCompact hook, got false")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fake.calls))
	}
}

// TestEventFilter_HookSkippedForWrongEvent verifies that a hook configured for
// one event does NOT run for a different event type.
func TestEventFilter_HookSkippedForWrongEvent(t *testing.T) {
	fake := &fakeCommandRunner{}
	// Do NOT enqueue any result — if the hook fires unexpectedly the fake would
	// use the allow default; we assert no calls happened.

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "post-only", Events: []app.HookEvent{app.HookPostToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	// Running a PreToolUse should skip the PostToolUse-only hook entirely.
	in := preToolUseInput("sess-6", "call-6", "glob")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true (no hooks for PreToolUse), got false")
	}
	if len(fake.calls) != 0 {
		t.Fatalf("expected 0 CommandRunner calls (hook skipped), got %d", len(fake.calls))
	}
}

// TestChainOrder_FirstBlockWins verifies that in a chain of hooks the FIRST
// block terminates evaluation (subsequent hooks are NOT invoked).
func TestChainOrder_FirstBlockWins(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addBlock("hook_blocked")
	// A second allow result should never be consumed because evaluation stops.
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "blocker", Events: []app.HookEvent{app.HookPreToolUse}},
			{Command: "later", Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preToolUseInput("sess-7", "call-7", "bash")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatalf("expected Allow=false (first block wins), got Allow=true")
	}
	if dec.Reason != "hook_blocked" {
		t.Fatalf("expected Reason=hook_blocked, got %q", dec.Reason)
	}
	// Only the first hook should have been called.
	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 CommandRunner call (early exit), got %d", len(fake.calls))
	}
}

// TestEmptyChain_AllowsByDefault verifies that a Runner with no configured
// hooks for the given event returns Allow=true.
func TestEmptyChain_AllowsByDefault(t *testing.T) {
	fake := &fakeCommandRunner{}
	runner := hooks.NewRunner(hooks.Config{}, fake)

	in := preToolUseInput("sess-8", "call-8", "read")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if !dec.Allow {
		t.Fatalf("expected Allow=true (empty chain), got false (reason: %q)", dec.Reason)
	}
}

// TestPayload_PreToolUse_ContainsArgs asserts that the JSON payload fed into a
// PreToolUse hook subprocess includes session_id, call_id, tool_name, and
// tool_args so the subprocess can make an informed decision.
func TestPayload_PreToolUse_ContainsArgs(t *testing.T) {
	fake := &fakeCommandRunner{}
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "inspect", Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preToolUseInput("sess-9", "call-9", "write")
	_, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if len(fake.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(fake.calls))
	}
	var payload map[string]any
	if err := json.Unmarshal(fake.calls[0].Stdin, &payload); err != nil {
		t.Fatalf("failed to unmarshal Stdin: %v", err)
	}
	for _, field := range []string{"event", "session_id", "call_id", "tool_name", "tool_args"} {
		if _, ok := payload[field]; !ok {
			t.Errorf("expected payload field %q to be present, but it was absent", field)
		}
	}
}

// TestContextCancellation_Propagates ensures that cancelling the context while
// a hook chain is running propagates into the CommandRunner (the fake returns
// immediately but the cancel check occurs before each hook invocation).
func TestContextCancellation_Propagates(t *testing.T) {
	fake := &fakeCommandRunner{}
	// Two hooks configured; the first will not run because context is already
	// cancelled.
	fake.addAllow()
	fake.addAllow()

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "hook-1", Events: []app.HookEvent{app.HookPreToolUse}},
			{Command: "hook-2", Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately before Run

	in := preToolUseInput("sess-10", "call-10", "bash")
	_, err := runner.Run(ctx, in)
	if err == nil {
		t.Fatalf("expected error from cancelled context, got nil")
	}
}

// TestNonZeroExit_TreatedAsBlock verifies that a hook process exiting non-zero
// is treated as a blocking decision with the stderr as the reason, so a
// mis-scripted hook never silently allows an action.
func TestNonZeroExit_TreatedAsBlock(t *testing.T) {
	fake := &fakeCommandRunner{}
	// Non-zero exit code, empty stdout, error message in stderr.
	fake.addResult(1, nil, "policy violation")

	cfg := hooks.Config{
		Hooks: []hooks.HookSpec{
			{Command: "strict-hook", Events: []app.HookEvent{app.HookPreToolUse}},
		},
	}
	runner := hooks.NewRunner(cfg, fake)

	in := preToolUseInput("sess-11", "call-11", "bash")
	dec, err := runner.Run(context.Background(), in)
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}
	if dec.Allow {
		t.Fatalf("expected Allow=false (non-zero exit), got Allow=true")
	}
}

// TestCompileTimeAssertions verifies compile-time interface satisfaction; the
// test body is always empty — the value is in compilation.
func TestCompileTimeAssertions(_ *testing.T) {
	// Verify hooks.Runner satisfies app.HookRunner.
	var _ app.HookRunner = (*hooks.Runner)(nil)
}
