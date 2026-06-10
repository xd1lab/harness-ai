// Package hooks implements the orchestrator's [app.HookRunner] port: a chain of
// user-configured hooks run at lifecycle points (PreToolUse, PostToolUse, Stop,
// PreCompact). Any hook in the chain returning block terminates evaluation and
// propagates the reason as Allow=false (first-block-wins; FR-EXT-03; architecture
// §2.3, §5.1 hooks).
//
// # Subprocess isolation
//
// The subprocess-invoking implementation is hidden behind a small [CommandRunner]
// port defined here. Tests supply a fake [CommandRunner] so no real process is
// ever spawned; the [OSCommandRunner] is the production implementation.
//
// # Decision protocol
//
// Each hook subprocess receives a JSON-encoded [HookPayload] on stdin. It must
// write a JSON-encoded [hookResponse] to stdout before exiting. A non-zero exit
// code is treated as a blocking decision (fail-safe) using stderr as the reason.
// An exit code of zero with a parsed {"continue":false} response is also a block.
// An exit code of zero with {"continue":true} (or any parseable allow response)
// is an allow. Context cancellation aborts the run and returns the context error.
package hooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"

	"github.com/boltrope/boltrope/internal/orchestrator/app"
)

// ---------------------------------------------------------------------------
// CommandRunner port
// ---------------------------------------------------------------------------

// CommandInput is the structured request passed to [CommandRunner.Run]: the
// command path plus optional arguments, environment overrides, and the JSON
// payload written to the subprocess's stdin.
type CommandInput struct {
	// Command is the executable path or name.
	Command string
	// Args are the command-line arguments.
	Args []string
	// Env is an optional list of "KEY=VALUE" environment variable overrides.
	Env []string
	// Stdin is the raw bytes written to the process's stdin (a JSON payload).
	Stdin []byte
}

// CommandResult is the raw outcome from a [CommandRunner.Run] invocation.
type CommandResult struct {
	// ExitCode is the process exit code (0 = success).
	ExitCode int
	// Stdout contains all bytes the process wrote to stdout.
	Stdout []byte
	// Stderr contains all bytes the process wrote to stderr (used as the block
	// reason on a non-zero exit).
	Stderr string
}

// CommandRunner is the port for running an external command. The production
// implementation executes a real subprocess; tests supply a deterministic fake
// so no process is ever spawned (architecture §5.1).
//
// Implementations must respect ctx cancellation: if the context is done before
// or during the call they must return ctx.Err().
type CommandRunner interface {
	// Run executes in.Command with in.Args, writes in.Stdin to stdin, and
	// returns the captured stdout, exit code, and stderr. It returns a non-nil
	// error only when the runner itself fails (e.g. the binary was not found, or
	// ctx was cancelled), not for non-zero exit codes — those are returned as
	// [CommandResult.ExitCode].
	Run(ctx context.Context, in CommandInput) (CommandResult, error)
}

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

// HookSpec is the configuration for one hook in the chain. It names the
// executable and the lifecycle events it should run for.
type HookSpec struct {
	// Command is the hook executable path or name passed to [CommandRunner.Run].
	Command string
	// Args are additional command-line arguments passed after Command.
	Args []string
	// Env is an optional list of "KEY=VALUE" environment variable overrides
	// injected into the subprocess environment.
	Env []string
	// Events is the set of [app.HookEvent] values this hook runs for. An empty
	// slice means the hook matches ALL events (catch-all).
	Events []app.HookEvent
}

// Config is the full hook pipeline configuration.
type Config struct {
	// Hooks is the ordered list of hook specifications. Hooks are evaluated in
	// slice order; the first block terminates the chain (first-block-wins).
	Hooks []HookSpec
}

// ---------------------------------------------------------------------------
// HookPayload / hookResponse (wire types for stdin/stdout)
// ---------------------------------------------------------------------------

// HookPayload is the JSON object written to a hook subprocess's stdin. It
// carries all fields relevant to the lifecycle event so the hook can make an
// informed decision without a separate out-of-band call.
type HookPayload struct {
	// Event is the lifecycle point being run (e.g. "PreToolUse").
	Event string `json:"event"`
	// SessionID is the owning session.
	SessionID string `json:"session_id"`
	// TurnID is the current turn id.
	TurnID string `json:"turn_id,omitempty"`
	// CallID is the tool call id (PreToolUse/PostToolUse only).
	CallID string `json:"call_id,omitempty"`
	// ToolName is the tool being called (tool-scoped events only).
	ToolName string `json:"tool_name,omitempty"`
	// ToolArgs is the parsed tool arguments (PreToolUse only).
	ToolArgs map[string]any `json:"tool_args,omitempty"`
	// ToolResult is the tool result content (PostToolUse only).
	ToolResult string `json:"tool_result,omitempty"`
}

// hookResponse is the JSON object the hook subprocess writes to stdout.
type hookResponse struct {
	// Continue is the allow/block flag: true = allow, false = block.
	Continue bool `json:"continue"`
	// Reason is a human-readable explanation surfaced when Continue is false.
	Reason string `json:"reason,omitempty"`
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

// Runner is the concrete [app.HookRunner] implementation. It evaluates the
// configured chain of [HookSpec] entries in order, invoking each applicable
// hook via the injected [CommandRunner], and returns the aggregate allow/block
// [app.HookDecision].
//
// Runner is safe for concurrent use: it holds no mutable state after
// construction.
type Runner struct {
	cfg       Config
	cmdRunner CommandRunner
}

// Compile-time assertion: Runner must satisfy [app.HookRunner].
var _ app.HookRunner = (*Runner)(nil)

// NewRunner constructs a [Runner] with the given config and CommandRunner.
// The CommandRunner must not be nil.
func NewRunner(cfg Config, cr CommandRunner) *Runner {
	return &Runner{cfg: cfg, cmdRunner: cr}
}

// Run executes the hook chain for in.Event and returns the aggregate
// [app.HookDecision]. It satisfies [app.HookRunner].
//
// Evaluation proceeds in [Config.Hooks] order. Hooks whose [HookSpec.Events]
// list does not include in.Event are skipped. The first hook that blocks
// short-circuits the chain (subsequent hooks are not invoked). If all hooks
// allow (or no hooks match), Allow=true is returned.
//
// A non-nil error is returned only when the hook mechanism itself fails —
// for example when ctx is cancelled. A clean block (Allow=false) is never
// an error.
func (r *Runner) Run(ctx context.Context, in app.HookInput) (app.HookDecision, error) {
	// Check for cancellation before doing any work.
	select {
	case <-ctx.Done():
		return app.HookDecision{}, ctx.Err()
	default:
	}

	payload, err := buildPayload(in)
	if err != nil {
		return app.HookDecision{}, fmt.Errorf("hooks: marshal payload: %w", err)
	}

	for _, spec := range r.cfg.Hooks {
		if !specMatchesEvent(spec, in.Event) {
			continue
		}

		// Re-check cancellation before each invocation.
		select {
		case <-ctx.Done():
			return app.HookDecision{}, ctx.Err()
		default:
		}

		res, err := r.cmdRunner.Run(ctx, CommandInput{
			Command: spec.Command,
			Args:    spec.Args,
			Env:     spec.Env,
			Stdin:   payload,
		})
		if err != nil {
			// The runner itself failed (e.g. context cancelled).
			return app.HookDecision{}, fmt.Errorf("hooks: run %q: %w", spec.Command, err)
		}

		dec, blockErr := parseResult(res, spec.Command)
		if blockErr != nil {
			// Parsing failed — treat as block (fail-safe).
			return app.HookDecision{Allow: false, Reason: blockErr.Error()}, nil
		}
		if !dec.Allow {
			// First block wins: stop the chain.
			return dec, nil
		}
	}

	return app.HookDecision{Allow: true}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// specMatchesEvent reports whether spec applies to the given event. An empty
// Events slice means catch-all (matches every event).
func specMatchesEvent(spec HookSpec, event app.HookEvent) bool {
	if len(spec.Events) == 0 {
		return true
	}
	for _, e := range spec.Events {
		if e == event {
			return true
		}
	}
	return false
}

// buildPayload serializes in to the JSON wire format expected on stdin.
func buildPayload(in app.HookInput) ([]byte, error) {
	p := HookPayload{
		Event:      string(in.Event),
		SessionID:  in.SessionID,
		TurnID:     in.TurnID,
		CallID:     in.CallID,
		ToolName:   in.ToolName,
		ToolArgs:   in.ToolArgs,
		ToolResult: in.ToolResult,
	}
	return json.Marshal(p)
}

// parseResult interprets a [CommandResult] into an [app.HookDecision].
//
//   - Non-zero exit code → block; reason is stderr (trimmed) or a generic message.
//   - Zero exit code, parseable JSON with continue=false → block; reason from JSON.
//   - Zero exit code, parseable JSON with continue=true → allow.
//   - Zero exit code, empty/unparseable stdout → allow (forward-compatible: a hook
//     that produces no JSON output is treated as "no opinion").
func parseResult(res CommandResult, cmd string) (app.HookDecision, error) {
	if res.ExitCode != 0 {
		reason := res.Stderr
		if reason == "" {
			reason = fmt.Sprintf("hook %q exited with code %d", cmd, res.ExitCode)
		}
		return app.HookDecision{Allow: false, Reason: reason}, nil
	}

	out := bytes.TrimSpace(res.Stdout)
	if len(out) == 0 {
		// No output → no opinion → allow.
		return app.HookDecision{Allow: true}, nil
	}

	var resp hookResponse
	if err := json.Unmarshal(out, &resp); err != nil {
		// Unparseable JSON from a zero-exit process → allow (forward-compatible).
		return app.HookDecision{Allow: true}, nil
	}

	if !resp.Continue {
		reason := resp.Reason
		if reason == "" {
			reason = fmt.Sprintf("hook %q blocked the action", cmd)
		}
		return app.HookDecision{Allow: false, Reason: reason}, nil
	}

	return app.HookDecision{Allow: true}, nil
}

// ---------------------------------------------------------------------------
// OSCommandRunner — production implementation
// ---------------------------------------------------------------------------

// OSCommandRunner is the production [CommandRunner] that invokes a real OS
// subprocess using [os/exec]. It is wired in infra/config; tests use a fake.
type OSCommandRunner struct{}

// NewOSCommandRunner returns an [OSCommandRunner].
func NewOSCommandRunner() *OSCommandRunner { return &OSCommandRunner{} }

// Run executes the command as a real OS subprocess. It captures stdout and
// stderr separately. A non-zero exit code is returned in [CommandResult.ExitCode],
// not as an error; errors are reserved for failures to start the process or for
// context cancellation.
func (o *OSCommandRunner) Run(ctx context.Context, in CommandInput) (CommandResult, error) {
	cmd := exec.CommandContext(ctx, in.Command, in.Args...) //nolint:gosec // command is operator-configured, not model-driven
	if len(in.Env) > 0 {
		cmd.Env = append(cmd.Environ(), in.Env...)
	}
	cmd.Stdin = bytes.NewReader(in.Stdin)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	// Distinguish "process exited non-zero" from "failed to start / ctx cancelled".
	if err != nil {
		if ctx.Err() != nil {
			return CommandResult{}, ctx.Err()
		}
		// exec.ExitError means the process ran but exited non-zero; wrap and check.
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			// Failed to start or other mechanism error.
			return CommandResult{}, fmt.Errorf("hooks: exec %q: %w", in.Command, err)
		}
	}

	return CommandResult{
		ExitCode: cmd.ProcessState.ExitCode(),
		Stdout:   stdout.Bytes(),
		Stderr:   stderr.String(),
	}, nil
}
