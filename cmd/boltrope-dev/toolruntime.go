// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"io"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// Compile-time assertion: the dev runtime satisfies the frozen
// [app.ToolRuntimePort] the loop depends on.
var _ app.ToolRuntimePort = (*Runtime)(nil)

// devExecDisabledMessage is the deterministic refusal the dev no-exec sandbox
// returns for any genuinely host-effecting tool (bash). It is asserted verbatim
// (case-insensitively) by the runtime test and surfaced in the startup banner's
// NO-EXEC marker (K-2: sandbox = in-process no-exec subset).
const devExecDisabledMessage = "dev sandbox exec disabled"

// Runtime is the dev binary's in-process, no-exec [app.ToolRuntimePort]. It
// promotes apptest.FakeToolRuntime into a dev-owned adapter that advertises a
// host-side-effect-free tool subset (read/compute/sub-agent) plus a bash
// PLACEHOLDER, so the loop's full dispatch path — read-only-vs-mutation
// scheduling, the policy/approval gate, and egress classification — runs with
// intact semantics WITHOUT executing model-generated commands on the developer's
// host (K-2).
//
// It deliberately does NOT reuse the docker per-session runtime (whose isolation
// is bound to a Docker daemon + PID-namespace reaping + --network none per
// ADR-0014 — the exact onboarding friction dev mode removes) and does NOT open
// bare local exec (no PID namespace, no egress severance, no cgroup limits). A
// real shell-capable local sandbox is explicitly re-scoped to roadmap and, if
// shipped, must hang on the existing Workspace/RuntimePort seam behind a second
// explicit --enable-local-exec opt-in (rejected today; see fence.go).
type Runtime struct {
	tools []app.ToolDescriptor
}

// newRuntime returns a dev [Runtime] advertising the safe no-exec subset.
func newRuntime() *Runtime {
	return &Runtime{tools: devToolSubset()}
}

// devToolSubset is the host-side-effect-free tool set the dev runtime advertises.
// Each carries the [domain.SideEffect]/[domain.EgressClass] the loop's scheduler
// and policy read to classify the call: the read/compute tools are read-only with
// no egress (eligible for the bounded read-only pool, off the egress/taint path);
// sub-agent is read-only/no-egress (it recurses the in-process loop, not the
// host); bash is mutating (so policy still gates it) but is a placeholder that
// refuses to exec.
func devToolSubset() []app.ToolDescriptor {
	return []app.ToolDescriptor{
		{
			Name:        "read",
			Description: "Read a file or value from the dev sandbox (no host side effects).",
			JSONSchema:  []byte(`{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`),
			SideEffect:  domain.SideEffectReadOnly,
			EgressClass: domain.EgressClassNone,
		},
		{
			Name:        "compute",
			Description: "Perform a pure in-process computation (no host side effects).",
			JSONSchema:  []byte(`{"type":"object","properties":{"expr":{"type":"string"}}}`),
			SideEffect:  domain.SideEffectReadOnly,
			EgressClass: domain.EgressClassNone,
		},
		{
			Name:        "sub-agent",
			Description: "Spawn a depth-limited child agent (in-process; no host side effects).",
			JSONSchema:  []byte(`{"type":"object","properties":{"task":{"type":"string"}},"required":["task"]}`),
			SideEffect:  domain.SideEffectReadOnly,
			EgressClass: domain.EgressClassNone,
		},
		{
			Name:        "bash",
			Description: "Run a shell command. DISABLED in dev no-exec mode (placeholder; gated by policy, never executed).",
			JSONSchema:  []byte(`{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}`),
			// Mutating so the loop serializes it and policy still gates it through the
			// approval path; the execution itself is refused below (no host exec).
			SideEffect:  domain.SideEffectMutating,
			EgressClass: domain.EgressClassNone,
		},
	}
}

// ListTools returns the advertised safe subset so the loop can build the model's
// tool definitions and the policy/scheduler can read each tool's
// SideEffect/EgressClass.
func (r *Runtime) ListTools(_ context.Context, _ string) ([]app.ToolDescriptor, error) {
	return append([]app.ToolDescriptor(nil), r.tools...), nil
}

// ExecuteTool dispatches one tool call and returns a [app.ToolStream] of a single
// terminal result. bash refuses with the deterministic devExecDisabledMessage and
// performs NO host side effect (it never runs the model-generated command); the
// read/compute/sub-agent tools return a benign, deterministic, host-effect-free
// result so the loop's read-only path stays fully exercisable in dev mode.
func (r *Runtime) ExecuteTool(_ context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	return newResultStream(r.dispatch(exec.Call)), nil
}

// dispatch computes the deterministic [app.ToolResult] for one tool call without
// touching the host.
func (r *Runtime) dispatch(call llm.ToolCall) app.ToolResult {
	switch call.Name {
	case "bash":
		// The only genuinely host-effecting tool: refused, never executed.
		return app.ToolResult{
			IsError: true,
			Content: devExecDisabledMessage + " (no-exec sandbox; --enable-local-exec is roadmap)",
		}
	case "read", "compute", "sub-agent":
		return app.ToolResult{
			Content: fmt.Sprintf("dev sandbox: %q acknowledged (no-exec subset; host-side-effect-free)", call.Name),
		}
	default:
		// Unknown tool: a non-host-effecting error result keeps dispatch total.
		return app.ToolResult{
			IsError: true,
			Content: fmt.Sprintf("dev sandbox: unknown tool %q", call.Name),
		}
	}
}

// resultStream is a minimal [app.ToolStream] that yields one terminal result then
// io.EOF (the dev no-exec tools produce no progress chunks).
type resultStream struct {
	result app.ToolResult
	done   bool
}

// newResultStream wraps a terminal result as a single-shot [app.ToolStream].
func newResultStream(result app.ToolResult) *resultStream {
	return &resultStream{result: result}
}

// Recv returns the terminal result once, then io.EOF.
func (s *resultStream) Recv() (app.ToolEvent, error) {
	if s.done {
		return app.ToolEvent{}, io.EOF
	}
	s.done = true
	r := s.result
	return app.ToolEvent{Result: &r}, nil
}

// Close is a no-op; there are no resources to release.
func (s *resultStream) Close() error { return nil }

var _ app.ToolStream = (*resultStream)(nil)
