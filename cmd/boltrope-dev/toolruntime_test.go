// SPDX-License-Identifier: Apache-2.0

package main

// T-2 (AC-10) — RED. The dev no-exec tool runtime must advertise a
// host-side-effect-free tool subset (read/compute/sub-agent + a bash placeholder)
// and REFUSE to execute model-generated shell, returning a deterministic
// "dev sandbox exec disabled" result, while keeping the loop's dispatch /
// read-only-vs-mutation scheduling / policy / egress-classification semantics
// intact (K-2: sandbox = no-exec subset). It references *Runtime / newRuntime,
// which do not exist yet → RED.

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AC-10 (iv) — compile assertion that the dev runtime satisfies the frozen
// app.ToolRuntimePort. *Runtime does not exist yet → no compile (RED).
var _ app.ToolRuntimePort = (*Runtime)(nil)

// drainToolResult drives a ToolStream to its single terminal result.
func drainToolResult(t *testing.T, s app.ToolStream) app.ToolResult {
	t.Helper()
	defer func() { _ = s.Close() }()
	for {
		ev, err := s.Recv()
		if errors.Is(err, io.EOF) {
			t.Fatalf("tool stream ended with no terminal result")
		}
		require.NoError(t, err)
		if ev.Result != nil {
			return *ev.Result
		}
	}
}

// TestRuntime_ListTools_AdvertisesSafeSubset asserts AC-10 (i): the safe subset is
// advertised with the correct domain.SideEffect / domain.EgressClass so the loop's
// scheduler and policy can classify each call. A read-only tool must be classified
// read-only (eligible for the bounded read-only pool); none of the safe subset may
// be classed as host-mutating exec.
func TestRuntime_ListTools_AdvertisesSafeSubset(t *testing.T) {
	rt := newRuntime() // RED
	tools, err := rt.ListTools(context.Background(), "sess-1")
	require.NoError(t, err)
	require.NotEmpty(t, tools, "dev runtime must advertise a non-empty safe subset")

	byName := map[string]app.ToolDescriptor{}
	for _, d := range tools {
		byName[d.Name] = d
	}

	// A read/compute tool must be present and classified read-only with NO network
	// egress, so it flows through the bounded read-only scheduling path (not the
	// mutation-serialized one) and is not pushed onto the egress/taint path.
	read, ok := byName["read"]
	require.True(t, ok, "the safe subset must include a read-only 'read' tool")
	assert.Equal(t, domain.SideEffectReadOnly, read.SideEffect)
	assert.Equal(t, domain.EgressClassNone, read.EgressClass)

	// The bash placeholder must be advertised (so the loop can dispatch it through
	// policy) — its refusal is exercised in the exec test below.
	_, hasBash := byName["bash"]
	assert.True(t, hasBash, "the bash placeholder must be advertised so policy still gates it")
}

// TestRuntime_Bash_RefusesExecWithDeterministicMessage asserts AC-10 (iii): the
// bash placeholder returns app.ToolResult{IsError:true} carrying a deterministic
// "dev sandbox exec disabled" message and performs NO host side effect (it never
// runs the model-generated command).
func TestRuntime_Bash_RefusesExecWithDeterministicMessage(t *testing.T) {
	rt := newRuntime() // RED
	stream, err := rt.ExecuteTool(context.Background(), app.ToolExecution{
		SessionID: "sess-1",
		Call: llm.ToolCall{
			ID:   "call-bash-1",
			Name: "bash",
			Args: map[string]any{"cmd": "rm -rf /tmp/should-not-run"},
		},
		IdempotencyKey: "k1",
	})
	require.NoError(t, err)

	res := drainToolResult(t, stream)
	assert.True(t, res.IsError, "bash must refuse in dev no-exec mode")
	assert.Truef(t,
		strings.Contains(strings.ToLower(res.Content), "dev sandbox exec disabled"),
		"refusal message must be the deterministic 'dev sandbox exec disabled' marker; got %q", res.Content)
}

// TestRuntime_ReadOnlyTool_StillProducesResult asserts AC-10 (ii): a read/compute
// tool in the safe subset still dispatches and yields a (non-error) result, so the
// loop's read-only path remains fully exercisable in dev mode.
func TestRuntime_ReadOnlyTool_StillProducesResult(t *testing.T) {
	rt := newRuntime() // RED
	stream, err := rt.ExecuteTool(context.Background(), app.ToolExecution{
		SessionID: "sess-1",
		Call: llm.ToolCall{
			ID:   "call-read-1",
			Name: "read",
			Args: map[string]any{"path": "README.md"},
		},
		IdempotencyKey: "k2",
	})
	require.NoError(t, err)

	res := drainToolResult(t, stream)
	assert.False(t, res.IsError, "a safe read-only tool must not be refused")
}
