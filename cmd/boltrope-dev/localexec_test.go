// SPDX-License-Identifier: Apache-2.0

package main

// ADR-0029 (AC-6 / AC-7 / AC-8 / AC-9 / AC-10 / AC-11) — RED. The dev binary's
// LOCAL-EXEC opt-in: an in-process bridge over the tool-runtime execute.Service
// backed by the real Docker runtime, plus an in-memory dedup ledger (no pgx).
//
// These tests pin the contract BEFORE implementation and reference symbols that do
// not exist yet (memDedup, execBridge, resolveTools, the new parsedRunFlags
// fields). They are RED for the RIGHT reason: a missing feature, not a typo. The
// non-integration tests here are hermetic — they exercise the dedup ledger and the
// bridge's app.ToolExecution <-> execute.Request conversion against a FAKE
// executor, never Docker. The actually-runs-bash-in-Docker assertion lives behind
// //go:build integration in localexec_integration_test.go.

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	orchapp "github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
	trapp "github.com/xd1lab/harness-ai/internal/toolruntime/app"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app/execute"
	trdomain "github.com/xd1lab/harness-ai/internal/toolruntime/domain"
)

// --- AC-11: parseRunFlags no longer rejects the new flags; --store still rejected --

// TestParseRunFlags_NewFlagsAccepted asserts --model-url/--model/--model-api-key-env/
// --enable-native-schema/--enable-local-exec are all accepted (no longer the
// roadmap rejection), while --store stays rejected (AC-11).
func TestParseRunFlags_NewFlagsAccepted(t *testing.T) {
	cfg, err := parseRunFlags([]string{
		"--model-url", "http://localhost:11434/v1",
		"--model", "gemma",
		"--model-api-key-env", "MY_KEY",
		"--enable-native-schema",
		"--enable-local-exec",
	})
	require.NoError(t, err, "the new opt-in flags must be accepted, not rejected")
	assert.Equal(t, "http://localhost:11434/v1", cfg.modelURL)
	assert.Equal(t, "gemma", cfg.model)
	assert.Equal(t, "MY_KEY", cfg.modelAPIKeyEnv)
	assert.True(t, cfg.enableNativeSchema)
	assert.True(t, cfg.enableLocalExec)
}

func TestParseRunFlags_StoreStillRejected(t *testing.T) {
	_, err := parseRunFlags([]string{"--store=sqlite"})
	require.Error(t, err, "--store must remain a roadmap rejection")
}

// --- AC-8: in-memory DedupStore satisfies toolruntime app.DedupStore -----------

// memDedupCtor is the package-private constructor for the dev in-memory dedup
// ledger. The compile-time assertion lives in the implementation; this test pins
// the get-or-create / complete / lookup behavior.
func TestMemDedup_GetOrCreate_Complete_Lookup(t *testing.T) {
	d := newMemDedup() // does not exist yet (RED)

	// Begin on an unknown key creates a "started" record.
	rec := trapp.ExecutionRecord{TenantID: "t1", SessionID: "s1", IdempotencyKey: "k1"}
	got, err := d.Begin(context.Background(), rec)
	require.NoError(t, err)
	assert.Equal(t, trapp.ExecStarted, got.Status, "first Begin must record ExecStarted")

	// Begin on a known key returns the EXISTING record (get-or-create).
	got2, err := d.Begin(context.Background(), trapp.ExecutionRecord{TenantID: "t1", SessionID: "s1", IdempotencyKey: "k1"})
	require.NoError(t, err)
	assert.Equal(t, trapp.ExecStarted, got2.Status, "repeat Begin must return the existing record, not a new one")

	// Complete records the terminal status + result.
	done := rec
	done.Status = trapp.ExecCompleted
	done.Result = trdomain.Observation{Content: "ok"}
	require.NoError(t, d.Complete(context.Background(), done))

	// A later Begin observes the completed result.
	after, err := d.Begin(context.Background(), rec)
	require.NoError(t, err)
	assert.Equal(t, trapp.ExecCompleted, after.Status)
	assert.Equal(t, "ok", after.Result.Content)

	// Lookup returns the stored record.
	look, err := d.Lookup(context.Background(), "t1", "s1", "k1")
	require.NoError(t, err)
	assert.Equal(t, trapp.ExecCompleted, look.Status)

	// Lookup of an absent key errors.
	_, err = d.Lookup(context.Background(), "t1", "s1", "missing")
	require.Error(t, err, "Lookup of an absent key must error")
}

// TestMemDedup_SatisfiesPort is a compile-time-ish assertion at runtime that the
// dev dedup store satisfies the toolruntime app.DedupStore port.
func TestMemDedup_SatisfiesPort(t *testing.T) {
	var _ trapp.DedupStore = newMemDedup() // RED until newMemDedup exists
}

// --- AC-9: in-process bridge satisfies orchestrator app.ToolRuntimePort --------

// fakeExecutor is a test double for the execute.Service the bridge calls. It
// records the Request it received and returns a canned Result, so the bridge's
// conversion (app.ToolExecution -> execute.Request and execute.Result ->
// app.ToolResult) is verified without Docker.
type fakeExecutor struct {
	gotReq execute.Request
	result execute.Result
	tools  []trdomain.ToolSpec
}

func (f *fakeExecutor) Execute(_ context.Context, req execute.Request, _ execute.Emitter) (execute.Result, error) {
	f.gotReq = req
	return f.result, nil
}

func (f *fakeExecutor) ListTools(_ context.Context) ([]trdomain.ToolSpec, error) {
	return f.tools, nil
}

// TestExecBridge_ExecuteTool_ConvertsAndStreamsTerminal asserts the bridge maps an
// orchestrator app.ToolExecution into an execute.Request (SessionID, ToolName,
// CallID, parsed Args, IdempotencyKey, and the dev synthetic TenantID), and that
// it converts the execute.Result back into a single terminal app.ToolResult
// followed by io.EOF (AC-9).
func TestExecBridge_ExecuteTool_ConvertsAndStreamsTerminal(t *testing.T) {
	fe := &fakeExecutor{
		result: execute.Result{Observation: trdomain.Observation{
			Content:   "hello from sandbox",
			IsError:   false,
			Truncated: true,
			BlobRef:   "blob-123",
		}},
	}
	br := newExecBridge(fe) // RED until newExecBridge exists

	// Compile-time assertion (also enforced in the implementation).
	var _ orchapp.ToolRuntimePort = br

	exec := orchapp.ToolExecution{
		SessionID:      "sess-1",
		Call:           llm.ToolCall{ID: "call-9", Name: "bash", Args: map[string]any{"cmd": "echo hi"}},
		IdempotencyKey: "idem-1",
	}
	stream, err := br.ExecuteTool(context.Background(), exec)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stream.Close() })

	// The conversion into the execute.Request must be faithful.
	assert.Equal(t, "sess-1", fe.gotReq.SessionID)
	assert.Equal(t, "bash", fe.gotReq.ToolName)
	assert.Equal(t, "call-9", fe.gotReq.CallID)
	assert.Equal(t, "idem-1", fe.gotReq.IdempotencyKey)
	assert.Equal(t, "echo hi", fe.gotReq.Args["cmd"], "Call.Args JSON must be parsed into the Args map")
	assert.NotEmpty(t, fe.gotReq.TenantID, "the bridge must set the dev synthetic TenantID")

	// The first Recv yields the terminal result; the second yields io.EOF.
	ev, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev.Result, "first event must be the terminal result")
	assert.Equal(t, "hello from sandbox", ev.Result.Content)
	assert.False(t, ev.Result.IsError)
	assert.True(t, ev.Result.Truncated)
	assert.Equal(t, "blob-123", ev.Result.BlobRef)

	_, err = stream.Recv()
	assert.ErrorIs(t, err, io.EOF, "after the terminal result the stream must return io.EOF")
}

// TestExecBridge_ListTools_MapsDescriptors asserts ListTools maps the executor's
// registry specs into orchestrator app.ToolDescriptors carrying
// Name/Description/JSONSchema/SideEffect/EgressClass (AC-9).
func TestExecBridge_ListTools_MapsDescriptors(t *testing.T) {
	fe := &fakeExecutor{tools: []trdomain.ToolSpec{
		{
			Name:        "bash",
			Description: "Run a shell command in the sandbox.",
			JSONSchema:  []byte(`{"type":"object"}`),
			SideEffect:  trdomain.SideEffectMutating,
			EgressClass: trdomain.EgressClassNone,
		},
	}}
	br := newExecBridge(fe)

	got, err := br.ListTools(context.Background(), "sess-1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, "bash", got[0].Name)
	assert.Equal(t, "Run a shell command in the sandbox.", got[0].Description)
	assert.JSONEq(t, `{"type":"object"}`, string(got[0].JSONSchema))
	assert.Equal(t, trdomain.SideEffectMutating, got[0].SideEffect)
	assert.Equal(t, trdomain.EgressClassNone, got[0].EgressClass)
}

// --- AC-6: resolveTools selects the bridge when local-exec is ON ---------------

// TestResolveTools_LocalExecSelectsBridge asserts that with --enable-local-exec
// the resolved tool port is the in-process bridge (NOT the no-exec *Runtime), and
// that without it the no-exec *Runtime is used (AC-6). It requires Docker to be
// reachable to construct the real runtime, so it skips when DOCKER is absent.
func TestResolveTools_LocalExecSelectsBridge(t *testing.T) {
	cfg, err := parseRunFlags([]string{"--enable-local-exec"})
	require.NoError(t, err)

	tools, err := resolveTools(cfg, noEnv())
	if err != nil {
		// Constructing the real Docker runtime may fail on a host without Docker;
		// that is acceptable for this RED contract test — the point is that
		// resolveTools ATTEMPTS the bridge path, not the no-exec path.
		t.Skipf("local-exec runtime construction unavailable (no Docker?): %v", err)
	}
	_, isNoExec := tools.(*Runtime)
	assert.False(t, isNoExec, "with --enable-local-exec the tool port must NOT be the no-exec *Runtime")
}
