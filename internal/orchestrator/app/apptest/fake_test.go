package apptest_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/apptest"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// ---------------------------------------------------------------------------
// Interface satisfaction (compile-time assertions surfaced as test)
// ---------------------------------------------------------------------------

func TestCompileTimeAssertions(_ *testing.T) {
	// These casts will fail to compile if the fakes no longer satisfy their ports.
	var _ app.EventLogPort = (*apptest.FakeEventLog)(nil)
	var _ app.ModelGatewayPort = (*apptest.FakeModelGateway)(nil)
	var _ app.ToolRuntimePort = (*apptest.FakeToolRuntime)(nil)
	var _ app.ToolStream = (*apptest.FakeToolStream)(nil)
	var _ app.ApprovalGate = (*apptest.FakeApprovalGate)(nil)
	var _ app.HookRunner = (*apptest.FakeHookRunner)(nil)
	var _ app.SubAgentPort = (*apptest.FakeSubAgent)(nil)
}

// ---------------------------------------------------------------------------
// FakeEventLog
// ---------------------------------------------------------------------------

func TestFakeEventLog_AppendAndLoad(t *testing.T) {
	el := apptest.NewFakeEventLog()
	ctx := context.Background()

	evs, err := el.Append(ctx, "sess-1", 0, 1, "req-1",
		app.AppendInput{Event: domain.SessionStarted{}, Actor: domain.ActorSystem},
	)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, int64(1), evs[0].Seq)

	loaded, err := el.Load(ctx, "sess-1", 1)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
}

func TestFakeEventLog_Fork(t *testing.T) {
	el := apptest.NewFakeEventLog()
	ctx := context.Background()
	_, _ = el.Append(ctx, "parent", 0, 1, "r1", app.AppendInput{Event: domain.SessionStarted{}})
	child, err := el.Fork(ctx, "parent", 1, "child")
	require.NoError(t, err)
	assert.Equal(t, "parent", child.ParentID)
	assert.Equal(t, int64(1), child.ForkedFromSeq)
}

func TestFakeEventLog_AppendErr(t *testing.T) {
	el := apptest.NewFakeEventLog()
	el.AppendErrs = []error{app.ConflictError}
	_, err := el.Append(context.Background(), "s", 0, 1, "r", app.AppendInput{Event: domain.SessionStarted{}})
	assert.ErrorIs(t, err, app.ConflictError)
}

func TestFakeEventLog_Subscribe(t *testing.T) {
	el := apptest.NewFakeEventLog()
	ctx, cancel := context.WithCancel(context.Background())
	_, _ = el.Append(ctx, "s", 0, 1, "r", app.AppendInput{Event: domain.SessionStarted{}})
	ch, err := el.Subscribe(ctx, "s", 0)
	require.NoError(t, err)
	env := <-ch
	assert.Equal(t, domain.EventSessionStarted, env.Type)
	cancel()
}

// ---------------------------------------------------------------------------
// FakeModelGateway
// ---------------------------------------------------------------------------

func TestFakeModelGateway_Generate(t *testing.T) {
	mg := apptest.NewFakeModelGateway()
	mg.AddGenerateText("hello")
	resp, err := mg.Generate(context.Background(), llm.Request{Model: "m"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, llm.StopEnd, resp.StopReason)
}

func TestFakeModelGateway_Stream(t *testing.T) {
	mg := apptest.NewFakeModelGateway()
	mg.AddStreamEvents(llm.StreamEvent{TextDelta: &llm.TextDelta{Text: "chunk"}})
	sr, err := mg.Stream(context.Background(), llm.Request{})
	require.NoError(t, err)
	ev, err2 := sr.Recv()
	require.NoError(t, err2)
	assert.NotNil(t, ev.TextDelta)
	ev2, err3 := sr.Recv()
	require.NoError(t, err3)
	assert.NotNil(t, ev2.Done) // auto-appended
	_, err4 := sr.Recv()
	assert.Equal(t, io.EOF, err4)
}

// ---------------------------------------------------------------------------
// FakeToolStream
// ---------------------------------------------------------------------------

func TestFakeToolStream_ProgressThenResult(t *testing.T) {
	s := apptest.NewFakeToolStream(
		app.ToolResult{Content: "done"},
		app.ToolProgress{Output: "step1"},
	)
	ev1, err := s.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev1.Progress)
	assert.Equal(t, "step1", ev1.Progress.Output)

	ev2, err := s.Recv()
	require.NoError(t, err)
	require.NotNil(t, ev2.Result)
	assert.Equal(t, "done", ev2.Result.Content)

	_, err = s.Recv()
	assert.Equal(t, io.EOF, err)
}

// ---------------------------------------------------------------------------
// FakeApprovalGate
// ---------------------------------------------------------------------------

func TestFakeApprovalGate_RequestAndResolve(t *testing.T) {
	gate := apptest.NewFakeApprovalGate()
	ctx := context.Background()

	req := app.ApprovalRequest{SessionID: "s1", CallID: "c1", ToolName: "bash"}
	resultCh := make(chan domain.AskResolution, 1)
	go func() {
		res, _ := gate.Request(ctx, req)
		resultCh <- res
	}()

	err := gate.Resolve(ctx, "s1", "c1", domain.AskAllowed)
	require.NoError(t, err)
	res := <-resultCh
	assert.Equal(t, domain.AskAllowed, res)
}

func TestFakeApprovalGate_CancelledContext(t *testing.T) {
	gate := apptest.NewFakeApprovalGate()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	req := app.ApprovalRequest{SessionID: "s", CallID: "c", ToolName: "edit"}
	_, err := gate.Request(ctx, req)
	assert.Error(t, err)
}

// ---------------------------------------------------------------------------
// FakeHookRunner
// ---------------------------------------------------------------------------

func TestFakeHookRunner_AllowByDefault(t *testing.T) {
	h := apptest.NewFakeHookRunner()
	d, err := h.Run(context.Background(), app.HookInput{Event: app.HookPreToolUse, ToolName: "bash"})
	require.NoError(t, err)
	assert.True(t, d.Allow)
}

func TestFakeHookRunner_Block(t *testing.T) {
	h := apptest.NewFakeHookRunner()
	h.AddDecision(false, "hook_blocked")
	d, err := h.Run(context.Background(), app.HookInput{Event: app.HookPreToolUse})
	require.NoError(t, err)
	assert.False(t, d.Allow)
	assert.Equal(t, "hook_blocked", d.Reason)
}

// ---------------------------------------------------------------------------
// FakeSubAgent
// ---------------------------------------------------------------------------

func TestFakeSubAgent_Spawn(t *testing.T) {
	sa := apptest.NewFakeSubAgent(3)
	sa.AddResult(app.ToolResult{Content: "child done"}, nil)
	result, err := sa.Spawn(context.Background(), app.SubAgentSpawn{Depth: 1, Task: "do something"})
	require.NoError(t, err)
	assert.Equal(t, "child done", result.Content)
	assert.Len(t, sa.Calls(), 1)
}

func TestFakeSubAgent_DepthExceeded(t *testing.T) {
	sa := apptest.NewFakeSubAgent(2)
	result, err := sa.Spawn(context.Background(), app.SubAgentSpawn{Depth: 3})
	require.NoError(t, err)
	assert.True(t, result.IsError)
	assert.Equal(t, "max sub-agent depth exceeded", result.Content)
}
