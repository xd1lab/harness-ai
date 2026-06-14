// SPDX-License-Identifier: Apache-2.0

package main

// T-1 (AC-9) — RED. The dev-owned in-memory event store must satisfy the
// 6-method igrpc.EventStore (the 5 app.EventLogPort methods PLUS CreateSession)
// and, driving the REAL agent.Loop, produce the SAME ordered event sequence the
// deterministic eval harness asserts for a text-only success (loop-equivalence;
// SPEC §3.3, BLOCKER 2). It references symbols (*Store, newStore) that do not
// exist yet, so this file does NOT compile — the TDD red proof for AC-9.

import (
	"context"
	"testing"
	"time"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
	"github.com/xd1lab/harness-ai/internal/platform/clock"
	"github.com/xd1lab/harness-ai/internal/platform/ids"
	"github.com/xd1lab/harness-ai/internal/platform/llm"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// AC-9 (i) — compile-time assertion that the dev store satisfies the 6-method
// igrpc.EventStore superset (NOT just the 5-method app.EventLogPort). This is the
// assertion the prior draft was missing; it is the exact superset grpc.Server
// requires (server.go:30-40) and the reason grpc/server_test.go had to add its
// own tailingEventLog. *Store does not exist yet → no compile (RED).
var (
	_ igrpc.EventStore = (*Store)(nil)
	_ app.EventLogPort = (*Store)(nil)
)

// TestStore_CreateSession_ActiveHeadZeroThenSessionStarted asserts AC-9 (iii):
// CreateSession returns an active aggregate at head_seq=0, and the first
// SessionStarted append bumps head_seq 0→1 (exactly the production half-and-half
// contract Server.CreateSession relies on).
func TestStore_CreateSession_ActiveHeadZeroThenSessionStarted(t *testing.T) {
	ctx := context.Background()
	st := newStore() // does not exist yet (RED)

	const sid = "sess-create-001"
	sess, err := st.CreateSession(ctx, sid, domain.ModeDefault)
	require.NoError(t, err)
	assert.Equal(t, domain.StatusActive, sess.Status, "new session must be active")
	assert.Equal(t, int64(0), sess.HeadSeq, "new session head_seq must be 0")

	// The creation half is followed by the first SessionStarted (head 0→1).
	envs, err := st.Append(ctx, sid, 0, 0, "req-1", app.AppendInput{
		Event: domain.SessionStarted{},
		Actor: domain.ActorSystem,
	})
	require.NoError(t, err)
	require.Len(t, envs, 1)
	assert.Equal(t, int64(1), envs[0].Seq, "first append must assign seq 1")

	after, err := st.LoadSession(ctx, sid)
	require.NoError(t, err)
	assert.Equal(t, int64(1), after.HeadSeq, "SessionStarted must bump head_seq to 1")
	assert.Equal(t, domain.StatusActive, after.Status)
}

// TestStore_DrivesRealLoop_GoldenTextOnlySuccess asserts AC-9 (ii): wiring the
// dev store as the EventLog of the REAL agent.Loop against a scripted text-only
// model turn produces the canonical golden ordered event shape that
// test/eval/scenarios_test.go pins for a text-only success:
//
//	MessageAppended, TurnStarted, AssistantMessage, TurnFinished
//
// proving the dev store's append/load/seq semantics are loop-equivalent to the
// production pgx store. It uses the real policy.Engine and the production
// clock.System/ids.System exactly as the dev binary will.
func TestStore_DrivesRealLoop_GoldenTextOnlySuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st := newStore() // RED

	const sid = "sess-golden-001"
	_, err := st.CreateSession(ctx, sid, domain.ModeDefault)
	require.NoError(t, err)
	_, err = st.Append(ctx, sid, 0, 0, "req-seed", app.AppendInput{
		Event: domain.SessionStarted{},
		Actor: domain.ActorSystem,
	})
	require.NoError(t, err)
	headAfterSeed, err := st.LoadSession(ctx, sid)
	require.NoError(t, err)

	// A scripted text-only model turn (one TextDelta then Done(StopEnd)), mirroring
	// the stub provider's deterministic single turn.
	model := newScriptedModel([]llm.StreamEvent{
		{TextDelta: &llm.TextDelta{Text: "I received your task and I am working on it."}},
		{Done: &llm.Done{StopReason: llm.StopEnd}},
	}) // newScriptedModel does not exist yet (RED)

	eng, err := policy.NewEngine(policy.Config{})
	require.NoError(t, err)

	loop := agent.NewLoop(agent.Deps{
		EventLog:  st,
		Model:     model,
		Tools:     newRuntime(), // dev no-exec runtime — does not exist yet (RED)
		Approvals: newDenyAllGate(),
		Hooks:     newAllowAllHooks(),
		Policy:    eng,
		Context:   nil, // nil Context → loop builds the window without compaction
		Clock:     clock.System{},
		IDs:       ids.System{},
	}, agent.Config{Model: "stub"})

	res, err := loop.Run(ctx, agent.RunInput{
		SessionID:   sid,
		UserMessage: userMsg("hello"),
	})
	require.NoError(t, err)
	assert.Equal(t, domain.Success, res.Reason)

	// Golden ordered event shape from the seed point onward (text-only success),
	// aligned with test/eval/scenarios_test.go.
	events, err := st.Load(ctx, sid, headAfterSeed.HeadSeq+1)
	require.NoError(t, err)
	got := make([]domain.EventType, 0, len(events))
	lastSeq := headAfterSeed.HeadSeq
	for _, e := range events {
		got = append(got, e.Type)
		assert.Equal(t, lastSeq+1, e.Seq, "per-session seq must be monotonic and contiguous")
		lastSeq = e.Seq
	}
	want := []domain.EventType{
		domain.EventMessageAppended,
		domain.EventTurnStarted,
		domain.EventAssistantMessage,
		domain.EventTurnFinished,
	}
	assert.Equal(t, want, got, "dev store must drive the real loop to the canonical golden shape")
}

// TestStore_Fork_CreatesChildAtSeq asserts the Fork half of AC-9: a child branch
// at atSeq with the parent unaffected.
func TestStore_Fork_CreatesChildAtSeq(t *testing.T) {
	ctx := context.Background()
	st := newStore() // RED

	const parent = "sess-parent-001"
	_, err := st.CreateSession(ctx, parent, domain.ModeDefault)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		_, err = st.Append(ctx, parent, int64(i), 0, "r", app.AppendInput{
			Event: domain.SessionStarted{}, Actor: domain.ActorSystem,
		})
		require.NoError(t, err)
	}

	child, err := st.Fork(ctx, parent, 2, "sess-child-001")
	require.NoError(t, err)
	assert.Equal(t, int64(2), child.ForkedFromSeq)
	assert.Equal(t, domain.StatusActive, child.Status)

	// Parent keeps its head; the fork is a branch, never a rewrite.
	p, err := st.LoadSession(ctx, parent)
	require.NoError(t, err)
	assert.Equal(t, int64(3), p.HeadSeq)
}
