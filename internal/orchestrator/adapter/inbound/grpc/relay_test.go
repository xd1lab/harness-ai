// Package grpc tests — relay terminal-path tests over bufconn, covering the
// finish/finishWithFlush branches the happy-path streaming tests miss: a loop
// that returns io.EOF (treated as clean completion: committed frames are still
// flushed and the typed terminal Result is emitted) and a loop that fails with a
// real infrastructural error (surfaced on the stream, with no Result frame).
package grpc

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genproto "github.com/boltrope/boltrope/gen/boltrope/v1"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// TestRun_LoopEOFFlushesCommittedFramesAndEmitsResult drives the relay's io.EOF
// branch: the loop's EOF is a clean completion, so committed frames the live
// tail may not have delivered are flushed from the durable log and the terminal
// Result still carries the typed outcome.
func TestRun_LoopEOFFlushesCommittedFramesAndEmitsResult(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-eof", "tenant-A")

	runner := &fakeRunner{log: log, fn: func(ctx context.Context, spec RunSpec, l *tailingEventLog) (RunOutcome, error) {
		appendAssistantText(ctx, l, spec.SessionID, "t1", "partial answer")
		return RunOutcome{Reason: domain.ErrorMaxTurns, FinalText: "partial answer", NumTurns: 4}, io.EOF
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-eof"})
	require.NoError(t, err)

	got := collectRunEvents(t, stream)
	require.NotEmpty(t, got)

	var sawText bool
	for _, ev := range got {
		if ev.GetTextDelta() != nil {
			sawText = true
			assert.Equal(t, "partial answer", ev.GetTextDelta().GetText())
		}
	}
	assert.True(t, sawText, "committed assistant text must be flushed despite the loop's io.EOF")

	res := got[len(got)-1].GetResult()
	require.NotNil(t, res, "an io.EOF loop completion must still emit the terminal Result frame")
	assert.Equal(t, genproto.TerminationSubtype_TERMINATION_SUBTYPE_ERROR_MAX_TURNS, res.GetSubtype())
	assert.Equal(t, "partial answer", res.GetFinalText())
	assert.Equal(t, int64(4), res.GetNumTurns())
}

// TestRun_LoopInfraErrorSurfacesOnStream drives the relay's infrastructural-
// failure branch: a non-EOF, non-cancellation loop error is returned to the
// client as the stream error, and no terminal Result frame is fabricated for it
// (the typed termination lives on RunOutcome only for clean completions).
func TestRun_LoopInfraErrorSurfacesOnStream(t *testing.T) {
	log := newTailingEventLog()
	log.seed("sess-err", "tenant-A")

	runner := &fakeRunner{log: log, fn: func(_ context.Context, _ RunSpec, _ *tailingEventLog) (RunOutcome, error) {
		return RunOutcome{}, errors.New("infra boom")
	}}

	h := devHarness(t, "tenant-A", runner, log)
	stream, err := h.client.Run(context.Background(), &genproto.RunRequest{TenantId: "tenant-A", SessionId: "sess-err"})
	require.NoError(t, err)

	var sawResult bool
	for {
		ev, recvErr := stream.Recv()
		if recvErr != nil {
			require.NotErrorIs(t, recvErr, io.EOF, "the stream must FAIL, not end cleanly")
			assert.Equal(t, codes.Unknown, status.Code(recvErr), "an untyped loop error surfaces with its default wire code")
			assert.Contains(t, status.Convert(recvErr).Message(), "infra boom")
			break
		}
		if ev.GetResult() != nil {
			sawResult = true
		}
	}
	assert.False(t, sawResult, "no terminal Result frame may be emitted for an infrastructural failure")
}
