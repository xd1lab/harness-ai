package grpc

import (
	"context"
	"errors"
	"io"
	"sync"

	genproto "github.com/xd1lab/harness-ai/gen/boltrope/v1"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// ApprovalNotifier is an OPTIONAL capability an [app.ApprovalGate] may implement
// so the Run relay can surface a pending ask as an ApprovalRequest frame on the
// client stream. When the injected gate implements it, the relay subscribes per
// run; when it does not, approvals still function (the client resolves them via
// Control out-of-band) but no in-band ApprovalRequest frame is emitted. This
// keeps the gate contract (app.ApprovalGate) frozen while allowing the rich
// streaming experience the proto supports.
type ApprovalNotifier interface {
	// SubscribeApprovals registers fn to be called for every approval request
	// raised on sessionID until the returned cancel func is called. fn must not
	// block. It is safe for concurrent use.
	SubscribeApprovals(sessionID string, fn func(app.ApprovalRequest)) (cancel func())
}

// relay drives one Run stream: it starts the loop via the [Runner], tails the
// durable event log via [app.EventLogPort.Subscribe] from afterSeq, and writes a
// [genproto.RunEvent] for every client-visible event (each carrying its seq for
// resumable reattach; FR-API-01), then writes the terminal RunResult frame when
// the loop completes. It also implements [ClientSink] so the loop can forward
// live text/thinking deltas; those are delivered as best-effort supplementary
// frames and never block the loop (NFR-REL-05).
type relay struct {
	server    *Server
	stream    genproto.OrchestratorService_RunServer
	sessionID string
	afterSeq  int64

	// sendMu serializes writes to the gRPC stream (the subscription goroutine and
	// the live-sink path both send; grpc.ServerStream.Send is not safe for
	// concurrent use).
	sendMu sync.Mutex
	// liveSeq tracks the highest durable seq observed, used to tag best-effort
	// live delta/approval frames so the client can still resume near them. Guarded
	// by sendMu.
	liveSeq int64
}

// run executes the loop and relays its events to the client. It blocks until the
// loop terminates (or the client/loop context is cancelled), returning the gRPC
// status to surface to the client.
func (r *relay) run(ctx context.Context, spec RunSpec) error {
	// The loop runs under a cancellable child context so Control.Interrupt can
	// cancel it independently of the client stream (FR-LOOP-03). Cancelling on
	// return also tears the loop down if the client disconnects first.
	loopCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.server.registerRun(r.sessionID, cancel)
	defer r.server.unregisterRun(r.sessionID)

	// Subscribe to the durable log BEFORE starting the loop so no committed event
	// between afterSeq and the loop's first append is missed.
	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	events, err := r.server.log.Subscribe(subCtx, r.sessionID, r.afterSeq)
	if err != nil {
		return err
	}

	// Surface pending approvals as in-band frames when the gate supports it.
	if notifier, ok := r.server.gate.(ApprovalNotifier); ok {
		unsub := notifier.SubscribeApprovals(r.sessionID, r.onApproval)
		defer unsub()
	}

	// Start the loop on a background goroutine; it tails the durable log so a slow
	// client never backpressures generation (NFR-REL-05).
	type loopDone struct {
		out RunOutcome
		err error
	}
	done := make(chan loopDone, 1)
	go func() {
		out, err := r.server.runner.Run(loopCtx, spec)
		done <- loopDone{out: out, err: err}
	}()

	// Relay committed events to the client until the loop completes, then drain
	// any remaining committed events up to the loop's terminal state and emit the
	// RunResult frame.
	for {
		select {
		case <-ctx.Done():
			// Client disconnected (or deadline). The loop keeps running under its
			// own decision via loopCtx unless this ctx is its parent; we cancel via
			// defer. The durable log preserves progress for a later reattach.
			return ctx.Err()

		case env, ok := <-events:
			if !ok {
				// Subscription closed (subCtx cancelled) — wait for the loop result.
				ld := <-done
				return r.finishWithFlush(ctx, ld.out, ld.err)
			}
			if err := r.relayEnvelope(env); err != nil {
				return err
			}

		case ld := <-done:
			// The loop finished. Emit any committed client-visible frames the live
			// tail had not yet delivered, then the terminal result.
			return r.finishWithFlush(ctx, ld.out, ld.err)
		}
	}
}

// finishWithFlush reconciles the durable log before emitting the terminal
// result: it loads the session from the resume cursor and relays every
// client-visible frame with a seq strictly greater than the highest already
// delivered (r.liveSeq), then emits the RunResult frame. This makes delivery
// complete and duplicate-free whether or not the Subscribe implementation tails
// live up to the final append (the real pgx store tails; a snapshot-only
// subscription is reconciled here), and closes the loop-completion/commit race
// deterministically.
func (r *relay) finishWithFlush(ctx context.Context, out RunOutcome, loopErr error) error {
	// An interrupt cancels the loop's context while the CLIENT stream context is
	// still alive (Control.Interrupt cancels loopCtx, not ctx). In that case the
	// run terminated with a typed reason (e.g. error_during_execution) recorded as
	// a TurnAborted in the log, so we still flush and emit the terminal Result —
	// the client learns the typed outcome rather than a bare Canceled. A genuine
	// client disconnect (ctx.Done) takes the cancellation path in finish.
	interrupted := errors.Is(loopErr, context.Canceled) && ctx.Err() == nil
	if loopErr == nil || errors.Is(loopErr, io.EOF) || interrupted {
		if err := r.flushFrom(ctx); err != nil {
			return err
		}
	}
	if interrupted {
		return r.sendResult(out)
	}
	return r.finish(out, loopErr)
}

// flushFrom loads the session from afterSeq and sends any client-visible frame
// not yet delivered (seq > liveSeq), advancing liveSeq as it goes.
func (r *relay) flushFrom(ctx context.Context) error {
	r.sendMu.Lock()
	from := r.afterSeq
	delivered := r.liveSeq
	r.sendMu.Unlock()

	events, err := r.server.log.Load(ctx, r.sessionID, from)
	if err != nil {
		// A load failure here does not invalidate the terminal result; the live
		// tail already delivered what it could. Surface nothing and let the result
		// frame carry the outcome.
		return nil
	}
	for _, env := range events {
		if env.Seq <= delivered {
			continue
		}
		if err := r.relayEnvelope(env); err != nil {
			return err
		}
	}
	return nil
}

// relayEnvelope maps one committed envelope to a client frame (when it has a
// client-visible payload) and sends it, advancing the live seq cursor.
func (r *relay) relayEnvelope(env domain.EventEnvelope) error {
	r.sendMu.Lock()
	if env.Seq > r.liveSeq {
		r.liveSeq = env.Seq
	}
	r.sendMu.Unlock()

	frame, ok := envelopeToFrame(env)
	if !ok {
		return nil
	}
	return r.send(frame)
}

// finish emits the terminal RunResult frame for the loop outcome (or maps an
// infrastructural loop error to a gRPC status). A context-cancellation loop error
// is surfaced as the canonical Canceled status.
func (r *relay) finish(out RunOutcome, loopErr error) error {
	if loopErr != nil {
		if errors.Is(loopErr, context.Canceled) {
			return loopErr
		}
		if errors.Is(loopErr, io.EOF) {
			// treat as clean completion with whatever outcome we have
			return r.sendResult(out)
		}
		return loopErr
	}
	return r.sendResult(out)
}

// sendResult writes the terminal RunResult frame. Its seq is the current head
// (the highest durable seq observed), so the client's Last-Event-ID after the
// result is the session head.
func (r *relay) sendResult(out RunOutcome) error {
	r.sendMu.Lock()
	seq := r.liveSeq
	r.sendMu.Unlock()
	return r.send(&genproto.RunEvent{
		Seq: seq,
		Payload: &genproto.RunEvent_Result{
			Result: &genproto.RunResult{
				Subtype:   toGenSubtype(out.Reason),
				FinalText: out.FinalText,
				Usage:     toGenUsage(out.Usage),
				CostUsd:   out.CostUSD,
				NumTurns:  out.NumTurns,
			},
		},
	})
}

// send writes one frame to the gRPC stream under the send mutex.
func (r *relay) send(frame *genproto.RunEvent) error {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	return r.stream.Send(frame)
}

// onApproval is invoked by an ApprovalNotifier-capable gate when a tool call
// needs approval; it emits an ApprovalRequest frame tagged with the current head
// seq. Failures to send are ignored (best-effort; the durable log remains the
// authority and the client can reattach).
func (r *relay) onApproval(req app.ApprovalRequest) {
	r.sendMu.Lock()
	seq := r.liveSeq
	r.sendMu.Unlock()
	_ = r.send(toGenApprovalFrame(seq, req.CallID, req.ToolName, req.Reason, req.Args))
}

// ---- ClientSink (live deltas) ----------------------------------------------
//
// In this transport, durable, resumable delivery is driven entirely by the
// event-log subscription (every frame carries a real seq, so reattach is exact
// and frames are never duplicated). The loop is still given this sink so a future
// low-latency live-delta path can be enabled without touching the loop contract;
// for now the sink methods intentionally do nothing for delivery — the assembled
// AssistantMessage / AssistantMessageDelta events on the subscription are the
// single authoritative, deduplicated source of the client's text frames. This
// also guarantees the loop is never backpressured by a slow client (NFR-REL-05),
// since these methods return immediately.

// OnTextDelta satisfies [ClientSink]; delivery is via the durable subscription
// (see the package comment above), so this is a no-op.
func (r *relay) OnTextDelta(_, _, _ string) {}

// OnThinkingDelta satisfies [ClientSink]; delivery is via the durable
// subscription, so this is a no-op.
func (r *relay) OnThinkingDelta(_, _, _ string) {}
