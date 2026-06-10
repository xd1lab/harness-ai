package eventstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	infradb "github.com/xd1lab/harness-ai/internal/orchestrator/infra/db"
)

const (
	// notifyChannel is the single LISTEN/NOTIFY channel the store uses as a
	// wakeup hint for subscribers. The NOTIFY payload is the session_id of the
	// just-appended stream; a subscriber filters by its own session and then
	// catches up from its cursor (architecture §7.1, §10.4: LISTEN/NOTIFY is only
	// a wakeup; the cursor read is authoritative). A single fixed channel avoids
	// per-session LISTEN identifier quoting.
	notifyChannel = "boltrope_events"

	// subscribePollInterval is the safety-net poll period: the subscriber catches
	// up from its cursor on every NOTIFY wakeup AND at least this often, so a
	// missed/coalesced notification (or appends from a connection that did not
	// notify) never strands an event. It bounds tail latency when notifications
	// are lost without requiring a notification per append for correctness.
	subscribePollInterval = 250 * time.Millisecond

	// subscribeChanBuffer is the buffered capacity of the delivery channel, so a
	// burst of catch-up events does not block the poller on a momentarily slow
	// reader (the relay decoupling; architecture §9.4).
	subscribeChanBuffer = 64
)

// Subscribe implements [app.EventLogPort.Subscribe]: it returns a channel that
// first delivers all committed envelopes with seq > fromSeq (cursor catch-up),
// then tails newly-appended events as they commit, until ctx is cancelled — at
// which point the channel is closed (architecture §3, §7.1). Delivery is
// per-session and ordered by seq.
//
// It runs a dedicated background goroutine holding its own connection: the
// connection LISTENs on [notifyChannel] for wakeups and the goroutine re-reads
// from its seq cursor on each wakeup and on a [subscribePollInterval] safety-net
// tick (so a lost/coalesced NOTIFY never strands an event; LISTEN/NOTIFY is only
// a hint, the cursor read is authoritative). The channel closes on ctx cancel,
// on the buffered backlog being abandoned by a gone reader, or on an
// unrecoverable read error.
func (s *Store) Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error) {
	tenantID, err := infradb.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	pc, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}

	out := make(chan domain.EventEnvelope, subscribeChanBuffer)
	go s.runSubscription(ctx, pc, out, sessionID, tenantID, fromSeq)
	return out, nil
}

// runSubscription is the subscriber goroutine. It owns pc for its lifetime and
// releases it on exit; it closes out on exit so a reader's range terminates.
func (s *Store) runSubscription(
	ctx context.Context,
	pc PooledConn,
	out chan<- domain.EventEnvelope,
	sessionID, tenantID string,
	fromSeq int64,
) {
	defer close(out)
	defer pc.Release()

	listener, ok := pc.(listenConn)
	// cursor is the last seq delivered; start just below fromSeq's exclusive
	// boundary so the first catch-up delivers seq > fromSeq.
	cursor := fromSeq

	// Establish LISTEN when the connection supports it (SimplePool's conn does;
	// a future pooled adapter may not, in which case we degrade to pure polling).
	var waker <-chan struct{}
	if ok {
		w, stop, lerr := listener.listen(ctx, notifyChannel)
		if lerr == nil {
			waker = w
			defer stop()
		}
	}

	ticker := time.NewTicker(subscribePollInterval)
	defer ticker.Stop()

	// Initial catch-up before waiting on any wakeup.
	var err error
	if cursor, err = s.drainNewEvents(ctx, pc, out, sessionID, tenantID, cursor); err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if cursor, err = s.drainNewEvents(ctx, pc, out, sessionID, tenantID, cursor); err != nil {
				return
			}
		case _, alive := <-waker:
			if !alive {
				// Listener closed; fall back to ticker-only by nil-ing the chan.
				waker = nil
				continue
			}
			if cursor, err = s.drainNewEvents(ctx, pc, out, sessionID, tenantID, cursor); err != nil {
				return
			}
		}
	}
}

// drainNewEvents reads all events with seq > cursor for the session (tenant
// scoped per-read via SET LOCAL on a short transaction), delivers them in order,
// and returns the new cursor. It respects ctx cancellation while delivering. A
// read error is returned so the caller can terminate the subscription.
func (s *Store) drainNewEvents(
	ctx context.Context,
	pc PooledConn,
	out chan<- domain.EventEnvelope,
	sessionID, tenantID string,
	cursor int64,
) (int64, error) {
	tx, err := pc.Begin(ctx)
	if err != nil {
		return cursor, fmt.Errorf("eventstore: subscribe begin: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	if err := setLocalTenant(ctx, tx, tenantID); err != nil {
		return cursor, err
	}
	rows, err := tx.Query(ctx, selectEventsAfterSeqSQL, sessionID, cursor)
	if err != nil {
		return cursor, fmt.Errorf("eventstore: subscribe query: %w", err)
	}
	envs, err := scanEnvelopes(rows, tenantID)
	rows.Close()
	if err != nil {
		return cursor, err
	}
	if err := tx.Commit(ctx); err != nil {
		return cursor, fmt.Errorf("eventstore: subscribe commit: %w", err)
	}
	committed = true

	for _, env := range envs {
		select {
		case out <- env:
			cursor = env.Seq
		case <-ctx.Done():
			return cursor, ctx.Err()
		}
	}
	return cursor, nil
}

// listenConn is implemented by a [PooledConn] whose backing connection supports
// LISTEN/NOTIFY. [simpleConn] implements it; a future pooled adapter may not, in
// which case Subscribe degrades to pure polling.
type listenConn interface {
	// listen issues LISTEN channel on the underlying connection and returns a
	// wakeup channel that receives once per NOTIFY (coalescing is fine — the
	// cursor read is authoritative), a stop func to end listening, and an error.
	listen(ctx context.Context, channel string) (<-chan struct{}, func(), error)
}

// listen implements [listenConn] for [simpleConn]. It runs LISTEN then spawns a
// goroutine blocked on WaitForNotification, forwarding a token per notification
// to the wakeup channel. The stop func cancels the wait goroutine.
func (c *simpleConn) listen(ctx context.Context, channel string) (<-chan struct{}, func(), error) {
	// pgx identifier-quotes the channel; a fixed constant name is safe.
	if _, err := c.conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return nil, nil, fmt.Errorf("eventstore: LISTEN: %w", err)
	}
	wakeCtx, cancel := context.WithCancel(ctx)
	wake := make(chan struct{}, 1)
	go func() {
		defer close(wake)
		for {
			_, err := c.conn.WaitForNotification(wakeCtx)
			if err != nil {
				// ctx cancelled or connection closed: stop forwarding.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
				return
			}
			select {
			case wake <- struct{}{}:
			default: // coalesce: a pending wakeup already covers this notification.
			}
		}
	}()
	return wake, cancel, nil
}
