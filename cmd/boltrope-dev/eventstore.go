// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sort"
	"sync"
	"time"

	igrpc "github.com/xd1lab/harness-ai/internal/orchestrator/adapter/inbound/grpc"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// Compile-time assertions: the dev store satisfies the 6-method
// [igrpc.EventStore] superset (the 5 [app.EventLogPort] methods PLUS
// CreateSession) that [igrpc.Server] requires, and the 5-method
// [app.EventLogPort] the agent loop depends on. These are the exact contracts the
// production pgx store satisfies; the dev store is the in-memory, single-writer,
// no-RLS, non-persistent stand-in (K-2: store = in-memory only).
var (
	_ igrpc.EventStore = (*Store)(nil)
	_ app.EventLogPort = (*Store)(nil)
)

// Store is the dev binary's in-memory event store. It promotes the semantics of
// apptest.FakeEventLog (which test/eval/harness.go already drives the REAL
// agent.Loop against, asserting golden event sequences — proving loop-equivalence
// to the production pgx store) into a clean cmd/boltrope-dev-owned adapter, and
// adds the CreateSession half so it satisfies the full [igrpc.EventStore]
// superset the transport server needs to open a stream.
//
// It is single-writer and single-process by construction: there is no RLS, no
// lease_epoch fencing, and no pg_notify — none of which exist in single-process
// dev mode (K-2). It is deliberately NOT a production or multi-tenant backend;
// every session lives only for the lifetime of the process.
//
// Store is safe for concurrent use (the loop's read-only tool pool and the
// Subscribe tailer touch it from multiple goroutines), guarded by a single mutex.
type Store struct {
	mu sync.Mutex
	// sessions maps session id -> its ordered event envelopes.
	sessions map[string][]domain.EventEnvelope
	// aggregates maps session id -> its mutable aggregate state (status, head,
	// fork lineage, mode), mirroring the sessions table row.
	aggregates map[string]*domain.Session
	// subscribers maps session id -> the set of live tail channels, fed on every
	// Append so an in-flight Subscribe (the Run stream) sees new events live.
	subscribers map[string][]chan domain.EventEnvelope
}

// newStore returns an empty in-memory dev [Store].
func newStore() *Store {
	return &Store{
		sessions:    make(map[string][]domain.EventEnvelope),
		aggregates:  make(map[string]*domain.Session),
		subscribers: make(map[string][]chan domain.EventEnvelope),
	}
}

// CreateSession inserts a fresh session aggregate (status=active, head_seq=0) with
// the given permission mode and returns it. It is the creation half of the
// CreateSession RPC; the transport appends the first SessionStarted afterwards to
// bump head_seq 0->1. A re-create of an existing session id is a no-op that
// returns the existing aggregate (the dev path never collides on the random
// session ids the server mints).
func (s *Store) CreateSession(_ context.Context, sessionID string, mode domain.PermissionMode) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if agg, ok := s.aggregates[sessionID]; ok {
		return *agg, nil
	}
	agg := &domain.Session{
		ID:      sessionID,
		Status:  domain.StatusActive,
		Mode:    mode.OrDefault(),
		HeadSeq: 0,
	}
	s.aggregates[sessionID] = agg
	return *agg, nil
}

// Append atomically appends events to sessionID's stream, assigning each a
// monotonic, contiguous per-session seq (head 0->1->2...). It mirrors the
// production optimistic-concurrency contract's success path; in single-writer dev
// mode there is no genuine write race, so expectedHeadSeq/leaseEpoch are accepted
// for signature parity but not used to manufacture a conflict (K-2: the pgx
// store's RLS/fencing machinery serves multi-writer/multi-tenant, neither of
// which exists here). The returned envelopes carry the assigned seqs; each is also
// fanned out to any live Subscribe tail for sessionID.
func (s *Store) Append(
	_ context.Context,
	sessionID string,
	_ int64, // expectedHeadSeq — single-writer dev mode never races
	_ int64, // leaseEpoch — no fencing in single-process dev mode
	requestID string,
	events ...app.AppendInput,
) ([]domain.EventEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	agg := s.aggregates[sessionID]
	if agg == nil {
		// A session that was forked or appended to without an explicit
		// CreateSession (the loop appends to a stream the transport already
		// created); materialize a default active aggregate so the seq is tracked.
		agg = &domain.Session{ID: sessionID, Status: domain.StatusActive}
		s.aggregates[sessionID] = agg
	}

	envelopes := make([]domain.EventEnvelope, 0, len(events))
	for _, e := range events {
		agg.HeadSeq++
		env := domain.EventEnvelope{
			Type:      e.Event.EventType(),
			Seq:       agg.HeadSeq,
			SessionID: sessionID,
			RequestID: requestID,
			Actor:     e.Actor,
			Event:     e.Event,
		}
		s.sessions[sessionID] = append(s.sessions[sessionID], env)
		envelopes = append(envelopes, env)
		s.fanout(sessionID, env)
	}
	return envelopes, nil
}

// fanout delivers env to every live subscriber of sessionID. The caller holds the
// mutex. Each subscriber channel is buffered; a non-blocking send keeps a slow
// reader from backpressuring the writer (the production store tails the durable
// log for the same reason; architecture §9.4).
func (s *Store) fanout(sessionID string, env domain.EventEnvelope) {
	for _, ch := range s.subscribers[sessionID] {
		select {
		case ch <- env:
		default:
		}
	}
}

// Load folds sessionID's events into an ordered slice from fromSeq (inclusive),
// oldest first.
func (s *Store) Load(_ context.Context, sessionID string, fromSeq int64) ([]domain.EventEnvelope, error) {
	s.mu.Lock()
	all := append([]domain.EventEnvelope(nil), s.sessions[sessionID]...)
	s.mu.Unlock()

	var out []domain.EventEnvelope
	for _, e := range all {
		if e.Seq >= fromSeq {
			out = append(out, e)
		}
	}
	return out, nil
}

// LoadSession returns the current aggregate (head seq, status, fork lineage, mode)
// for sessionID. An unknown session reads as a fresh active aggregate at head 0
// (loop-equivalent to the fake, and the transport always CreateSessions first).
func (s *Store) LoadSession(_ context.Context, sessionID string) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if agg, ok := s.aggregates[sessionID]; ok {
		return *agg, nil
	}
	return domain.Session{ID: sessionID, Status: domain.StatusActive}, nil
}

// Subscribe streams committed envelopes for sessionID starting just after fromSeq,
// then continues delivering newly-appended events live until ctx is cancelled (at
// which point the channel is closed). It is the existing-then-tail feed the Run
// stream relays (architecture §3, §7.1).
func (s *Store) Subscribe(ctx context.Context, sessionID string, fromSeq int64) (<-chan domain.EventEnvelope, error) {
	out := make(chan domain.EventEnvelope, 64)

	s.mu.Lock()
	existing := append([]domain.EventEnvelope(nil), s.sessions[sessionID]...)
	// Register a live tail channel BEFORE releasing the lock so no event appended
	// between the snapshot and registration is lost.
	tail := make(chan domain.EventEnvelope, 256)
	s.subscribers[sessionID] = append(s.subscribers[sessionID], tail)
	s.mu.Unlock()

	go func() {
		defer close(out)
		defer s.unsubscribe(sessionID, tail)

		// Replay the snapshot (events strictly after fromSeq) first.
		seen := fromSeq
		for _, e := range existing {
			if e.Seq <= fromSeq {
				continue
			}
			select {
			case out <- e:
				seen = e.Seq
			case <-ctx.Done():
				return
			}
		}
		// Then tail live appends, skipping any the snapshot already delivered.
		for {
			select {
			case e := <-tail:
				if e.Seq <= seen {
					continue
				}
				select {
				case out <- e:
					seen = e.Seq
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

// ListSessions returns the dev store's session aggregates matching q (status
// OR-filter, half-open created_at window, (created_at, id) keyset, descending,
// limit), mirroring the production store's keyset query (Feature I / ADR-0027).
// Dev mode is single-process and single-tenant (K-2: no RLS), so it lists every
// stored aggregate that passes the filters — there is no tenant scoping to apply.
func (s *Store) ListSessions(_ context.Context, q igrpc.ListSessionsQuery) ([]domain.Session, error) {
	s.mu.Lock()
	rows := make([]domain.Session, 0, len(s.aggregates))
	statusSet := map[domain.SessionStatus]bool{}
	for _, st := range q.Statuses {
		statusSet[st] = true
	}
	for _, agg := range s.aggregates {
		sess := *agg
		if len(statusSet) > 0 && !statusSet[sess.Status] {
			continue
		}
		if !q.CreatedAfter.IsZero() && sess.CreatedAt.Before(q.CreatedAfter) {
			continue
		}
		if !q.CreatedBefore.IsZero() && !sess.CreatedAt.Before(q.CreatedBefore) {
			continue // half-open [after, before): before is exclusive
		}
		rows = append(rows, sess)
	}
	s.mu.Unlock()

	less := func(a, b domain.Session) bool {
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID // (created_at, id) total order
	}
	sort.Slice(rows, func(i, j int) bool {
		if q.Descending {
			return less(rows[j], rows[i])
		}
		return less(rows[i], rows[j])
	})

	// Keyset cursor: keep rows strictly after the cursor in sort order.
	if q.Cursor.ID != "" {
		cur := domain.Session{ID: q.Cursor.ID, CreatedAt: time.UnixMilli(q.Cursor.CreatedAtMs).UTC()}
		filtered := rows[:0:0]
		for _, sess := range rows {
			after := less(cur, sess)
			if q.Descending {
				after = less(sess, cur)
			}
			if after {
				filtered = append(filtered, sess)
			}
		}
		rows = filtered
	}

	if q.Limit > 0 && len(rows) > q.Limit {
		rows = rows[:q.Limit]
	}
	return rows, nil
}

// unsubscribe removes tail from sessionID's subscriber set.
func (s *Store) unsubscribe(sessionID string, tail chan domain.EventEnvelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := s.subscribers[sessionID]
	for i, ch := range subs {
		if ch == tail {
			s.subscribers[sessionID] = append(subs[:i:i], subs[i+1:]...)
			break
		}
	}
}

// Fork creates a new child session branching parentID at atSeq, captured as the
// child's immutable ForkedFromSeq; the child's own seqs continue from atSeq+1 so
// the composed timeline has a single monotonic namespace. The parent is
// unaffected (a fork is a new branch, never a rewrite; architecture §6.6).
func (s *Store) Fork(_ context.Context, parentID string, atSeq int64, newSessionID string) (domain.Session, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	child := &domain.Session{
		ID:            newSessionID,
		ParentID:      parentID,
		ForkedFromSeq: atSeq,
		HeadSeq:       atSeq,
		Status:        domain.StatusActive,
	}
	if parent, ok := s.aggregates[parentID]; ok {
		child.Mode = parent.Mode // forks inherit the parent's permission mode.
	}
	s.aggregates[newSessionID] = child
	s.sessions[newSessionID] = nil
	return *child, nil
}
