// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"sync"

	trapp "github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// memDedup is the dev binary's in-process, in-memory tool-execution dedup ledger.
//
// It satisfies the tool-runtime [trapp.DedupStore] port WITHOUT the production
// PostgreSQL/pgx-backed pool (internal/toolruntime/adapter/outbound/dedup), which
// the dev binary is forbidden to import (it would drag github.com/jackc/pgx/v5
// into the binary; ADR-0029, AC-8/AC-16). It depends ONLY on the clean
// internal/toolruntime/app ports + types.
//
// It is deliberately ephemeral: the ledger lives for the lifetime of the dev
// process and is wiped on restart. That is acceptable for the loopback-only,
// NOT-FOR-PRODUCTION dev binary, where at-most-once-across-crashes durability is
// out of scope. Records are keyed by (TenantID, SessionID, IdempotencyKey),
// matching the production primary key. Safe for concurrent use.
type memDedup struct {
	mu      sync.Mutex
	records map[string]trapp.ExecutionRecord
}

// compile-time assertion: memDedup satisfies the tool-runtime dedup port.
var _ trapp.DedupStore = (*memDedup)(nil)

// newMemDedup constructs an empty in-memory dedup ledger.
func newMemDedup() *memDedup {
	return &memDedup{records: make(map[string]trapp.ExecutionRecord)}
}

// dedupKey derives the composite ledger key from the tenant/session/idempotency
// triple — the same (TenantID, SessionID, IdempotencyKey) namespace the
// production primary key uses.
func dedupKey(tenantID, sessionID, idempotencyKey string) string {
	return fmt.Sprintf("%q\x00%q\x00%q", tenantID, sessionID, idempotencyKey)
}

// Begin is get-or-create: if the key is already known it returns the stored
// record (so a retried call observes the prior status/result rather than
// starting a duplicate); otherwise it stores a copy with Status==ExecStarted and
// returns it.
func (m *memDedup) Begin(_ context.Context, rec trapp.ExecutionRecord) (trapp.ExecutionRecord, error) {
	key := dedupKey(rec.TenantID, rec.SessionID, rec.IdempotencyKey)

	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, ok := m.records[key]; ok {
		return existing, nil
	}

	started := rec
	started.Status = trapp.ExecStarted
	m.records[key] = started
	return started, nil
}

// Complete overwrites the stored record's terminal status + result for the key
// (typically ExecCompleted with the observation, or ExecFailed/ExecUnknown). It
// records even when no prior Begin was observed, so the ledger always reflects
// the latest terminal outcome.
func (m *memDedup) Complete(_ context.Context, rec trapp.ExecutionRecord) error {
	key := dedupKey(rec.TenantID, rec.SessionID, rec.IdempotencyKey)

	m.mu.Lock()
	defer m.mu.Unlock()

	m.records[key] = rec
	return nil
}

// Lookup returns the stored record for the (tenantID, sessionID,
// idempotencyKey) triple, or a non-nil error when no record exists.
func (m *memDedup) Lookup(_ context.Context, tenantID, sessionID, idempotencyKey string) (trapp.ExecutionRecord, error) {
	key := dedupKey(tenantID, sessionID, idempotencyKey)

	m.mu.Lock()
	defer m.mu.Unlock()

	rec, ok := m.records[key]
	if !ok {
		return trapp.ExecutionRecord{}, fmt.Errorf("boltrope-dev dedup: no record for tenant=%q session=%q key=%q", tenantID, sessionID, idempotencyKey)
	}
	return rec, nil
}
