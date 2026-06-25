// Package inmem provides the in-memory [app.MemoryStore] implementation used by
// the single-process cmd/boltrope-dev binary's local-exec path (ADR-0030;
// AC-8). It mirrors the semantics of the pgx-backed
// [github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/memory]
// Store — UPSERT by (namespace, key), case-insensitive value-substring search
// with tag AND-filtering, newest-first ordering and the shared
// [app.DefaultMemorySearchLimit] cap — but holds the data in a process-local
// map instead of Postgres.
//
// It is a SEPARATE leaf package from the pgx-backed parent so importing it does
// NOT drag github.com/jackc/pgx/v5 into the dev binary's transitive graph: this
// package imports ONLY the standard library, the toolruntime app port, and the
// clean tenant-context helper (AC-15). The dev binary is fenced from pgx, so
// the in-memory store and the Postgres store must never share an import edge.
//
// # Tenant isolation
//
// The owning tenant is taken from the request context via
// [tenantctx.TenantFromContext] (NEVER a method argument) — exactly as the
// Postgres store does — and a missing tenant FAILS CLOSED with
// [tenantctx.ErrNoTenant] before any map access. There is no ” default bucket
// and no shared namespace: tenant A's entries live under a distinct top-level
// map key from tenant B's, so cross-tenant reads/writes/deletes are impossible
// in dev exactly as RLS makes them impossible in prod.
package inmem

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// Store is the in-memory, tenant-keyed [app.MemoryStore]. It is safe for
// concurrent use — all access is guarded by a single [sync.RWMutex].
//
// The map is keyed tenantID -> namespace -> mem_key -> entry, so tenant
// isolation is a structural property of the data layout, not a filter applied
// after lookup.
type Store struct {
	mu   sync.RWMutex
	data map[string]map[string]map[string]app.MemoryEntry
}

// New returns a fresh, empty in-memory [Store].
func New() *Store {
	return &Store{data: make(map[string]map[string]map[string]app.MemoryEntry)}
}

// Compile-time assertion that *Store satisfies the MemoryStore port.
var _ app.MemoryStore = (*Store)(nil)

// cloneTags returns a defensive copy of tags as a non-nil slice, so a stored
// entry never aliases a caller's slice and the column-equivalent is '{}' rather
// than nil (matching the PG NOT NULL DEFAULT '{}').
func cloneTags(tags []string) []string {
	out := make([]string, len(tags))
	copy(out, tags)
	return out
}

// Put UPSERTs value (and tags) under (namespace, key) for the context's tenant.
// On an existing entry it preserves CreatedAt and bumps UpdatedAt; on a new
// entry CreatedAt == UpdatedAt. It fails closed when the context carries no
// tenant.
func (s *Store) Put(ctx context.Context, namespace, key, value string, tags []string) error {
	tenantID, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	nsMap, ok := s.data[tenantID]
	if !ok {
		nsMap = make(map[string]map[string]app.MemoryEntry)
		s.data[tenantID] = nsMap
	}
	keyMap, ok := nsMap[namespace]
	if !ok {
		keyMap = make(map[string]app.MemoryEntry)
		nsMap[namespace] = keyMap
	}

	created := now
	if prev, ok := keyMap[key]; ok {
		created = prev.CreatedAt // preserve original creation time across upsert
	}
	keyMap[key] = app.MemoryEntry{
		Namespace: namespace,
		Key:       key,
		Value:     value,
		Tags:      cloneTags(tags),
		CreatedAt: created,
		UpdatedAt: now,
	}
	return nil
}

// Get returns the entry under (namespace, key) for the context's tenant. found
// is false (nil error) when no such entry exists — a miss is a normal outcome.
// It fails closed when the context carries no tenant.
func (s *Store) Get(ctx context.Context, namespace, key string) (app.MemoryEntry, bool, error) {
	tenantID, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		return app.MemoryEntry{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, ok := s.data[tenantID][namespace][key]
	if !ok {
		return app.MemoryEntry{}, false, nil
	}
	// Return a copy so the caller cannot mutate the stored entry's slice.
	entry.Tags = cloneTags(entry.Tags)
	return entry, true, nil
}

// Search returns the context tenant's entries matching the filters: query is a
// case-insensitive SUBSTRING over the entry value (value only — the pinned
// recall surface), and every tag in tags must be present on the entry (tag
// AND-semantics). An empty query and empty tags list recent entries (newest
// first by UpdatedAt). limit caps the result count; limit <= 0 applies
// [app.DefaultMemorySearchLimit] and any larger value is hard-capped to it. It
// fails closed when the context carries no tenant.
func (s *Store) Search(ctx context.Context, query string, tags []string, limit int) ([]app.MemoryEntry, error) {
	tenantID, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > app.DefaultMemorySearchLimit {
		limit = app.DefaultMemorySearchLimit
	}
	needle := strings.ToLower(query)

	s.mu.RLock()
	defer s.mu.RUnlock()

	var matches []app.MemoryEntry
	for _, keyMap := range s.data[tenantID] {
		for _, entry := range keyMap {
			if needle != "" && !strings.Contains(strings.ToLower(entry.Value), needle) {
				continue
			}
			if !hasAllTags(entry.Tags, tags) {
				continue
			}
			cp := entry
			cp.Tags = cloneTags(entry.Tags)
			matches = append(matches, cp)
		}
	}

	// Newest-first by UpdatedAt, mirroring the PG `ORDER BY updated_at DESC`.
	// Key is the deterministic tiebreaker so map-iteration order does not leak
	// into results.
	sort.Slice(matches, func(i, j int) bool {
		if !matches[i].UpdatedAt.Equal(matches[j].UpdatedAt) {
			return matches[i].UpdatedAt.After(matches[j].UpdatedAt)
		}
		return matches[i].Key < matches[j].Key
	})

	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

// hasAllTags reports whether every tag in want is present in have (AND-match,
// mirroring the PG `tags @> $`). An empty want matches every entry.
func hasAllTags(have, want []string) bool {
	if len(want) == 0 {
		return true
	}
	set := make(map[string]struct{}, len(have))
	for _, t := range have {
		set[t] = struct{}{}
	}
	for _, t := range want {
		if _, ok := set[t]; !ok {
			return false
		}
	}
	return true
}

// Delete removes the entry under (namespace, key) for the context's tenant.
// Deleting an absent entry is not an error (idempotent). It fails closed when
// the context carries no tenant.
func (s *Store) Delete(ctx context.Context, namespace, key string) error {
	tenantID, err := tenantctx.TenantFromContext(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if keyMap, ok := s.data[tenantID][namespace]; ok {
		delete(keyMap, key)
	}
	return nil
}
