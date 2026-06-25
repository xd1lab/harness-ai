package inmem_test

// RED (TDD) — ADR-0030 AC-8 / AC-14. Unit tests for the IN-MEMORY app.MemoryStore
// used by the dev binary's local-exec path. They prove tenant isolation purely
// in-process (no Postgres): a value written under tenant A is invisible to tenant
// B for Get/Search, a Delete under B leaves A intact, and every method fails closed
// (ErrNoTenant) when the context carries no tenant.
//
// The package under test (internal/toolruntime/adapter/outbound/memory/inmem) and
// the app.MemoryStore port do not exist yet, so this file is expected to FAIL to
// compile until the feature is implemented (feature absent).

import (
	"context"
	"errors"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/memory/inmem"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
	tenantctx "github.com/xd1lab/harness-ai/internal/toolruntime/infra/tenant"
)

// ctxA / ctxB return background contexts scoped to two distinct tenants via the
// clean toolruntime tenant-context helper (AC-4) that the store reads.
func ctxA() context.Context { return tenantctx.WithTenant(context.Background(), "tenant-A") }
func ctxB() context.Context { return tenantctx.WithTenant(context.Background(), "tenant-B") }

// newStore returns a fresh in-memory store as the app.MemoryStore port (the test
// only depends on the port surface).
func newStore() app.MemoryStore { return inmem.New() }

// TestPutThenGetRoundTrips verifies a value written under a tenant is read back
// within the same tenant, with the entry's fields populated.
func TestPutThenGetRoundTrips(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "fav-color", "blue", []string{"pref"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, ok, err := st.Get(ctxA(), "default", "fav-color")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("Get: ok=false, want the written entry")
	}
	if got.Value != "blue" {
		t.Errorf("Get value = %q, want %q", got.Value, "blue")
	}
	if got.Key != "fav-color" {
		t.Errorf("Get key = %q, want %q", got.Key, "fav-color")
	}
	if got.Namespace != "default" {
		t.Errorf("Get namespace = %q, want %q", got.Namespace, "default")
	}
	if len(got.Tags) != 1 || got.Tags[0] != "pref" {
		t.Errorf("Get tags = %v, want [pref]", got.Tags)
	}
}

// TestPutUpserts verifies a second Put on the same (namespace,key) overwrites the
// value (UPSERT semantics).
func TestPutUpserts(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k", "v1", nil); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := st.Put(ctxA(), "default", "k", "v2", []string{"t"}); err != nil {
		t.Fatalf("Put v2: %v", err)
	}
	got, ok, err := st.Get(ctxA(), "default", "k")
	if err != nil || !ok {
		t.Fatalf("Get after upsert: ok=%v err=%v", ok, err)
	}
	if got.Value != "v2" {
		t.Errorf("upsert value = %q, want %q", got.Value, "v2")
	}
}

// TestGetMissingReturnsNotFound verifies a miss is (zero, false, nil) — not an
// error — so callers distinguish "no such key" from a failure.
func TestGetMissingReturnsNotFound(t *testing.T) {
	t.Parallel()
	st := newStore()

	_, ok, err := st.Get(ctxA(), "default", "nope")
	if err != nil {
		t.Fatalf("Get missing: unexpected error %v", err)
	}
	if ok {
		t.Error("Get missing: ok=true, want false")
	}
}

// TestTenantIsolationGet is the core invariant: tenant A's value is invisible to
// tenant B (no cross-tenant read).
func TestTenantIsolationGet(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "secret", "A-only", nil); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	_, ok, err := st.Get(ctxB(), "default", "secret")
	if err != nil {
		t.Fatalf("Get as B: unexpected error %v", err)
	}
	if ok {
		t.Fatal("tenant B read tenant A's memory — cross-tenant isolation FAILED")
	}
}

// TestTenantIsolationSearch verifies Search under tenant B never returns tenant A's
// rows.
func TestTenantIsolationSearch(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k1", "alpha beta", []string{"x"}); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	hits, err := st.Search(ctxB(), "alpha", nil, 0)
	if err != nil {
		t.Fatalf("Search as B: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("tenant B Search saw %d of tenant A's rows — isolation FAILED", len(hits))
	}
}

// TestTenantIsolationDelete verifies a Delete under tenant B does not affect tenant
// A's row.
func TestTenantIsolationDelete(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k", "A-value", nil); err != nil {
		t.Fatalf("Put as A: %v", err)
	}
	// Tenant B attempts to delete the same (namespace,key); A's row must survive.
	if err := st.Delete(ctxB(), "default", "k"); err != nil {
		t.Fatalf("Delete as B: %v", err)
	}
	got, ok, err := st.Get(ctxA(), "default", "k")
	if err != nil || !ok {
		t.Fatalf("Get as A after B delete: ok=%v err=%v — A's row was wrongly removed", ok, err)
	}
	if got.Value != "A-value" {
		t.Errorf("A's value = %q after B delete, want %q", got.Value, "A-value")
	}
}

// TestDeleteWithinTenant verifies a tenant can delete its own row.
func TestDeleteWithinTenant(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k", "v", nil); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.Delete(ctxA(), "default", "k"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, err := st.Get(ctxA(), "default", "k")
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if ok {
		t.Error("Get after own-tenant delete: ok=true, want false (row should be gone)")
	}
}

// TestSearchSubstringCaseInsensitive verifies Search matches query as a
// case-insensitive substring over the value.
func TestSearchSubstringCaseInsensitive(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k1", "The Quick Brown Fox", nil); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	if err := st.Put(ctxA(), "default", "k2", "lazy dog", nil); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	hits, err := st.Search(ctxA(), "quick", nil, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("Search 'quick' returned %d hits, want 1", len(hits))
	}
	if hits[0].Key != "k1" {
		t.Errorf("Search hit key = %q, want %q", hits[0].Key, "k1")
	}
}

// TestSearchTagsAreANDed verifies a tag filter requires ALL supplied tags to be
// present (AND semantics).
func TestSearchTagsAreANDed(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "default", "k1", "v1", []string{"red", "blue"}); err != nil {
		t.Fatalf("Put k1: %v", err)
	}
	if err := st.Put(ctxA(), "default", "k2", "v2", []string{"red"}); err != nil {
		t.Fatalf("Put k2: %v", err)
	}
	// Require both red AND blue: only k1 qualifies.
	hits, err := st.Search(ctxA(), "", []string{"red", "blue"}, 0)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 1 || hits[0].Key != "k1" {
		t.Fatalf("Search tags [red,blue] = %d hits (%v), want exactly k1", len(hits), hits)
	}
}

// TestSearchEmptyListsAll verifies an all-empty search (no query, no tags) lists
// entries up to the cap rather than erroring.
func TestSearchEmptyListsAll(t *testing.T) {
	t.Parallel()
	st := newStore()

	for _, k := range []string{"a", "b", "c"} {
		if err := st.Put(ctxA(), "default", k, "val-"+k, nil); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	hits, err := st.Search(ctxA(), "", nil, 0)
	if err != nil {
		t.Fatalf("Search empty: %v", err)
	}
	if len(hits) != 3 {
		t.Errorf("Search empty returned %d, want 3", len(hits))
	}
}

// TestSearchLimitCaps verifies a positive limit caps the result count.
func TestSearchLimitCaps(t *testing.T) {
	t.Parallel()
	st := newStore()

	for _, k := range []string{"a", "b", "c", "d"} {
		if err := st.Put(ctxA(), "default", k, "match-me", nil); err != nil {
			t.Fatalf("Put %s: %v", k, err)
		}
	}
	hits, err := st.Search(ctxA(), "match", nil, 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 {
		t.Errorf("Search limit=2 returned %d, want 2", len(hits))
	}
}

// TestNamespaceScoping verifies keys in different namespaces are independent.
func TestNamespaceScoping(t *testing.T) {
	t.Parallel()
	st := newStore()

	if err := st.Put(ctxA(), "ns1", "k", "in-ns1", nil); err != nil {
		t.Fatalf("Put ns1: %v", err)
	}
	if err := st.Put(ctxA(), "ns2", "k", "in-ns2", nil); err != nil {
		t.Fatalf("Put ns2: %v", err)
	}
	got1, ok1, err := st.Get(ctxA(), "ns1", "k")
	if err != nil || !ok1 {
		t.Fatalf("Get ns1: ok=%v err=%v", ok1, err)
	}
	if got1.Value != "in-ns1" {
		t.Errorf("ns1 value = %q, want %q", got1.Value, "in-ns1")
	}
	got2, ok2, err := st.Get(ctxA(), "ns2", "k")
	if err != nil || !ok2 {
		t.Fatalf("Get ns2: ok=%v err=%v", ok2, err)
	}
	if got2.Value != "in-ns2" {
		t.Errorf("ns2 value = %q, want %q", got2.Value, "in-ns2")
	}
}

// TestFailClosedNoTenant verifies every method returns ErrNoTenant when the
// context carries no tenant (mirrors RLS fail-closed in prod).
func TestFailClosedNoTenant(t *testing.T) {
	t.Parallel()
	st := newStore()
	bg := context.Background()

	if err := st.Put(bg, "default", "k", "v", nil); !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("Put no-tenant err = %v, want errors.Is(_, ErrNoTenant)", err)
	}
	if _, _, err := st.Get(bg, "default", "k"); !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("Get no-tenant err = %v, want errors.Is(_, ErrNoTenant)", err)
	}
	if _, err := st.Search(bg, "x", nil, 0); !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("Search no-tenant err = %v, want errors.Is(_, ErrNoTenant)", err)
	}
	if err := st.Delete(bg, "default", "k"); !errors.Is(err, tenantctx.ErrNoTenant) {
		t.Errorf("Delete no-tenant err = %v, want errors.Is(_, ErrNoTenant)", err)
	}
}

// TestSatisfiesPort is a compile-time-ish assertion that the in-mem constructor
// returns something satisfying the app.MemoryStore port.
func TestSatisfiesPort(t *testing.T) {
	t.Parallel()
	var _ app.MemoryStore = inmem.New()
}
