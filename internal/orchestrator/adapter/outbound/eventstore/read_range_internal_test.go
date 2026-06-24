package eventstore

// TDD (red) pure (no-DB) tests pinning the SHAPE of Feature M's read-only
// queries. These run in the default build (no integration tag) so they give a
// fast red proof without Docker: they reference SQL constants that do NOT yet
// exist (selectEventsRangeSQL, selectEventsUpToSeqSQL), so the package fails to
// compile until the read path lands.
//
// The query shapes are load-bearing for the resolved decisions:
//   - selectEventsRangeSQL: keyset page — WHERE session_id=$1 AND seq > $2
//     ORDER BY seq LIMIT $3 (rides idx_events_session_seq; no OFFSET deep-paging).
//   - selectEventsUpToSeqSQL: bounded fold window — WHERE session_id=$1 AND
//     seq <= $2 ORDER BY seq (inclusive upper bound for at-seq reconstruction).
// Both select the shared eventColumns so the scan order cannot drift.

import (
	"strings"
	"testing"
)

// TestSelectEventsRangeSQLShape pins the keyset-page query: strictly-greater
// cursor, seq ordering, a LIMIT, and the shared column list.
func TestSelectEventsRangeSQLShape(t *testing.T) {
	t.Parallel()
	q := selectEventsRangeSQL
	for _, want := range []string{
		"SELECT " + eventColumns,
		"FROM events",
		"session_id = $1",
		"seq > $2",     // keyset: STRICTLY greater than the cursor
		"ORDER BY seq", // oldest-first, rides idx_events_session_seq
		"LIMIT $3",     // page-size cap (keyset, not OFFSET)
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectEventsRangeSQL missing %q\n full: %s", want, q)
		}
	}
	if strings.Contains(q, "OFFSET") {
		t.Errorf("selectEventsRangeSQL must not use OFFSET (keyset paging only): %s", q)
	}
}

// TestSelectEventsUpToSeqSQLShape pins the bounded fold window query: an
// INCLUSIVE upper bound (seq <= $2), seq ordering, the shared column list, and
// no LIMIT (the whole [1..at_seq] window is folded).
func TestSelectEventsUpToSeqSQLShape(t *testing.T) {
	t.Parallel()
	q := selectEventsUpToSeqSQL
	for _, want := range []string{
		"SELECT " + eventColumns,
		"FROM events",
		"session_id = $1",
		"seq <= $2", // INCLUSIVE upper bound for at-seq reconstruction
		"ORDER BY seq",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectEventsUpToSeqSQL missing %q\n full: %s", want, q)
		}
	}
}
