package eventstore

// TDD (red) pure (no-DB) tests pinning the SHAPE of Feature I's read-only
// ListSessions query and its opaque page-cursor codec. They run in the default
// build (no integration tag) so they give a fast red proof without Docker: they
// reference symbols that do NOT yet exist (selectSessionsListSQL, the
// ListSessionsQuery/listCursor types, and encodeListCursor/decodeListCursor), so
// the package fails to compile until the admin list path lands.
//
// The query shape is load-bearing for the resolved decisions
// (DECISIONS.md 2026-06-24, SPEC AC-3/AC-4/AC-5):
//   - selectSessionsListSQL: a single keyset page over the sessions table —
//     a status OR-filter ($1::text[] IS NULL OR status = ANY($1)), a half-open
//     created_at window ($2 created>=after, $3 created<before), a (created_at, id)
//     keyset predicate, ORDER BY created_at,id, and a LIMIT (page_size+1 to detect
//     has_more). NO OFFSET (no deep-pagination degradation). It selects the SAME
//     session columns loadSessionTx scans so the projection cannot drift.
//   - The page_token is an OPAQUE base64 of (created_at_ms, id, descending): a
//     round-trip preserves all three; a malformed token is a typed error the
//     server maps to InvalidArgument.

import (
	"strings"
	"testing"
)

// TestSelectSessionsListSQLShape pins the keyset list query: a status OR-filter,
// a half-open created_at window, a (created_at, id) keyset predicate, the
// (created_at, id) ordering, a LIMIT, and NO OFFSET.
func TestSelectSessionsListSQLShape(t *testing.T) {
	t.Parallel()
	q := selectSessionsListSQL
	for _, want := range []string{
		"FROM sessions",
		"status = ANY($1)", // status OR-filter (empty/NULL = all)
		"created_at >= $2", // created_after lower bound (inclusive)
		"created_at <  $3", // created_before upper bound (exclusive, half-open)
		"(created_at, id)", // composite keyset cursor predicate (id tie-breaks)
		"ORDER BY",         // (created_at, id) ordering
		"LIMIT",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("selectSessionsListSQL missing %q\n full: %s", want, q)
		}
	}
	if strings.Contains(q, "OFFSET") {
		t.Errorf("selectSessionsListSQL must not use OFFSET (keyset paging only): %s", q)
	}
	// The list query must select the SAME session columns loadSessionTx scans, so
	// the row projection cannot silently drift from domain.Session.
	for _, col := range []string{
		"id", "tenant_id", "parent_id", "forked_from_seq", "status", "head_seq",
		"last_event_at", "created_at", "updated_at", "mode",
	} {
		if !strings.Contains(q, col) {
			t.Errorf("selectSessionsListSQL missing session column %q (must match loadSessionTx)\n full: %s", col, q)
		}
	}
}

// TestListCursorRoundTrip pins the opaque page_token codec: encoding then
// decoding (created_at_ms, id, descending) yields the original values, for both
// directions.
func TestListCursorRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cur  listCursor
	}{
		{"ascending", listCursor{CreatedAtMs: 1_717_000_000_000, ID: "11111111-1111-4111-8111-111111111111", Descending: false}},
		{"descending", listCursor{CreatedAtMs: 1_717_000_000_999, ID: "22222222-2222-4222-8222-222222222222", Descending: true}},
		{"same-ms-tie", listCursor{CreatedAtMs: 5_000, ID: "tie-b", Descending: false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			token := encodeListCursor(tc.cur)
			if token == "" {
				t.Fatalf("encodeListCursor returned an empty token for %+v", tc.cur)
			}
			got, err := decodeListCursor(token)
			if err != nil {
				t.Fatalf("decodeListCursor(%q) error: %v", token, err)
			}
			if got.CreatedAtMs != tc.cur.CreatedAtMs || got.ID != tc.cur.ID || got.Descending != tc.cur.Descending {
				t.Fatalf("round-trip mismatch: got %+v, want %+v", got, tc.cur)
			}
		})
	}
}

// TestDecodeListCursor_RejectsMalformed pins the defensive decode: a non-base64 /
// garbage token is a typed error (so the server maps it to InvalidArgument rather
// than silently treating it as the first page or leaking rows).
func TestDecodeListCursor_RejectsMalformed(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{
		"!!!not-base64!!!",
		"Zm9vYmFy", // valid base64 of "foobar" — not the cursor wire shape
		"random-junk",
	} {
		if _, err := decodeListCursor(bad); err == nil {
			t.Errorf("decodeListCursor(%q) returned nil error, want a typed decode error", bad)
		}
	}
}

// TestDecodeListCursor_EmptyIsFirstPage pins that an empty token decodes to a
// zero cursor (the first page), NOT an error — empty page_token means "start".
func TestDecodeListCursor_EmptyIsFirstPage(t *testing.T) {
	t.Parallel()
	got, err := decodeListCursor("")
	if err != nil {
		t.Fatalf("decodeListCursor(\"\") error: %v (empty token must be the first page, not an error)", err)
	}
	if got.ID != "" || got.CreatedAtMs != 0 {
		t.Fatalf("empty token decoded to a non-zero cursor %+v, want the zero (first-page) cursor", got)
	}
}
