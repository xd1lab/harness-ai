package projection

import "testing"

// TestCursor_Less covers the composite (transaction_id, global_id) ordering the
// safe-advance cursor uses: transaction_id dominates, global_id breaks ties.
func TestCursor_Less(t *testing.T) {
	tests := []struct {
		name     string
		a, b     Cursor
		wantLess bool
	}{
		{"lower txn", Cursor{TransactionID: 5, GlobalID: 100}, Cursor{TransactionID: 6, GlobalID: 1}, true},
		{"higher txn", Cursor{TransactionID: 7, GlobalID: 1}, Cursor{TransactionID: 6, GlobalID: 999}, false},
		{"same txn lower global", Cursor{TransactionID: 5, GlobalID: 10}, Cursor{TransactionID: 5, GlobalID: 11}, true},
		{"same txn higher global", Cursor{TransactionID: 5, GlobalID: 12}, Cursor{TransactionID: 5, GlobalID: 11}, false},
		{"equal", Cursor{TransactionID: 5, GlobalID: 11}, Cursor{TransactionID: 5, GlobalID: 11}, false},
		{"zero before anything", Cursor{}, Cursor{TransactionID: 1, GlobalID: 1}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.Less(tc.b); got != tc.wantLess {
				t.Fatalf("%s.Less(%s) = %v, want %v", tc.a, tc.b, got, tc.wantLess)
			}
		})
	}
}

// TestCursor_Advance_InOrder asserts the pure cursor fold moves the checkpoint to
// the last row of an in-order batch and reports the count.
func TestCursor_Advance_InOrder(t *testing.T) {
	start := Cursor{TransactionID: 10, GlobalID: 5}
	rows := []rowCursor{
		{TransactionID: 10, GlobalID: 6}, // same txn, next global
		{TransactionID: 11, GlobalID: 7}, // next txn
		{TransactionID: 11, GlobalID: 8}, // same txn, next global
		{TransactionID: 13, GlobalID: 9}, // a gap in txn ids (12 was a rolled-back/other-db txn) is fine
	}
	got, n := start.Advance(rows)
	if n != 4 {
		t.Fatalf("advanced over %d rows, want 4", n)
	}
	want := Cursor{TransactionID: 13, GlobalID: 9}
	if got != want {
		t.Fatalf("new cursor = %s, want %s", got, want)
	}
}

// TestCursor_Advance_Empty asserts an empty batch leaves the cursor unchanged
// (nothing settled below xmin since the last poll).
func TestCursor_Advance_Empty(t *testing.T) {
	start := Cursor{TransactionID: 42, GlobalID: 7}
	got, n := start.Advance(nil)
	if n != 0 || got != start {
		t.Fatalf("empty Advance = (%s, %d), want (%s, 0)", got, n, start)
	}
}

// TestCursor_Advance_NeverRegresses asserts that the fold is strictly monotonic:
// a non-increasing row (a contract violation that would double-count or regress
// the checkpoint and corrupt projections) panics rather than silently corrupting
// state.
func TestCursor_Advance_NeverRegresses(t *testing.T) {
	start := Cursor{TransactionID: 10, GlobalID: 5}

	cases := map[string][]rowCursor{
		"row equals cursor":           {{TransactionID: 10, GlobalID: 5}},
		"row below cursor":            {{TransactionID: 9, GlobalID: 100}},
		"out-of-order within a txn":   {{TransactionID: 11, GlobalID: 8}, {TransactionID: 11, GlobalID: 8}},
		"second row regresses global": {{TransactionID: 11, GlobalID: 8}, {TransactionID: 11, GlobalID: 7}},
	}
	for name, rows := range cases {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("Advance(%v) did not panic on a non-increasing row", rows)
				}
			}()
			_, _ = start.Advance(rows)
		})
	}
}
