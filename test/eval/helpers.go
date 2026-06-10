package eval

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
)

// mustJSON marshals v to compact JSON, panicking on error. Scenario inputs are
// static maps that always marshal, so a panic here is a programming error in a
// scenario definition, surfaced loudly.
func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("eval: marshal scenario arg: %v", err))
	}
	return b
}

// deterministicIDs returns a generous, stable id sequence ("eid-1".."eid-N") for
// the fake [github.com/xd1lab/harness-ai/internal/platform/ids.IDGenerator]. The
// loop mints turn ids and per-append request ids from it; the count is sized to
// comfortably outlast any scenario's appends.
func deterministicIDs(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("eid-%d", i+1)
	}
	return out
}

// assertEventTypes asserts that got equals want exactly, reporting the full
// expected/actual sequences and the first differing index for a readable golden
// diff. It uses t.Errorf so other assertions in the same scenario still run.
func assertEventTypes(t *testing.T, want, got []domain.EventType) {
	t.Helper()
	if len(want) != len(got) {
		t.Errorf("event-log shape length = %d, want %d\n got: %v\nwant: %v", len(got), len(want), got, want)
		return
	}
	for i := range want {
		if want[i] != got[i] {
			t.Errorf("event-log shape mismatch at index %d: got %q, want %q\n got: %v\nwant: %v",
				i, got[i], want[i], got, want)
			return
		}
	}
}

// approxEqual reports whether two USD amounts are equal within a cent-fraction
// tolerance, so float accumulation across turns does not produce spurious
// mismatches.
func approxEqual(a, b float64) bool {
	const eps = 1e-9
	return math.Abs(a-b) <= eps
}

// PayloadsOf returns the typed payloads of type T appended to the result's event
// log, in order. It lets a scenario's extra check assert on specific payload
// fields (e.g. the terminal [domain.TurnFinished] reason or a
// [domain.PermissionDecided] decision).
func PayloadsOf[T domain.Event](r Result) []T {
	var out []T
	for _, e := range r.Events {
		if p, ok := e.Event.(T); ok {
			out = append(out, p)
		}
	}
	return out
}

// CountEventType returns how many envelopes of the given type are in the result.
func CountEventType(r Result, typ domain.EventType) int {
	n := 0
	for _, et := range r.EventTypes {
		if et == typ {
			n++
		}
	}
	return n
}
