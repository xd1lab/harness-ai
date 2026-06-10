// Package idstest provides a deterministic fake [ids.IDGenerator] for tests.
// It returns a scripted sequence of identifiers so tests can assert the exact
// turn_id, session_id, or request_id that a flow will produce.
//
// Usage:
//
//	gen := idstest.NewFake("id-1", "id-2", "id-3")
//	gen.NewID()        // "id-1"
//	gen.NewSessionID() // "id-2"
//	gen.NewRequestID() // "id-3"
//	gen.NewID()        // panics: sequence exhausted (or wraps if Cyclic)
package idstest

import (
	"fmt"
	"sync/atomic"

	"github.com/boltrope/boltrope/internal/platform/ids"
)

// Compile-time assertion that Fake satisfies ids.IDGenerator.
var _ ids.IDGenerator = (*Fake)(nil)

// Fake is a deterministic IDGenerator that returns IDs from a fixed scripted
// sequence. All three methods (NewID, NewSessionID, NewRequestID) draw from
// the same sequence counter, so the caller can predict exactly which id each
// call produces.
//
// If the sequence is exhausted and Cyclic is false, the next call panics with a
// descriptive message. If Cyclic is true, the counter wraps and the sequence
// replays from the beginning.
type Fake struct {
	ids    []ids.ID
	idx    atomic.Int64
	Cyclic bool
}

// NewFake returns a Fake whose calls to NewID/NewSessionID/NewRequestID return
// the given ids in order. Providing at least one id is required.
func NewFake(sequence ...string) *Fake {
	if len(sequence) == 0 {
		panic("idstest.NewFake: sequence must not be empty")
	}
	out := make([]ids.ID, len(sequence))
	for i, s := range sequence {
		out[i] = ids.ID(s)
	}
	return &Fake{ids: out}
}

// Sequential returns a Fake that generates ids of the form "id-1", "id-2",
// etc., up to count. This is handy when the test does not care about the exact
// values but needs them to be distinct.
func Sequential(count int) *Fake {
	seq := make([]string, count)
	for i := range seq {
		seq[i] = fmt.Sprintf("id-%d", i+1)
	}
	return NewFake(seq...)
}

// next returns the next id in the sequence.
func (f *Fake) next() ids.ID {
	idx := f.idx.Add(1) - 1 // 0-based
	if int(idx) < len(f.ids) {
		return f.ids[idx]
	}
	if f.Cyclic {
		wrapped := int(idx) % len(f.ids)
		return f.ids[wrapped]
	}
	panic(fmt.Sprintf("idstest.Fake: sequence exhausted after %d calls (had %d ids)", idx+1, len(f.ids)))
}

// NewID returns the next id in the scripted sequence.
func (f *Fake) NewID() ids.ID { return f.next() }

// NewSessionID returns the next id in the scripted sequence.
func (f *Fake) NewSessionID() ids.ID { return f.next() }

// NewRequestID returns the next id in the scripted sequence.
func (f *Fake) NewRequestID() ids.ID { return f.next() }

// Calls returns the total number of times any of the New* methods has been
// invoked.
func (f *Fake) Calls() int { return int(f.idx.Load()) }
