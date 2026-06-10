package idstest_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/ids/idstest"
)

// TestFake_Determinism: given a fixed sequence, calls return ids in order.
func TestFake_Determinism(t *testing.T) {
	gen := idstest.NewFake("a", "b", "c")
	assert.Equal(t, "a", gen.NewID().String())
	assert.Equal(t, "b", gen.NewSessionID().String())
	assert.Equal(t, "c", gen.NewRequestID().String())
}

// TestFake_Sequential: Sequential helper returns distinct ids.
func TestFake_Sequential(t *testing.T) {
	gen := idstest.Sequential(5)
	seen := map[string]bool{}
	for i := 0; i < 5; i++ {
		id := gen.NewID()
		require.False(t, id.IsZero(), "id should not be zero")
		require.False(t, seen[id.String()], "duplicate id: %s", id)
		seen[id.String()] = true
	}
	assert.Equal(t, 5, gen.Calls())
}

// TestFake_ExhaustedPanics: exceeding the sequence panics.
func TestFake_ExhaustedPanics(t *testing.T) {
	gen := idstest.NewFake("only-one")
	_ = gen.NewID() // consume the only id
	assert.Panics(t, func() { gen.NewID() })
}

// TestFake_Cyclic: with Cyclic set, the sequence wraps around.
func TestFake_Cyclic(t *testing.T) {
	gen := idstest.NewFake("x", "y")
	gen.Cyclic = true
	assert.Equal(t, "x", gen.NewID().String())
	assert.Equal(t, "y", gen.NewID().String())
	assert.Equal(t, "x", gen.NewID().String()) // wraps
}

// TestFake_Calls: Calls returns the total invocations across all methods.
func TestFake_Calls(t *testing.T) {
	gen := idstest.Sequential(10)
	gen.NewID()
	gen.NewSessionID()
	gen.NewRequestID()
	assert.Equal(t, 3, gen.Calls())
}

// TestFake_EmptyPanics: constructing with no ids panics.
func TestFake_EmptyPanics(t *testing.T) {
	assert.Panics(t, func() { idstest.NewFake() })
}
