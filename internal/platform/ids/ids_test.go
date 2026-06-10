package ids_test

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/boltrope/boltrope/internal/platform/ids"
)

// TestSystem_InterfaceCompliance ensures ids.System satisfies ids.IDGenerator at
// compile time. The runtime assertion is here so a test failure names the type.
func TestSystem_InterfaceCompliance(_ *testing.T) {
	var _ ids.IDGenerator = ids.System{}
}

// TestSystem_NewID_NonEmpty asserts that NewID never returns an empty ID.
func TestSystem_NewID_NonEmpty(t *testing.T) {
	g := ids.System{}
	id := g.NewID()
	require.False(t, id.IsZero(), "NewID must not return an empty id")
}

// TestSystem_NewSessionID_NonEmpty asserts that NewSessionID never returns an
// empty ID.
func TestSystem_NewSessionID_NonEmpty(t *testing.T) {
	g := ids.System{}
	id := g.NewSessionID()
	require.False(t, id.IsZero(), "NewSessionID must not return an empty id")
}

// TestSystem_NewRequestID_NonEmpty asserts that NewRequestID never returns an
// empty ID.
func TestSystem_NewRequestID_NonEmpty(t *testing.T) {
	g := ids.System{}
	id := g.NewRequestID()
	require.False(t, id.IsZero(), "NewRequestID must not return an empty id")
}

// TestSystem_Parseable asserts that every returned ID is a valid UUID string.
// All three minting methods are exercised.
func TestSystem_Parseable(t *testing.T) {
	g := ids.System{}
	for _, fn := range []struct {
		name string
		mint func() ids.ID
	}{
		{"NewID", g.NewID},
		{"NewSessionID", g.NewSessionID},
		{"NewRequestID", g.NewRequestID},
	} {
		t.Run(fn.name, func(t *testing.T) {
			id := fn.mint()
			_, err := uuid.Parse(id.String())
			require.NoError(t, err, "%s returned an unparseable UUID: %q", fn.name, id)
		})
	}
}

// TestSystem_UUIDv7 asserts that every returned ID is a version-7 UUID.
func TestSystem_UUIDv7(t *testing.T) {
	g := ids.System{}
	for _, fn := range []struct {
		name string
		mint func() ids.ID
	}{
		{"NewID", g.NewID},
		{"NewSessionID", g.NewSessionID},
		{"NewRequestID", g.NewRequestID},
	} {
		t.Run(fn.name, func(t *testing.T) {
			id := fn.mint()
			u, err := uuid.Parse(id.String())
			require.NoError(t, err)
			assert.Equal(t, uuid.Version(7), u.Version(),
				"%s must return a UUIDv7, got version %d", fn.name, u.Version())
		})
	}
}

// TestSystem_DistinctAcross10kCalls asserts that 10 000 calls across all three
// methods produce no duplicate IDs (collision-free guarantee, NFR-TEST-01).
func TestSystem_DistinctAcross10kCalls(t *testing.T) {
	const total = 10_000
	g := ids.System{}

	seen := make(map[string]struct{}, total)
	mints := []func() ids.ID{g.NewID, g.NewSessionID, g.NewRequestID}

	for i := 0; i < total; i++ {
		id := mints[i%3]()
		s := id.String()
		require.NotEmpty(t, s, "call %d returned an empty id", i)
		_, dup := seen[s]
		require.False(t, dup, "duplicate id on call %d: %s", i, s)
		seen[s] = struct{}{}
	}
}

// TestSystem_ConcurrentDistinct exercises ids.System under concurrent use to
// verify there are no races or duplicates when goroutines call NewID
// simultaneously.
func TestSystem_ConcurrentDistinct(t *testing.T) {
	const goroutines = 50
	const callsPerGoroutine = 200

	g := ids.System{}
	results := make(chan ids.ID, goroutines*callsPerGoroutine)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range callsPerGoroutine {
				results <- g.NewID()
			}
		}()
	}
	wg.Wait()
	close(results)

	seen := make(map[string]struct{}, goroutines*callsPerGoroutine)
	for id := range results {
		s := id.String()
		require.NotEmpty(t, s)
		_, dup := seen[s]
		require.False(t, dup, "concurrent duplicate id: %s", s)
		seen[s] = struct{}{}
	}
}
