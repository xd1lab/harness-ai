//go:build integration

package eventstore

import (
	"testing"

	"github.com/google/uuid"
)

// mustUUIDv7 returns a fresh UUIDv7 string or fails the test. UUIDv7 matches the
// platform ids.System minting (roughly time-sortable), which is convenient for
// readable session ids in test logs.
func mustUUIDv7(t *testing.T) string {
	t.Helper()
	u, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7: %v", err)
	}
	return u.String()
}
