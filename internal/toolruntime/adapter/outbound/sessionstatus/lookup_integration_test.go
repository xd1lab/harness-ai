//go:build integration

package sessionstatus

import (
	"context"
	"testing"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/runtime"
)

// itCtx is a convenience for a plain background context — deliberately
// carrying NO tenant: the whole point of the definer function is that the
// reaper has no tenant principal.
var itCtx = context.Background()

// TestStatus_Integration_ResolvesStatusesAcrossTenantsWithoutTenantGUC is the
// end-to-end proof of the 0005 design: connected as the RLS-bound boltrope_app
// role with NO app.current_tenant set, the Lookup resolves the status of
// sessions belonging to DIFFERENT tenants — exactly the cross-tenant,
// principal-less view the reaper needs (architecture §10.6).
func TestStatus_Integration_ResolvesStatusesAcrossTenantsWithoutTenantGUC(t *testing.T) {
	h := newStatusHarness(t)
	t.Logf("mode: %s", h.mode)

	cases := []struct {
		column string
		want   runtime.SessionStatus
	}{
		{column: "active", want: runtime.SessionActive},
		{column: "finished", want: runtime.SessionFinished},
		{column: "failed", want: runtime.SessionFailed},
	}
	for _, tc := range cases {
		t.Run(tc.column, func(t *testing.T) {
			sessionID := h.seedSession(t, tc.column) // fresh tenant per seed

			got, err := h.lookup.Status(itCtx, sessionID)
			if err != nil {
				t.Fatalf("Status(%s session): %v", tc.column, err)
			}
			if got != tc.want {
				t.Errorf("Status = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestStatus_Integration_DirectTableReadStaysRLSGuarded proves the definer
// function did not widen the table: the SAME app-role connection that the
// Lookup uses cannot SELECT the sessions row directly, because the fail-closed
// tenant-GUC policy raises when app.current_tenant is unset (migration 0003).
// The narrow function is the ONLY principal-less path.
func TestStatus_Integration_DirectTableReadStaysRLSGuarded(t *testing.T) {
	h := newStatusHarness(t)
	sessionID := h.seedSession(t, "finished")

	conn, err := h.pool.Acquire(itCtx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer conn.Release()

	var status string
	err = conn.QueryRow(itCtx, "SELECT status FROM sessions WHERE id = $1::uuid", sessionID).Scan(&status)
	if err == nil {
		t.Fatalf("direct SELECT on sessions succeeded without a tenant GUC (got %q); RLS no longer guards the table", status)
	}
}

// TestStatus_Integration_MissingSessionIsUnknownWithError verifies the
// ambiguity mapping end-to-end: a UUID with no sessions row yields
// SessionUnknown + error (retain; the reaper falls back to TTLs).
func TestStatus_Integration_MissingSessionIsUnknownWithError(t *testing.T) {
	h := newStatusHarness(t)

	got, err := h.lookup.Status(itCtx, newUUID(t))
	if err == nil {
		t.Fatal("Status returned nil error for a missing session")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
}

// TestStatus_Integration_MalformedSessionIDIsUnknownWithError verifies the
// ::uuid cast surfaces a malformed id as a server-side error → SessionUnknown.
func TestStatus_Integration_MalformedSessionIDIsUnknownWithError(t *testing.T) {
	h := newStatusHarness(t)

	got, err := h.lookup.Status(itCtx, "not-a-uuid")
	if err == nil {
		t.Fatal("Status returned nil error for a malformed session id")
	}
	if got != runtime.SessionUnknown {
		t.Errorf("Status = %v, want SessionUnknown", got)
	}
}
