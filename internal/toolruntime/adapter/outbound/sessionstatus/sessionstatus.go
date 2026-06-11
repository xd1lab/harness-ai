// Package sessionstatus implements the sandbox reaper's session-status lookup
// ([runtime.SessionStatusFunc]) over the sessions table (architecture §10.6):
// the authority that lets the tool-runtime reclaim sandboxes of
// finished/failed sessions immediately instead of waiting out the TTLs.
//
// # RLS and the definer function
//
// sessions has FORCE ROW LEVEL SECURITY keyed on the app.current_tenant GUC
// (migration 0003), but the reaper holds only a session id — it has no
// verified tenant principal, and the GUC read is fail-closed. The lookup
// therefore calls session_status_for_reaper(uuid), a SECURITY DEFINER
// function installed by migration 0005 that exposes exactly one fact (the
// status text for an exact session UUID) to the boltrope_app role without
// widening any table policy.
//
// # Fail-safe mapping
//
// Every ambiguity maps to [runtime.SessionUnknown] PLUS an error so the
// reaper retains the sandbox and falls back to TTL reaping — it never reaps a
// sandbox it cannot positively classify as ended:
//
//   - a NULL from the function (session row missing — or hidden because the
//     migration role lacked superuser/BYPASSRLS, which FORCE RLS would bind);
//   - a status text outside the sessions_status_chk vocabulary;
//   - any connection or query failure.
//
// The schema has no "abandoned" status: abandonment manifests as an expired
// lease on a still-'active' row, which the orchestrator's recovery sweeper may
// re-claim and resume — so it maps to [runtime.SessionActive] (retain; the
// idle/absolute TTLs still bound the sandbox's lifetime, and clean-workspace
// resume makes retention safe either way).
//
// # Pool
//
// Like the dedup store, the lookup works over a consumer-defined [Pool]
// interface; [SimplePool] (fresh pgx connection per acquire) is the shipped
// implementation and production wiring reuses the event-store DSN.
package sessionstatus

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/runtime"
)

// statusQuery calls the SECURITY DEFINER lookup from migration 0005. The
// explicit ::uuid cast makes a malformed session id a server-side error (→
// SessionUnknown) instead of silently matching nothing.
const statusQuery = `SELECT session_status_for_reaper($1::uuid)`

// Pool is the minimal connection-acquisition surface the [Lookup] needs,
// declared here (in the package that uses it) per the adapter pattern shared
// with the dedup store and the event store.
type Pool interface {
	// Acquire borrows a connection. The caller owns it until Release.
	Acquire(ctx context.Context) (PooledConn, error)
	// Close releases pool resources.
	Close()
}

// PooledConn is a borrowed connection plus its release hook.
type PooledConn interface {
	// QueryRow runs a single-row query on the borrowed connection.
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	// Release returns (or closes) the connection.
	Release()
}

// Lookup resolves session statuses for the reaper. Its [Lookup.Status] method
// satisfies [runtime.SessionStatusFunc]; wire it with
// runtime.WithSessionStatus(lookup.Status). Safe for concurrent use (each call
// acquires its own connection).
type Lookup struct {
	pool Pool
}

// New returns a [Lookup] over pool.
func New(pool Pool) *Lookup { return &Lookup{pool: pool} }

// compile-time assertion that Status satisfies the reaper's port.
var _ runtime.SessionStatusFunc = (*Lookup)(nil).Status

// Status reports the lifecycle status of sessionID from the authoritative
// sessions table. See the package doc for the fail-safe mapping contract.
func (l *Lookup) Status(ctx context.Context, sessionID string) (runtime.SessionStatus, error) {
	if sessionID == "" {
		return runtime.SessionUnknown, fmt.Errorf("sessionstatus: empty session id")
	}

	conn, err := l.pool.Acquire(ctx)
	if err != nil {
		return runtime.SessionUnknown, fmt.Errorf("sessionstatus: acquiring connection: %w", err)
	}
	defer conn.Release()

	// Scan into a nullable: the scalar function always returns one row, NULL
	// when no visible sessions row matches.
	var status *string
	if err := conn.QueryRow(ctx, statusQuery, sessionID).Scan(&status); err != nil {
		return runtime.SessionUnknown, fmt.Errorf("sessionstatus: querying status of session %q: %w", sessionID, err)
	}
	if status == nil {
		return runtime.SessionUnknown, fmt.Errorf("sessionstatus: session %q not visible: row missing or hidden by RLS (migration role without BYPASSRLS?)", sessionID)
	}

	switch *status {
	case "active":
		return runtime.SessionActive, nil
	case "finished":
		return runtime.SessionFinished, nil
	case "failed":
		return runtime.SessionFailed, nil
	default:
		return runtime.SessionUnknown, fmt.Errorf("sessionstatus: session %q has unrecognized status %q", sessionID, *status)
	}
}
