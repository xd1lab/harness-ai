package agentctx

import (
	"errors"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// ErrFailedPrecondition is the sentinel returned by [ValidateClear] when a
// requested [domain.ToolResultCleared] violates the append-time precondition:
// the target (session_id, seq) does not exist, or it exists but is not a
// [domain.ToolResult]. It maps to the gRPC FAILED_PRECONDITION the transport
// surfaces (architecture §6.5; FR-CTX-02 AC-2). Recover it with [errors.Is].
//
// Note that clearing an already-cleared result is NOT this error — that case is
// an idempotent no-op (see [ErrAlreadyCleared] / [IsCleared]); only a missing or
// wrong-typed target is a precondition failure.
var ErrFailedPrecondition = errors.New("agentctx: tool-result clear failed precondition (target missing or not a ToolResult)")

// ErrAlreadyCleared is a non-fatal sentinel reporting that the target ToolResult
// was already superseded by a prior [domain.ToolResultCleared]. A re-clear is an
// idempotent no-op rather than a precondition failure (architecture §6.5), so
// [ValidateClear] returns nil in this case; this sentinel is provided so a
// caller that wants to distinguish "freshly cleared" from "already cleared" can,
// via [IsCleared]. Recover it with [errors.Is].
var ErrAlreadyCleared = errors.New("agentctx: tool result already cleared (no-op)")

// ValidateClear checks whether a [domain.ToolResultCleared] targeting
// (clearedSessionID, clearedSeq) is admissible against the folded events,
// enforcing the append-time rules of architecture §6.5 (FR-CTX-02 AC-2):
//
//   - The target must EXIST in events and be a [domain.ToolResult]. Otherwise
//     ValidateClear returns [ErrFailedPrecondition].
//   - The match is on the (session_id, seq) pair — a seq that exists under a
//     different session does not match (fork-safety).
//   - If the target exists, is a ToolResult, and is ALREADY cleared by a prior
//     [domain.ToolResultCleared] in events, the clear is an idempotent no-op:
//     ValidateClear returns nil (NOT an error), so replay and fork composition
//     are order-insensitive.
//
// It is a pure function over events and performs no I/O. The loop calls it
// before appending the ToolResultCleared so an invalid clear is rejected without
// touching the log.
func ValidateClear(events []domain.EventEnvelope, clearedSessionID string, clearedSeq int64) error {
	if _, found := findToolResult(events, clearedSessionID, clearedSeq); !found {
		return ErrFailedPrecondition
	}
	// The target exists and is a ToolResult. Whether or not it was already
	// cleared, the clear is admissible: a fresh clear supersedes it, and a
	// re-clear is an idempotent no-op (architecture §6.5). Neither is a
	// precondition failure, so nil is returned in both cases; a caller that
	// needs to distinguish the two uses [IsCleared].
	return nil
}

// IsCleared reports whether the ToolResult identified by (clearedSessionID,
// clearedSeq) is already superseded by a [domain.ToolResultCleared] in events.
// It returns [ErrFailedPrecondition] if the target does not exist or is not a
// [domain.ToolResult], so a caller cannot mistake "absent" for "not cleared".
// It is pure and performs no I/O.
func IsCleared(events []domain.EventEnvelope, clearedSessionID string, clearedSeq int64) (bool, error) {
	if _, found := findToolResult(events, clearedSessionID, clearedSeq); !found {
		return false, ErrFailedPrecondition
	}
	return alreadyCleared(events, clearedSessionID, clearedSeq), nil
}

// findToolResult locates the [domain.ToolResult] payload at (sessionID, seq) in
// events. It returns (payload, true) only when an envelope at that
// (session_id, seq) exists AND its payload is a ToolResult; (nil, false)
// otherwise (missing target or wrong type), which the caller maps to
// [ErrFailedPrecondition].
func findToolResult(events []domain.EventEnvelope, sessionID string, seq int64) (*domain.ToolResult, bool) {
	for i := range events {
		env := events[i]
		if env.SessionID != sessionID || env.Seq != seq {
			continue
		}
		tr, ok := env.Event.(domain.ToolResult)
		if !ok {
			// An event exists at this (session, seq) but it is not a ToolResult.
			return nil, false
		}
		return &tr, true
	}
	return nil, false
}

// alreadyCleared reports whether some [domain.ToolResultCleared] in events
// targets (sessionID, seq).
func alreadyCleared(events []domain.EventEnvelope, sessionID string, seq int64) bool {
	for _, env := range events {
		c, ok := env.Event.(domain.ToolResultCleared)
		if !ok {
			continue
		}
		if c.ClearedSessionID == sessionID && c.ClearedSeq == seq {
			return true
		}
	}
	return false
}
