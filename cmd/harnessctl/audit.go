// SPDX-License-Identifier: Apache-2.0

// This file implements harnessctl's OPERATOR-TIER "audit" subcommand group
// (Batch-5B, ADR-0034, AC-11/AC-12), currently with a single action:
//
//	harnessctl audit verify-checkpoints
//
// # Why this does NOT use the orchestrator gRPC client
//
// Every OrchestratorService RPC is TENANT-scoped (RLS): a call is bound to one
// authenticated tenant. The signed audit-checkpoint chain (audit_checkpoints) is
// GLOBAL — one chain spans ALL tenants, anchored to an Ed25519 key held OUTSIDE
// the events DB. Verifying it means re-reading the events' content_hash leaves
// ACROSS tenants over each checkpoint's covered [covers_from_global_id,
// covers_to_global_id] range and checking the stored signature against the
// recomputed checkpoint_hash. That cross-tenant, operator-tier read is exactly
// what the tenant-scoped orchestrator gRPC CANNOT reach (AC-12 open question #5).
//
// Per AC-12 the spec's default is NO proto change: rather than punch a
// cross-tenant operator RPC through the tenant edge (and the RBAC/SPIFFE surface
// that implies), harnessctl verify-checkpoints runs the operator-tier read
// DIRECTLY against the operator/owner Postgres connection — the same tier the
// projection signer (T5) and the store method VerifyAuditCheckpoints (T6) run on
// — and verifies signatures locally with the Ed25519 PUBLIC key
// ([auditsign.Verifier], BOLTROPE_AUDIT_PUBLIC_KEY or the key derived from a
// configured signing key). gen/ stays byte-identical (no proto / no RPC added).
//
// # Operator-tier, not the egress broker (ADR-0013 / ADR-0034)
//
// This path holds only PUBLIC key material and reads Postgres directly; it makes
// no MODEL-INFLUENCED outbound call, so it deliberately does NOT route through
// the toolruntime egress broker (which governs model-influenced channels only).
// It is operator infrastructure, like the SIEM exporter and the OTLP/metrics
// path (AC-14).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/eventstore"
	"github.com/xd1lab/harness-ai/internal/platform/auditsign"
	"github.com/xd1lab/harness-ai/internal/platform/secret"
)

// auditActionKind is the parsed identity of an "audit" sub-action.
type auditActionKind int

const (
	// auditUnknown is the zero value for an unrecognized/absent action.
	auditUnknown auditActionKind = iota
	// auditVerifyCheckpoints is the "verify-checkpoints" action.
	auditVerifyCheckpoints
)

// auditDatabaseURLEnv names the operator/owner Postgres DSN harnessctl connects
// to for the operator-tier checkpoint verify. It MUST connect as an
// operator/owner role that bypasses events' RLS (the same connection tier the
// projection signer runs on) so the cross-tenant content_hash re-read is
// permitted. When unset, BOLTROPE_POSTGRES__DSN (the rest of the stack's DSN, set
// for operator deployments) is used as a fallback.
const (
	auditDatabaseURLEnv = "BOLTROPE_AUDIT_DATABASE_URL"
	postgresDSNEnv      = "BOLTROPE_POSTGRES__DSN"
)

// parseAuditAction maps the audit sub-action tokens (the args AFTER "audit") to a
// parsed [auditActionKind]. A missing action or an unrecognized action is an
// error — surfaced BEFORE any DB connection so an argument mistake never opens a
// connection or runs a query.
func parseAuditAction(args []string) (auditActionKind, error) {
	if len(args) == 0 {
		return auditUnknown, errors.New("audit: action required; valid actions: verify-checkpoints")
	}
	switch args[0] {
	case "verify-checkpoints":
		return auditVerifyCheckpoints, nil
	default:
		return auditUnknown, fmt.Errorf("audit: unknown action %q; valid actions: verify-checkpoints", args[0])
	}
}

// auditCommand dispatches the "audit" subcommand group. subcmdArgs are the tokens
// AFTER "audit" (e.g. ["verify-checkpoints"]). The action is parsed first so an
// argument error surfaces before any Postgres connection is attempted (AC-11).
func auditCommand(ctx context.Context, cfg *cliConfig, subcmdArgs []string, w io.Writer) error {
	action, err := parseAuditAction(subcmdArgs)
	if err != nil {
		return err
	}
	switch action {
	case auditVerifyCheckpoints:
		return verifyCheckpointsCommand(ctx, cfg, w)
	default:
		// Unreachable: parseAuditAction returns an error for anything else.
		return errors.New("audit: unhandled action")
	}
}

// auditDSN resolves the operator/owner Postgres DSN for the operator-tier verify,
// preferring BOLTROPE_AUDIT_DATABASE_URL and falling back to BOLTROPE_POSTGRES__DSN.
// An empty result is an actionable error (no silent skip of the tamper-PROOF check).
func auditDSN() (string, error) {
	if v := os.Getenv(auditDatabaseURLEnv); v != "" {
		return v, nil
	}
	if v := os.Getenv(postgresDSNEnv); v != "" {
		return v, nil
	}
	return "", fmt.Errorf(
		"audit verify-checkpoints needs the operator/owner Postgres DSN; set %s (or %s). "+
			"It MUST be an operator/owner role that bypasses events' RLS, the same tier the projection signer runs on",
		auditDatabaseURLEnv, postgresDSNEnv)
}

// verifyCheckpointsCommand runs the tamper-PROOF check (AC-9/AC-11): it opens the
// operator/owner Postgres connection, constructs the Ed25519 [auditsign.Verifier]
// from the configured PUBLIC key, runs [eventstore.Store.VerifyAuditCheckpoints]
// over the GLOBAL audit_checkpoints + events.content_hash, prints the result, and
// returns a non-nil error on INVALID so the process exits non-zero.
//
// Output:
//
//	VALID:   "audit checkpoints VALID checked=<n>"
//	INVALID: "audit checkpoints INVALID checked=<n> first_bad_checkpoint_id=<id> reason=<…>"
//	         + a non-nil error (non-zero exit).
//	empty:   "audit checkpoints VALID checked=0" (nothing anchored yet is not a tamper).
func verifyCheckpointsCommand(ctx context.Context, cfg *cliConfig, w io.Writer) error {
	_ = cfg // the operator-tier verify reaches Postgres directly, not the gRPC endpoint.

	dsn, err := auditDSN()
	if err != nil {
		return err
	}

	// The Verifier holds only PUBLIC material (BOLTROPE_AUDIT_PUBLIC_KEY, or the
	// public key derived from a configured BOLTROPE_AUDIT_SIGNING_KEY). The bare env
	// names match the signer's resolution (no secret prefix), so a deployment that
	// runs the signer can verify with the same env.
	secrets := secret.NewEnvSecrets()
	verifier, err := auditsign.NewVerifier(ctx, secrets)
	if err != nil {
		if errors.Is(err, auditsign.ErrSigningDisabled) {
			return fmt.Errorf(
				"audit verify-checkpoints needs a public key: set %s (or %s to derive it). %w",
				auditsign.EnvPublicKey, auditsign.EnvSigningKey, err)
		}
		return fmt.Errorf("audit verify-checkpoints: build verifier: %w", err)
	}

	// SimplePool opens a fresh operator/owner connection per acquire (no puddle
	// dependency); it is the operator pool the Store needs for the RLS-exempt,
	// cross-tenant checkpoint read. The application pool is unused for this read, so
	// the same operator pool is passed for both arguments of NewWithOperator.
	pool, err := eventstore.NewSimplePool(dsn)
	if err != nil {
		return fmt.Errorf("audit verify-checkpoints: %w", err)
	}
	defer pool.Close()

	store := eventstore.NewWithOperator(pool, pool)

	res, err := store.VerifyAuditCheckpoints(ctx, verifier)
	if err != nil {
		return fmt.Errorf("audit verify-checkpoints: %w", err)
	}

	if res.Valid {
		if _, werr := fmt.Fprintf(w, "audit checkpoints VALID checked=%d\n", res.Checked); werr != nil {
			return werr
		}
		return nil
	}

	if _, werr := fmt.Fprintf(w,
		"audit checkpoints INVALID checked=%d first_bad_checkpoint_id=%d reason=%s\n",
		res.Checked, res.FirstBadCheckpointID, res.Reason); werr != nil {
		return werr
	}
	// Non-nil error => non-zero exit (main() prints and os.Exit(1)).
	return fmt.Errorf("audit checkpoints INVALID: first bad checkpoint id=%d: %s",
		res.FirstBadCheckpointID, res.Reason)
}
