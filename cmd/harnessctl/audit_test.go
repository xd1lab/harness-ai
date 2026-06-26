// RED (test-first) tests for harnessctl's `audit verify-checkpoints` subcommand
// (AC-11). Authored BEFORE the implementation; they reference symbols that do
// NOT exist yet — the "audit" subcommand dispatch and its argument parsing — so
// the package does NOT compile / the dispatch rejects the subcommand. That
// absence is the RED proof.
//
// Pinned (SPEC AC-11):
//   - a new "audit" subcommand GROUP with "verify-checkpoints" as its action,
//     wired into splitArgs/globalFlagValueArity dispatch WITHOUT breaking the
//     existing session/run/approve/deny/interrupt/fork dispatch.
//   - the command prints VALID/INVALID + Checked, and on failure the
//     FirstBadCheckpointID + Reason, with a non-zero exit on INVALID.

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSplitArgs_AuditSubcommand asserts the new "audit verify-checkpoints" form
// splits cleanly: global flags stay global, "audit" is the subcommand, and
// "verify-checkpoints" is its first arg (the action). This must not regress the
// existing subcommands.
func TestSplitArgs_AuditSubcommand(t *testing.T) {
	glob, cmd, sub := splitArgs([]string{"--endpoint", "h:1", "audit", "verify-checkpoints"})
	assert.Equal(t, []string{"--endpoint", "h:1"}, glob, "global args")
	assert.Equal(t, "audit", cmd, "subcommand group")
	require.Len(t, sub, 1)
	assert.Equal(t, "verify-checkpoints", sub[0], "action")
}

// TestAuditCommand_ParsesVerifyCheckpointsAction asserts the audit dispatch
// recognizes the verify-checkpoints action and rejects an unknown action — the
// argument-parsing surface the spec requires a unit test for.
func TestAuditCommand_ParsesVerifyCheckpointsAction(t *testing.T) {
	// auditAction maps the action token to a parsed kind (the function the audit
	// dispatch uses). It does NOT exist yet — that is the RED proof.
	kind, err := parseAuditAction([]string{"verify-checkpoints"})
	require.NoError(t, err)
	assert.Equal(t, auditVerifyCheckpoints, kind)

	_, err = parseAuditAction([]string{"frobnicate"})
	require.Error(t, err, "an unknown audit action must error")

	_, err = parseAuditAction(nil)
	require.Error(t, err, "a missing audit action must error")
}

// TestRun_AuditUnknownAction asserts the top-level run() surfaces an error for an
// unknown audit action rather than silently succeeding (it must reach the audit
// dispatch, proving the subcommand is wired). It uses --insecure with an
// unreachable endpoint; the parse/dispatch error must come BEFORE any RPC.
func TestRun_AuditUnknownAction(t *testing.T) {
	t.Setenv(devInsecureEnv, "")
	var out testWriter
	err := run([]string{"--endpoint", "127.0.0.1:1", "--insecure", "audit", "frobnicate"}, &out)
	require.Error(t, err, "an unknown audit action must error")
}

// testWriter is a no-op io.Writer for tests that only assert the error.
type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }
