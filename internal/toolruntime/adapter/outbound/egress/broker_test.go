// Package egress_test exercises the [egress.Broker] implementation: the
// per-session deny-by-default allowlist that every model-influenced egress path
// routes through (ADR-0013 §8.4; architecture §8.4).
//
// All tests in this file are pure unit tests — no Docker, no network.
package egress_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/egress"
	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// ctx is the background context shared by all sub-tests.
var ctx = context.Background()

// ---------------------------------------------------------------------------
// Interface satisfaction
// ---------------------------------------------------------------------------

// TestBroker_ImplementsEgressBroker ensures Broker satisfies the frozen
// app.EgressBroker port at compile time.
func TestBroker_ImplementsEgressBroker(t *testing.T) {
	t.Helper()
	var _ app.EgressBroker = egress.New()
}

// ---------------------------------------------------------------------------
// FR-TOOL-06 AC-1 — deny-by-default (empty allowlist)
// ---------------------------------------------------------------------------

// TestBroker_DenyByDefault verifies that a freshly-created broker with no
// policy installed denies all hosts (deny-by-default; architecture §8.4).
func TestBroker_DenyByDefault(t *testing.T) {
	b := egress.New()
	allowed, err := b.Allow(ctx, "sess-1", "example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "no policy installed: must deny by default")
}

// TestBroker_EmptyAllowlistDeniesAll verifies that explicitly setting a policy
// with an empty AllowedHosts slice still denies everything (empty ≠ allow-all).
func TestBroker_EmptyAllowlistDeniesAll(t *testing.T) {
	b := egress.New()
	err := b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-1",
		AllowedHosts: []string{},
	})
	require.NoError(t, err)

	allowed, err := b.Allow(ctx, "sess-1", "example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "empty allowlist must deny all (not allow-all)")
}

// TestBroker_NilAllowlistDeniesAll verifies that a nil AllowedHosts slice also
// denies everything.
func TestBroker_NilAllowlistDeniesAll(t *testing.T) {
	b := egress.New()
	err := b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-2",
		AllowedHosts: nil,
	})
	require.NoError(t, err)

	allowed, err := b.Allow(ctx, "sess-2", "api.github.com")
	require.NoError(t, err)
	assert.False(t, allowed, "nil allowlist must deny all")
}

// ---------------------------------------------------------------------------
// Allowlisted host is permitted
// ---------------------------------------------------------------------------

// TestBroker_AllowlistedHostPermitted verifies that a host explicitly on the
// session's allowlist is allowed.
func TestBroker_AllowlistedHostPermitted(t *testing.T) {
	b := egress.New()
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-3",
		AllowedHosts: []string{"api.example.com", "cdn.example.com"},
	}))

	allowed, err := b.Allow(ctx, "sess-3", "api.example.com")
	require.NoError(t, err)
	assert.True(t, allowed)
}

// TestBroker_NonAllowlistedHostDenied verifies that a host absent from the
// allowlist is denied even when other hosts are allowed.
func TestBroker_NonAllowlistedHostDenied(t *testing.T) {
	b := egress.New()
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-4",
		AllowedHosts: []string{"safe.example.com"},
	}))

	allowed, err := b.Allow(ctx, "sess-4", "evil.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "non-allowlisted host must be denied")
}

// ---------------------------------------------------------------------------
// Suffix/wildcard matching (architecture §8.4 allows "configured allowlist")
// ---------------------------------------------------------------------------

// TestBroker_SuffixWildcard verifies that a "*.example.com" entry permits any
// direct subdomain of example.com (e.g. api.example.com) but does not permit
// example.com itself or deeper nesting like a.b.example.com.
func TestBroker_SuffixWildcard(t *testing.T) {
	b := egress.New()
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-5",
		AllowedHosts: []string{"*.example.com"},
	}))

	tests := []struct {
		host    string
		want    bool
		comment string
	}{
		{"api.example.com", true, "direct subdomain of *.example.com"},
		{"cdn.example.com", true, "direct subdomain of *.example.com"},
		{"example.com", false, "apex not matched by *.example.com"},
		{"a.b.example.com", false, "deeper nesting not matched by *.example.com"},
		{"evil.com", false, "completely different domain"},
		{"notexample.com", false, "suffix-only match would be wrong"},
	}

	for _, tc := range tests {
		allowed, err := b.Allow(ctx, "sess-5", tc.host)
		require.NoError(t, err, "host=%s", tc.host)
		assert.Equal(t, tc.want, allowed, "host=%s: %s", tc.host, tc.comment)
	}
}

// TestBroker_ExactMatch verifies that non-wildcard entries require exact host
// equality (no accidental suffix matching).
func TestBroker_ExactMatch(t *testing.T) {
	b := egress.New()
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-6",
		AllowedHosts: []string{"example.com"},
	}))

	// Exact match is allowed.
	allowed, err := b.Allow(ctx, "sess-6", "example.com")
	require.NoError(t, err)
	assert.True(t, allowed)

	// A subdomain of an exact entry must NOT be implicitly allowed.
	allowed, err = b.Allow(ctx, "sess-6", "sub.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "subdomain must not be implicitly allowed by exact-match entry")
}

// ---------------------------------------------------------------------------
// Session isolation
// ---------------------------------------------------------------------------

// TestBroker_SessionIsolation verifies that an allowlist on sess-A does not
// affect sess-B (per-session scoping).
func TestBroker_SessionIsolation(t *testing.T) {
	b := egress.New()
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    "sess-A",
		AllowedHosts: []string{"allowed.example.com"},
	}))

	// sess-B has no policy → deny.
	allowed, err := b.Allow(ctx, "sess-B", "allowed.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "sess-B has no policy; must be denied")
}

// ---------------------------------------------------------------------------
// Policy replacement (SetPolicy is idempotent / overwrite semantics)
// ---------------------------------------------------------------------------

// TestBroker_PolicyReplacement verifies that a second SetPolicy call replaces
// the first (widening is honored; tightening is honored).
func TestBroker_PolicyReplacement(t *testing.T) {
	b := egress.New()
	const sid = "sess-repl"

	// First policy: allows host-A.
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    sid,
		AllowedHosts: []string{"host-a.example.com"},
	}))
	allowed, err := b.Allow(ctx, sid, "host-a.example.com")
	require.NoError(t, err)
	assert.True(t, allowed)

	// Replace policy: only host-B allowed (tighten).
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    sid,
		AllowedHosts: []string{"host-b.example.com"},
	}))

	// host-A is now denied.
	allowed, err = b.Allow(ctx, sid, "host-a.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "tightened policy must deny previously-allowed host")

	// host-B is now allowed.
	allowed, err = b.Allow(ctx, sid, "host-b.example.com")
	require.NoError(t, err)
	assert.True(t, allowed)
}

// ---------------------------------------------------------------------------
// Audit / decision recording
// ---------------------------------------------------------------------------

// TestBroker_DecisionsRecorded verifies that Allow calls are recorded and
// retrievable via [egress.Broker.Decisions].
func TestBroker_DecisionsRecorded(t *testing.T) {
	b := egress.New()
	const sid = "sess-audit"

	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    sid,
		AllowedHosts: []string{"allowed.com"},
	}))

	_, _ = b.Allow(ctx, sid, "allowed.com")    // allow
	_, _ = b.Allow(ctx, sid, "denied.com")     // deny
	_, _ = b.Allow(ctx, sid, "allowed.com")    // allow again
	_, _ = b.Allow(ctx, "other-sess", "x.com") // different session, no policy

	decisions := b.Decisions(sid)
	require.Len(t, decisions, 3, "three Allow calls were made for sess-audit")

	assert.Equal(t, "allowed.com", decisions[0].Host)
	assert.True(t, decisions[0].Allowed)

	assert.Equal(t, "denied.com", decisions[1].Host)
	assert.False(t, decisions[1].Allowed)

	assert.Equal(t, "allowed.com", decisions[2].Host)
	assert.True(t, decisions[2].Allowed)
}

// TestBroker_DecisionsEmptyForUnknownSession verifies that Decisions returns an
// empty slice for a session with no recorded calls.
func TestBroker_DecisionsEmptyForUnknownSession(t *testing.T) {
	b := egress.New()
	assert.Empty(t, b.Decisions("unknown-sess"))
}

// ---------------------------------------------------------------------------
// Operator default allowlist (per-session fallback)
// ---------------------------------------------------------------------------

// TestBroker_DefaultAllowlistAppliesToAnySession verifies that an operator-
// configured default allowlist (WithDefaultAllowedHosts) governs EVERY session
// that has no explicit policy installed — sessions arrive implicitly with each
// ExecuteTool call, so per-session policy cannot be pre-installed at startup.
func TestBroker_DefaultAllowlistAppliesToAnySession(t *testing.T) {
	b := egress.New(egress.WithDefaultAllowedHosts([]string{"api.example.com", "*.internal.example.com"}))

	allowed, err := b.Allow(ctx, "sess-a", "api.example.com")
	require.NoError(t, err)
	assert.True(t, allowed, "default allowlist must apply to a session with no explicit policy")

	allowed, err = b.Allow(ctx, "sess-b", "svc.internal.example.com")
	require.NoError(t, err)
	assert.True(t, allowed, "default wildcard must apply to any session")

	allowed, err = b.Allow(ctx, "sess-a", "attacker.tld")
	require.NoError(t, err)
	assert.False(t, allowed, "hosts outside the default allowlist stay denied")
}

// TestBroker_EmptyDefaultStaysDenyAll verifies the safe default is unchanged:
// no default allowlist configured means deny-all for sessions without a policy.
func TestBroker_EmptyDefaultStaysDenyAll(t *testing.T) {
	b := egress.New(egress.WithDefaultAllowedHosts(nil))
	allowed, err := b.Allow(ctx, "sess-a", "api.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "empty default allowlist must deny all")
}

// TestBroker_ExplicitSessionPolicyOverridesDefault verifies that an explicitly
// installed per-session policy takes precedence over the operator default —
// including an explicit EMPTY policy, which is a deliberate deny-all tighten.
func TestBroker_ExplicitSessionPolicyOverridesDefault(t *testing.T) {
	b := egress.New(egress.WithDefaultAllowedHosts([]string{"api.example.com"}))
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{SessionID: "locked", AllowedHosts: nil}))
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{SessionID: "widened", AllowedHosts: []string{"other.example.com"}}))

	allowed, err := b.Allow(ctx, "locked", "api.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "an explicit empty policy is a deliberate deny-all override")

	allowed, err = b.Allow(ctx, "widened", "other.example.com")
	require.NoError(t, err)
	assert.True(t, allowed, "explicit session policy must govern that session")

	allowed, err = b.Allow(ctx, "widened", "api.example.com")
	require.NoError(t, err)
	assert.False(t, allowed, "explicit session policy replaces (not merges with) the default")
}

// ---------------------------------------------------------------------------
// Concurrency safety
// ---------------------------------------------------------------------------

// TestBroker_ConcurrentSafe exercises concurrent SetPolicy+Allow calls to
// surface data races under -race.
func TestBroker_ConcurrentSafe(t *testing.T) {
	b := egress.New()
	const sid = "sess-race"

	// Pre-install a policy.
	require.NoError(t, b.SetPolicy(ctx, app.EgressPolicy{
		SessionID:    sid,
		AllowedHosts: []string{"safe.com"},
	}))

	done := make(chan struct{})
	for range 20 {
		go func() {
			_, _ = b.Allow(ctx, sid, "safe.com")
			_, _ = b.Allow(ctx, sid, "unknown.com")
			done <- struct{}{}
		}()
		go func() {
			_ = b.SetPolicy(ctx, app.EgressPolicy{
				SessionID:    sid,
				AllowedHosts: []string{"safe.com"},
			})
			done <- struct{}{}
		}()
	}
	for range 40 {
		<-done
	}
}
