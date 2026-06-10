// Package egress implements the [app.EgressBroker] port: a per-session
// deny-by-default host allowlist. It is the egress POLICY layer (ADR-0013
// §"Egress broker"; architecture §8.4).
//
// # What this broker is, and is NOT, in v1
//
// The broker decides POLICY — whether a session is permitted to reach a given
// host — but it is not, on its own, the v1 network containment. In v1 the actual
// containment is the per-session sandbox: it runs with `--network none` by
// DEFAULT, so ALL tools (including bash) have no external network at all. The
// network namespace is what severs egress; this allowlist is the policy that a
// future egress-proxy network path will consult to gate allowlisted egress.
//
// Concretely in v1:
//   - The sandbox's `--network none` default means in-sandbox bash and any other
//     tool simply cannot reach the network, independent of this broker. bash is
//     therefore NOT individually gated by the broker — the namespace gates it.
//   - webfetch / websearch carry EgressClass=External and consult this
//     broker before fetching, but because there is no egress network path wired
//     in v1 (and the default policy is deny-all), they are effectively disabled
//     unless an operator both configures an allowlist AND provisions an egress
//     path. The egress-proxy data path is a roadmap item (ADR-0003 deferred).
//
// So this package supplies the allowlist decision; combined with the future
// egress-proxy, it will gate allowlisted egress. It does not by itself open or
// gate a live network connection in v1.
//
// # Design
//
// The broker is the policy half of the "external communication" trifecta leg
// (architecture §8.4); the sandbox network namespace is the enforcement half in
// v1. It is an INFRA control (policy, non-bypassable by mode):
//   - Its deny-by-default posture remains in force even under
//     [policy.ModeBypass] (architecture §8.13); bypass collapses the
//     allow/deny/ask pipeline, never this infra allowlist.
//   - It fails closed (deny) on any ambiguity — an absent session policy, an
//     empty allowlist, or a nil allowlist all mean deny-all.
//   - AllowedHosts entries support one-level wildcard prefix ("*.example.com")
//     that matches exactly one subdomain label. Deeper nesting and apex matches
//     are never granted by a wildcard entry; callers must add those explicitly.
//
// # Host matching rules
//
// For each entry in AllowedHosts:
//
//   - If the entry begins with "*.", it is a single-level wildcard: it matches
//     exactly the hosts whose name is "<single-label>.<rest>", where <rest> is
//     the suffix after the "*." prefix. It does not match the apex (<rest>
//     itself) and does not match deeper nesting (<label1>.<label2>.<rest>).
//   - Otherwise the entry is an exact string match (case-sensitive, no implicit
//     suffix matching).
//
// # Audit
//
// Every [Broker.Allow] call is recorded as a [Decision] and retrievable via
// [Broker.Decisions]. This supports the audit-trail requirement for egress
// decisions (architecture §8.4).
//
// # Concurrency
//
// Broker is safe for concurrent use.
package egress

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/xd1lab/harness-ai/internal/toolruntime/app"
)

// Decision records one evaluated egress request. It is the audit record for
// a single [Broker.Allow] call.
type Decision struct {
	// SessionID is the session that made the request.
	SessionID string
	// Host is the target host that was evaluated.
	Host string
	// Allowed reports whether the request was permitted.
	Allowed bool
	// At is the wall-clock time the decision was made. (Adapter/infra layer
	// — time.Now is permitted here per the golangci exclusion rules.)
	At time.Time
}

// Broker is the concrete [app.EgressBroker] implementation.
// Construct it with [New].
type Broker struct {
	mu        sync.RWMutex
	policies  map[string]app.EgressPolicy // keyed by SessionID
	decisions map[string][]Decision       // keyed by SessionID
}

// New returns a new deny-by-default [Broker] with no policies installed.
// An empty broker denies all hosts for all sessions.
func New() *Broker {
	return &Broker{
		policies:  make(map[string]app.EgressPolicy),
		decisions: make(map[string][]Decision),
	}
}

// Allow reports whether sessionID may make an outbound connection to host
// under the session's current [app.EgressPolicy].
//
// The default (when no policy is installed, or when the allowlist is empty or
// nil) is deny: Allow returns false. A denied decision surfaces to the calling
// tool as an error observation so the model is informed it was blocked (per
// FR-TOOL-06 AC-1; architecture §8.4).
//
// The decision (allowed or denied) is appended to the session's audit log and
// is retrievable via [Broker.Decisions].
func (b *Broker) Allow(_ context.Context, sessionID, host string) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	policy, ok := b.policies[sessionID]
	allowed := ok && matchesPolicy(host, policy.AllowedHosts)

	b.decisions[sessionID] = append(b.decisions[sessionID], Decision{
		SessionID: sessionID,
		Host:      host,
		Allowed:   allowed,
		At:        time.Now(),
	})
	return allowed, nil
}

// SetPolicy installs or replaces the [app.EgressPolicy] for a session.
// Calling SetPolicy is the deliberate, operator-driven operation that widens
// or tightens a session's allowlist; it is never driven by the model
// (architecture §8.4). The replacement takes effect immediately: subsequent
// [Broker.Allow] calls observe the new policy.
func (b *Broker) SetPolicy(_ context.Context, policy app.EgressPolicy) error {
	b.mu.Lock()
	b.policies[policy.SessionID] = policy
	b.mu.Unlock()
	return nil
}

// Decisions returns a copy of all recorded [Decision]s for sessionID, in the
// order they were evaluated. It returns nil (not an error) when no decisions
// have been recorded for the session.
func (b *Broker) Decisions(sessionID string) []Decision {
	b.mu.RLock()
	src := b.decisions[sessionID]
	b.mu.RUnlock()
	if len(src) == 0 {
		return nil
	}
	out := make([]Decision, len(src))
	copy(out, src)
	return out
}

// ---------------------------------------------------------------------------
// Internal host-matching logic
// ---------------------------------------------------------------------------

// matchesPolicy reports whether host is permitted by any entry in allowed.
// Returns false immediately when allowed is nil or empty (deny-by-default).
func matchesPolicy(host string, allowed []string) bool {
	for _, entry := range allowed {
		if matchEntry(host, entry) {
			return true
		}
	}
	return false
}

// matchEntry evaluates a single allowlist entry against host.
//
// Wildcard rule: an entry of the form "*.suffix" matches exactly the hosts
// whose name is "<one-label>.suffix" — i.e. the host has exactly one
// additional label prepended to suffix, with no further dots. This prevents
// over-broad wildcard grants (*.example.com must not grant a.b.example.com).
//
// Exact rule: any other entry must equal host exactly (case-sensitive).
func matchEntry(host, entry string) bool {
	if strings.HasPrefix(entry, "*.") {
		// Wildcard entry: "*.suffix"
		suffix := entry[2:] // everything after "*."
		if !strings.HasSuffix(host, "."+suffix) {
			return false
		}
		// The label(s) before the suffix must be exactly one label (no dots).
		prefix := host[:len(host)-len(suffix)-1] // strip "." + suffix
		return !strings.Contains(prefix, ".")
	}
	// Exact match.
	return host == entry
}
