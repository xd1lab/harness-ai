package policy

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// ErrBypassForbidden is returned by [Engine.Evaluate] when [ModeBypass] is
// requested for a dispatch whose session carries untrusted-content taint
// ([Input.Tainted] is true). Bypass is forbidden when untrusted content is
// present (FR-PERM-02 AC-2; ADR-0013 §"Constrained bypass"); the engine refuses
// to collapse the pipeline rather than silently downgrading the mode. It is
// matchable with [errors.Is].
var ErrBypassForbidden = errors.New("policy: bypass mode forbidden while untrusted content is present")

// Config configures a [NewEngine]. It is the engine's construction-time input;
// the [RuleSet] is the data-driven deny/allow rules and EditTools is the set of
// tool names treated as file-edit/write tools for [ModeAcceptEdits]
// auto-approval (defaulted when empty).
type Config struct {
	// RuleSet is the ordered deny/allow rule collection the engine evaluates.
	RuleSet RuleSet
	// EditTools is the set of tool names that [ModeAcceptEdits] auto-approves
	// (e.g. "edit", "write"). It is configurable because the edit/write-vs-bash
	// distinction is not derivable from [domain.SideEffect] alone — bash is also
	// mutating, yet must still be gated under acceptEdits (FR-PERM-02 AC-1). When
	// empty, [DefaultEditTools] is used.
	EditTools []string
}

// DefaultEditTools is the default set of tool names [ModeAcceptEdits]
// auto-approves when [Config.EditTools] is left empty: the in-tree file
// mutation tools.
var DefaultEditTools = []string{"edit", "write"}

// Engine is the concrete [PolicyEngine]: a pure, immutable evaluator of the
// deny→mode→allow→ask pipeline (architecture §8.13). It holds a pre-partitioned,
// read-only view of its [RuleSet], so [Engine.Evaluate] performs no mutation and
// is safe for concurrent use by construction.
type Engine struct {
	denyRules  []Rule
	allowRules []Rule
	editTools  map[string]struct{}
}

// Compile-time assertion that *Engine satisfies the frozen PolicyEngine contract.
var _ PolicyEngine = (*Engine)(nil)

// NewEngine builds an [Engine] from cfg, validating and partitioning the rules
// once up front. It returns an error if any rule carries an unrecognized
// [RuleEffect] (so a misconfigured rule fails fast at construction rather than
// silently no-opping at evaluate time). The returned engine is immutable and
// concurrency-safe.
func NewEngine(cfg Config) (*Engine, error) {
	eng := &Engine{editTools: make(map[string]struct{})}
	for i, r := range cfg.RuleSet.Rules {
		switch r.Effect {
		case EffectDeny:
			eng.denyRules = append(eng.denyRules, r)
		case EffectAllow:
			eng.allowRules = append(eng.allowRules, r)
		default:
			return nil, fmt.Errorf("policy: rule %d (id=%q): invalid effect %q (want %q or %q)",
				i, r.ID, r.Effect, EffectDeny, EffectAllow)
		}
	}
	edits := cfg.EditTools
	if len(edits) == 0 {
		edits = DefaultEditTools
	}
	for _, name := range edits {
		eng.editTools[name] = struct{}{}
	}
	return eng, nil
}

// Evaluate applies the deny→mode→allow→ask pipeline to in and returns the
// [Result]. The ordering is load-bearing (architecture §8.13):
//
//  1. Deny rules first — a match returns [Deny] unconditionally, beating any
//     allow rule and any mode (including [ModeBypass]).
//  2. Mode — [ModeBypass] (rejected outright when tainted), then [ModeAcceptEdits]
//     auto-allow of edit tools, then [ModePlan] hold of mutating tools.
//  3. Allow rules — a match returns [Allow], except an external-comms allow is
//     subject to the taint escalation below.
//  4. Ask — the default fallthrough; external-comms tools to non-allowlisted
//     hosts and taint-escalated external calls resolve here too.
//
// It performs no I/O and never mutates the engine; it is safe for concurrent use.
func (e *Engine) Evaluate(_ context.Context, in Input) (Result, error) {
	// --- Step 1: DENY (unconditional, beats everything downstream). ---
	if r, ok := e.firstMatch(e.denyRules, in); ok {
		return Result{
			Decision: Deny,
			RuleID:   r.ID,
			Reason:   denyReason(r),
		}, nil
	}

	// --- Step 2: MODE. ---
	switch in.Mode {
	case ModeBypass:
		// Bypass is forbidden while untrusted content is present (FR-PERM-02
		// AC-2). Deny already had its say above; here we refuse rather than
		// collapse. The non-bypassable infra controls (egress/tenant) live
		// outside this package, so collapsing allow/ask here does not touch them.
		if in.Tainted {
			return Result{}, ErrBypassForbidden
		}
		return Result{
			Decision: Allow,
			RuleID:   string(ModeBypass),
			Reason:   "bypass mode: allow/ask pipeline collapsed by operator (infra controls still enforced)",
		}, nil
	case ModeAcceptEdits:
		// Auto-approve file-edit/write tools only. bash is mutating but is not
		// an edit tool, so it falls through to the normal allow/ask pipeline
		// (FR-PERM-02 AC-1).
		if e.isEditTool(in) {
			return Result{
				Decision: Allow,
				RuleID:   string(ModeAcceptEdits),
				Reason:   "acceptEdits mode: file edit auto-approved",
			}, nil
		}
	case ModePlan:
		// Plan is read-only: a mutating tool is not permitted to execute and is
		// held for human approval (FR-PERM-01 AC-2). External-comms tools remain
		// subject to the egress/taint gate in the ask stage below.
		if in.SideEffect == domain.SideEffectMutating {
			return Result{
				Decision: Ask,
				RuleID:   string(ModePlan),
				Reason:   "plan mode: mutating tool held for approval (planning only)",
			}, nil
		}
	case ModeDefault:
		// No auto-resolution; fall through to allow/ask.
	default:
		// No auto-resolution; fall through to allow/ask.
	}

	// --- Step 3: ALLOW. ---
	if r, ok := e.firstMatch(e.allowRules, in); ok {
		// The taint gate can only ESCALATE an otherwise-allowed external-comms
		// action to ask (architecture §8.4); it never loosens. An allow rule
		// that matched an external call is overridden to ask when tainted.
		if isExternal(in) && in.Tainted {
			return Result{
				Decision: Ask,
				RuleID:   r.ID,
				Reason:   "external-comms escalated to approval: untrusted content present (taint gate)",
			}, nil
		}
		return Result{
			Decision: Allow,
			RuleID:   r.ID,
			Reason:   allowReason(r),
		}, nil
	}

	// --- Step 4: ASK (default fallthrough). ---
	// An external-comms tool that no allow rule placed on the allowlist is gated
	// to ask whether or not the session is tainted — a read of an attacker URL
	// is a write to the attacker (FR-PERM-03 AC-1/AC-2; architecture §8.4).
	if isExternal(in) {
		return Result{
			Decision: Ask,
			Reason:   "external-comms to non-allowlisted host requires approval",
		}, nil
	}
	return Result{
		Decision: Ask,
		Reason:   "no matching allow rule: action requires approval",
	}, nil
}

// firstMatch returns the first rule in rules that matches in, in slice order.
func (e *Engine) firstMatch(rules []Rule, in Input) (Rule, bool) {
	for _, r := range rules {
		if ruleMatches(r, in) {
			return r, true
		}
	}
	return Rule{}, false
}

// isEditTool reports whether the dispatch is a file-edit/write tool eligible for
// acceptEdits auto-approval.
func (e *Engine) isEditTool(in Input) bool {
	_, ok := e.editTools[in.ToolName]
	return ok
}

// isExternal reports whether the dispatch is an external-comms action subject to
// the egress allowlist and the taint gate. Per the domain contract the unset
// zero value ("") is treated as external (deny-by-default; ADR-0014), so only an
// explicit None/Internal classification is exempt.
func isExternal(in Input) bool {
	switch in.EgressClass {
	case domain.EgressClassNone, domain.EgressClassInternal:
		return false
	default:
		// EgressClassExternal and the unset zero value both gate as external.
		return true
	}
}

// ruleMatches reports whether rule r applies to dispatch in: the tool name must
// match (empty ToolName is a catch-all) and the optional [Rule.ArgMatch]
// predicate, if present, must match the tool arguments.
func ruleMatches(r Rule, in Input) bool {
	if r.ToolName != "" && r.ToolName != in.ToolName {
		return false
	}
	if r.ArgMatch == "" {
		return true
	}
	return argMatches(r.ArgMatch, in.ToolArgs)
}

// argMatches evaluates the engine's small, deterministic [Rule.ArgMatch] grammar
// against the parsed tool arguments. The grammar (owned by this engine) is:
//
//   - "host:<host>" — matches when the dispatch's egress target host (extracted
//     from a "url" or "host" argument) equals <host> or is a subdomain of it.
//     Used by allow rules to express a per-session host allowlist entry.
//   - "<prefix>*"  — matches when any string argument has the literal prefix
//     before the trailing '*' (a simple glob, e.g. "rm -rf*").
//   - "<literal>"  — matches when any string argument contains the literal as a
//     substring.
//
// An expression that cannot be satisfied returns false (fail-closed for deny is
// irrelevant — a non-matching deny simply does not fire; a non-matching allow
// falls through to ask).
func argMatches(expr string, args map[string]any) bool {
	if host, ok := strings.CutPrefix(expr, "host:"); ok {
		return hostMatches(host, args)
	}
	if prefix, ok := strings.CutSuffix(expr, "*"); ok {
		return anyStringArg(args, func(v string) bool { return strings.HasPrefix(v, prefix) })
	}
	return anyStringArg(args, func(v string) bool { return strings.Contains(v, expr) })
}

// hostMatches reports whether the egress target host of the dispatch equals want
// or is a subdomain of want. The target host is taken from a "host" argument
// when present, otherwise parsed from a "url" argument.
func hostMatches(want string, args map[string]any) bool {
	want = strings.ToLower(strings.TrimSpace(want))
	if want == "" {
		return false
	}
	got := targetHost(args)
	if got == "" {
		return false
	}
	return got == want || strings.HasSuffix(got, "."+want)
}

// targetHost extracts the lower-cased egress target host from a dispatch's args,
// preferring an explicit "host" key and falling back to the host of a "url" key.
// It returns "" when no host can be determined.
func targetHost(args map[string]any) string {
	if h, ok := stringArg(args, "host"); ok && h != "" {
		return strings.ToLower(h)
	}
	if raw, ok := stringArg(args, "url"); ok && raw != "" {
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			return strings.ToLower(u.Hostname())
		}
	}
	return ""
}

// stringArg returns the string value at key in args, if present and a string.
func stringArg(args map[string]any, key string) (string, bool) {
	v, ok := args[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// anyStringArg reports whether pred returns true for any string-valued argument.
func anyStringArg(args map[string]any, pred func(string) bool) bool {
	for _, v := range args {
		if s, ok := v.(string); ok && pred(s) {
			return true
		}
	}
	return false
}

// denyReason renders a human-readable reason for a deny decision, preferring the
// rule's id so the audit event and approval surface name the blocking rule.
func denyReason(r Rule) string {
	if r.ID != "" {
		return "denied by rule " + r.ID
	}
	return "denied by deny rule"
}

// allowReason renders a human-readable reason for an allow decision.
func allowReason(r Rule) string {
	if r.ID != "" {
		return "allowed by rule " + r.ID
	}
	return "allowed by allow rule"
}
