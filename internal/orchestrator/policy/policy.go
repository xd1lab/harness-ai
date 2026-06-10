// Package policy defines the orchestrator's permission/guardrail contract: the
// [PolicyEngine] that decides allow/deny/ask for each tool dispatch, the [Decision]
// and [Mode] vocabularies, and the [Rule] types. It is an in-process package, not a
// service (ADR-0009; architecture §2.3), evaluated synchronously once per tool
// dispatch.
//
// # Evaluation order: deny → mode → allow → ask
//
// The pipeline is ordered and its ordering is load-bearing (ADR-0013 §"Constrained
// bypass"; architecture §8.13):
//
//  1. DENY rules are evaluated first. A matching deny rule returns [Deny]
//     immediately. DENY ALWAYS WINS UNCONDITIONALLY — regardless of any allow rule,
//     regardless of the operating [Mode] (including [ModeBypass]), and regardless of
//     taint state. Nothing downstream can overturn a deny.
//  2. MODE is consulted next. The operating [Mode] can auto-resolve a decision
//     (e.g. [ModeAcceptEdits] auto-allows edits, [ModePlan] forbids mutations,
//     [ModeBypass] collapses the remaining allow/ask stages into allow) — but a mode
//     can never override a deny from step 1, and a mode can never disable the
//     non-bypassable infra controls (egress broker denial and tenant isolation;
//     ADR-0013; architecture §8.13).
//  3. ALLOW rules are evaluated next. A matching allow rule returns [Allow].
//  4. ASK is the default fallthrough: an action neither denied, mode-resolved, nor
//     explicitly allowed requires human approval ([Ask]). The taint gate can
//     ESCALATE an otherwise-allowed external-comms action to [Ask] (ADR-0013
//     §"Taint-tracking gate"; architecture §8.4) — escalation only ever tightens,
//     never loosens.
//
// # Infra controls are not policy
//
// Egress restriction (the per-session deny-by-default allowlist enforced by the
// egress broker) and tenant isolation (RLS) are INFRA controls, not policy. They
// are non-bypassable and are NOT collapsed by [ModeBypass]; this package gates the
// allow/deny/ask pipeline only (ADR-0013 §"Constrained bypass"; architecture
// §8.13). The taint gate's *ask escalation* lives here because it is a policy
// decision; the actual network denial lives in the tool-runtime's egress broker.
//
// # Purity
//
// Contract-only: interfaces, the decision/mode/rule types, and doc comments. It
// imports the orchestrator domain (for the persisted decision vocabulary and tool
// classification) and the standard library, nothing from gen/.
package policy

import (
	"context"

	"github.com/boltrope/boltrope/internal/orchestrator/domain"
)

// Decision is the outcome of evaluating the permission pipeline for a single tool
// dispatch.
type Decision string

const (
	// Allow permits the action without prompting. It is produced by a matching
	// allow rule or by a mode that auto-resolves to allow (e.g. [ModeAcceptEdits],
	// [ModeBypass]).
	Allow Decision = "allow"
	// Deny forbids the action unconditionally. It is produced only by a matching
	// deny rule and CANNOT be overturned by any allow rule or mode (architecture
	// §8.13: "deny always wins unconditionally").
	Deny Decision = "deny"
	// Ask requires human-in-the-loop approval via the
	// [github.com/boltrope/boltrope/internal/orchestrator/app.ApprovalGate]. It is
	// the default fallthrough for an action that is neither denied, mode-resolved,
	// nor explicitly allowed, and is also the result of a taint-gate escalation of
	// an external-comms action (architecture §8.4).
	Ask Decision = "ask"
)

// ToPersisted maps a live [Decision] to the domain's persisted
// [domain.PermissionDecision] vocabulary recorded on a
// [domain.PermissionDecided] event. The two are kept as separate types so the
// domain stays free of the policy package; this is the one-way bridge.
func (d Decision) ToPersisted() domain.PermissionDecision {
	switch d {
	case Allow:
		return domain.PermissionAllow
	case Deny:
		return domain.PermissionDeny
	default:
		return domain.PermissionAsk
	}
}

// Mode is the operating mode that influences the middle of the pipeline (step 2 in
// the package doc). A mode can auto-resolve or tighten decisions but can never
// overturn a [Deny] and never disables the non-bypassable infra controls
// (architecture §8.13).
type Mode string

const (
	// ModeDefault is the standard mode: deny rules, then allow rules, then ask for
	// the remainder. It introduces no auto-allow of its own.
	ModeDefault Mode = "default"
	// ModeAcceptEdits auto-allows file-edit/write tools (so routine edits proceed
	// without prompting) while leaving other ask-tier actions to the normal
	// pipeline. It still cannot overturn a deny.
	ModeAcceptEdits Mode = "accept_edits"
	// ModePlan is a read-only planning mode: mutating tools ([domain.SideEffectMutating])
	// are not permitted to execute (planning only), so a mutating action is denied
	// or held regardless of allow rules. External-comms tools remain subject to the
	// egress/taint gate.
	ModePlan Mode = "plan"
	// ModeBypass collapses ONLY the allow/ask stages into allow for an operator who
	// explicitly enabled it. It is operator-only, server-side, and audited; it is
	// forbidden when untrusted content is present or in multi-tenant mode, and is
	// never settable by the client request or by the model/hooks. Even under
	// bypass, deny rules still win and the infra controls (egress denial, tenant
	// isolation) remain non-bypassable (ADR-0013 §"Constrained bypass";
	// architecture §8.13).
	ModeBypass Mode = "bypass"
)

// RuleEffect is the effect a [Rule] asserts when it matches: contribute to the deny
// set or the allow set. (There is no "ask rule": ask is the default fallthrough and
// the taint-gate escalation, not a rule effect.)
type RuleEffect string

const (
	// EffectDeny marks a rule that, when matched, denies the action unconditionally
	// (evaluated first; never overridden).
	EffectDeny RuleEffect = "deny"
	// EffectAllow marks a rule that, when matched, allows the action (evaluated
	// after deny and mode).
	EffectAllow RuleEffect = "allow"
)

// Rule is a single permission rule matched against a tool dispatch. Matching is by
// tool name (optionally with an argument predicate the engine evaluates); the
// concrete matcher implementation is supplied by the engine, while this type fixes
// the rule's identity and effect for configuration and for the audited RuleID on a
// decision.
type Rule struct {
	// ID is a stable identifier for the rule, recorded as
	// [domain.PermissionDecided.RuleID] for audit when the rule decides an action.
	ID string
	// Effect is whether the rule denies or allows on match.
	Effect RuleEffect
	// ToolName is the tool the rule applies to. An empty ToolName matches any tool
	// (a catch-all), subject to [Rule.ArgMatch].
	ToolName string
	// ArgMatch is an optional, opaque argument-matching expression the engine
	// interprets (e.g. a path glob for an edit tool, a host pattern for a fetch
	// tool). Empty means the rule matches regardless of arguments. Its grammar is
	// owned by the engine implementation; it is carried here so rules are
	// data-driven and configurable.
	ArgMatch string
}

// RuleSet is the ordered collection of rules an engine evaluates. Deny rules are
// always consulted before allow rules regardless of their position in the set (the
// pipeline ordering is structural, not positional); within an effect, the engine's
// documented precedence applies.
type RuleSet struct {
	// Rules are the configured permission rules.
	Rules []Rule
}

// Input is everything the [PolicyEngine] needs to decide a single tool dispatch.
type Input struct {
	// SessionID is the session the dispatch belongs to.
	SessionID string
	// CallID is the [github.com/boltrope/boltrope/internal/platform/llm.ToolCall]
	// id of the dispatch.
	CallID string
	// ToolName is the tool being dispatched.
	ToolName string
	// ToolArgs is the parsed tool arguments, for argument-predicate matching and
	// for extracting the egress target host of an external-comms tool.
	ToolArgs map[string]any
	// SideEffect is the tool's mutation classification (drives [ModePlan] and is
	// recorded for scheduling; architecture §9.2).
	SideEffect domain.SideEffect
	// EgressClass is the tool's external-communication classification; an
	// [domain.EgressClassExternal] tool is subject to the egress allowlist and the
	// taint gate (architecture §8.4).
	EgressClass domain.EgressClass
	// Mode is the current operating mode (step 2 of the pipeline).
	Mode Mode
	// Tainted reports whether untrusted content has entered the session's context.
	// When true, an external-comms action targeting a non-allowlisted host is
	// escalated to [Ask] for the rest of the turn/session (the taint gate;
	// ADR-0013; architecture §8.4).
	Tainted bool
}

// Result is the outcome of a [PolicyEngine.Evaluate] call: the [Decision] plus the
// matched rule (or mode) that produced it and a human-readable reason, suitable for
// raising an
// [github.com/boltrope/boltrope/internal/orchestrator/app.ApprovalRequest] on [Ask]
// and for recording a [domain.PermissionDecided] event.
type Result struct {
	// Decision is the pipeline outcome.
	Decision Decision
	// RuleID is the id of the rule that decided the action, or a sentinel for a
	// mode/taint-driven decision (e.g. the mode name). Empty when the decision was
	// the bare ask fallthrough.
	RuleID string
	// Reason is a short, human-readable explanation, surfaced in the approval
	// request and audit event (e.g. "mutating tool requires approval",
	// "external-comms blocked: untrusted content present").
	Reason string
}

// PolicyEngine evaluates the permission pipeline for a single tool dispatch and
// returns the [Decision]. It is called once per dispatch, synchronously, by the
// loop before a tool is executed (architecture §3, §8.13). Implementations MUST
// honor the ordering in the package doc: deny first and unconditional, then mode,
// then allow, then ask as the default — with the taint gate able only to escalate
// an external-comms action to ask, never to loosen. Implementations must be safe
// for concurrent use.
type PolicyEngine interface {
	// Evaluate applies the deny→mode→allow→ask pipeline to in against the engine's
	// configured [RuleSet] and returns the [Result]. It never performs the action
	// or any I/O beyond pure evaluation; raising the human ask and enforcing egress
	// are the caller's and the infra controls' jobs respectively.
	Evaluate(ctx context.Context, in Input) (Result, error)
}
