// Package policytest provides a deterministic fake [policy.PolicyEngine] for
// tests. It returns scripted [policy.Result] values in queue order so unit
// tests can assert the loop's permission-pipeline handling without a real
// engine implementation.
package policytest

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/xd1lab/harness-ai/internal/orchestrator/policy"
)

// Compile-time assertion that FakePolicyEngine satisfies policy.PolicyEngine.
var _ policy.PolicyEngine = (*FakePolicyEngine)(nil)

// EvaluateCall records one call to FakePolicyEngine.Evaluate for inspection.
type EvaluateCall struct {
	In policy.Input
}

// FakePolicyEngine is a scriptable [policy.PolicyEngine]. Scripted results are
// consumed in queue order from the scripted queue. If the queue is exhausted,
// the engine returns [policy.Allow] with no rule id (permissive default).
type FakePolicyEngine struct {
	mu      sync.Mutex
	calls   []EvaluateCall
	results []policy.Result
	errs    []error
	idx     atomic.Int64
}

// NewFakePolicyEngine returns an empty FakePolicyEngine that defaults to Allow
// for any unscripted call.
func NewFakePolicyEngine() *FakePolicyEngine { return &FakePolicyEngine{} }

// AddResult enqueues one scripted result returned by the next Evaluate call.
func (f *FakePolicyEngine) AddResult(r policy.Result, err error) {
	f.mu.Lock()
	f.results = append(f.results, r)
	f.errs = append(f.errs, err)
	f.mu.Unlock()
}

// AddAllow enqueues an Allow decision with the given ruleID and reason.
func (f *FakePolicyEngine) AddAllow(ruleID, reason string) {
	f.AddResult(policy.Result{Decision: policy.Allow, RuleID: ruleID, Reason: reason}, nil)
}

// AddDeny enqueues a Deny decision with the given ruleID and reason.
func (f *FakePolicyEngine) AddDeny(ruleID, reason string) {
	f.AddResult(policy.Result{Decision: policy.Deny, RuleID: ruleID, Reason: reason}, nil)
}

// AddAsk enqueues an Ask decision with the given ruleID and reason.
func (f *FakePolicyEngine) AddAsk(ruleID, reason string) {
	f.AddResult(policy.Result{Decision: policy.Ask, RuleID: ruleID, Reason: reason}, nil)
}

// Evaluate records the input and returns the next scripted result, or
// {Allow} if the queue is exhausted.
func (f *FakePolicyEngine) Evaluate(_ context.Context, in policy.Input) (policy.Result, error) {
	f.mu.Lock()
	f.calls = append(f.calls, EvaluateCall{In: in})
	f.mu.Unlock()
	idx := int(f.idx.Add(1) - 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if idx >= len(f.results) {
		return policy.Result{Decision: policy.Allow}, nil
	}
	return f.results[idx], f.errs[idx]
}

// Calls returns a snapshot of all Evaluate inputs recorded.
func (f *FakePolicyEngine) Calls() []EvaluateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]EvaluateCall(nil), f.calls...)
}

// MustHaveCall panics with a descriptive message if the number of recorded
// calls does not equal n. Useful for quick assertions in tests.
func (f *FakePolicyEngine) MustHaveCall(t interface{ Fatalf(string, ...any) }, n int) {
	calls := f.Calls()
	if len(calls) != n {
		t.Fatalf("policytest: expected %d Evaluate call(s), got %d (calls: %v)", n, len(calls), calls)
	}
}

// LastInput returns the input of the last Evaluate call, or panics if no calls
// have been made.
func (f *FakePolicyEngine) LastInput() policy.Input {
	calls := f.Calls()
	if len(calls) == 0 {
		panic("policytest.FakePolicyEngine: no calls recorded")
	}
	return calls[len(calls)-1].In
}
