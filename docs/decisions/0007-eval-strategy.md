# 7. Evaluation strategy

Date: 2026-06-10
Status: Accepted

## Context

Evals are distinct from unit tests: they measure whether the *harness as a whole*
behaves correctly and whether each feature actually helps. SWE-agent showed that
harness/tool (ACI) design alone moves benchmark scores materially. Full SWE-bench is
heavy (containerized real-repo issue→patch→test) and slow for an MVP's CI.

## Decision

- Build a **bespoke, deterministic eval harness** for v1, runnable in CI:
  - **Golden-scenario evals** drive the full agent loop against a **scripted/fake
    Provider** (and a fake clock) so outcomes are deterministic: assert correct tool
    selection, termination subtype, turn/budget caps, event-log shape, and
    compaction/permission behavior — no network, no flakiness.
  - **Live smoke evals** (small) run the loop against real providers, **gated by the
    presence of API keys** (skipped otherwise), to catch adapter drift.
- Wire the deterministic suite into CI as a required gate; the live suite is
  opt-in/manual.
- **Defer** full SWE-bench / SWE-bench-Lite integration to post-v1 as an external,
  optional benchmark target.

## Consequences

- ✅ Fast, deterministic, network-free CI gate that exercises real harness behavior.
- ✅ Adapter regressions caught by opt-in live smokes without making CI flaky.
- ⚠️ Bespoke scenarios are less authoritative than SWE-bench; documented as a v1
  limitation with SWE-bench on the roadmap.
