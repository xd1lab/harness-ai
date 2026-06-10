# Agentic Coding Harness — Research Report

> **Status:** Foundational research synthesis. This document drives the system specification for an open-source **Go + PostgreSQL** agentic coding harness (backend microservices; no frontend yet).
> **Date:** 2026-06-10
> **Scope:** Core feature taxonomy, reference-OSS survey, multi-LLM provider abstraction, OSS engineering best practices, and risks/open questions.
> **Method:** Synthesis of four research streams plus 20 independent fact-checks. Refuted/corrected claims have been adjusted in-line; see [Fact-Check Corrections Applied](#fact-check-corrections-applied).

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [What Is an Agent Harness](#what-is-an-agent-harness)
3. [Necessary Feature Taxonomy (MUST / SHOULD / COULD)](#necessary-feature-taxonomy-must--should--could)
4. [Survey of Reference OSS Projects](#survey-of-reference-oss-projects)
5. [Multi-LLM Provider Abstraction](#multi-llm-provider-abstraction)
6. [OSS Best-Practices Checklist](#oss-best-practices-checklist-for-this-project)
7. [Risks & Open Questions](#risks--open-questions)
8. [Fact-Check Corrections Applied](#fact-check-corrections-applied)
9. [References](#references)

---

## Executive Summary

An **agentic coding harness** is the orchestration layer that wraps an LLM in an iterative reason–act loop, gives it tools, manages its finite context window, persists its state, and constrains it with permissions and sandboxing. Across the leading open-source implementations a strikingly **consistent architecture** has converged: a single-threaded ReAct-style agentic loop ("gather context → take action → verify work → repeat") that runs tool calls until the model emits no more, surrounded by ~7–9 supporting subsystems (model interface, tool registry, context/memory manager, planner, execution/sandbox engine, persistence, permissions/guardrails, sub-agent orchestration, observability) ([Claude Code agent-loop docs](https://code.claude.com/docs/en/agent-sdk/agent-loop); [Software Agent SDK paper, arXiv 2511.03690](https://arxiv.org/html/2511.03690v1); [MindStudio: agent harness architecture](https://www.mindstudio.ai/blog/what-is-agent-harness-architecture-explained)).

**Strategic context for this project.** None of the leading OSS harnesses surveyed are written in Go. Python dominates research/eval harnesses and frameworks (Aider, SWE-agent, OpenHands SDK, LangGraph, CrewAI, smolagents); TypeScript dominates IDE-embedded agents (Cline, Gemini CLI, current opencode); Rust is the choice for performance-sensitive standalone binaries (Codex CLI, Goose, Tabby). The single genuinely **Go**, design-worthy peer is **Charm's Crush** (`charmbracelet/crush`, ~98% Go, licensed FSL-1.1-MIT — *not* MIT) ([Crush repo](https://github.com/charmbracelet/crush)). **Note:** Goose is **Rust, not Go**, contrary to a common misconception — it is ~64% Rust / ~29% TypeScript, Apache-2.0, now under the Linux Foundation's Agentic AI Foundation ([block/goose](https://github.com/block/goose); [aaif-goose/goose](https://github.com/aaif-goose/goose)). A Go harness is therefore **greenfield**: an advantage for goroutine-based concurrency and single-binary distribution, but it means **porting patterns** (event sourcing, condensers, sandbox profiles, ACI tool design) rather than reusing libraries.

**Top-line recommendations:**

1. **Build a single-threaded gather–act–verify loop with a flat history first.** Claude Code's production bet is that a simple, transparent loop beats an opaque multi-agent system for reliability; sub-agents can be added later as ordinary tools (the OpenHands pattern) without re-architecting ([Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop); [arXiv 2511.03690](https://arxiv.org/html/2511.03690v1)).
2. **Make an append-only, event-sourced log the single source of truth from day one.** Resume, fork, time-travel, crash recovery, and observability all fall out of it cheaply; retrofitting persistence is expensive ([arXiv 2511.03690](https://arxiv.org/html/2511.03690v1); [LangGraph checkpointing](https://callsphere.ai/blog/langgraph-checkpointing-persistence-time-travel-agent-workflows)). PostgreSQL is a natural backing store for this log.
3. **Treat context as a finite budget.** Implement token accounting + automatic compaction + prompt-caching of stable prefixes as MUST-have MVP features; add tool-result clearing and just-in-time retrieval next ([Anthropic: effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
4. **Adopt a uniform Action → Execute → Observation contract** that native, custom, and MCP tools all flow through, with JSON-Schema input validation **before** execution. This makes adding MCP/remote tools a translation problem, not a new code path ([arXiv 2511.03690](https://arxiv.org/html/2511.03690v1); [Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
5. **Layer permissions** (deny → mode → allow → tool check) with a human-in-the-loop approval callback, and explicitly design against the **"lethal trifecta"** (private-data access + untrusted content + external communication) ([awesome-harness-engineering](https://github.com/ai-boost/awesome-harness-engineering); [Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)).
6. **Decouple isolation behind a Workspace/Runtime abstraction** (OpenHands' "local-first, deploy-anywhere"): ship containerized isolation for MVP, keep the interface able to swap in OS-level sandboxes (Seatbelt/Landlock/seccomp) and microVMs (Firecracker/gVisor) later ([OpenHands runtime](https://docs.openhands.dev/openhands/usage/architecture/runtime); [Codex sandbox investigation](https://agent-safehouse.dev/docs/agent-investigations/codex)).
7. **Get reliability primitives right early:** cooperative cancellation, streaming, and retries that **honor `Retry-After`** before exponential backoff with full jitter (retry only 429/5xx/529, never 4xx) ([clawpulse rate-limiting](https://www.clawpulse.org/blog/llm-api-rate-limiting-best-practices-avoid-429-errors-and-save-40-on-costs); [iotools: 429 survival](https://iotools.cloud/journal/api-rate-limiting-headers-exponential-backoff-and-surviving-the-429/)).
8. **Instrument with OpenTelemetry GenAI semantic conventions** and stand up a **SWE-bench-style eval harness** wired to CI before piling on features — SWE-agent showed harness/ACI design alone moves benchmark scores (12.5% vs a 3.8% RAG baseline) ([OTel GenAI](https://opentelemetry.io/blog/2026/genai-observability/); [SWE-agent NeurIPS 2024](https://arxiv.org/abs/2405.15793)).
9. **Choose Apache-2.0** (explicit patent grant + retaliation clause) over MIT for backend infrastructure others build on, with a NOTICE file, SPDX headers, and DCO sign-off ([FOSSA: Apache-2.0](https://fossa.com/blog/open-source-licenses-101-apache-license-2-0/); [Apache GPL-compatibility](https://www.apache.org/licenses/GPL-compatibility.html)).

---

## What Is an Agent Harness

An agent harness is **everything around the model**: it is *not* the model and *not* a single prompt, but the runtime that turns a stateless text-completion API into a stateful, tool-using, self-correcting agent ([MindStudio](https://www.mindstudio.ai/blog/what-is-agent-harness-architecture-explained); [arXiv 2511.03690](https://arxiv.org/html/2511.03690v1)).

### The core loop

Every surveyed harness implements the same **ReAct-style agentic loop**. The model receives `prompt + system prompt + tool definitions + history`, then either emits text, requests one or more tool calls, or both; the harness executes the tools, feeds the structured results back, and repeats. Claude Code frames this as four phases — **gather context → take action → verify work → repeat** ([Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)).

- A **"turn"** is one full round-trip (model output containing tool calls + their execution + results fed back). Turns continue **without yielding to the caller** until the model produces a text-only response ([Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)).
- **Termination is multi-pronged and must surface a typed result:** natural completion (no tool calls / `stop_reason = end_turn`), a max-turns cap (counting tool-use turns only), a max-budget-USD cap, error/cancellation, structured-output-retry exhaustion, plus loop/"doom-loop"/stuck detection as guardrails. The Claude Code/Agent SDK result subtypes are `success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, and `error_max_structured_output_retries`; the typed result also carries final text, token usage, cost, `num_turns`, and a session id ([Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)).
- Claude Code's explicit design bet: a **single-threaded loop with a flat history** is more valuable in production than an opaque multi-agent system ([Miles K: system design of Claude Code](https://medium.com/@milesk_33/the-system-design-of-claude-code-agent-explained-318d17496534)).

### The supporting subsystems

Around the loop sit roughly seven to nine subsystems that recur across implementations ([arXiv 2511.03690](https://arxiv.org/html/2511.03690v1); [MindStudio](https://www.mindstudio.ai/blog/what-is-agent-harness-architecture-explained)):

| Subsystem | Responsibility |
|---|---|
| **Model interface** | Provider-portable adapter: streaming, tool calls, token counting, capability flags. |
| **Tool registry** | Name + description + JSON-Schema input → execution logic; validate-then-execute; structured observations. |
| **Context / memory manager** | Token accounting, compaction, tool-result clearing, external memory, just-in-time retrieval, prompt caching. |
| **Planner** | Plan-then-execute / task tracking; may be implicit in the loop. |
| **Execution / sandbox engine** | Runs shell/code in an isolated workspace (container → OS-sandbox → microVM). |
| **Persistence** | Append-only event log → resume / fork / replay. |
| **Permissions / guardrails** | Ordered allow/deny/ask pipeline, risk tiers, human-in-the-loop gates, secret masking. |
| **Sub-agent orchestration** | Spawn fresh-context workers that return condensed summaries. |
| **Observability** | Tracing (OTel GenAI), structured logging/hooks, token + cost accounting, evals. |

### Why context engineering is the central discipline

Anthropic's thesis is that **context is a finite resource with diminishing returns** because of *context rot*: recall degrades as tokens accumulate, since transformer attention budget is limited and each token adds n² pairwise relations. The goal is "the smallest set of high-signal tokens per inference step." Stable content (system prompt, tool defs, `CLAUDE.md`/`AGENTS.md`) should be **prompt-cached** so only the first request pays full cost ([Anthropic: effective context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).

Four named long-horizon techniques recur:

1. **Compaction** — summarize older history near the limit and reinitiate from the summary; balance recall vs precision. Claude Code uses a multi-layer pipeline (microcompact → context collapse → auto-compact) and emits a `compact_boundary` event; a `PreCompact` hook can archive the full transcript first ([Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents); [Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop)). OpenHands' `Condenser` (default `LLMSummarizingCondenser`) replaces old events with `CondensationEvent` summaries, reporting up to ~2× cost reduction ([arXiv 2511.03690](https://arxiv.org/html/2511.03690v1)).
2. **Tool-result clearing** — drop stale tool outputs once stored; the lightest-touch, safest form of compaction ([Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
3. **Structured note-taking / external memory** — the agent writes notes/state to files outside the window and reloads on demand (Anthropic shipped a beta **memory tool**) ([Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
4. **Just-in-time retrieval** — keep lightweight identifiers (paths, queries, links) and load data at runtime via tools rather than pre-stuffing; a hybrid (some upfront + autonomous exploration) works best ([Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).

Codebase-specific indexing remains relevant: **Aider** builds a tree-sitter "repo map" of def/ref tags and ranks symbols with **PageRank** over a file-dependency graph (literally `networkx.pagerank` in `aider/repomap.py`) to select the most relevant context ([Aider repomap docs](https://aider.chat/docs/repomap.html); [Aider 2023 repomap post](https://aider.chat/2023/10/22/repomap.html)).

### State: event sourcing enables resume, fork, replay

The dominant pattern is an **append-only, event-sourced log** as the single source of truth. OpenHands' `EventLog` records typed events (`MessageEvent`, `ActionEvent`, `ObservationEvent`, `AgentErrorEvent`, `CondensationEvent`, internal state updates); LLM-convertible events are visible to the model while internal events are hidden, and a conversation **resumes by loading base state and replaying events** ([arXiv 2511.03690](https://arxiv.org/html/2511.03690v1)). Codex-rs uses an async **submit/event** model: clients submit `Op`s (`UserTurn`, `Interrupt`, `Shutdown`) and the core streams `EventMsg`s (`TurnStarted`, `AgentMessageDelta`, `ExecCommandBegin/End`, `PatchApplied`, `TokensUsed`, `TurnFinished`), fully decoupling rendering from the loop ([codex-rs architecture](https://codex.danielvaughan.com/2026/03/28/codex-rs-rust-rewrite-architecture/)). LangGraph generalizes this with a **checkpointer** that snapshots full graph state each super-step into threads, giving crash recovery, cross-session memory, and **time travel** — resuming from a chosen checkpoint with modified state creates a **new branch (a fork, not a rewrite)** ([LangGraph checkpointing](https://callsphere.ai/blog/langgraph-checkpointing-persistence-time-travel-agent-workflows)).

---

## Necessary Feature Taxonomy (MUST / SHOULD / COULD)

Prioritized for an MVP→mature roadmap. Synthesized primarily from the Claude Code agent loop, the OpenHands Software Agent SDK paper, the awesome-harness-engineering taxonomy, and Anthropic's context-engineering guidance ([Claude Code agent-loop](https://code.claude.com/docs/en/agent-sdk/agent-loop); [arXiv 2511.03690](https://arxiv.org/html/2511.03690v1); [awesome-harness-engineering](https://github.com/ai-boost/awesome-harness-engineering); [Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).

| Priority | Feature | Rationale / Notes |
|---|---|---|
| **MUST** | **Agentic loop** — model call → tool dispatch → feed results → repeat; turns; max-turns + max-budget caps; typed termination subtypes | The irreducible core of every harness. Result subtypes per Claude Code: `success`, `error_max_turns`, `error_max_budget_usd`, `error_during_execution`, `error_max_structured_output_retries`. |
| **MUST** | **Model interface abstraction** — provider-portable; pin model ids; honor streaming | Decouples the loop from any single API; see [Multi-LLM](#multi-llm-provider-abstraction). |
| **MUST** | **Tool registry** — JSON-Schema validation, Action→Execute→Observation, error-as-observation, core tools (Read/Edit/Write, Glob/Grep, Bash, web fetch/search) | Validate input **before** executing (OpenHands uses Pydantic). Tools must be token-efficient, self-contained, minimally overlapping, clearly named. |
| **MUST** | **Context accounting + automatic compaction + prompt caching** of stable prefixes | Highest-leverage, lowest-cost win per Anthropic. Treat tokens as finite. |
| **MUST** | **Session persistence** — append-only event log with resume | Foundation for fork/replay/recovery/observability. PostgreSQL-backed for this project. |
| **MUST** | **Permissions** — allow/deny/ask rules + ≥ default/acceptEdits/plan/bypass modes + human-in-the-loop approval callback | Layered, ordered pipeline; deny wins regardless. |
| **MUST** | **Sandboxed execution** — start with containerized/workspace isolation; deny-by-default network | Never give an autonomous loop raw unsandboxed shell. |
| **MUST** | **Reliability primitives** — streaming output, cooperative cancellation, retries (honor `Retry-After` → backoff + jitter), basic rate limiting | Only retry 429/500/502/503/504/529; never 4xx. |
| **MUST** | **Token + cost accounting** on every result; structured logging | Every result carries `total_cost_usd`, `usage`, `num_turns`. |
| **SHOULD** | **Tool-result clearing + just-in-time retrieval** | Next-cheapest context wins after compaction. |
| **SHOULD** | **External/structured memory** (notes tool) + project context files (`AGENTS.md`/`CLAUDE.md`) | Long-horizon state outside the window. |
| **SHOULD** | **Session fork + replay / time-travel** | Falls out of the event log nearly for free. |
| **SHOULD** | **Sub-agents** with fresh context returning condensed summaries (~1–2k tokens) | Implemented as ordinary tools (OpenHands); keep shallow with a depth limit (Claude Code). |
| **SHOULD** | **MCP CLIENT support** with **deferred/lazy** schema loading | De-facto integration standard; defer schemas to protect context budget. |
| **SHOULD** | **OS-level sandbox option** (Seatbelt/Landlock/seccomp) and/or **microVM** backend | For untrusted/multi-tenant code. |
| **SHOULD** | **Risk-tiered approval** (LLM risk classifier) + **secret registry** with output masking | OpenHands `SecurityAnalyzer` + `ConfirmationPolicy` model. |
| **SHOULD** | **Hooks / middleware pipeline** (PreToolUse/PostToolUse/Stop/PreCompact) | Run in host process, not model context; `PreToolUse` can block a call. |
| **SHOULD** | **OpenTelemetry GenAI tracing** | Agent/workflow/tool/model spans + token & latency metrics. |
| **SHOULD** | **Eval harness** (SWE-bench-style) wired to CI | Measures whether each feature actually helps. |
| **SHOULD** | **Codebase indexing** (tree-sitter repo map / semantic search) | Whole-repo awareness without dumping files. |
| **SHOULD** | **Parallel read-only tool execution** | Run read-only tools concurrently; serialize state-mutating ones. A natural fit for Go goroutines. |
| **COULD** | **MCP SERVER mode** + **A2A** interoperability | Expose the harness to IDEs/other agents. |
| **COULD** | **Multi-agent topologies** beyond shallow sub-agents (routers, handoffs, dynamic topology) | Orchestrator-worker, plan-then-execute, hierarchical. |
| **COULD** | **Snapshot/restore microVMs** for fast fork | Firecracker 5–30 ms snapshot-restore. |
| **COULD** | **Model routing** (cheap vs multimodal vs reasoning) + effort-level control | A `RouterLLM` routes by content. |
| **COULD** | **Non-native function-calling fallback** + constrained decoding (regex/CFG/JSON-Schema) | For weaker/local models (OpenHands `NonNativeToolCallingMixin`). |
| **COULD** | **Interactive workspace access** (embedded VS Code / VNC / browser) | For debugging. |
| **COULD** | **Virtual filesystem mounts** (S3/GitHub/Slack) as context | "Deploy-anywhere" context sources. |
| **COULD** | **Self-evolving / validation-gated skills**; advanced doom-loop detection | Differentiators. |

---

## Survey of Reference OSS Projects

> **Verification caveat:** Secondary sources frequently get language/license facts wrong. This survey reflects fact-checked verdicts: **Goose is Rust (not Go)**, **Cline is Apache-2.0 (not MIT)**, **Tabby is Rust (not Go)**, **Crush is FSL-1.1-MIT (not plain MIT)**, and **Codex CLI was rewritten TypeScript → Rust**. Always confirm from a repo's own LICENSE file and GitHub language sidebar before depending on it.

| Name | Language | License | Takeaway (borrowable idea) |
|---|---|---|---|
| **OpenHands** (`OpenHands/OpenHands`; SDK `OpenHands/software-agent-sdk`) | Python (~63%; ~35% TS web UI) | MIT (except `enterprise/`) | **Event-sourced agent state with deterministic replay**; Workspace abstraction (local→Docker→remote) decouples isolation from agent code. ([repo](https://github.com/OpenHands/OpenHands); [SDK paper](https://arxiv.org/abs/2511.03690)) |
| **OpenAI Codex CLI** (`openai/codex`, `codex-rs`) | Rust (~96%) — rewritten from TypeScript/Node | Apache-2.0 | **Async submit/event protocol + publishable reusable core crate** cleanly separates UI from agent loop; OS-native sandboxing applied to the whole process tree. ([repo](https://github.com/openai/codex); [architecture](https://codex.danielvaughan.com/2026/03/28/codex-rs-rust-rewrite-architecture/)) |
| **Crush** (`charmbracelet/crush`) | **Go** (~98%) | **FSL-1.1-MIT** (2-yr MIT fallback) | **The Go peer.** Single static binary, broadest cross-platform support; **LSP servers as a context source** (gopls etc.); MCP over stdio/http/sse. ([repo](https://github.com/charmbracelet/crush)) |
| **opencode** (`sst/opencode`) | TypeScript/Bun (TUI now OpenTUI; *original* TUI was Go/Bubble Tea) | MIT | **Persistent-server + thin-client** model: one long-lived agent server, many attachable clients, sessions outlive any connection. Go lineage now lives in Crush. ([repo](https://github.com/sst/opencode)) |
| **Goose** (`block/goose` → `aaif-goose/goose`) | **Rust** (~64%; ~29% TS Electron UI) — **NOT Go** | Apache-2.0 | **"Recipes"** — reusable, parameterized workflow templates that turn ad-hoc sessions into shareable automations; 70+ MCP extensions. Now under LF's AAIF. ([repo](https://github.com/block/goose)) |
| **Aider** (`Aider-AI/aider`) | Python (~80%) | Apache-2.0 | **Tree-sitter repo map + PageRank** symbol ranking (the most-copied context technique); **auto-commits to git** appending `(aider)` to author. ([repo](https://github.com/Aider-AI/aider); [repomap](https://aider.chat/docs/repomap.html)) |
| **Cline** (`cline/cline`) | TypeScript (~98%) | **Apache-2.0** (not MIT) | **Plan/Act mode separation** — force an approved plan before file mutation; reusable agent-core SDK powers CLI + IDE + Kanban board. ([repo](https://github.com/cline/cline)) |
| **SWE-agent** (`SWE-agent/SWE-agent`) | Python (~95%) | MIT | **Agent-Computer Interface (ACI)** — design the tool surface *for the model*, not raw bash; `SWEEnv` is a thin wrapper over `SWE-ReX`. ([repo](https://github.com/SWE-agent/SWE-agent); [NeurIPS 2024](https://arxiv.org/abs/2405.15793)) |
| **SWE-bench** (`SWE-bench/SWE-bench`) | Python | MIT | **Containerized, test-based eval harness** (issue → patch → run repo tests) for measuring harness quality. ([repo](https://github.com/SWE-bench/SWE-bench)) |
| **Gemini CLI** (`google-gemini/gemini-cli`) | TypeScript (npm) | Apache-2.0 | Tidy **npm-workspaces split** (`cli`/`core`/`sdk`/`a2a-server`/`vscode-ide-companion`). **Cautionary tale:** free model access was gated after soliciting 6,000 community PRs. ([repo](https://github.com/google-gemini/gemini-cli)) |
| **smolagents** (`huggingface/smolagents`) | Python (<1k LOC core) | Apache-2.0 | **Code-as-action** — `CodeAgent` emits executable Python to orchestrate tools (collapses multi-step plans); mandates a hardened sandbox (Docker/E2B/Modal/WASM). ([repo](https://github.com/huggingface/smolagents)) |
| **gptme** (`gptme/gptme`) | Python | MIT (confirm via repo) | **Name-triggered "Skills" + keyword-triggered "Lessons"** — lightweight, file-based on-demand instruction injection; ACP support (Zed/JetBrains). ([repo](https://github.com/gptme/gptme)) |
| **Tabby** (`TabbyML/tabby`) | **Rust** (also Python/C++) — **NOT Go** | Confirm via repo LICENSE | **Pluggable RAG "Context Provider" interface** + fully self-hostable, OpenAI-API-compatible serving for air-gapped/on-prem use. ([repo](https://github.com/TabbyML/tabby)) |
| **LangGraph** (`langchain-ai/langgraph`) | Python (+ JS) | MIT | **Durable execution + checkpoint/resume + human-in-the-loop state editing** — the gold standard for reliability; deliberately does *not* abstract prompts. ([repo](https://github.com/langchain-ai/langgraph)) |
| **AutoGen** (`microsoft/autogen`) | Python + .NET | MIT (code) + CC-BY-4.0 (docs) | Event-driven multi-agent message passing. **Now in maintenance mode** — superseded by the merged Microsoft Agent Framework. ([repo](https://github.com/microsoft/autogen)) |
| **CrewAI** (`crewAIInc/crewAI`) | Python | MIT | **Crews-vs-Flows split** mirrors the autonomy-vs-determinism tradeoff a harness must expose; standalone (not built on LangChain). ([repo](https://github.com/crewAIInc/crewAI)) |
| **Eliza** (`elizaOS/eliza`) | TypeScript | MIT | Web3-leaning "agentic OS": `AgentRuntime` + plugin loader + many social connectors. ([repo](https://github.com/elizaOS/eliza)) |
| **Continue** (`continuedev/continue`) | TypeScript (~84%) | Apache-2.0 | IDE autocomplete/chat. **Cautionary note:** flagship monorepo archived/read-only. ([repo](https://github.com/continuedev/continue)) |

### Cross-cutting architectural patterns worth adopting

1. **Layered "reusable core + thin clients."** Codex (`codex-core`), Cline (`sdk/`), opencode (server), Goose (`goose` crate), and OpenHands (`openhands.sdk`) all isolate agent logic into an embeddable library/server and ship CLI/IDE/desktop/CI as interchangeable frontends. **For Go:** a `core` package + `cmd/<app>` entry points ([codex-rs architecture](https://codex.danielvaughan.com/2026/03/28/codex-rs-rust-rewrite-architecture/); [arXiv 2511.03690](https://arxiv.org/abs/2511.03690)).
2. **Event-sourced / streaming loop decoupled from UI** (Codex submit/event; OpenHands immutable event log) → deterministic replay, resumable/forkable sessions, testable logic.
3. **Sandboxed tool execution is mandatory** — container-based (OpenHands/SWE-bench/smolagents) or OS-native (Codex Seatbelt/Landlock). Never expose raw shell to an autonomous loop.
4. **MCP as the universal tool/extension bus** — Goose, Crush, Cline, OpenHands, CrewAI, gptme, smolagents all standardize on it, so tools are portable across harnesses.
5. **Whole-repo context via tree-sitter repo maps (Aider) or LSP (Crush)** instead of naive file dumps.
6. **Purpose-built model-facing tool surface (SWE-agent ACI)** beats exposing raw bash.
7. **Persistent sessions that survive client disconnects** (opencode, Crush, Goose).

### Cross-cutting pitfalls and governance risks

- **Governance/licensing instability is rampant:** the opencode↔Crush ownership fork; Gemini CLI gating free model access after 6,000 community PRs; Continue archiving its monorepo; AutoGen entering maintenance mode; Goose relocating to the Linux Foundation. **Choose a license + governance model deliberately, and don't couple the harness to one proprietary model backend** ([Charm/Crush coverage](https://biggo.com/news/202507310715_Charm_Crush_AI_Coding_Agent); [Gemini CLI gating](https://www.techtimes.com/articles/317056/20260523/google-accepted-6000-gemini-cli-contributions-then-closed-tool-enterprise-only.htm)).
- **Non-standard "fair-source" licenses can surprise users:** Crush ships FSL-1.1-MIT (2-year delayed MIT), not OSI-approved on day one ([Crush repo](https://github.com/charmbracelet/crush)).
- **OS-native sandboxes are OS-specific** (Codex's Windows path differs from macOS/Linux), so cross-platform parity is hard.
- **Heavy runtime dependencies create install friction** — Codex's Node v22 requirement was a stated motivation for the Rust rewrite; a single static Go/Rust binary sidesteps this ([InfoQ: Codex Rust rewrite](https://www.infoq.com/news/2025/06/codex-cli-rust-native-rewrite/)).

---

## Multi-LLM Provider Abstraction

Four provider families must be supported, and they **differ on every axis**: system-prompt placement, tool-call wrappers, stop-reason vocabularies, streaming shapes, and token counting. The strategy is a **normalized internal model + a small `Provider` interface + capability flags + one adapter per provider**, with self-hosted servers plugged in as a configured OpenAI-compatible adapter.

### 1. System-prompt handling (structurally different)

| Family | Where the system prompt goes |
|---|---|
| **Anthropic** | Top-level `system` parameter (string or array of text blocks with `cache_control`); **there is no `"system"` role** for messages — valid roles are only `user`/`assistant`. ([Messages API](https://platform.claude.com/docs/en/api/messages)) |
| **Google Gemini** | `systemInstruction` field in `GenerateContentConfig` (separate from `contents`). ([function-calling docs](https://ai.google.dev/gemini-api/docs/function-calling)) |
| **OpenAI Chat Completions** | A `{role: "system"\|"developer", content}` message inside `messages`. ([migrate-to-responses](https://developers.openai.com/api/docs/guides/migrate-to-responses)) |
| **OpenAI Responses** | Top-level `instructions` field (or a system/developer-role item). ([migrate-to-responses](https://developers.openai.com/api/docs/guides/migrate-to-responses)) |

### 2. Tool / function calling (same JSON-Schema core, four wrappers)

| Family | Tool definition | Model emits | You reply with |
|---|---|---|---|
| **Anthropic** | `{name, description, input_schema}` | `tool_use` block `{id, name, input}` (parsed object) | `tool_result` block `{tool_use_id, content, is_error?}` in a user message ([tool-use overview](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview)) |
| **Gemini** | `tools[].functionDeclarations[]` = `{name, description, parameters}` | `functionCall {id, name, args}` (parsed object) | `functionResponse {id, name, response}` ([function-calling](https://ai.google.dev/gemini-api/docs/function-calling)) |
| **OpenAI Chat Completions** | externally-tagged `{type:"function", function:{name, parameters}}` | `tool_calls[]` with `function.name` / `function.arguments` (**a JSON string — parse it**) | `role:"tool"` message keyed by `tool_call_id` ([function-calling](https://developers.openai.com/api/docs/guides/function-calling)) |
| **OpenAI Responses** | flat `{type:"function", name, parameters}` | typed `function_call` item | typed `function_call_output` item ([schema diff](https://medium.com/@laurentkubaski/openai-tool-schema-differences-between-the-response-api-and-the-chat-completion-api-8f99ce8a9371)) |

**Key gotchas:** OpenAI Chat Completions delivers tool arguments as a **serialized JSON string** (parse it); Anthropic and Gemini deliver parsed objects. All support an optional call `id` for parallel calls. `tool_choice` (auto/any/required/specific) exists everywhere but names differ.

### 3. Stop / finish reasons (disjoint vocabularies → normalize to one enum)

- **Anthropic `stop_reason`:** `end_turn`, `max_tokens`, `stop_sequence`, `tool_use`, `pause_turn`, `refusal` (non-exhaustive — `model_context_window_exceeded` also documented). `pause_turn` is returned when a server-side tool sampling loop hits its iteration limit; `refusal` returns as a normal HTTP 200 ([handling stop reasons](https://platform.claude.com/docs/en/build-with-claude/handling-stop-reasons)).
- **OpenAI Chat Completions `finish_reason`:** `stop`, `length`, `tool_calls`, `content_filter` (Responses uses typed output items + a `status`/incomplete reason instead) ([function-calling](https://developers.openai.com/api/docs/guides/function-calling)).
- **Gemini `finishReason`:** `STOP`, `MAX_TOKENS`, `SAFETY`, `RECITATION`, etc. ([tokens API](https://ai.google.dev/api/tokens)).

Map all to an internal set: `{Stop, MaxTokens, ToolUse, StopSequence, ContentFilter, Pause}`.

### 4. Streaming (SSE for hosted APIs; NDJSON for some self-hosted)

- **Anthropic:** named SSE events — `message_start`, `content_block_start`, `content_block_delta` (`text_delta` / `input_json_delta` [field `partial_json`] / `thinking_delta` / `signature_delta`), `content_block_stop`, `message_delta` (carries `stop_reason` **nested under `delta`** + cumulative `usage`), `message_stop` ([streaming](https://platform.claude.com/docs/en/build-with-claude/streaming)).
- **OpenAI:** SSE `chat.completion.chunk` deltas (Responses streams typed `response.*` events) terminated by `data: [DONE]` ([migrate-to-responses](https://developers.openai.com/api/docs/guides/migrate-to-responses)).
- **Gemini:** `streamGenerateContent` (SSE when `?alt=sse`) emitting partial `GenerateContentResponse` chunks ([tokens API](https://ai.google.dev/api/tokens)).
- **Ollama:** its **native** `/api/chat` streams **newline-delimited JSON (`application/x-ndjson`)**, not SSE — but its OpenAI-compatible `/v1/chat/completions` follows OpenAI SSE ([Ollama streaming](https://docs.ollama.com/api/streaming)).

The adapter layer must normalize all of these into one internal channel (`TextDelta` / `ThinkingDelta` / `ToolCallDelta` / `Done{StopReason, Usage}`).

### 5. Self-hosted / OpenAI-compatible servers

**vLLM, Ollama, LM Studio (`localhost:1234`), llama.cpp's `llama-server` (`localhost:8080`), and HuggingFace TGI all expose an OpenAI-compatible `/v1/chat/completions`** — so a single OpenAI-Chat-Completions adapter pointed at a configurable base URL (with a placeholder API key) covers them all ([bizon LLM engines](https://bizon-tech.com/blog/best-llm-inference-engines); [Ollama API](https://github.com/ollama/ollama/blob/main/docs/api.md)). **Caveat:** tool-calling fidelity varies — vLLM is most complete; **LM Studio lacks streaming tool calls and parallel function invocation**; Ollama supports tools on `/api/chat` and added streaming tool calls ([Ollama streaming-tool](https://ollama.com/blog/streaming-tool)). → **Capability flags must be per-endpoint, not just per-provider.**

**LiteLLM** (`BerriAI/litellm`) is an open-source proxy normalizing 100+ providers to OpenAI format (and can translate to/from native Anthropic `/v1/messages`); support it as just another OpenAI-compatible base URL while keeping native adapters for full-fidelity features (Anthropic thinking, Gemini multimodal) ([LiteLLM](https://github.com/BerriAI/litellm/); [LiteLLM Anthropic](https://docs.litellm.ai/docs/anthropic_unified/)).

### 6. Official Go SDKs (verified import paths, licenses, and method locations)

| Provider | Import path | License | Key entry points |
|---|---|---|---|
| **Anthropic** | `github.com/anthropics/anthropic-sdk-go` | MIT | `client.Messages.New` / `.NewStreaming`; `option.WithAPIKey` / `.WithBaseURL`; beta `toolrunner` subpackage. ([repo](https://github.com/anthropics/anthropic-sdk-go)) |
| **Google Gemini** | `google.golang.org/genai` (repo `googleapis/go-genai`) | Apache-2.0 | `genai.NewClient(ctx, &genai.ClientConfig{Backend: genai.BackendGeminiAPI \| BackendVertexAI, APIKey:...})`; `client.Models.GenerateContent` / `.GenerateContentStream` (returns a Go 1.23 `iter.Seq2`). **Old `github.com/google/generative-ai-go` is deprecated (EOL 2025-11-30).** ([repo](https://github.com/googleapis/go-genai)) |
| **OpenAI** | `github.com/openai/openai-go/v3` (latest v3.39.0, 2026-06-03) | Apache-2.0 | Requires **Go 1.22+**. `client.Responses.New` / `.NewStreaming` live in subpackage **`responses`** (params `responses.ResponseNewParams`). `client.Chat.Completions.New` / `.NewStreaming` are declared in the **root `openai` package** (`chatcompletion.go`; params `openai.ChatCompletionNewParams`) — **not** a `chat/completions` subpackage; `Chat`/`Completions` are chained struct fields. `option.WithBaseURL` for OpenAI-compatible servers. ([pkg.go.dev](https://pkg.go.dev/github.com/openai/openai-go/v3); [chatcompletion.go](https://github.com/openai/openai-go/blob/main/chatcompletion.go)) |

### 7. Token counting & multimodal (provider-specific; gate via capabilities)

- **Token counting:** Anthropic `POST /v1/messages/count_tokens` (→ `input_tokens`); Gemini `countTokens` (→ `totalTokens`) plus `usageMetadata` on every generate; **OpenAI has no pre-count endpoint** — return `Unsupported` or use a local tokenizer (`o200k_base`) **for OpenAI only**. **Never use tiktoken to estimate Anthropic or Gemini tokens.** Always read actual `usage`/`usageMetadata` for billing ([Anthropic count-tokens](https://platform.claude.com/docs/en/build-with-claude/token-counting.md); [Gemini tokens](https://ai.google.dev/api/tokens)).
- **Multimodal:** Anthropic `image`/`document` blocks (base64/url/file_id); Gemini `inlineData` `Blob{MIMEType, Data}` or `fileData`; OpenAI `image_url` (Chat) / `input_image` (Responses). Normalize as an internal `ImagePart{MediaType, Data|URL|FileRef}`.
- **Errors/retry:** all return 429 + `Retry-After` and 5xx; the official SDKs auto-retry with backoff (Anthropic default 2 retries). Surface a normalized `ProviderError{Kind: RateLimited|InvalidRequest|Auth|Overloaded|Server|Timeout, RetryAfter, Raw}`.

### Recommended Go `Provider` interface sketch

```go
// Package llm defines a provider-agnostic model interface. Each provider gets
// one adapter; self-hosted/OpenAI-compatible servers reuse the OpenAI adapter
// with option.WithBaseURL(endpoint). Capability flags are set per adapter AND
// overridable per endpoint.
package llm

import (
	"context"
	"encoding/json"
)

// --- Normalized internal model (provider-agnostic) ---

type Role string

const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// ContentPart is a discriminated union; exactly one field is non-nil.
type ContentPart struct {
	Text       *TextPart
	Image      *ImagePart
	ToolCall   *ToolCallPart
	ToolResult *ToolResultPart
	Thinking   *ThinkingPart
}

type TextPart     struct{ Text string }
type ThinkingPart struct{ Text, Signature string }

// ImagePart keeps a MediaType so each adapter can render the correct wire shape
// (Anthropic image block / Gemini inlineData Blob / OpenAI image_url|input_image).
type ImagePart struct {
	MediaType string // e.g. "image/png"
	Data      []byte // base64-encoded inline bytes, OR
	URL       string // remote URL, OR
	FileRef   string // provider file id
}

// ToolCallPart: Args is a parsed object. The OpenAI adapter parses the
// JSON-string `function.arguments` into this; Anthropic/Gemini already deliver objects.
type ToolCallPart struct {
	ID   string
	Name string
	Args map[string]any
}

type ToolResultPart struct {
	CallID  string
	Content string
	IsError bool
}

type Message struct {
	Role    Role
	Content []ContentPart
}

// ToolDef parameters stay as raw JSON Schema — all four providers accept
// JSON-Schema-shaped params; only the wrapper differs.
type ToolDef struct {
	Name        string
	Description string
	JSONSchema  json.RawMessage
}

type ToolChoice string // "auto" | "any" | "required" | "<tool name>"

type Request struct {
	Model       string
	System      string // first-class; each adapter places it correctly
	Messages    []Message
	Tools       []ToolDef
	ToolChoice  ToolChoice
	MaxTokens   int
	Temperature *float64 // nil = provider default
	Stream      bool
}

// StopReason is the normalized union of all providers' finish/stop vocabularies.
type StopReason string

const (
	StopEnd           StopReason = "stop"
	StopMaxTokens     StopReason = "max_tokens"
	StopToolUse       StopReason = "tool_use"
	StopStopSequence  StopReason = "stop_sequence"
	StopContentFilter StopReason = "content_filter"
	StopPause         StopReason = "pause"
)

type Usage struct {
	InputTokens      int
	OutputTokens     int
	CacheReadTokens  int
	CacheWriteTokens int
}

type Response struct {
	Content    []ContentPart
	StopReason StopReason
	Usage      Usage
}

// --- Streaming ---

type StreamEvent struct {
	TextDelta     string
	ThinkingDelta string
	ToolCallDelta *ToolCallDelta // partial, accumulate by Index/ID
	Done          *Done          // terminal event
}

type ToolCallDelta struct {
	Index       int
	ID          string
	Name        string
	ArgsPartial string // partial JSON; concatenate then parse on Done
}

type Done struct {
	StopReason StopReason
	Usage      Usage
}

type StreamReader interface {
	Recv() (StreamEvent, error) // io.EOF terminates
	Close() error
}

// --- Capabilities (per adapter, overridable per endpoint) ---

type Capabilities struct {
	SupportsTools             bool
	SupportsParallelToolCalls bool
	SupportsStreamingToolCalls bool
	SupportsVision            bool
	SupportsSystemPrompt      bool
	SupportsThinking          bool
	SupportsTokenCounting     bool
	SupportsJSONSchemaStrict  bool
	MaxOutputTokens           int
}

// --- Normalized error ---

type ErrorKind string

const (
	ErrRateLimited    ErrorKind = "rate_limited"
	ErrInvalidRequest ErrorKind = "invalid_request"
	ErrAuth           ErrorKind = "auth"
	ErrOverloaded     ErrorKind = "overloaded"
	ErrServer         ErrorKind = "server"
	ErrTimeout        ErrorKind = "timeout"
)

type ProviderError struct {
	Kind       ErrorKind
	RetryAfter int // seconds; honor before exponential backoff + full jitter
	Raw        error
}

func (e *ProviderError) Error() string { return string(e.Kind) + ": " + e.Raw.Error() }

// --- The core abstraction ---

type Provider interface {
	Generate(ctx context.Context, req Request) (*Response, error)
	Stream(ctx context.Context, req Request) (StreamReader, error)
	// CountTokens is capability-gated: Anthropic/Gemini call a server endpoint;
	// OpenAI returns ErrInvalidRequest (Unsupported) or uses a local tokenizer.
	CountTokens(ctx context.Context, req Request) (int, error)
	Capabilities() Capabilities
}
```

**Design rules baked into the sketch** (per the multi-LLM stream):

- Keep `ToolDef.JSONSchema` as raw JSON Schema; only the wrapper differs per provider.
- For OpenAI, default to the **Responses** surface (`client.Responses`, `input` + `instructions`, flat tools, `previous_response_id`) and keep **Chat Completions** (`client.Chat.Completions`, system-role message, `function`-wrapped tools, parse the `arguments` JSON string) behind a sub-flag for compatibility — remembering the Chat Completions params type lives in the **root `openai` package**, not a subpackage.
- Centralize stop-reason and error normalization in the adapter layer; lean on the official SDKs' built-in backoff and add a **single** harness-level retry policy keyed on `ProviderError.Kind`.
- Set `Capabilities` **per endpoint** so vLLM-with-tools and LM-Studio-without are distinguishable; fail fast (or degrade) rather than sending a request the backend will reject.

---

## OSS Best-Practices Checklist for This Project

A great, widely-adopted Go backend OSS project succeeds on three axes: **trustworthy governance/legal hygiene**, **frictionless onboarding**, and **engineering rigor that is automated end-to-end**. Adopt this checklist wholesale.

### Legal & governance

- [ ] **License: Apache-2.0.** Adds an **explicit patent grant + patent-retaliation clause** (MIT has *no* express patent grant); the choice of most CNCF projects for infrastructure others build on. Costs: ~10× longer, needs a `NOTICE` file + statement of changes, and is **incompatible with GPLv2** (MIT is GPLv2/v3-compatible). ([FOSSA: Apache-2.0](https://fossa.com/blog/open-source-licenses-101-apache-license-2-0/); [Apache GPL-compatibility](https://www.apache.org/licenses/GPL-compatibility.html); [CNCF projects](https://www.cncf.io/projects/))
- [ ] Add a **`NOTICE` file** and **SPDX headers** in source.
- [ ] Require **DCO sign-off** (`git commit -s`) rather than a heavyweight CLA, to keep contribution friction low.

### Project layout

- [ ] **Follow the official [go.dev/doc/modules/layout](https://go.dev/doc/modules/layout):** `cmd/<app>/main.go` for entrypoints, `internal/` for everything not meant as a public API (compiler-enforced privacy), packages at root only if intended to be imported.
- [ ] **Do NOT add `pkg/`, `api/`, `web/`, `configs/`, `deployments/`** scaffolding until a concrete need exists. The hugely popular `golang-standards/project-layout` **explicitly states in its own README it is NOT an official standard** and that `pkg/` "is not universally accepted." ([golang-standards/project-layout](https://github.com/golang-standards/project-layout); [laurentsv critique](https://laurentsv.com/blog/2024/10/19/no-nonsense-go-package-layout.html))
- [ ] Keep it **flat and feature-oriented first.**

### Architecture

- [ ] **Pragmatic hexagonal:** pure **domain** (business rules), infra-agnostic **application** (use cases), **ports** (HTTP/gRPC entry), **adapters** (DB/external APIs) — applied **selectively** where complexity justifies it. ([Three Dots Labs: clean architecture](https://threedots.tech/post/introducing-clean-architecture/))
- [ ] **Hard rule:** dependency direction is outer→inner; the domain knows nothing about HTTP/SQL/frameworks.
- [ ] **Go idiom:** define small interfaces **in the consumer package** (implicit satisfaction), not a dedicated "ports" layer. Resist "15 layers / 50 interfaces for a CRUD API." ([hexagonal in Go](https://skoredin.pro/blog/golang/hexagonal-architecture-go))

### Logging & observability

- [ ] **Structured logging via stdlib `log/slog`** (added in **Go 1.21**; ships exactly two handlers, `TextHandler` and `JSONHandler`). Use `JSONHandler` in prod, level from config, request/trace IDs via `context.Context`, and implement the **`LogValuer` interface** on secret-bearing types for redaction. ([Go blog: slog](https://go.dev/blog/slog); [pkg.go.dev slog](https://pkg.go.dev/log/slog))
- [ ] **OpenTelemetry-Go:** traces + metrics are **stable**, logs are **beta** (as of 2025–2026). Use `otelhttp`/`otelgrpc` auto-instrumentation + manual spans on key paths, OTLP export to a **Collector**, correlate `trace_id`/`span_id` into slog; **register a shutdown function** for clean flush. ([OTel Go docs](https://opentelemetry.io/docs/languages/go/))

### Testing & coverage

- [ ] **Table-driven** unit tests with consumer-interface mocks; `httptest` for handlers; **`testcontainers-go`** for real-DB/integration tests behind a `//go:build integration` tag so the fast loop stays fast. (PostgreSQL integration tests are a natural fit here.)
- [ ] Always run `go test -race` in CI.
- [ ] **Coverage is a confidence signal, not a goal** — enforce a pragmatic floor (~**70–80%**, where the Go stdlib itself sits), *not* 100%. ([Go testing best practices](https://backendbytes.com/articles/go-testing-best-practices/); [coverage tracking](https://getotterwise.com/blog/go-code-coverage-tracking-best-practices-cicd))

### Linting & formatting

- [ ] **`golangci-lint` v2** (~March 2025): the old `enable-all`/`disable-all` are replaced by a single **`linters.default`** key (`standard`/`all`/`fast`/`none`); **no exclusions by default** (enable human-readable presets). Use `linters.default: standard` + a curated `enable` list (`errcheck`, `govet`, `staticcheck`, `revive`, `gosec`, `misspell`, `gocritic`, `bodyclose`), formatters `gofumpt`/`goimports`, run via the official `golangci/golangci-lint-action` with a **pinned version**, fail the build on findings. ([golangci-lint v2](https://ldez.github.io/blog/2025/03/23/golangci-lint-v2/); [config docs](https://golangci-lint.run/docs/linters/configuration/))

### CI/CD & release

- [ ] **GitHub Actions** jobs: lint, unit (matrix on Go versions), integration (testcontainers), build. **Pin all third-party actions to commit SHAs** and set minimal `GITHUB_TOKEN` permissions.
- [ ] **Versioning:** enforce **Conventional Commits v1.0.0** (commitlint + PR-title check). Mapping: `fix`→PATCH, `feat`→MINOR, `!`/`BREAKING CHANGE`→MAJOR; only `feat` and `fix` are required types. ([Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/))
- [ ] **Changelog/release:** use **release-please** (Google) — it opens a release PR accumulating the version bump + generated `CHANGELOG.md`; merging it tags the release. Lower-risk and language-agnostic with first-class Go support; pairs cleanly with GoReleaser. ([semantic-release](https://github.com/semantic-release/semantic-release); [automated releases](https://devopsil.com/articles/2026-03-21-semantic-versioning-automated-releases))
- [ ] **Artifacts: GoReleaser on tag** — multi-arch static binaries (amd64+arm64) with `ldflags` version stamping, multi-arch Docker images to GHCR, **SBOM generation (syft by default)**, and **keyless cosign signing** (via GitHub Actions OIDC) + SLSA provenance for binaries and images. ([GoReleaser supply chain](https://goreleaser.com/blog/supply-chain-security/); [example-supply-chain](https://github.com/goreleaser/example-supply-chain))

### Error handling & configuration

- [ ] **Errors:** wrap with `fmt.Errorf("...: %w", err)` by default; inspect with **`errors.Is`/`errors.As`** (never `==`/type assertions — matching must survive multi-level wrapping). Define exported **sentinel** errors only for conditions callers branch on; **typed** errors only where HTTP/gRPC layers map to status codes. Avoid `errors.Is` on hot paths. ([Dave Cheney: handle errors gracefully](https://dave.cheney.net/2016/04/27/dont-just-check-errors-handle-them-gracefully))
- [ ] **Config:** typed `Config` struct, precedence **flags > env > file > defaults**, **validate on startup and fail fast**, secrets via env only, document every variable. Prefer **`knadh/koanf`** (lean deps) over `spf13/viper` (which force-lowercases keys and bloats binaries); stdlib `flag` + `os.Getenv` suffices for simple services. ([koanf](https://github.com/knadh/koanf); [12-factor config](https://blog.container-solutions.com/golang-configuration-in-12-factor-applications))

### Community & supply-chain trust

- [ ] **`SECURITY.md`** (root, `/docs`, or `/.github`) with **private vulnerability reporting** enabled via GitHub Security Advisories (never public issues), supported-versions table, disclosure timeline. ([GitHub security policy](https://docs.github.com/en/code-security/getting-started/adding-a-security-policy-to-your-repository))
- [ ] **`CONTRIBUTING.md`** (with DCO), **Contributor Covenant `CODE_OF_CONDUCT.md`**, issue/PR templates.
- [ ] **Dependabot/Renovate** + pinned action SHAs; the **OpenSSF Scorecard Action** (18+ checks, 0–10); target the **OpenSSF Best Practices Badge "passing"** tier from day one. ([OpenSSF Scorecard](https://scorecard.dev/); [CNCF lifecycle](https://contribute.cncf.io/projects/lifecycle/))
- [ ] **README:** copy-paste quickstart in the first screenful + **~3–6 trust badges** (CI, coverage, Go Reference, license). Consider an `ARCHITECTURE.md` to ease onboarding. ([awesome-readme](https://github.com/matiassingers/awesome-readme))

---

## Risks & Open Questions

### Technical risks

1. **Sandbox escape is the dominant technical risk** for any code-executing autonomous loop. Containers alone are insufficient for *untrusted* code — the kernel boundary is shared. Plan a Workspace abstraction now so microVMs (Firecracker: ~100–125 ms cold start, <5 MiB overhead, 5–30 ms snapshot-restore) or gVisor can be slotted in for untrusted/multi-tenant execution later ([Northflank sandboxing](https://northflank.com/blog/how-to-sandbox-ai-agents); [Spheron sandbox](https://www.spheron.network/blog/ai-agent-code-execution-sandbox-e2b-daytona-firecracker/)). OS-native sandboxes are **OS-specific**, so cross-platform parity (especially Windows) is hard.
2. **The "lethal trifecta"** (private-data access + untrusted content + external communication) is the key threat model; prompt-injection via tool outputs/web content can exfiltrate secrets. Design permissions and the secret registry against it explicitly ([awesome-harness-engineering](https://github.com/ai-boost/awesome-harness-engineering)).
3. **Context-window/cost management drives most architecture** — naive context stuffing fails on large repos. Repo maps, condensers, and RAG are not optional at scale ([Anthropic context engineering](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)).
4. **Greenfield Go means porting, not reusing.** No leading OSS harness is Go; budget time to port patterns (event sourcing, condensers, sandbox profiles, ACI tool design) and validate provider-API facts against current upstream docs before committing.
5. **OTel GenAI conventions are mostly still experimental** as of early 2026 (logs signal is beta); expect churn in span/attribute names ([OTel GenAI](https://opentelemetry.io/blog/2026/genai-observability/)).
6. **Self-hosted tool-calling fidelity varies wildly** (LM Studio lacks streaming/parallel tool calls); per-endpoint capability flags are mandatory or requests will fail at runtime.

### Governance risks

7. **Don't couple the harness to one proprietary model backend** — Gemini CLI's withdrawal of free access after soliciting 6,000 PRs is the cautionary case ([Gemini CLI gating](https://www.techtimes.com/articles/317056/20260523/google-accepted-6000-gemini-cli-contributions-then-closed-tool-enterprise-only.htm)).
8. **Avoid delayed/"fair-source" licenses** (e.g., Crush's FSL-1.1-MIT) unless there is a specific commercial-protection reason; they are not OSI-approved on day one.

### Open questions (need a project decision)

- **PostgreSQL as the event store:** single append-only `events` table per session vs. partitioned/sharded? How are large tool outputs (and their *clearing*) represented — inline JSONB, or offloaded to object storage with a reference (favoring just-in-time retrieval)?
- **Microservice boundaries:** which subsystems are separate services vs. in-process packages? (Candidate split: model-gateway, tool/execution-runtime, persistence/event-store, orchestrator.) How do they communicate — gRPC, the event log, or both?
- **Sandbox MVP choice:** Docker-per-action (OpenHands style) vs. a longer-lived per-session container? What is the network policy default (deny-all + explicit egress allowlist)?
- **OpenAI surface default:** ship the **Responses** API as primary with Chat Completions as fallback, or vice versa? (Recommendation: Responses primary.)
- **MCP server mode in scope for v1?** It is a COULD, but exposing the harness to IDEs early could drive adoption.
- **Native-Ollama (NDJSON) adapter:** worth building, or is the OpenAI-compatible endpoint sufficient?
- **Eval target:** full SWE-bench, SWE-bench-Lite, or a bespoke internal suite for the first CI gate?
- **License confirmation:** confirm `gptme` and `Tabby` licenses from their repos' own LICENSE files before citing them as references or dependencies.
- **Concurrency model for parallel read-only tools:** bounded worker pool size, and how cancellation propagates to in-flight goroutines mid-turn.

---

## Fact-Check Corrections Applied

The following corrections from the 20-verdict fact-check pass are reflected throughout this report; refuted claims are **not** restated as fact.

1. **OpenAI Go SDK — Chat Completions package location (REFUTED claim corrected).** `client.Chat.Completions.New` is **not** in a `chat/completions` subpackage. `ChatCompletionService` and its methods are declared in the **root `openai` package** (`chatcompletion.go`), with params `openai.ChatCompletionNewParams`. `Chat`/`Completions` are chained struct fields off the root client. Only **`Responses`** lives in a real subpackage (`responses`, params `responses.ResponseNewParams`). Confirmed: import path `github.com/openai/openai-go/v3` (latest **v3.39.0**, 2026-06-03), Apache-2.0, Go 1.22+, `option.WithBaseURL`. ([pkg.go.dev](https://pkg.go.dev/github.com/openai/openai-go/v3); [chatcompletion.go](https://github.com/openai/openai-go/blob/main/chatcompletion.go))
2. **Codex CLI crate count (stale figure corrected).** Rust-primary (~96%), Apache-2.0, and the April-2025-TypeScript→Rust-rewrite history are confirmed. But the workspace is **~144 member crates** as of June 2026, not "~60–70" (that figure was accurate only for mid-2025). ([codex Cargo.toml](https://github.com/openai/codex/blob/main/codex-rs/Cargo.toml))
3. **Goose is Rust, NOT Go** (~64% Rust / ~29% TS, Apache-2.0); the Go peer is **Crush** (FSL-1.1-MIT). **Tabby is Rust, NOT Go.** **Cline is Apache-2.0, NOT MIT.** **Crush is FSL-1.1-MIT, NOT plain MIT.** ([block/goose](https://github.com/block/goose); [Crush](https://github.com/charmbracelet/crush); [Cline](https://github.com/cline/cline))
4. **Anthropic streaming nuances:** `stop_reason` sits **nested under `delta`** in the `message_delta` event (not top-level); the `content_block_delta` delta-type list is **non-exhaustive** (`signature_delta`, `citations_delta` also occur). The six `stop_reason` values are confirmed but also non-exhaustive (`model_context_window_exceeded` is documented). ([streaming](https://platform.claude.com/docs/en/build-with-claude/streaming); [handling stop reasons](https://platform.claude.com/docs/en/api/messages))
5. **Gemini Go SDK:** `google.golang.org/genai` is Apache-2.0 (pkg.go.dev shows "Apache-2.0, BSD-3-Clause" only because of an embedded Go Authors notice); old `github.com/google/generative-ai-go` is **EOL 2025-11-30**. ([go-genai](https://github.com/googleapis/go-genai))
6. **Confirmed without change:** OpenHands (Python, MIT except `enterprise/`); Aider (Python ~80%, Apache-2.0, tree-sitter repo map + NetworkX PageRank, `(aider)` git attribution — note newer `--attribute-co-authored-by` default-True option can supersede the name suffix); SWE-agent (Python, MIT, `SWEEnv` thin-wraps `SWE-ReX`, 12.5% vs 3.8% RAG, NeurIPS 2024); Gemini CLI (TypeScript, Apache-2.0); Anthropic Go SDK (`github.com/anthropics/anthropic-sdk-go`, MIT, beta `toolrunner`); Anthropic Messages API shape; Apache-2.0 vs MIT patent/GPL facts; go.dev layout guidance; `golang-standards/project-layout` self-disclaimer; `log/slog` (Go 1.21, two handlers); OTel-Go (traces/metrics stable, logs beta); golangci-lint v2 `linters.default`; GoReleaser syft+cosign; Conventional Commits→SemVer; release-please PR flow; GitHub `SECURITY.md` + private reporting; OpenSSF Scorecard/Badge tiers; CNCF lifecycle stages.

---

## References

*Deduplicated; grouped for readability.*

### Core architecture & context engineering
- https://code.claude.com/docs/en/agent-sdk/agent-loop
- https://platform.claude.com/docs/en/agent-sdk/sessions
- https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents
- https://www.mindstudio.ai/blog/what-is-agent-harness-architecture-explained
- https://medium.com/@milesk_33/the-system-design-of-claude-code-agent-explained-318d17496534
- https://arxiv.org/html/2511.03690v1
- https://arxiv.org/abs/2511.03690
- https://callsphere.ai/blog/langgraph-checkpointing-persistence-time-travel-agent-workflows
- https://github.com/ai-boost/awesome-harness-engineering
- https://github.com/VILA-Lab/Dive-into-Claude-Code

### Reference OSS projects
- https://github.com/OpenHands/OpenHands
- https://github.com/OpenHands/software-agent-sdk
- https://docs.openhands.dev/openhands/usage/architecture/runtime
- https://docs.openhands.dev/sdk
- https://www.openhands.dev/blog/the-path-to-openhands-v1
- https://github.com/openai/codex
- https://github.com/openai/codex/blob/main/LICENSE
- https://github.com/openai/codex/blob/main/codex-rs/Cargo.toml
- https://github.com/openai/codex/blob/main/codex-rs/README.md
- https://codex.danielvaughan.com/2026/03/28/codex-rs-rust-rewrite-architecture/
- https://www.infoq.com/news/2025/06/codex-cli-rust-native-rewrite/
- https://agent-safehouse.dev/docs/agent-investigations/codex
- https://github.com/charmbracelet/crush
- https://biggo.com/news/202507310715_Charm_Crush_AI_Coding_Agent
- https://aicoolies.com/reviews/crush-review
- https://github.com/sst/opencode
- https://opencode.ai/
- https://github.com/block/goose
- https://github.com/aaif-goose/goose
- https://goose-docs.ai/
- https://block.xyz/inside/block-open-source-introduces-codename-goose
- https://github.com/Aider-AI/aider
- https://github.com/Aider-AI/aider/blob/main/LICENSE.txt
- https://aider.chat/docs/repomap.html
- https://aider.chat/2023/10/22/repomap.html
- https://aider.chat/docs/git.html
- https://github.com/cline/cline
- https://cline.ghost.io/introducing-cline-sdk-the-upgraded-agent-runtime/
- https://github.com/continuedev/continue
- https://github.com/SWE-agent/SWE-agent
- https://github.com/SWE-agent/SWE-ReX
- https://swe-agent.com/latest/background/architecture/
- https://arxiv.org/abs/2405.15793
- https://proceedings.neurips.cc/paper_files/paper/2024/file/5a7c947568c1b1328ccc5230172e1e7c-Paper-Conference.pdf
- https://github.com/SWE-bench/SWE-bench
- https://www.swebench.com/SWE-bench/
- https://github.com/google-gemini/gemini-cli
- https://www.techtimes.com/articles/317056/20260523/google-accepted-6000-gemini-cli-contributions-then-closed-tool-enterprise-only.htm
- https://github.com/huggingface/smolagents
- https://huggingface.co/docs/smolagents/en/index
- https://github.com/gptme/gptme
- https://gptme.org/docs/agents.html
- https://github.com/TabbyML/tabby
- https://www.tabbyml.com/
- https://github.com/langchain-ai/langgraph
- https://github.com/microsoft/autogen
- https://github.com/crewAIInc/crewAI
- https://github.com/elizaOS/eliza

### MCP & interoperability
- https://modelcontextprotocol.io/specification/2025-11-25
- https://www.webfuse.com/mcp-cheat-sheet

### Multi-LLM provider APIs & Go SDKs
- https://platform.claude.com/docs/en/api/messages
- https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview
- https://platform.claude.com/docs/en/build-with-claude/handling-stop-reasons
- https://platform.claude.com/docs/en/build-with-claude/streaming
- https://platform.claude.com/docs/en/build-with-claude/token-counting.md
- https://platform.claude.com/docs/en/api/messages-count-tokens
- https://platform.claude.com/docs/en/api/sdks/go
- https://github.com/anthropics/anthropic-sdk-go
- https://pkg.go.dev/github.com/anthropics/anthropic-sdk-go/option
- https://pkg.go.dev/github.com/anthropics/anthropic-sdk-go/toolrunner
- https://ai.google.dev/gemini-api/docs/function-calling
- https://ai.google.dev/api/tokens
- https://ai.google.dev/gemini-api/docs/libraries
- https://github.com/googleapis/go-genai
- https://pkg.go.dev/google.golang.org/genai
- https://github.com/google/generative-ai-go
- https://developers.openai.com/api/docs/guides/function-calling
- https://developers.openai.com/api/docs/guides/migrate-to-responses
- https://pkg.go.dev/github.com/openai/openai-go/v3
- https://pkg.go.dev/github.com/openai/openai-go/v3/responses
- https://pkg.go.dev/github.com/openai/openai-go/v3/option
- https://github.com/openai/openai-go/blob/main/chatcompletion.go
- https://medium.com/@laurentkubaski/openai-tool-schema-differences-between-the-response-api-and-the-chat-completion-api-8f99ce8a9371
- https://bizon-tech.com/blog/best-llm-inference-engines
- https://github.com/ollama/ollama/blob/main/docs/api.md
- https://docs.ollama.com/api/streaming
- https://ollama.com/blog/streaming-tool
- https://github.com/BerriAI/litellm/
- https://docs.litellm.ai/docs/anthropic_unified/
- https://docs.litellm.ai/docs/providers/litellm_proxy

### Reliability, observability & evals
- https://www.clawpulse.org/blog/llm-api-rate-limiting-best-practices-avoid-429-errors-and-save-40-on-costs
- https://iotools.cloud/journal/api-rate-limiting-headers-exponential-backoff-and-surviving-the-429/
- https://opentelemetry.io/blog/2026/genai-observability/
- https://greptime.com/blogs/2026-05-09-opentelemetry-genai-semantic-conventions
- https://uptrace.dev/blog/opentelemetry-ai-systems
- https://opentelemetry.io/docs/languages/go/
- https://opentelemetry.io/docs/languages/go/instrumentation/
- https://github.com/open-telemetry/opentelemetry-go

### Sandboxing & isolation
- https://northflank.com/blog/how-to-sandbox-ai-agents
- https://www.spheron.network/blog/ai-agent-code-execution-sandbox-e2b-daytona-firecracker/

### Go OSS engineering best practices
- https://fossa.com/blog/open-source-licenses-101-apache-license-2-0/
- https://www.wiz.io/academy/application-security/mit-licenses-explained
- https://www.apache.org/licenses/LICENSE-2.0
- https://www.apache.org/licenses/GPL-compatibility.html
- https://opensource.org/license/mit
- https://www.cncf.io/projects/
- https://go.dev/doc/modules/layout
- https://github.com/golang-standards/project-layout
- https://laurentsv.com/blog/2024/10/19/no-nonsense-go-package-layout.html
- https://itnext.io/go-standard-project-layout-a-mildly-unhinged-rant-be20cb793d0d
- https://threedots.tech/post/introducing-clean-architecture/
- https://skoredin.pro/blog/golang/hexagonal-architecture-go
- https://alamrafiul.com/posts/go-hexagonal-architecture/
- https://go.dev/blog/slog
- https://pkg.go.dev/log/slog
- https://betterstack.com/community/guides/logging/logging-in-go/
- https://uptrace.dev/blog/golang-logging
- https://backendbytes.com/articles/go-testing-best-practices/
- https://getotterwise.com/blog/go-code-coverage-tracking-best-practices-cicd
- https://www.glukhov.org/post/2025/11/unit-tests-in-go/
- https://ldez.github.io/blog/2025/03/23/golangci-lint-v2/
- https://golangci-lint.run/docs/linters/configuration/
- https://gist.github.com/maratori/47a4d00457a92aa426dbd48a18776322
- https://goreleaser.com/blog/supply-chain-security/
- https://github.com/goreleaser/goreleaser-action
- https://github.com/goreleaser/example-supply-chain
- https://goreleaser.com/customization/sign/docker_sign/
- https://www.conventionalcommits.org/en/v1.0.0/
- https://github.com/semantic-release/semantic-release
- https://devopsil.com/articles/2026-03-21-semantic-versioning-automated-releases
- https://backendbytes.com/articles/go-error-handling-patterns/
- https://dave.cheney.net/2016/04/27/dont-just-check-errors-handle-them-gracefully
- https://www.dolthub.com/blog/2024-05-31-benchmarking-go-error-handling/
- https://github.com/knadh/koanf
- https://blog.container-solutions.com/golang-configuration-in-12-factor-applications
- https://itnext.io/golang-configuration-management-library-viper-vs-koanf-eea60a652a22
- https://docs.github.com/en/code-security/getting-started/adding-a-security-policy-to-your-repository
- https://scorecard.dev/
- https://contribute.cncf.io/projects/lifecycle/
- https://github.com/matiassingers/awesome-readme
