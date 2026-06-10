# 8. Project name: Boltrope

Date: 2026-06-10
Status: Accepted

## Context

The working directory is `harness`, but "harness" as a brand collides with
**harness.io** (a major CI/CD company), hurting OSS discoverability and risking
trademark confusion. We need a distinctive, memorable, collision-light name and a Go
**module path** — which threads through every source file, so it must be settled early.

## Decision

Name the project **Boltrope** — the rope sewn into the edge of a sail to strengthen it
and bind it to the rigging. It is a nautical metaphor for the harness that binds and
controls the "sail" (the LLM). Module path: **`github.com/boltrope/boltrope`** (the
owner segment is a placeholder; the maintainer should change it to their real GitHub
org/user before publishing). A background research agent checked for obvious
collisions with prominent AI/dev-tool projects before selecting it.

## Consequences

- ✅ Distinctive, memorable, no known collision with a major dev tool; the nautical
  theme is brandable.
- ⚠️ Less literally descriptive than "harness" — the README tagline must say what it is
  ("a provider-portable, event-sourced AI agent harness").
- 📌 The module owner segment is a placeholder requiring a one-time rename before
  publishing.
- 📝 The naming agent also wrote `LICENSE` (Apache-2.0, verbatim) and `NOTICE`; the
  remaining community files were authored directly after the agent's final output was
  blocked by a content filter (triggered by the Contributor Covenant text).
