# 5. Sandbox isolation: containers behind a Workspace abstraction

Date: 2026-06-10
Status: Accepted

## Context

An autonomous agent loop executes shell commands and edits files; it must never run
against the host with raw, unsandboxed access. The research identifies **sandbox
escape** and the **lethal trifecta** as the dominant risks. OS-native sandboxes
(Seatbelt/Landlock/seccomp) are OS-specific and hard to keep at cross-platform
parity (the dev host here is Windows); microVMs (Firecracker/gVisor) are powerful but
heavy for an MVP.

## Decision

- Execute all tools/code inside **containers**, reached only through a
  **`Workspace`/`Runtime` port** (interface). The agent code never calls Docker
  directly — it calls the port.
- **Deny-by-default network egress** from the sandbox; explicit allowlist when needed.
- Standardize on **containers for portability** in v1 (works on the Windows dev host
  and in CI). The port is shaped so an **OS-native sandbox or microVM backend** can be
  added later without touching agent code.
- The precise container lifecycle (per-action vs per-session) and the event/workspace
  binding are finalized in the architecture document and its own ADR.

## Consequences

- ✅ Portable, simple MVP; the kernel-boundary upgrade path (microVM) is preserved.
- ⚠️ Containers share the host kernel — insufficient for *untrusted* multi-tenant
  code; documented as a known limitation with microVM as the planned mitigation.
- 📌 Sandbox network policy and filesystem scoping become part of the security model.
