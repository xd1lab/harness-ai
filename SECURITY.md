# Security Policy

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

Use GitHub's **Private Vulnerability Reporting**: the repository's **Security** tab →
**Report a vulnerability** (GitHub Security Advisories).

Please include: affected version/commit, a description, reproduction steps or a PoC,
and an impact assessment.

> **Maintainer note:** private vulnerability reporting is an opt-in repository
> feature. Enable it in the repository settings (Code security → Private
> vulnerability reporting) when this repository is first published, or the
> **Report a vulnerability** button will not appear.

## Disclosure timeline

- We acknowledge new reports within **3 business days**.
- We aim to give an initial assessment within **10 business days**.
- We practice **coordinated disclosure**: we will work with you on a fix and a public
  advisory and credit you (unless you prefer otherwise). Please allow a reasonable
  window (typically up to 90 days) before public disclosure.

## Supported versions

Boltrope is pre-1.0. Until `1.0.0`, only the latest `0.x` minor release receives
security fixes.

| Version       | Supported |
|---------------|-----------|
| latest `0.x`  | ✅        |
| older         | ❌        |

## Scope notes

Boltrope executes model-directed tools and code inside sandboxes. **Sandbox escape**,
**prompt-injection** enabling the "lethal trifecta" (private-data access + untrusted
content + external communication), and **secret exfiltration** are explicitly in
scope. See the threat model in the architecture documentation.
