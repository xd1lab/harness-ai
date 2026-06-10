# 2. License: Apache-2.0 with DCO sign-off

Date: 2026-06-10
Status: Accepted

## Context

We must pick an OSS license for backend infrastructure that others will embed and
build on. The research synthesis and the hostile Gate-1 review both recommend
**Apache-2.0**: it adds an **explicit patent grant + patent-retaliation clause**
(MIT grants no express patent rights) and is the default for CNCF-style
infrastructure projects. Costs: it is incompatible with GPLv2, and it requires a
`NOTICE` file and a statement of changes. For contribution intake we choose between
a heavyweight CLA and a lightweight DCO.

## Decision

- License the project under **Apache-2.0**.
- Ship `LICENSE`, a `NOTICE` file, and an **SPDX header** in every source file:
  `// SPDX-License-Identifier: Apache-2.0`.
- Require a **Developer Certificate of Origin** sign-off (`git commit -s`) on every
  commit instead of a CLA, enforced by a CI check, to keep contribution friction low.

## Consequences

- ✅ Explicit patent protection; enterprise- and embed-friendly; familiar to OSS users.
- ⚠️ Incompatible with GPLv2 (acceptable; we are not linking GPLv2 code).
- 📌 Must keep `NOTICE` current and lint SPDX headers in CI.
