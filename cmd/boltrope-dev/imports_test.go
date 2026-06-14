// SPDX-License-Identifier: Apache-2.0

package main

// T-15 (AC-11 / AC-12 / AC-16-structural) — RED. A single import-graph guard that
// satisfies three ACs at once: the dev binary must import NONE of the production
// daemon's heavy/sensitive edges — the pgx event store, mTLS/SPIRE creds, the OIDC
// keyfunc, the model-gateway app.Service, or the modelgw gRPC model client. This is
// simultaneously: AC-11 (no prod daemon / Postgres deps), the K-1 "can't be run in
// prod by accident" build-time property, and AC-16's structural half (no Service /
// no modelgw.Adapter constructed). It shells out to `go list -deps` over
// cmd/boltrope-dev. RED today because the package has no buildable non-test files
// yet, so the dependency set cannot be enumerated / does not yet exclude these.

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// forbiddenDeps is the set of import paths the dev binary must NEVER pull in. Each
// encodes a K-1/K-2 invariant: in-memory store (not pgx), no mTLS/SPIRE/OIDC, no
// model-gateway daemon/Service, no orchestrator→model-gateway gRPC client.
var forbiddenDeps = []string{
	// pgx event store (Postgres / RLS) — dev mode is in-memory only.
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/eventstore",
	// raw pgx driver — no database at all in dev mode.
	"github.com/jackc/pgx/v5",
	// SPIFFE/SPIRE mTLS identity — dev mode is loopback plaintext.
	"github.com/spiffe/go-spiffe",
	// model-gateway application service (BLOCKER 4: has no Generate; NewService
	// fail-closed) — dev wraps the stub llm.Provider directly.
	"github.com/xd1lab/harness-ai/internal/modelgateway/app",
	// orchestrator→model-gateway gRPC client adapter (the unexported assembler
	// behind the gRPC boundary) — dev has no model-gateway daemon.
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/modelgw",
	// tool-runtime gRPC client adapter — dev runs an in-process no-exec runtime.
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/toolrt",
}

// TestDevBinary_DoesNotImportProductionEdges enumerates cmd/boltrope-dev's full
// transitive import set via `go list -deps` and asserts none of the forbidden
// production edges appear.
func TestDevBinary_DoesNotImportProductionEdges(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps",
		"github.com/xd1lab/harness-ai/cmd/boltrope-dev").CombinedOutput()
	require.NoErrorf(t, err, "go list -deps must enumerate the dev package; output:\n%s", string(out))

	deps := string(out)
	for _, forbidden := range forbiddenDeps {
		for _, line := range strings.Split(deps, "\n") {
			dep := strings.TrimSpace(line)
			if dep == "" {
				continue
			}
			require.Falsef(t, dep == forbidden || strings.HasPrefix(dep, forbidden+"/"),
				"cmd/boltrope-dev must NOT import %q (found %q) — violates the K-1 prod-exclusion / AC-11 / AC-16-structural invariant",
				forbidden, dep)
		}
	}
}
