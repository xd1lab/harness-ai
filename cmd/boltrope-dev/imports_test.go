// SPDX-License-Identifier: Apache-2.0

package main

// T-15 + ADR-0029 (AC-15 / AC-16) — REFINED. The dev binary's import-graph guard.
//
// The dev binary must import NONE of the production daemon's heavy/sensitive edges
// — the pgx event store, mTLS/SPIRE creds, the OIDC keyfunc, the model-gateway
// app.Service, the modelgw gRPC model client, or the toolrt gRPC tool client. This
// is the K-1 "can't be run in prod by accident" build-time property and the
// structural half of the ADR-0024 prod-exclusion invariant.
//
// ADR-0029 REFINEMENT: the dev binary now legitimately depends on the leaf
// package internal/modelgateway/app/capabilities (pure data; no I/O; no
// pgx/spiffe/Service). That path is UNDER internal/modelgateway/app, so the old
// dep==forbidden OR HasPrefix(dep, forbidden+"/") rule would FALSELY flag it.
// Therefore the modelgateway/app SERVICE package is now forbidden EXACTLY
// (dep == the exact path) while every OTHER forbidden entry keeps the prefix rule.
// The test ALSO positively asserts the capabilities leaf IS present, so a future
// refactor that accidentally drops the real-model wiring is caught.

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// modelgwServicePkg is the model-gateway APPLICATION SERVICE package. It is
// forbidden by EXACT match (not prefix) so that its leaf sub-package
// internal/modelgateway/app/capabilities — which the dev real-model wiring now
// legitimately imports — is NOT swept up by a prefix match (ADR-0029).
const modelgwServicePkg = "github.com/xd1lab/harness-ai/internal/modelgateway/app"

// capabilitiesLeafPkg is the pure-data capability registry the dev real-model
// wiring depends on (--model-url / --enable-native-schema). It lives UNDER
// modelgwServicePkg but contains no I/O, no pgx, no spiffe, and not the Service —
// so it is explicitly PERMITTED, and its presence is asserted positively.
const capabilitiesLeafPkg = "github.com/xd1lab/harness-ai/internal/modelgateway/app/capabilities"

// prefixForbiddenDeps is the set of import paths the dev binary must NEVER pull in,
// matched by dep==forbidden OR HasPrefix(dep, forbidden+"/"). Each encodes a
// K-1/K-2 invariant: in-memory store (not pgx), no mTLS/SPIRE/OIDC, no
// orchestrator→model-gateway gRPC client, no orchestrator→tool-runtime gRPC client.
// The modelgateway/app SERVICE is NOT here — it is enforced by EXACT match below so
// the capabilities leaf stays permitted (ADR-0029).
var prefixForbiddenDeps = []string{
	// pgx event store (Postgres / RLS) — dev mode is in-memory only.
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/eventstore",
	// raw pgx driver — no database at all in dev mode.
	"github.com/jackc/pgx/v5",
	// SPIFFE/SPIRE mTLS identity — dev mode is loopback plaintext.
	"github.com/spiffe/go-spiffe",
	// orchestrator→model-gateway gRPC client adapter (the unexported assembler
	// behind the gRPC boundary) — dev wraps the llm.Provider directly.
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/modelgw",
	// tool-runtime gRPC client adapter — dev runs an in-process bridge, not a
	// cross-process gRPC client (ADR-0029).
	"github.com/xd1lab/harness-ai/internal/orchestrator/adapter/outbound/toolrt",
	// tool-runtime production dedup adapter — its Pool is pgx-backed, so importing
	// it for a type/constant would drag github.com/jackc/pgx/v5 into the dev binary.
	// The dev local-exec path MUST hand-roll an in-memory app.DedupStore that depends
	// only on the clean internal/toolruntime/app ports (ADR-0029, AC-8/AC-16).
	"github.com/xd1lab/harness-ai/internal/toolruntime/adapter/outbound/dedup",
}

// devDeps shells out to `go list -deps` over cmd/boltrope-dev and returns the
// transitive import set as a trimmed, non-empty line slice.
func devDeps(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("go", "list", "-deps",
		"github.com/xd1lab/harness-ai/cmd/boltrope-dev").CombinedOutput()
	require.NoErrorf(t, err, "go list -deps must enumerate the dev package; output:\n%s", string(out))

	var deps []string
	for _, line := range strings.Split(string(out), "\n") {
		if dep := strings.TrimSpace(line); dep != "" {
			deps = append(deps, dep)
		}
	}
	require.NotEmpty(t, deps, "go list -deps must enumerate at least one dependency")
	return deps
}

// TestDevBinary_DoesNotImportProductionEdges enumerates cmd/boltrope-dev's full
// transitive import set via `go list -deps` and asserts none of the forbidden
// production edges appear, with the modelgateway/app SERVICE forbidden by EXACT
// match (ADR-0029) so the permitted capabilities leaf is not falsely flagged.
func TestDevBinary_DoesNotImportProductionEdges(t *testing.T) {
	deps := devDeps(t)

	for _, dep := range deps {
		// (1) The model-gateway app SERVICE is forbidden by EXACT match only, so
		// the capabilities leaf (a strict sub-path) remains permitted.
		assert.NotEqualf(t, modelgwServicePkg, dep,
			"cmd/boltrope-dev must NOT import the model-gateway app Service %q "+
				"(it has no Generate and NewService fails closed) — ADR-0024/0029 prod-exclusion",
			modelgwServicePkg)

		// (2) Every other forbidden edge keeps the dep== OR prefix rule.
		for _, forbidden := range prefixForbiddenDeps {
			require.Falsef(t, dep == forbidden || strings.HasPrefix(dep, forbidden+"/"),
				"cmd/boltrope-dev must NOT import %q (found %q) — violates the K-1 "+
					"prod-exclusion / ADR-0024 invariant", forbidden, dep)
		}
	}
}

// TestDevBinary_ImportsCapabilitiesLeaf positively asserts the dev binary DOES pull
// in the pure-data capabilities leaf (the real-model wiring's per-(endpoint,model)
// resolver). It guards the ADR-0029 refinement from regressing into either (a) a
// dropped real-model dependency or (b) a future prefix rule that would re-forbid it.
func TestDevBinary_ImportsCapabilitiesLeaf(t *testing.T) {
	deps := devDeps(t)
	assert.Containsf(t, deps, capabilitiesLeafPkg,
		"cmd/boltrope-dev must import the permitted capabilities leaf %q for --model-url / "+
			"--enable-native-schema wiring (ADR-0029)", capabilitiesLeafPkg)
}
