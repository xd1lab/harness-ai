// Package subagent internal test: asserts the per-spawn child-config derivation
// (childConfig) threads in.Depth into the child loop's own recursion depth
// WITHOUT mutating the shared base config (FR-EXT-04 AC-16).
//
// This is a white-box test (package subagent, not subagent_test) because the
// invariant under test — childCfg.Depth == in.Depth on a per-spawn COPY — is an
// internal construction detail not observable through the black-box Spawn API:
// agent.Loop does not expose its Config.Depth, and the in-loop spawn_subagent
// advertising gate that would otherwise reveal it is wired in a later task. The
// deterministic, today-passing proof of AC-16 is therefore the helper assertion
// below.
package subagent

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/app/agent"
)

// TestChildConfig_SetsDepthFromSpawnInput proves that the child loop config is
// built with Depth == in.Depth for a range of depths (including the boundary
// in.Depth == MaxDepth), so the child runs at — and advertises/recurses from —
// its own correct depth rather than inheriting the root spawner's Depth 0.
func TestChildConfig_SetsDepthFromSpawnInput(t *testing.T) {
	const maxDepth = 2

	for _, depth := range []int{0, 1, maxDepth} {
		depth := depth
		t.Run("", func(t *testing.T) {
			// Base config is the spawner-shared template; Depth 0 (root spawner).
			base := agent.Config{Model: "base-model", MaxTurns: 4}
			s := New(Config{MaxDepth: maxDepth, LoopCfg: base})

			child := s.childConfig(app.SubAgentSpawn{
				ParentSessionID: "parent",
				Depth:           depth,
				Task:            "t",
			})

			assert.Equal(t, depth, child.Depth,
				"child loop must run at the spawn-requested depth (AC-16)")
		})
	}
}

// TestChildConfig_DoesNotMutateSharedBase proves that childConfig works on a COPY
// and never mutates the shared s.loopCfg — two spawns at different depths must not
// see each other's depth, and the base template's Depth stays 0 throughout. This
// guards the race/leak that mutating the shared config would introduce across
// concurrent children.
func TestChildConfig_DoesNotMutateSharedBase(t *testing.T) {
	const maxDepth = 3
	base := agent.Config{Model: "base-model", MaxTurns: 4}
	s := New(Config{MaxDepth: maxDepth, LoopCfg: base})

	c1 := s.childConfig(app.SubAgentSpawn{ParentSessionID: "p", Depth: 1, Task: "a"})
	c2 := s.childConfig(app.SubAgentSpawn{ParentSessionID: "p", Depth: 2, Task: "b"})

	assert.Equal(t, 1, c1.Depth, "first child keeps its own depth")
	assert.Equal(t, 2, c2.Depth, "second child keeps its own depth")
	assert.Equal(t, 0, s.loopCfg.Depth,
		"shared base config Depth must remain untouched (no mutation, no leak)")
}

// TestChildConfig_ModelOverride proves the model override semantics are preserved
// alongside the depth threading: a non-empty in.Model wins, an empty one keeps
// the parent's model. (Regression guard for the refactor that extracted
// childConfig.)
func TestChildConfig_ModelOverride(t *testing.T) {
	base := agent.Config{Model: "parent-model", MaxTurns: 4}
	s := New(Config{MaxDepth: 2, LoopCfg: base})

	withOverride := s.childConfig(app.SubAgentSpawn{Depth: 1, Model: "child-model"})
	assert.Equal(t, "child-model", withOverride.Model, "non-empty model override wins")
	assert.Equal(t, 1, withOverride.Depth, "depth still threaded with a model override")

	noOverride := s.childConfig(app.SubAgentSpawn{Depth: 1})
	assert.Equal(t, "parent-model", noOverride.Model, "empty model keeps the parent's model")
}
