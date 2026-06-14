// SPDX-License-Identifier: Apache-2.0

package main

// Shared deterministic test helpers for the cmd/boltrope-dev RED suite. These are
// re-implemented cleanly here (NOT imported from internal/orchestrator/app/apptest,
// per the K-2 convention that the dev package must not depend on a test-helper
// package); they are _test.go-only and never ship in the binary. They reference no
// production symbols, so this file COMPILES on its own — the red comes from the
// per-AC test files that reference the not-yet-written production types.

import (
	"context"
	"io"

	"github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/orchestrator/domain"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// userMsg builds a user llm.Message with a single text part.
func userMsg(text string) llm.Message {
	return llm.Message{
		Role:    llm.RoleUser,
		Content: []llm.ContentPart{{Text: &llm.TextPart{Text: text}}},
	}
}

// scriptedModel is a deterministic app.ModelGatewayPort that streams a single
// canned event sequence. It exists only to drive the real loop in the eventstore
// golden test without a live provider; the production dev model is devModel over
// the stub provider.
type scriptedModel struct{ events []llm.StreamEvent }

func newScriptedModel(events []llm.StreamEvent) *scriptedModel { return &scriptedModel{events: events} }

func (m *scriptedModel) Stream(_ context.Context, _ llm.Request) (llm.StreamReader, error) {
	return &sliceReader{events: m.events}, nil
}

func (m *scriptedModel) Generate(_ context.Context, _ llm.Request) (*llm.Response, error) {
	return &llm.Response{StopReason: llm.StopEnd}, nil
}

func (m *scriptedModel) CountTokens(_ context.Context, _ llm.Request) (int, error) { return 1, nil }

func (m *scriptedModel) Capabilities(_ context.Context, _ string) (llm.Capabilities, error) {
	return llm.Capabilities{SupportsTools: true, SupportsSystemPrompt: true}, nil
}

var _ app.ModelGatewayPort = (*scriptedModel)(nil)

// sliceReader replays a fixed []llm.StreamEvent then io.EOF.
type sliceReader struct {
	events []llm.StreamEvent
	pos    int
}

func (r *sliceReader) Recv() (llm.StreamEvent, error) {
	if r.pos >= len(r.events) {
		return llm.StreamEvent{}, io.EOF
	}
	ev := r.events[r.pos]
	r.pos++
	return ev, nil
}

func (r *sliceReader) Close() error { return nil }

var _ llm.StreamReader = (*sliceReader)(nil)

// denyAllGate is an app.ApprovalGate that denies every ask immediately, so a loop
// that reaches the human gate never blocks the deterministic tests. The text-only
// golden path never raises an ask.
type denyAllGate struct{}

func newDenyAllGate() *denyAllGate { return &denyAllGate{} }

func (denyAllGate) Request(_ context.Context, _ app.ApprovalRequest) (domain.AskResolution, error) {
	return domain.AskDenied, nil
}

func (denyAllGate) Resolve(_ context.Context, _, _ string, _ domain.AskResolution) error { return nil }

var _ app.ApprovalGate = (*denyAllGate)(nil)

// allowAllHooks is a no-op app.HookRunner that allows every lifecycle event.
type allowAllHooks struct{}

func newAllowAllHooks() *allowAllHooks { return &allowAllHooks{} }

func (allowAllHooks) Run(_ context.Context, _ app.HookInput) (app.HookDecision, error) {
	return app.HookDecision{Allow: true}, nil
}

var _ app.HookRunner = (*allowAllHooks)(nil)
