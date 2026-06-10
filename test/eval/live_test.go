//go:build livesmoke

// Package eval live-smoke tier (DOD-04). It is build-tagged so it is EXCLUDED
// from the default unit suite and the per-PR eval gate; run it explicitly with:
//
//	go test -tags livesmoke ./test/eval/...
//
// It drives the REAL agent loop end-to-end against a REAL model-gateway provider
// adapter (Anthropic, OpenAI/Responses, or any OpenAI-compatible self-hosted
// endpoint such as Ollama/vLLM), wired from environment variables, and SKIPS when
// no provider key is configured — so it is safe to leave in the tree and never
// makes a network call without an explicit key.
//
// The single live task is a minimal "coding" exercise: the model is given an
// in-memory workspace with `write` and `read` tools and asked to write a small Go
// function to a file and then read it back to confirm. This exercises real tool
// selection and the loop's permission/scheduling/round-trip path against genuine
// provider output, catching adapter drift that the deterministic scenarios cannot.
//
// Provider selection (every configured provider runs as its OWN subtest, each
// skipping independently when its key/endpoint is absent — so a fully-keyed run
// demonstrates hosted AND self-hosted providers in one pass; DOD-04):
//
//   - anthropic:     ANTHROPIC_API_KEY (model: ANTHROPIC_SMOKE_MODEL or a default)
//   - openai-compat: OPENAI_API_KEY + OPENAI_BASE_URL -> OpenAI-compatible endpoint
//     (self-hosted: Ollama/vLLM)
//   - openai:        OPENAI_API_KEY with NO OPENAI_BASE_URL -> native OpenAI
//     (Responses API). When a base URL is set the key belongs to the
//     self-hosted endpoint, so the native subtest skips rather than aim a
//     non-OpenAI key at api.openai.com.
package eval

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/anthropic"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/openai"
	"github.com/boltrope/boltrope/internal/modelgateway/adapter/outbound/providers/openaicompat"
	"github.com/boltrope/boltrope/internal/orchestrator/app"
	"github.com/boltrope/boltrope/internal/orchestrator/app/agent"
	"github.com/boltrope/boltrope/internal/orchestrator/app/apptest"
	"github.com/boltrope/boltrope/internal/orchestrator/domain"
	"github.com/boltrope/boltrope/internal/orchestrator/policy"
	"github.com/boltrope/boltrope/internal/platform/clock"
	"github.com/boltrope/boltrope/internal/platform/ids"
)

// liveProviderSpec is one provider tier of the live smoke suite: a stable
// subtest name plus a resolver that builds the provider from the environment or
// SKIPS the subtest when its key/endpoint is absent. Each resolved provider
// satisfies [app.ModelGatewayPort] (its method set matches), so it feeds the
// real loop directly with no adapter.
type liveProviderSpec struct {
	name    string
	resolve func(t *testing.T) (app.ModelGatewayPort, string)
}

// liveProviders enumerates EVERY provider the live tier knows how to wire, in a
// stable order. Each runs as its own subtest and skips individually, so a
// fully-keyed environment exercises hosted and self-hosted providers in one
// pass instead of first-configured-wins (DOD-04).
func liveProviders() []liveProviderSpec {
	return []liveProviderSpec{
		{name: "anthropic", resolve: resolveAnthropic},
		{name: "openai-compat", resolve: resolveOpenAICompat},
		{name: "openai", resolve: resolveOpenAI},
	}
}

// resolveAnthropic wires the hosted Anthropic provider, or skips when no key is
// set.
func resolveAnthropic(t *testing.T) (app.ModelGatewayPort, string) {
	t.Helper()
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY not set; skipping anthropic live smoke")
	}
	model := envOr("ANTHROPIC_SMOKE_MODEL", "claude-haiku-4-5")
	return anthropic.New(anthropic.WithAPIKey(key), anthropic.WithDefaultMaxTokens(1024)), model
}

// resolveOpenAICompat wires the OpenAI-COMPATIBLE adapter against a configured
// base URL — the path for self-hosted endpoints (Ollama, vLLM, LM Studio, …) —
// or skips when the key/endpoint pair is incomplete.
func resolveOpenAICompat(t *testing.T) (app.ModelGatewayPort, string) {
	t.Helper()
	key := os.Getenv("OPENAI_API_KEY")
	base := os.Getenv("OPENAI_BASE_URL")
	if key == "" || base == "" {
		t.Skip("OPENAI_API_KEY + OPENAI_BASE_URL not both set; skipping openai-compatible (self-hosted) live smoke")
	}
	p, err := openaicompat.New(openaicompat.Config{BaseURL: base, APIKey: key})
	if err != nil {
		t.Fatalf("live: build openai-compatible provider: %v", err)
	}
	return p, envOr("OPENAI_SMOKE_MODEL", "llama3")
}

// resolveOpenAI wires the native OpenAI provider (Responses API by default), or
// skips when no key is set. A configured OPENAI_BASE_URL means the key belongs
// to a self-hosted endpoint (the openai-compat subtest runs it), so the native
// subtest skips rather than send that key to api.openai.com.
func resolveOpenAI(t *testing.T) (app.ModelGatewayPort, string) {
	t.Helper()
	key := os.Getenv("OPENAI_API_KEY")
	if key == "" {
		t.Skip("OPENAI_API_KEY not set; skipping openai live smoke")
	}
	if os.Getenv("OPENAI_BASE_URL") != "" {
		t.Skip("OPENAI_BASE_URL set: OPENAI_API_KEY targets the openai-compat subtest; skipping native openai")
	}
	p, err := openai.New(openai.Config{APIKey: key})
	if err != nil {
		t.Fatalf("live: build openai provider: %v", err)
	}
	return p, envOr("OPENAI_SMOKE_MODEL", "gpt-4o-mini")
}

// envOr returns the env var named key, or def when unset/empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestLive_CodingTask drives ONE real coding task end-to-end per configured
// provider: write a Go function to a file with `write`, then read it back with
// `read`. EVERY provider tier runs as its own subtest (skipping individually
// when unconfigured; DOD-04), each asserting the run terminates without an
// infrastructural error, the loop performed at least one model round-trip, and
// (best-effort) that the model engaged the tools — exercising the full loop
// against genuine model output.
func TestLive_CodingTask(t *testing.T) {
	for _, spec := range liveProviders() {
		t.Run(spec.name, func(t *testing.T) {
			provider, model := spec.resolve(t)
			runLiveCodingTask(t, spec.name, provider, model)
		})
	}
}

// runLiveCodingTask runs the live coding exercise against one resolved provider,
// with its own in-memory workspace and a per-provider session id so subtests
// never share state.
func runLiveCodingTask(t *testing.T, name string, provider app.ModelGatewayPort, model string) {
	t.Helper()

	ws := newMemWorkspace()

	loop := agent.NewLoop(agent.Deps{
		EventLog:  apptest.NewFakeEventLog(),
		Model:     provider,
		Tools:     ws, // a real (in-memory) tool runtime: write/read actually mutate/read files
		Approvals: liveAllowGate{},
		Hooks:     apptest.NewFakeHookRunner(),
		Policy:    mustAllowAllEngine(t),
		Clock:     clock.System{},
		IDs:       ids.System{},
		// No budget cost function: the live task is small and the budget cap is off.
	}, agent.Config{
		Model:    model,
		MaxTurns: 8,
		Mode:     policy.ModeDefault,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	task := "You have two tools: `write(path, content)` writes a file, and `read(path)` reads it back. " +
		"Write a Go function named Add that returns the sum of two ints to the file add.go, " +
		"then read add.go back to confirm it was written. When done, reply with a short confirmation."

	res, err := loop.Run(ctx, agent.RunInput{
		SessionID:   "live-coding-task-" + name,
		UserMessage: userMessage(task),
	})
	if err != nil {
		t.Fatalf("live: loop run returned an infrastructural error: %v", err)
	}

	t.Logf("live run finished: reason=%s turns=%d usage=%+v writes=%d reads=%d",
		res.Reason, res.NumTurns, res.Usage, ws.writeCount(), ws.readCount())

	// The run must reach a real terminal state (not hang); a refusal or a cap is a
	// legitimate model outcome we surface but do not fail on, since live model
	// behavior varies. We DO require that the loop actually called the model.
	if res.NumTurns < 1 {
		t.Errorf("live: expected at least one model round-trip, got %d turns", res.NumTurns)
	}
	if res.Reason == "" {
		t.Errorf("live: run produced no terminal reason")
	}

	// Best-effort: a capable model should have written the file. Log rather than
	// hard-fail on tool engagement so the smoke test is robust across providers,
	// but assert the workspace is consistent if a write did happen.
	if ws.writeCount() == 0 {
		t.Logf("live: model did not call write; tool engagement not exercised this run (provider/model dependent)")
	} else if got, ok := ws.get("add.go"); !ok || !strings.Contains(got, "Add") {
		t.Errorf("live: add.go after write = %q (ok=%v); want content mentioning Add", got, ok)
	}
}

// ----------------------------------------------------------------------------
// liveAllowGate / mustAllowAllEngine — permissive wiring for the live task.
// ----------------------------------------------------------------------------

// liveAllowGate auto-allows any ask. The live task's policy allows all tools, so
// no ask should be raised; this is a safety net that keeps the smoke test from
// blocking if one is.
type liveAllowGate struct{}

func (liveAllowGate) Request(context.Context, app.ApprovalRequest) (domain.AskResolution, error) {
	return domain.AskAllowed, nil
}

func (liveAllowGate) Resolve(context.Context, string, string, domain.AskResolution) error {
	return nil
}

// mustAllowAllEngine builds the real policy engine with a single catch-all allow
// rule so the live coding task's tools dispatch without prompting.
func mustAllowAllEngine(t *testing.T) policy.PolicyEngine {
	t.Helper()
	eng, err := policy.NewEngine(policy.Config{
		RuleSet: policy.RuleSet{Rules: []policy.Rule{{ID: "allow-all", Effect: policy.EffectAllow}}},
	})
	if err != nil {
		t.Fatalf("live: build policy engine: %v", err)
	}
	return eng
}

// ----------------------------------------------------------------------------
// memWorkspace — a real, in-memory ToolRuntimePort with working write/read tools.
// ----------------------------------------------------------------------------

// memWorkspace is an [app.ToolRuntimePort] backed by an in-memory file map. Its
// `write` and `read` tools actually mutate and read that map, so the live task is
// a genuine (if tiny) coding loop: the model's tool calls have real effects and
// real observations, end to end, without touching the host filesystem or a
// sandbox. It is concurrency-safe (the loop may dispatch read-only tools in
// parallel).
type memWorkspace struct {
	mu     sync.Mutex
	files  map[string]string
	writes int
	reads  int
}

func newMemWorkspace() *memWorkspace {
	return &memWorkspace{files: make(map[string]string)}
}

// liveToolSchema is the shared input schema for write/read: a required string
// `path` and (for write) a `content` field. additionalProperties is permitted so
// minor provider argument quirks do not fail validation upstream.
const (
	writeSchema = `{"type":"object","properties":{"path":{"type":"string"},"content":{"type":"string"}},"required":["path","content"]}`
	readSchema  = `{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}`
)

func (w *memWorkspace) ExecuteTool(_ context.Context, exec app.ToolExecution) (app.ToolStream, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	path, _ := exec.Call.Args["path"].(string)
	switch exec.Call.Name {
	case "write":
		content, _ := exec.Call.Args["content"].(string)
		if path == "" {
			return apptest.NewFakeToolStream(app.ToolResult{IsError: true, Content: "write: missing path"}), nil
		}
		w.files[path] = content
		w.writes++
		return apptest.NewFakeToolStream(app.ToolResult{Content: "wrote " + path}), nil
	case "read":
		w.reads++
		content, ok := w.files[path]
		if !ok {
			return apptest.NewFakeToolStream(app.ToolResult{IsError: true, Content: "read: no such file: " + path}), nil
		}
		return apptest.NewFakeToolStream(app.ToolResult{Content: content}), nil
	default:
		return apptest.NewFakeToolStream(app.ToolResult{IsError: true, Content: "unknown tool: " + exec.Call.Name}), nil
	}
}

func (w *memWorkspace) ListTools(context.Context, string) ([]app.ToolDescriptor, error) {
	return []app.ToolDescriptor{
		{
			Name:        "write",
			Description: "Write a file. Arguments: path (string), content (string).",
			JSONSchema:  []byte(writeSchema),
			SideEffect:  domain.SideEffectMutating,
			EgressClass: domain.EgressClassNone,
		},
		{
			Name:        "read",
			Description: "Read a file and return its contents. Argument: path (string).",
			JSONSchema:  []byte(readSchema),
			SideEffect:  domain.SideEffectReadOnly,
			EgressClass: domain.EgressClassNone,
		},
	}, nil
}

func (w *memWorkspace) get(path string) (string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.files[path]
	return v, ok
}

func (w *memWorkspace) writeCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writes
}

func (w *memWorkspace) readCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reads
}

// Compile-time assertions: the provider adapters satisfy the loop's model port
// directly (their method set matches app.ModelGatewayPort), and memWorkspace
// satisfies the tool-runtime port.
var (
	_ app.ToolRuntimePort  = (*memWorkspace)(nil)
	_ app.ModelGatewayPort = (*anthropic.Provider)(nil)
	_ app.ModelGatewayPort = (*openai.Provider)(nil)
	_ app.ModelGatewayPort = (*openaicompat.Provider)(nil)
)
