// SPDX-License-Identifier: Apache-2.0

//go:build integration

package main

// ADR-0029 (AC-6 / AC-7 / AC-10) — RED, integration-tagged. The actually-runs-bash
// proof: with --enable-local-exec the resolved tool port executes a real shell
// command inside an isolated Docker sandbox (per-session container, --network none,
// cgroup/PID limits via runtime.DefaultConfig), while the DEFAULT no-exec port
// REFUSES bash and never runs it on the host.
//
// This test requires a reachable Docker daemon and the configured tool-runtime
// sandbox image (BOLTROPE_TOOLRT_IMAGE). It is compiled only under the
// `integration` build tag and skips when Docker is unavailable, so the default
// `go test ./...` gate stays hermetic.
//
// It is RED today because resolveTools and the new --enable-local-exec flag do not
// exist yet.

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"strings"
	"testing"
	"time"

	orchapp "github.com/xd1lab/harness-ai/internal/orchestrator/app"
	"github.com/xd1lab/harness-ai/internal/platform/llm"
)

// requireDocker skips the test when the docker CLI / daemon is not reachable.
func requireDocker(t *testing.T) {
	t.Helper()
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skipf("docker not available, skipping integration test: %v", err)
	}
}

// drainTerminal drives a ToolStream to its terminal result.
func drainTerminal(t *testing.T, s orchapp.ToolStream) orchapp.ToolResult {
	t.Helper()
	t.Cleanup(func() { _ = s.Close() })
	var last orchapp.ToolResult
	for {
		ev, err := s.Recv()
		if errors.Is(err, io.EOF) {
			return last
		}
		if err != nil {
			t.Fatalf("stream Recv: %v", err)
		}
		if ev.Result != nil {
			last = *ev.Result
		}
	}
}

// TestLocalExec_RunsBashInDockerSandbox is the load-bearing integration proof: the
// local-exec tool port runs a real `echo` inside the Docker sandbox and returns its
// stdout, demonstrating actual (isolated) execution (AC-6 / AC-7).
func TestLocalExec_RunsBashInDockerSandbox(t *testing.T) {
	requireDocker(t)

	cfg, err := parseRunFlags([]string{"--enable-local-exec"})
	if err != nil {
		t.Fatalf("parseRunFlags: %v", err)
	}
	tools, err := resolveTools(cfg, noEnv())
	if err != nil {
		t.Skipf("local-exec runtime unavailable (image/daemon?): %v", err)
	}
	if _, isNoExec := tools.(*Runtime); isNoExec {
		t.Fatal("with --enable-local-exec the tool port must NOT be the no-exec *Runtime")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const marker = "boltrope-local-exec-ok"
	stream, err := tools.ExecuteTool(ctx, orchapp.ToolExecution{
		SessionID:      "it-sess-1",
		Call:           llm.ToolCall{ID: "c1", Name: "bash", Args: map[string]any{"command": "echo " + marker}},
		IdempotencyKey: "it-idem-1",
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	res := drainTerminal(t, stream)
	if res.IsError {
		t.Fatalf("bash in sandbox returned an error result: %q", res.Content)
	}
	if !strings.Contains(res.Content, marker) {
		t.Fatalf("expected sandbox stdout to contain %q, got %q", marker, res.Content)
	}
}

// TestDefaultNoExec_RefusesBash asserts that WITHOUT --enable-local-exec the
// default no-exec tool port refuses bash with an error result and never executes
// it (AC-1 contrast with AC-6).
func TestDefaultNoExec_RefusesBash(t *testing.T) {
	cfg, err := parseRunFlags(nil)
	if err != nil {
		t.Fatalf("parseRunFlags: %v", err)
	}
	tools, err := resolveTools(cfg, noEnv())
	if err != nil {
		t.Fatalf("resolveTools (default): %v", err)
	}
	if _, isNoExec := tools.(*Runtime); !isNoExec {
		t.Fatal("default tool port must be the no-exec *Runtime")
	}

	stream, err := tools.ExecuteTool(context.Background(), orchapp.ToolExecution{
		SessionID: "it-sess-2",
		Call:      llm.ToolCall{ID: "c2", Name: "bash", Args: map[string]any{"cmd": "echo should-not-run"}},
	})
	if err != nil {
		t.Fatalf("ExecuteTool: %v", err)
	}
	res := drainTerminal(t, stream)
	if !res.IsError {
		t.Fatal("the no-exec runtime must refuse bash with an error result")
	}
	if !strings.Contains(strings.ToLower(res.Content), "disabled") {
		t.Fatalf("the no-exec refusal must explain it is disabled; got %q", res.Content)
	}
}
