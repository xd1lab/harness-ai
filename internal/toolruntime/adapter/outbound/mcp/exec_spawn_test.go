package mcp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

// The tests in this file cover the REAL subprocess path — execSpawn and
// execTransport — by compiling the tiny stdio MCP server under
// testdata/echoserver once and launching it through the client's default
// spawn, exactly as a deployment launches an operator-configured server.

var (
	echoBuildOnce sync.Once
	echoBuildDir  string
	echoBuildBin  string
	echoBuildErr  error
)

// TestMain cleans up the compiled echoserver helper after the package's tests
// have run (the lazily-built binary lives outside any single test's TempDir).
func TestMain(m *testing.M) {
	code := m.Run()
	if echoBuildDir != "" {
		_ = os.RemoveAll(echoBuildDir)
	}
	os.Exit(code)
}

// goTool locates the go command: PATH first, then $GOROOT/bin (the test binary
// inherits the toolchain env `go test` ran under).
func goTool() (string, error) {
	if p, err := exec.LookPath("go"); err == nil {
		return p, nil
	}
	if root := os.Getenv("GOROOT"); root != "" {
		candidate := filepath.Join(root, "bin", "go")
		if runtime.GOOS == "windows" {
			candidate += ".exe"
		}
		if _, err := os.Stat(candidate); err == nil { //nolint:gosec // GOROOT is the trusted toolchain root, not attacker input
			return candidate, nil
		}
	}
	return "", errors.New("go tool found neither on PATH nor under GOROOT")
}

// buildEchoServer compiles testdata/echoserver once per test process and
// returns the binary path. The BUILD runs with the full parent environment
// (the compiler is not the confined process); only the spawned server is
// environment-scrubbed.
func buildEchoServer(t *testing.T) string {
	t.Helper()
	echoBuildOnce.Do(func() {
		tool, err := goTool()
		if err != nil {
			echoBuildErr = err
			return
		}
		dir, err := os.MkdirTemp("", "mcp-echoserver-")
		if err != nil {
			echoBuildErr = err
			return
		}
		echoBuildDir = dir
		bin := filepath.Join(dir, "echoserver")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		cmd := exec.Command(tool, "build", "-o", bin, "./testdata/echoserver") //nolint:gosec // builds the in-repo test fixture with the trusted toolchain
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			echoBuildErr = fmt.Errorf("build echoserver: %w\n%s", err, out)
			return
		}
		echoBuildBin = bin
	})
	if echoBuildErr != nil {
		// Without a toolchain the real-subprocess path cannot be exercised at
		// all; skip loudly rather than fake coverage. CI always has go.
		t.Skipf("cannot build echoserver helper: %v", echoBuildErr)
	}
	return echoBuildBin
}

// TestExecSpawn_RealSubprocessRoundTrip drives ListTools and CallTool through
// the default os/exec spawn against a real child process: the full initialize
// handshake, tools/list, tools/call, and the Close teardown all run over the
// child's actual stdin/stdout (the execTransport Read/Write/Close path).
func TestExecSpawn_RealSubprocessRoundTrip(t *testing.T) {
	bin := buildEchoServer(t)
	c := New(WithServer("real", bin, nil, nil))
	ref := app.MCPServerRef{Name: "real", Transport: "stdio"}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	specs, err := c.ListTools(ctx, ref)
	if err != nil {
		t.Fatalf("ListTools over a real subprocess: %v", err)
	}
	if len(specs) != 2 || specs[0].Name != "echo" {
		t.Fatalf("want the echoserver catalog (echo, env), got %+v", specs)
	}

	obs, err := c.CallTool(ctx, ref, "sess", "echo", map[string]any{"msg": "hi"})
	if err != nil {
		t.Fatalf("CallTool over a real subprocess: %v", err)
	}
	if obs.IsError {
		t.Fatalf("unexpected error observation: %+v", obs)
	}
	if !strings.Contains(obs.Content, `"msg":"hi"`) {
		t.Fatalf("the subprocess must echo the arguments back, got %q", obs.Content)
	}
}

// TestExecSpawn_EnvironmentConfinement is the ADR-0013 confinement property on
// the REAL exec path: the spawned server sees ONLY the explicitly configured
// environment — a canary set in the parent process must not leak in, and the
// configured variable must arrive.
func TestExecSpawn_EnvironmentConfinement(t *testing.T) {
	bin := buildEchoServer(t)

	// Plant a canary in the PARENT environment. If execSpawn inherited
	// os.Environ() the child would see it.
	t.Setenv("MCP_LEAK_CANARY", "leaked-from-parent")

	c := New(WithServer("real", bin, nil, []string{"MCP_CANARY=42"}))
	ref := app.MCPServerRef{Name: "real", Transport: "stdio"}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	obs, err := c.CallTool(ctx, ref, "sess", "env", nil)
	if err != nil {
		t.Fatalf("CallTool(env): %v", err)
	}
	if !strings.Contains(obs.Content, "MCP_CANARY=42") {
		t.Fatalf("the configured env must reach the subprocess, got:\n%s", obs.Content)
	}
	if strings.Contains(obs.Content, "MCP_LEAK_CANARY") {
		t.Fatalf("the parent environment leaked into the spawned MCP server:\n%s", obs.Content)
	}
}

// TestExecSpawn_NoCommandConfigured asserts a server name with no registered
// launch configuration is refused with an actionable error instead of
// exec'ing an empty command.
func TestExecSpawn_NoCommandConfigured(t *testing.T) {
	c := New() // default spawn, nothing registered

	_, err := c.ListTools(context.Background(), app.MCPServerRef{Name: "unconfigured", Transport: "stdio"})
	if err == nil {
		t.Fatalf("an unconfigured server must be refused")
	}
	if !strings.Contains(err.Error(), "no command configured") {
		t.Fatalf("want a no-command-configured error, got %v", err)
	}
}

// TestExecSpawn_StartFailure asserts a configured command that cannot be
// started (the binary does not exist) surfaces as a wrapped spawn error.
func TestExecSpawn_StartFailure(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "definitely-not-here")
	if runtime.GOOS == "windows" {
		missing += ".exe"
	}
	c := New(WithServer("ghost", missing, nil, nil))

	_, err := c.ListTools(context.Background(), app.MCPServerRef{Name: "ghost", Transport: "stdio"})
	if err == nil {
		t.Fatalf("starting a nonexistent binary must fail")
	}
	if !strings.Contains(err.Error(), "spawn server") {
		t.Fatalf("want a wrapped spawn error, got %v", err)
	}
}
