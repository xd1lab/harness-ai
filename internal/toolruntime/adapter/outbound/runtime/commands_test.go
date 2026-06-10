package runtime

import (
	"strconv"
	"testing"
	"time"

	"github.com/boltrope/boltrope/internal/toolruntime/app"
)

func TestCreateArgs_HardLimitsAndDenyByDefaultNetwork(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MemoryBytes = 256 * 1024 * 1024
	cfg.CPUs = 0.5
	cfg.PidsLimit = 64
	cfg.Image = "debian:stable-slim"
	cfg.Workdir = "/workspace"

	args := cfg.createArgs("boltrope-sbx-s1", app.EgressPolicy{SessionID: "s1"})

	if got := args[0]; got != "create" {
		t.Fatalf("subcommand = %q, want create", got)
	}
	// Deny-by-default network.
	if got := argValue(args, "--network"); got != networkNone {
		t.Errorf("--network = %q, want %q (deny-by-default)", got, networkNone)
	}
	// Hard resource limits present and exact.
	if got := argValue(args, "--memory"); got != strconv.FormatInt(cfg.MemoryBytes, 10) {
		t.Errorf("--memory = %q, want %d", got, cfg.MemoryBytes)
	}
	if got := argValue(args, "--cpus"); got != "0.5" {
		t.Errorf("--cpus = %q, want 0.5", got)
	}
	if got := argValue(args, "--pids-limit"); got != strconv.FormatInt(cfg.PidsLimit, 10) {
		t.Errorf("--pids-limit = %q, want %d", got, cfg.PidsLimit)
	}
	// Workspace working dir.
	if got := argValue(args, "--workdir"); got != "/workspace" {
		t.Errorf("--workdir = %q, want /workspace", got)
	}
	// --init so double-forked zombies are reaped inside the namespace.
	if !hasArg(args, "--init") {
		t.Errorf("expected --init in create args, got %v", args)
	}
	// Image and the long-lived sleep entrypoint are the final positional args.
	if args[len(args)-3] != "debian:stable-slim" || args[len(args)-2] != "sleep" || args[len(args)-1] != "infinity" {
		t.Errorf("tail args = %v, want [... debian:stable-slim sleep infinity]", args[len(args)-3:])
	}
}

func TestCreateArgs_NetworkIsNoneEvenWithAllowlist(t *testing.T) {
	// v1: host allowlisting is enforced by the egress broker, not by handing the
	// container a bridge. The container network stays deny-by-default "none".
	cfg := DefaultConfig()
	args := cfg.createArgs("boltrope-sbx-s2", app.EgressPolicy{SessionID: "s2", AllowedHosts: []string{"example.com"}})
	if got := argValue(args, "--network"); got != networkNone {
		t.Errorf("--network = %q, want %q even with an allowlist", got, networkNone)
	}
}

func TestCreateArgs_ExtraCreateArgsAppended(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ExtraCreateArgs = []string{"--read-only", "--cap-drop=ALL"}
	args := cfg.createArgs("boltrope-sbx-s3", app.EgressPolicy{SessionID: "s3"})
	if !hasArg(args, "--read-only") || !hasArg(args, "--cap-drop=ALL") {
		t.Errorf("extra create args not appended: %v", args)
	}
}

func TestExecArgs_WorkdirEnvAndStdinFlag(t *testing.T) {
	req := app.ExecRequest{
		Cmd:     []string{"echo", "hi"},
		WorkDir: "/tmp",
		Env:     []string{"FOO=bar", "BAZ=qux"},
		Stdin:   []byte("input"),
	}
	args := execArgs("boltrope-sbx-s1", "/workspace", req)

	if args[0] != "exec" {
		t.Fatalf("subcommand = %q, want exec", args[0])
	}
	if !hasArg(args, "--interactive") {
		t.Errorf("expected --interactive when stdin present, got %v", args)
	}
	if got := argValue(args, "--workdir"); got != "/tmp" {
		t.Errorf("--workdir = %q, want /tmp (req overrides default)", got)
	}
	if got := argValue(args, "--env"); got != "FOO=bar" {
		t.Errorf("first --env = %q, want FOO=bar", got)
	}
	// The container name then the command must be the trailing args, in order.
	if args[len(args)-3] != "boltrope-sbx-s1" || args[len(args)-2] != "echo" || args[len(args)-1] != "hi" {
		t.Errorf("tail = %v, want [boltrope-sbx-s1 echo hi]", args[len(args)-3:])
	}
}

func TestExecArgs_DefaultWorkdirWhenUnset(t *testing.T) {
	args := execArgs("c", "/workspace", app.ExecRequest{Cmd: []string{"ls"}})
	if got := argValue(args, "--workdir"); got != "/workspace" {
		t.Errorf("--workdir = %q, want /workspace default", got)
	}
	if hasArg(args, "--interactive") {
		t.Errorf("did not expect --interactive without stdin: %v", args)
	}
}

func TestKillArgs_Signal(t *testing.T) {
	term := killArgs("c1", "TERM")
	if term[0] != "kill" || !hasArg(term, "--signal=TERM") || term[len(term)-1] != "c1" {
		t.Errorf("kill TERM args = %v", term)
	}
	kill := killArgs("c1", "KILL")
	if !hasArg(kill, "--signal=KILL") {
		t.Errorf("kill KILL args = %v", kill)
	}
}

func TestRemoveArgs_ForceAndVolumes(t *testing.T) {
	args := removeArgs("c1")
	if args[0] != "rm" || !hasArg(args, "--force") || !hasArg(args, "--volumes") || args[len(args)-1] != "c1" {
		t.Errorf("rm args = %v, want [rm --force --volumes c1]", args)
	}
}

func TestContainerName_DeterministicAndSanitized(t *testing.T) {
	if got := containerName("sess-123"); got != "boltrope-sbx-sess-123" {
		t.Errorf("containerName = %q", got)
	}
	// Disallowed characters collapse to '-'; same input → same name.
	a := containerName("a/b:c d")
	b := containerName("a/b:c d")
	if a != b {
		t.Errorf("containerName not deterministic: %q vs %q", a, b)
	}
	if a != "boltrope-sbx-a-b-c-d" {
		t.Errorf("sanitized name = %q, want boltrope-sbx-a-b-c-d", a)
	}
	if containerName("") != "boltrope-sbx-default" {
		t.Errorf("empty session name = %q", containerName(""))
	}
}

func TestConfig_WithDefaultsAndValidate(t *testing.T) {
	c := Config{}.withDefaults()
	if c.Image != defaultImage || c.Workdir != defaultWorkdir || c.PidsLimit != defaultPidsLimit {
		t.Errorf("withDefaults did not fill: %+v", c)
	}
	if c.WallClock != defaultWallClock || c.KillGrace != defaultKillGrace {
		t.Errorf("withDefaults did not fill durations: %+v", c)
	}
	if err := c.validate(); err != nil {
		t.Errorf("default config invalid: %v", err)
	}
	// A negative explicit limit is rejected.
	if err := (Config{PidsLimit: -1}).validate(); err == nil {
		t.Error("expected validate error for negative PidsLimit")
	}
	// Idle > Absolute is rejected.
	if err := (Config{IdleTTL: 2 * time.Hour, AbsoluteTTL: time.Hour}).validate(); err == nil {
		t.Error("expected validate error for IdleTTL > AbsoluteTTL")
	}
}
