package runtime

import (
	"fmt"
	"time"
)

// Default lifecycle and limit values. They are deliberately conservative so a
// misconfigured deployment fails closed (small limits, short caps) rather than
// exposing the host (architecture §9.3, §10.6).
const (
	// defaultImage is the small Linux base image a session container is started
	// from when Config.Image is empty.
	defaultImage = "debian:stable-slim"

	// defaultWorkdir is the in-container working directory and the root of the
	// session workspace when Config.Workdir is empty.
	defaultWorkdir = "/workspace"

	// defaultMemoryBytes is the hard memory limit (`--memory`) when unset (512 MiB).
	defaultMemoryBytes int64 = 512 * 1024 * 1024

	// defaultCPUs is the hard CPU limit (`--cpus`) when unset (1.0 CPU). Docker's
	// --cpus expresses the number of CPUs as a decimal quota.
	defaultCPUs float64 = 1.0

	// defaultPidsLimit is the hard process-count limit (`--pids-limit`) when unset.
	// This is the host protection that bounds a fork bomb (architecture §9.3).
	defaultPidsLimit int64 = 256

	// defaultWallClock is the absolute wall-clock cap for a single Exec when unset.
	defaultWallClock = 5 * time.Minute

	// defaultKillGrace is the SIGTERM→SIGKILL escalation window for both the
	// host-side process group and the `docker kill` reaper (architecture §9.3).
	defaultKillGrace = 5 * time.Second

	// defaultIdleTTL is how long a workspace may sit without an Exec before the
	// reaper may destroy it (architecture §10.6).
	defaultIdleTTL = 15 * time.Minute

	// defaultAbsoluteTTL is the maximum total lifetime of a workspace regardless of
	// activity (architecture §10.6).
	defaultAbsoluteTTL = 60 * time.Minute

	// defaultMaxLive is the max number of concurrently live sandboxes per node; a
	// Create beyond this applies backpressure (architecture §10.6).
	defaultMaxLive = 32

	// defaultDockerBin is the docker CLI binary name resolved from PATH.
	defaultDockerBin = "docker"

	// defaultReapInterval is how often the reconciliation loop sweeps for sandboxes
	// to reap (architecture §10.6).
	defaultReapInterval = 30 * time.Second
)

// Config holds the tunable parameters of the container [Runtime]: the base image,
// the hard resource limits stamped onto every session container, the wall-clock cap
// per Exec, the SIGTERM→SIGKILL grace, and the sandbox lifecycle bounds. The zero
// value is not valid; build one with [DefaultConfig] and override fields, then pass
// to [New]. All durations and limits are validated by [Config.validate].
type Config struct {
	// DockerBin is the docker CLI binary (resolved from PATH if relative).
	DockerBin string
	// Image is the Linux base image a session container runs.
	Image string
	// Workdir is the in-container working directory / workspace root.
	Workdir string

	// MemoryBytes is the hard memory limit (`--memory`).
	MemoryBytes int64
	// CPUs is the hard CPU limit (`--cpus`, number of CPUs as a decimal).
	CPUs float64
	// PidsLimit is the hard process-count limit (`--pids-limit`); the host
	// protection against a fork bomb.
	PidsLimit int64

	// WallClock is the absolute wall-clock cap applied to every Exec; on expiry the
	// process tree is killed (architecture §9.3).
	WallClock time.Duration
	// KillGrace is the SIGTERM→SIGKILL escalation window for cancellation/kill.
	KillGrace time.Duration

	// IdleTTL is the per-sandbox idle timeout before reaping.
	IdleTTL time.Duration
	// AbsoluteTTL is the per-sandbox absolute lifetime cap.
	AbsoluteTTL time.Duration
	// MaxLive is the max concurrently live sandboxes (backpressure beyond it).
	MaxLive int
	// ReapInterval is the reconciliation-loop sweep period.
	ReapInterval time.Duration

	// ExtraCreateArgs are additional `docker create`/`run` flags appended verbatim
	// (e.g. `--read-only`, `--cap-drop=ALL`); never model-driven.
	ExtraCreateArgs []string
}

// DefaultConfig returns a Config populated with the conservative defaults. Callers
// override individual fields before passing it to [New].
func DefaultConfig() Config {
	return Config{
		DockerBin:    defaultDockerBin,
		Image:        defaultImage,
		Workdir:      defaultWorkdir,
		MemoryBytes:  defaultMemoryBytes,
		CPUs:         defaultCPUs,
		PidsLimit:    defaultPidsLimit,
		WallClock:    defaultWallClock,
		KillGrace:    defaultKillGrace,
		IdleTTL:      defaultIdleTTL,
		AbsoluteTTL:  defaultAbsoluteTTL,
		MaxLive:      defaultMaxLive,
		ReapInterval: defaultReapInterval,
	}
}

// withDefaults returns a copy of c with any zero-valued field replaced by its
// default, so a partially-populated Config is always usable.
func (c Config) withDefaults() Config {
	if c.DockerBin == "" {
		c.DockerBin = defaultDockerBin
	}
	if c.Image == "" {
		c.Image = defaultImage
	}
	if c.Workdir == "" {
		c.Workdir = defaultWorkdir
	}
	if c.MemoryBytes <= 0 {
		c.MemoryBytes = defaultMemoryBytes
	}
	if c.CPUs <= 0 {
		c.CPUs = defaultCPUs
	}
	if c.PidsLimit <= 0 {
		c.PidsLimit = defaultPidsLimit
	}
	if c.WallClock <= 0 {
		c.WallClock = defaultWallClock
	}
	if c.KillGrace <= 0 {
		c.KillGrace = defaultKillGrace
	}
	if c.IdleTTL <= 0 {
		c.IdleTTL = defaultIdleTTL
	}
	if c.AbsoluteTTL <= 0 {
		c.AbsoluteTTL = defaultAbsoluteTTL
	}
	if c.MaxLive <= 0 {
		c.MaxLive = defaultMaxLive
	}
	if c.ReapInterval <= 0 {
		c.ReapInterval = defaultReapInterval
	}
	return c
}

// validate reports a human-readable error if c carries a nonsensical value that
// cannot be salvaged by a default (e.g. a negative limit a caller set explicitly).
func (c Config) validate() error {
	if c.PidsLimit < 0 {
		return fmt.Errorf("runtime: PidsLimit must be > 0, got %d", c.PidsLimit)
	}
	if c.MemoryBytes < 0 {
		return fmt.Errorf("runtime: MemoryBytes must be > 0, got %d", c.MemoryBytes)
	}
	if c.CPUs < 0 {
		return fmt.Errorf("runtime: CPUs must be > 0, got %v", c.CPUs)
	}
	if c.AbsoluteTTL > 0 && c.IdleTTL > 0 && c.IdleTTL > c.AbsoluteTTL {
		return fmt.Errorf("runtime: IdleTTL (%s) must not exceed AbsoluteTTL (%s)", c.IdleTTL, c.AbsoluteTTL)
	}
	return nil
}
