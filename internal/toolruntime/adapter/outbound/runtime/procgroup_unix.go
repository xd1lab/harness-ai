//go:build unix

package runtime

import (
	"os/exec"
	"syscall"
)

// setProcessGroup configures cmd to run in its own process group (Setpgid) so the
// whole group — not just the leader — can be signaled on cancellation. This is the
// host-side complement to the in-container `docker kill` reaper (architecture §9.3).
func setProcessGroup(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return nil
}

// signalGroup signals cmd's process group. When kill is true it sends SIGKILL,
// otherwise SIGTERM. When the process was started in its own group (groupSet), the
// signal is delivered to the negative PID so every member receives it; otherwise it
// falls back to signaling the leader alone. A nil/exited process is a no-op.
func signalGroup(cmd *exec.Cmd, groupSet, kill bool) error {
	if cmd.Process == nil {
		return nil
	}
	sig := syscall.SIGTERM
	if kill {
		sig = syscall.SIGKILL
	}
	if groupSet {
		// Negative PID targets the entire process group (architecture §9.3).
		if err := syscall.Kill(-cmd.Process.Pid, sig); err == nil {
			return nil
		}
	}
	return syscall.Kill(cmd.Process.Pid, sig)
}
