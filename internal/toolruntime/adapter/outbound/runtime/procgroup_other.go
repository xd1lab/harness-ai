//go:build !unix

package runtime

import "os/exec"

// setProcessGroup reports that host-side process-group signaling is unsupported on
// this platform (e.g. the Windows dev host). The container backend then relies
// solely on the `docker kill` reaper against the container's PID namespace, which is
// the guaranteed in-container reap regardless of host-side support (architecture
// §9.3). The integration adversarial-kill tests run on Linux where the reaper is
// exercised against real container processes.
func setProcessGroup(_ *exec.Cmd) error { return errProcessGroupUnsupported }

// signalGroup terminates the host-side command process. Without process-group
// support it can only target the leader; the in-container tree is reaped by
// `docker kill`. A nil/exited process is a no-op.
func signalGroup(cmd *exec.Cmd, _, _ bool) error {
	if cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
