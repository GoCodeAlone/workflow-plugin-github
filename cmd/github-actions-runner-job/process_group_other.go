//go:build !linux && !darwin

package main

import "os/exec"

func configureRunnerProcessGroup(_ *exec.Cmd) {}

func forceKillRunnerProcessGroup(cmd *exec.Cmd) error {
	if cmd == nil || cmd.Process == nil {
		return nil
	}
	return cmd.Process.Kill()
}
