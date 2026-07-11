//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

func prepareRunnerProcessIsolation() error {
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		return fmt.Errorf("enable runner child subreaper: %w", err)
	}
	return nil
}

func cleanupDetachedRunnerProcesses() error {
	pids, err := runnerChildPIDs()
	if err != nil || len(pids) == 0 {
		return err
	}
	cleanupErr := signalRunnerChildren(pids, unix.SIGTERM)
	if waitForRunnerChildren(500 * time.Millisecond) {
		return cleanupErr
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		pids, err = runnerChildPIDs()
		if err != nil {
			return err
		}
		if len(pids) == 0 {
			return nil
		}
		cleanupErr = errors.Join(cleanupErr, signalRunnerChildren(pids, unix.SIGKILL))
		if time.Now().After(deadline) {
			return errors.Join(cleanupErr, fmt.Errorf("detached runner processes survived cleanup: %v", pids))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForRunnerChildren(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		pids, err := runnerChildPIDs()
		if err == nil && len(pids) == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runnerChildPIDs() ([]int, error) {
	reapRunnerChildren()
	tasks, err := os.ReadDir("/proc/self/task")
	if err != nil {
		return nil, fmt.Errorf("list runner process tasks: %w", err)
	}
	seen := map[int]struct{}{}
	for _, task := range tasks {
		data, readErr := os.ReadFile(filepath.Join("/proc/self/task", task.Name(), "children"))
		if readErr != nil {
			if errors.Is(readErr, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("read runner child processes: %w", readErr)
		}
		for _, field := range strings.Fields(string(data)) {
			pid, parseErr := strconv.Atoi(field)
			if parseErr == nil && pid > 0 && pid != os.Getpid() {
				seen[pid] = struct{}{}
			}
		}
	}
	pids := make([]int, 0, len(seen))
	for pid := range seen {
		pids = append(pids, pid)
	}
	return pids, nil
}

func reapRunnerChildren() {
	for {
		pid, err := unix.Wait4(-1, nil, unix.WNOHANG, nil)
		if pid <= 0 || err != nil {
			return
		}
	}
}

func signalRunnerChildren(pids []int, signal unix.Signal) error {
	var result error
	for _, pid := range pids {
		if err := unix.Kill(pid, signal); err != nil && !errors.Is(err, unix.ESRCH) {
			result = errors.Join(result, fmt.Errorf("signal detached runner process %d: %w", pid, err))
		}
	}
	return result
}
