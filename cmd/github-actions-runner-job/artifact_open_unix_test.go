//go:build linux || darwin

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestT916OpenWorkloadArtifactRejectsFIFOWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "_work", "repo", "repo"), 0o700); err != nil {
		t.Fatalf("create workload directory: %v", err)
	}
	fifo := filepath.Join(root, "_work", "repo", "repo", "result.pipe")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatalf("create FIFO: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		file, err := openWorkloadArtifact(root, "_work/repo/repo/result.pipe")
		if file != nil {
			_ = file.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("FIFO artifact error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		writer, err := unix.Open(fifo, unix.O_WRONLY|unix.O_NONBLOCK, 0)
		if err == nil {
			_ = unix.Close(writer)
		}
		t.Fatal("FIFO artifact open blocked")
	}
}
