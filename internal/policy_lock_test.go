package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunnerPolicyLockWaitCancellationAndStableFile(t *testing.T) {
	f := newPolicyAPIFake(t)
	dir := t.TempDir()
	r := policyReconcilerFixture(t, f, dir)
	root, err := r.openAudit()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	lock, err := r.lockAudit(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	before, err := root.Lstat(runnerPolicyLockFile)
	if err != nil || before.Mode().Perm()&0077 != 0 {
		t.Fatalf("unsafe lock file: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	other := policyReconcilerFixture(t, f, dir)
	if _, err := other.Plan(ctx, "wait", runnerPolicyFixture()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lock wait did not honor cancellation: %v", err)
	}
	if _, err := root.Stat(runnerPolicyAuditFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled lock wait appended authority")
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Plan(context.Background(), "wait", runnerPolicyFixture()); err != nil {
		t.Fatal(err)
	}
	after, err := root.Lstat(runnerPolicyLockFile)
	if err != nil || !os.SameFile(before, after) {
		t.Fatal("unlock/reacquire replaced the shared lock inode")
	}
}

func TestRunnerPolicyUnsafeLockDenied(t *testing.T) {
	for _, mode := range []string{"directory", "symlink", "public"} {
		t.Run(mode, func(t *testing.T) {
			f := newPolicyAPIFake(t)
			dir := t.TempDir()
			path := filepath.Join(dir, runnerPolicyLockFile)
			var err error
			switch mode {
			case "directory":
				err = os.Mkdir(path, 0700)
			case "symlink":
				err = os.Symlink(filepath.Join(t.TempDir(), "outside"), path)
			case "public":
				err = os.WriteFile(path, nil, 0600)
				if err == nil {
					err = os.Chmod(path, 0644)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			r := policyReconcilerFixture(t, f, dir)
			if _, err := r.Plan(context.Background(), "unsafe-lock", runnerPolicyFixture()); err == nil {
				t.Fatal("unsafe process lock accepted")
			}
			if _, err := os.Stat(filepath.Join(dir, runnerPolicyAuditFile)); !errors.Is(err, os.ErrNotExist) || len(f.writes) != 0 {
				t.Fatal("lock denial appended authority or mutated GitHub")
			}
		})
	}
}
