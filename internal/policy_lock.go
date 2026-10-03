package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
)

const runnerPolicyLockFile = "runner-policy.lock"

func (r *runnerPolicyReconciler) lockAudit(ctx context.Context, root *os.Root) (*flock.Flock, error) {
	file, err := root.OpenFile(runnerPolicyLockFile, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err == nil {
		if err := file.Close(); err != nil {
			return nil, errors.New("policy lock file could not be closed")
		}
	} else if !errors.Is(err, os.ErrExist) {
		return nil, errors.New("policy lock file could not be created")
	}
	info, err := root.Lstat(runnerPolicyLockFile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("policy lock must be an owner-only regular file")
	}
	// Keep one stable inode across processes and commands. Never remove the
	// sidecar: the OS releases its lock when the holder exits, including crashes.
	lock := flock.New(filepath.Join(r.dir, runnerPolicyLockFile), flock.SetFlag(os.O_RDWR))
	locked, err := lock.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil || !locked {
		_ = lock.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("policy audit lock could not be acquired")
	}
	return lock, nil
}
