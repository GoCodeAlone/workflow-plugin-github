//go:build darwin || linux

package retainedprovider

import (
	"errors"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

func openNoFollowFile(path string, mode fs.FileMode) (*os.File, error) {
	descriptor, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, uint32(mode.Perm()))
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	file := os.NewFile(uintptr(descriptor), path)
	if file == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("convert no-follow file descriptor")
	}
	return file, nil
}

func lockFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return ErrInstallLocked
	}
	return err
}

func unlockFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
