//go:build linux || darwin

package main

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"
)

func openWorkloadArtifact(root, relative string) (*os.File, error) {
	current, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open workload root: %w", err)
	}
	parts := strings.Split(relative, "/")
	for _, component := range parts[:len(parts)-1] {
		next, openErr := unix.Openat(current, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		_ = unix.Close(current)
		if openErr != nil {
			return nil, workloadArtifactOpenError(relative, openErr)
		}
		current = next
	}
	fd, openErr := unix.Openat(current, parts[len(parts)-1], unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	_ = unix.Close(current)
	if openErr != nil {
		return nil, workloadArtifactOpenError(relative, openErr)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("stat workload artifact path %q: %w", relative, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("workload artifact path %q must be a regular file", relative)
	}
	return os.NewFile(uintptr(fd), relative), nil
}

func workloadArtifactOpenError(relative string, err error) error {
	if errors.Is(err, unix.ELOOP) || errors.Is(err, unix.ENOTDIR) {
		return fmt.Errorf("workload artifact path %q must not contain symbolic links: %w", relative, err)
	}
	return fmt.Errorf("open workload artifact path %q: %w", relative, err)
}
