//go:build linux

package main

import "golang.org/x/sys/unix"

func protectProviderProcessSecrets() error {
	return unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0)
}
