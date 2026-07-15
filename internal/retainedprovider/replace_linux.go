//go:build linux

package retainedprovider

import "syscall"

func replaceProcess(path string, args, environment []string) error {
	return syscall.Exec(path, args, environment)
}
