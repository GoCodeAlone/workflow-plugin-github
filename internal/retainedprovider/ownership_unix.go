//go:build darwin || linux

package retainedprovider

import (
	"fmt"
	"os"
	"syscall"
)

func validateOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("ownership metadata is unavailable")
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("owner uid %d does not match current uid", stat.Uid)
	}
	return nil
}

func validateManagedPathAuthority(info os.FileInfo) error {
	if err := validateOwner(info); err != nil {
		return err
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("managed path must not be group- or world-writable")
	}
	return nil
}

func validateExecutableAuthority(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("ownership metadata is unavailable")
	}
	if stat.Uid != 0 && stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("executable owner uid %d is not trusted", stat.Uid)
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("executable must not be group- or world-writable")
	}
	return nil
}
