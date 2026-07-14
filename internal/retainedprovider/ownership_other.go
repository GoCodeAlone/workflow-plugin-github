//go:build !darwin && !linux

package retainedprovider

import (
	"fmt"
	"os"
)

func validateOwner(os.FileInfo) error { return nil }

func validateManagedPathAuthority(info os.FileInfo) error {
	return validateOwner(info)
}

func validateExecutableAuthority(info os.FileInfo) error {
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("executable must not be group- or world-writable")
	}
	return nil
}
