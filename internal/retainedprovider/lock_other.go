//go:build !darwin && !linux && !windows

package retainedprovider

import (
	"fmt"
	"os"
)

func lockFile(*os.File) error {
	return fmt.Errorf("install locking is unsupported on this platform")
}

func unlockFile(*os.File) error { return nil }
