//go:build !darwin && !linux && !windows

package retainedprovider

import (
	"fmt"
	"io/fs"
	"os"
)

func openNoFollowFile(string, fs.FileMode) (*os.File, error) {
	return nil, fmt.Errorf("secure no-follow file open is unsupported on this platform")
}

func lockFile(*os.File) error {
	return fmt.Errorf("install locking is unsupported on this platform")
}

func unlockFile(*os.File) error { return nil }
