//go:build !windows

package internal

import (
	"errors"
	"fmt"
	"os"
)

func syncJITOwnershipJournalDirectoryPlatform(root *os.Root, _ string) error {
	directory, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open journal directory: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}
