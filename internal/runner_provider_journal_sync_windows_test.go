//go:build windows

package internal

import (
	"os"
	"testing"
)

func TestT916RunnerProviderWindowsDirectorySyncUsesWriteCapableHandle(t *testing.T) {
	stateDir := t.TempDir()
	root, err := os.OpenRoot(stateDir)
	if err != nil {
		t.Fatalf("open state root: %v", err)
	}
	defer root.Close()

	if err := syncJITOwnershipJournalDirectoryPlatform(root, stateDir); err != nil {
		t.Fatalf("sync Windows state directory: %v", err)
	}
}
