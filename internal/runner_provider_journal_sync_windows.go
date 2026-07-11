//go:build windows

package internal

import (
	"errors"
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func syncJITOwnershipJournalDirectoryPlatform(root *os.Root, stateDir string) error {
	path, err := windows.UTF16PtrFromString(stateDir)
	if err != nil {
		return fmt.Errorf("encode journal directory path: %w", err)
	}
	handle, err := windows.CreateFile(
		path,
		windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		return fmt.Errorf("open journal directory for sync: %w", err)
	}
	directory := os.NewFile(uintptr(handle), stateDir)
	if directory == nil {
		_ = windows.CloseHandle(handle)
		return errors.New("open journal directory for sync: invalid handle")
	}
	directoryInfo, statErr := directory.Stat()
	rootInfo, rootStatErr := root.Stat(".")
	if err := errors.Join(statErr, rootStatErr); err != nil {
		_ = directory.Close()
		return fmt.Errorf("verify journal directory sync handle: %w", err)
	}
	if !os.SameFile(directoryInfo, rootInfo) {
		_ = directory.Close()
		return errors.New("journal directory changed while opening sync handle")
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if err := errors.Join(syncErr, closeErr); err != nil {
		return fmt.Errorf("sync journal directory: %w", err)
	}
	return nil
}
