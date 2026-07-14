//go:build darwin || linux

package retainedprovider

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

func TestSecuritySensitiveOpenRejectsRacedSymlink(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(string, func(string, fs.FileMode) (*os.File, error)) (*os.File, error)
	}{
		{name: "install lock", open: openRegularLockFileWith},
		{name: "lifecycle audit", open: openLifecycleAuditWith},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			target := filepath.Join(directory, "target")
			const targetContents = "must remain untouched"
			if err := os.WriteFile(target, []byte(targetContents), 0o600); err != nil {
				t.Fatalf("write target: %v", err)
			}
			path := filepath.Join(directory, "managed")
			opener := func(path string, mode fs.FileMode) (*os.File, error) {
				if err := os.Symlink(target, path); err != nil {
					t.Fatalf("race symlink into place: %v", err)
				}
				return openNoFollowFile(path, mode)
			}
			file, err := tc.open(path, opener)
			if file != nil {
				_ = file.Close()
			}
			if err == nil {
				t.Fatal("security-sensitive open followed raced symlink")
			}
			contents, readErr := os.ReadFile(target)
			if readErr != nil {
				t.Fatalf("read target: %v", readErr)
			}
			if string(contents) != targetContents {
				t.Fatalf("target contents = %q", contents)
			}
		})
	}
}
