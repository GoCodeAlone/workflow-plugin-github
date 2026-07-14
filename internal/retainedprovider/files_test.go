package retainedprovider

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAtomicWriteJSONUsesRestrictiveRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state", "active.json")
	value := Status{ProtocolVersion: StatusProtocolVersion, Installed: true}
	if err := AtomicWriteJSON(path, value); err != nil {
		t.Fatalf("atomic write: %v", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat active state: %v", err)
	}
	if !info.Mode().IsRegular() || (runtime.GOOS != "windows" && info.Mode().Perm() != 0o600) {
		t.Fatalf("active state mode = %v", info.Mode())
	}
	var got Status
	if err := ReadStrictJSONFile(path, &got); err != nil {
		t.Fatalf("strict read: %v", err)
	}
	if !got.Installed {
		t.Fatalf("active state = %+v", got)
	}
	value.ServiceActive = true
	if err := AtomicWriteJSON(path, value); err != nil {
		t.Fatalf("atomic replacement: %v", err)
	}
	if err := ReadStrictJSONFile(path, &got); err != nil || !got.ServiceActive {
		t.Fatalf("replacement state = %+v err=%v", got, err)
	}

	if err := os.Remove(path); err != nil {
		t.Fatalf("remove state: %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "outside"), path); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := AtomicWriteJSON(path, value); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink destination err = %v", err)
	}
}

func TestDurableDirectoryCreationSyncsEachNewParentEntry(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "one", "two")
	var synced []string
	if err := mkdirAllDurableWithSync(target, 0o700, func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}); err != nil {
		t.Fatalf("create durable directory: %v", err)
	}
	want := []string{base, filepath.Join(base, "one")}
	if strings.Join(synced, "\n") != strings.Join(want, "\n") {
		t.Fatalf("directory sync order = %v want %v", synced, want)
	}
}

func TestReadStrictJSONFileRejectsUnknownAndOversizedData(t *testing.T) {
	dir := t.TempDir()
	unknown := filepath.Join(dir, "unknown.json")
	if err := os.WriteFile(unknown, []byte(`{"protocol_version":"retained-provider.status.v1","installed":true,"unknown":1}`), 0o600); err != nil {
		t.Fatalf("write unknown: %v", err)
	}
	var status Status
	if err := ReadStrictJSONFile(unknown, &status); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field err = %v", err)
	}
	oversized := filepath.Join(dir, "oversized.json")
	if err := os.WriteFile(oversized, []byte(strings.Repeat("x", MaxStateFileBytes+1)), 0o600); err != nil {
		t.Fatalf("write oversized: %v", err)
	}
	if err := ReadStrictJSONFile(oversized, &status); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized err = %v", err)
	}
}

func TestReadStrictJSONFileRejectsSymlinksAndPermissiveModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}
	dir := t.TempDir()
	permissive := filepath.Join(dir, "permissive.json")
	if err := os.WriteFile(permissive, []byte(`{"protocol_version":"retained-provider.status.v1"}`), 0o644); err != nil {
		t.Fatalf("write permissive state: %v", err)
	}
	var status Status
	if err := ReadStrictJSONFile(permissive, &status); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("permissive mode err = %v", err)
	}

	secure := filepath.Join(dir, "secure.json")
	if err := os.WriteFile(secure, []byte(`{"protocol_version":"retained-provider.status.v1"}`), 0o600); err != nil {
		t.Fatalf("write secure state: %v", err)
	}
	linked := filepath.Join(dir, "linked.json")
	if err := os.Symlink(secure, linked); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := ReadStrictJSONFile(linked, &status); err == nil || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("symlink state err = %v", err)
	}
}

func TestValidateUserPathRejectsSymlinkedAncestorAndOutsideHome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permission varies on Windows")
	}
	home := t.TempDir()
	realDir := filepath.Join(home, "real")
	if err := os.Mkdir(realDir, 0o700); err != nil {
		t.Fatalf("mkdir real: %v", err)
	}
	alias := filepath.Join(home, "alias")
	if err := os.Symlink(realDir, alias); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	if err := ValidateUserPath(home, filepath.Join(alias, "state.json"), false); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlink ancestor err = %v", err)
	}
	if err := ValidateUserPath(home, filepath.Join(filepath.Dir(home), "outside"), false); err == nil || !strings.Contains(err.Error(), "home") {
		t.Fatalf("outside-home err = %v", err)
	}
}

func TestValidateUserPathRejectsSymlinkedHomeAndWritableAuthority(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix directory authority semantics")
	}
	root := t.TempDir()
	realHome := filepath.Join(root, "real-home")
	if err := os.Mkdir(realHome, 0o700); err != nil {
		t.Fatalf("mkdir real home: %v", err)
	}
	linkedHome := filepath.Join(root, "linked-home")
	if err := os.Symlink(realHome, linkedHome); err != nil {
		t.Fatalf("symlink home: %v", err)
	}
	if err := ValidateUserPath(linkedHome, filepath.Join(linkedHome, "state.json"), false); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("symlinked home err = %v", err)
	}

	secureHome := filepath.Join(root, "secure-home")
	if err := os.Mkdir(secureHome, 0o700); err != nil {
		t.Fatalf("mkdir secure home: %v", err)
	}
	if err := os.Chmod(secureHome, 0o777); err != nil {
		t.Fatalf("make home writable: %v", err)
	}
	if err := ValidateUserPath(secureHome, filepath.Join(secureHome, "state.json"), false); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("writable home err = %v", err)
	}
	if err := os.Chmod(secureHome, 0o700); err != nil {
		t.Fatalf("restore home mode: %v", err)
	}
	writable := filepath.Join(secureHome, "writable")
	if err := os.Mkdir(writable, 0o700); err != nil {
		t.Fatalf("mkdir writable ancestor: %v", err)
	}
	if err := os.Chmod(writable, 0o777); err != nil {
		t.Fatalf("make ancestor writable: %v", err)
	}
	if err := ValidateUserPath(secureHome, filepath.Join(writable, "state.json"), false); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("writable ancestor err = %v", err)
	}
}

func TestDurableDirectoryCreationRejectsWritableExistingAncestor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix directory authority semantics")
	}
	base := t.TempDir()
	authority := filepath.Join(base, "authority")
	if err := os.Mkdir(authority, 0o700); err != nil {
		t.Fatalf("mkdir authority: %v", err)
	}
	if err := os.Chmod(authority, 0o777); err != nil {
		t.Fatalf("make authority writable: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(authority, 0o700) })
	target := filepath.Join(authority, "managed", "state")
	if err := mkdirAllDurableWithSync(target, 0o700, func(string) error { return nil }); err == nil || !strings.Contains(err.Error(), "writable") {
		t.Fatalf("writable durable ancestor err = %v", err)
	}
	if _, err := os.Lstat(filepath.Join(authority, "managed")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("durable creation mutated insecure authority: %v", err)
	}
}

func TestCloneRegularTreeCopiesOnlyBoundedRegularFiles(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	destination := filepath.Join(t.TempDir(), "destination")
	if err := os.MkdirAll(filepath.Join(source, "nested"), 0o700); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "state.json"), []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := CloneRegularTree(source, destination, CloneLimits{MaxFiles: 10, MaxBytes: 1024}); err != nil {
		t.Fatalf("clone: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(destination, "nested", "state.json"))
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("cloned data=%q err=%v", data, err)
	}

	symlinkSource := filepath.Join(t.TempDir(), "symlink-source")
	if err := os.Mkdir(symlinkSource, 0o700); err != nil {
		t.Fatalf("mkdir symlink source: %v", err)
	}
	if err := os.Symlink(filepath.Join(source, "nested", "state.json"), filepath.Join(symlinkSource, "link")); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	if err := CloneRegularTree(symlinkSource, filepath.Join(t.TempDir(), "rejected"), CloneLimits{MaxFiles: 10, MaxBytes: 1024}); err == nil || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("symlink clone err = %v", err)
	}
	if err := CloneRegularTree(source, filepath.Join(t.TempDir(), "too-small"), CloneLimits{MaxFiles: 10, MaxBytes: 1}); err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("byte limit err = %v", err)
	}
}

func TestCloneRegularTreeSyncsCreatedDirectoriesBottomUp(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source")
	if err := os.MkdirAll(filepath.Join(source, "nested", "deep"), 0o700); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "deep", "state.json"), []byte(`{"ok":true}`), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	destinationParent := t.TempDir()
	destination := filepath.Join(destinationParent, "destination")
	var synced []string
	if err := cloneRegularTreeWithSync(source, destination, CloneLimits{MaxFiles: 10, MaxBytes: 1024}, func(path string) error {
		synced = append(synced, filepath.Clean(path))
		return nil
	}); err != nil {
		t.Fatalf("clone: %v", err)
	}
	want := []string{
		filepath.Join(destination, "nested", "deep"),
		filepath.Join(destination, "nested"),
		destination,
		destinationParent,
	}
	if strings.Join(synced, "\n") != strings.Join(want, "\n") {
		t.Fatalf("directory sync order = %v want %v", synced, want)
	}
}

func TestInstallLockIsExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.lock")
	first, err := AcquireInstallLock(path)
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	defer func() { _ = first.Release() }()
	if _, err := AcquireInstallLock(path); err == nil || !errors.Is(err, ErrInstallLocked) {
		t.Fatalf("second lock err = %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release first: %v", err)
	}
	second, err := AcquireInstallLock(path)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release second: %v", err)
	}
}

func TestInstallLockRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink permission varies on Windows")
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "target.lock")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatalf("write lock target: %v", err)
	}
	linked := filepath.Join(dir, "linked.lock")
	if err := os.Symlink(target, linked); err != nil {
		t.Fatalf("symlink lock: %v", err)
	}
	if _, err := AcquireInstallLock(linked); err == nil || !strings.Contains(err.Error(), "regular") {
		t.Fatalf("symlink lock err = %v", err)
	}
}

func TestLifecycleLockRemainsExclusiveWhileInstallRootIsPurged(t *testing.T) {
	home := t.TempDir()
	paths := LifecyclePathsFor(validTestConfig(home))
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatalf("mkdir install root: %v", err)
	}
	lock, err := AcquireInstallLock(paths.InstallLock)
	if err != nil {
		t.Fatalf("acquire lifecycle lock: %v", err)
	}
	defer func() { _ = lock.Release() }()
	if err := os.RemoveAll(paths.Root); err != nil {
		t.Fatalf("purge install root: %v", err)
	}
	contender, err := AcquireInstallLock(paths.InstallLock)
	if contender != nil {
		_ = contender.Release()
	}
	if !errors.Is(err, ErrInstallLocked) {
		t.Fatalf("contender acquired replacement lock inode: %v", err)
	}
}

func TestStatusJSONHasStableShape(t *testing.T) {
	data, err := json.Marshal(Status{ProtocolVersion: StatusProtocolVersion})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(data), `"protocol_version":"retained-provider.status.v1"`) {
		t.Fatalf("status JSON = %s", data)
	}
}
