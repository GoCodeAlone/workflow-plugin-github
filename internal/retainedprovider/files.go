package retainedprovider

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const MaxStateFileBytes = 1 << 20

type CloneLimits struct {
	MaxFiles int
	MaxBytes int64
}

func AtomicWriteJSON(path string, value any) (returnErr error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > MaxStateFileBytes {
		return fmt.Errorf("encoded JSON exceeds %d bytes", MaxStateFileBytes)
	}
	directory := filepath.Dir(path)
	if err := mkdirAllDurable(directory, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	if err := rejectNonRegularDestination(path); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".retained-provider-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary state: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if removeErr := os.Remove(temporaryPath); returnErr == nil && removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			returnErr = fmt.Errorf("remove temporary state: %w", removeErr)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("restrict temporary state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		return fmt.Errorf("write temporary state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync temporary state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary state: %w", err)
	}
	if err := rejectNonRegularDestination(path); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace state: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	return nil
}

func mkdirAllDurable(path string, mode fs.FileMode) error {
	return mkdirAllDurableWithSync(path, mode, syncDirectory)
}

func mkdirAllDurableWithSync(path string, mode fs.FileMode, syncDir func(string) error) error {
	if path == "" || mode.Perm() != mode || mode&0o077 != 0 || syncDir == nil {
		return errors.New("durable directory path, mode, and sync are required")
	}
	path = filepath.Clean(path)
	missing := make([]string, 0, 4)
	ancestor := path
	for {
		info, err := os.Lstat(ancestor)
		if err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("durable directory ancestor must be a real directory: %s", ancestor)
			}
			if err := validateManagedPathAuthority(info); err != nil {
				return fmt.Errorf("durable directory ancestor authority: %w", err)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect durable directory ancestor: %w", err)
		}
		missing = append(missing, ancestor)
		parent := filepath.Dir(ancestor)
		if parent == ancestor {
			return errors.New("durable directory has no existing ancestor")
		}
		ancestor = parent
	}
	for index := len(missing) - 1; index >= 0; index-- {
		if err := os.Mkdir(missing[index], mode); err != nil {
			return fmt.Errorf("create durable directory: %w", err)
		}
		if err := syncDir(filepath.Dir(missing[index])); err != nil {
			return fmt.Errorf("sync durable directory parent: %w", err)
		}
	}
	return nil
}

func ReadStrictJSONFile(path string, target any) error {
	entry, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect state file: %w", err)
	}
	if !entry.Mode().IsRegular() {
		return fmt.Errorf("state file must be regular")
	}
	if err := validateStateMode(entry); err != nil {
		return err
	}
	if err := validateOwner(entry); err != nil {
		return fmt.Errorf("state file ownership: %w", err)
	}
	if entry.Size() > MaxStateFileBytes {
		return fmt.Errorf("state file exceeds %d bytes", MaxStateFileBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open state file: %w", err)
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened state file: %w", err)
	}
	if !opened.Mode().IsRegular() || !os.SameFile(entry, opened) {
		return fmt.Errorf("state file changed during open or is not regular")
	}
	if opened.Size() > MaxStateFileBytes {
		return fmt.Errorf("state file exceeds %d bytes", MaxStateFileBytes)
	}
	data, err := io.ReadAll(io.LimitReader(file, MaxStateFileBytes+1))
	if err != nil {
		return fmt.Errorf("read state file: %w", err)
	}
	if len(data) > MaxStateFileBytes {
		return fmt.Errorf("state file exceeds %d bytes", MaxStateFileBytes)
	}
	if err := decodeStrictJSON(bytes.NewReader(data), target); err != nil {
		return fmt.Errorf("decode state file: %w", err)
	}
	return nil
}

// ValidateUserPath enforces a lexical user-home boundary and rejects symlinks
// or untrusted ownership and writability in every existing component.
func ValidateUserPath(home, path string, requireExisting bool) error {
	if !filepath.IsAbs(home) || !filepath.IsAbs(path) {
		return fmt.Errorf("path and home must be absolute")
	}
	home = filepath.Clean(home)
	path = filepath.Clean(path)
	homeInfo, err := os.Lstat(home)
	if err != nil {
		return fmt.Errorf("inspect home authority: %w", err)
	}
	if !homeInfo.IsDir() || homeInfo.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("home authority must be a real directory without symlinks")
	}
	if err := validateManagedPathAuthority(homeInfo); err != nil {
		return fmt.Errorf("home authority: %w", err)
	}
	relative, err := filepath.Rel(home, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return fmt.Errorf("path must remain within home")
	}
	current := home
	if relative != "." {
		for _, component := range strings.Split(relative, string(filepath.Separator)) {
			current = filepath.Join(current, component)
			info, statErr := os.Lstat(current)
			if errors.Is(statErr, os.ErrNotExist) {
				if requireExisting {
					return fmt.Errorf("path does not exist: %s", current)
				}
				return nil
			}
			if statErr != nil {
				return fmt.Errorf("inspect path: %w", statErr)
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("path contains symlink: %s", current)
			}
			if err := validateManagedPathAuthority(info); err != nil {
				return fmt.Errorf("path authority: %w", err)
			}
		}
	}
	if requireExisting {
		if _, err := os.Lstat(path); err != nil {
			return fmt.Errorf("path does not exist: %w", err)
		}
	}
	return nil
}

func CloneRegularTree(source, destination string, limits CloneLimits) (returnErr error) {
	return cloneRegularTreeWithSync(source, destination, limits, syncDirectory)
}

func cloneRegularTreeWithSync(source, destination string, limits CloneLimits, syncDir func(string) error) (returnErr error) {
	if limits.MaxFiles <= 0 || limits.MaxBytes < 0 {
		return fmt.Errorf("clone limits must be positive")
	}
	if syncDir == nil {
		return fmt.Errorf("clone directory sync is required")
	}
	root, err := os.Lstat(source)
	if err != nil {
		return fmt.Errorf("inspect clone source: %w", err)
	}
	if !root.IsDir() || root.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("clone source must be a regular directory")
	}
	if _, err := os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return fmt.Errorf("clone destination already exists")
		}
		return fmt.Errorf("inspect clone destination: %w", err)
	}
	missingDirectories := make([]string, 0, 2)
	existingAncestor := destination
	for {
		info, err := os.Lstat(existingAncestor)
		if err == nil {
			if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("clone destination ancestor must be a regular directory")
			}
			if err := validateManagedPathAuthority(info); err != nil {
				return fmt.Errorf("clone destination ancestor authority: %w", err)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect clone destination ancestor: %w", err)
		}
		missingDirectories = append(missingDirectories, existingAncestor)
		parent := filepath.Dir(existingAncestor)
		if parent == existingAncestor {
			return fmt.Errorf("clone destination has no existing ancestor")
		}
		existingAncestor = parent
	}
	for index := len(missingDirectories) - 1; index >= 0; index-- {
		if err := os.Mkdir(missingDirectories[index], 0o700); err != nil {
			return fmt.Errorf("create clone destination: %w", err)
		}
	}
	defer func() {
		if returnErr != nil {
			_ = os.RemoveAll(destination)
		}
	}()
	files := 0
	var bytesCopied int64
	createdTreeDirectories := make([]string, 0)
	if err := filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		target := filepath.Join(destination, relative)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("clone source entries must be regular files or directories: %s", relative)
		}
		if info.IsDir() {
			if err := os.Mkdir(target, 0o700); err != nil {
				return err
			}
			createdTreeDirectories = append(createdTreeDirectories, target)
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("clone source entries must be regular files or directories: %s", relative)
		}
		files++
		if files > limits.MaxFiles {
			return fmt.Errorf("clone file limit exceeded")
		}
		if info.Size() > limits.MaxBytes-bytesCopied {
			return fmt.Errorf("clone byte limit exceeded")
		}
		if err := cloneRegularFile(path, target, info); err != nil {
			return err
		}
		bytesCopied += info.Size()
		return nil
	}); err != nil {
		return fmt.Errorf("clone regular tree: %w", err)
	}
	for index := len(createdTreeDirectories) - 1; index >= 0; index-- {
		if err := syncDir(createdTreeDirectories[index]); err != nil {
			return err
		}
	}
	for _, directory := range missingDirectories {
		if err := syncDir(directory); err != nil {
			return err
		}
	}
	return syncDir(existingAncestor)
}

func cloneRegularFile(source, destination string, expected os.FileInfo) (returnErr error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	opened, err := input.Stat()
	if err != nil || !opened.Mode().IsRegular() || !os.SameFile(expected, opened) {
		return fmt.Errorf("clone source file changed during open")
	}
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := output.Close(); returnErr == nil && closeErr != nil {
			returnErr = closeErr
		}
	}()
	if _, err := io.CopyN(output, input, expected.Size()); err != nil {
		return err
	}
	var extra [1]byte
	if count, err := input.Read(extra[:]); err != io.EOF || count != 0 {
		return fmt.Errorf("clone source file grew during copy")
	}
	return output.Sync()
}

func rejectNonRegularDestination(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect state destination: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("state destination must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("state destination must be regular")
	}
	if err := validateOwner(info); err != nil {
		return fmt.Errorf("state destination ownership: %w", err)
	}
	if err := validateStateMode(info); err != nil {
		return err
	}
	return nil
}

var ErrInstallLocked = errors.New("retained provider install is already locked")

type InstallLock struct {
	mu       sync.Mutex
	file     *os.File
	released bool
}

func AcquireInstallLock(path string) (*InstallLock, error) {
	if err := mkdirAllDurable(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create lock directory: %w", err)
	}
	file, err := openRegularLockFile(path)
	if err != nil {
		return nil, fmt.Errorf("open install lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("restrict install lock: %w", err)
	}
	if err := lockFile(file); err != nil {
		_ = file.Close()
		if errors.Is(err, ErrInstallLocked) {
			return nil, ErrInstallLocked
		}
		return nil, fmt.Errorf("acquire install lock: %w", err)
	}
	return &InstallLock{file: file}, nil
}

func openRegularLockFile(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err == nil {
		if !before.Mode().IsRegular() {
			return nil, fmt.Errorf("install lock must be a regular file")
		}
		if err := validateOwner(before); err != nil {
			return nil, fmt.Errorf("install lock ownership: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	after, lstatErr := os.Lstat(path)
	if statErr != nil || lstatErr != nil || !opened.Mode().IsRegular() || !after.Mode().IsRegular() || !os.SameFile(opened, after) || (before != nil && !os.SameFile(before, opened)) {
		_ = file.Close()
		return nil, fmt.Errorf("install lock must remain the same regular file during open")
	}
	if err := validateOwner(after); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("install lock ownership: %w", err)
	}
	return file, nil
}

func (lock *InstallLock) Release() error {
	if lock == nil {
		return nil
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.released {
		return nil
	}
	lock.released = true
	unlockErr := unlockFile(lock.file)
	closeErr := lock.file.Close()
	return errors.Join(unlockErr, closeErr)
}
