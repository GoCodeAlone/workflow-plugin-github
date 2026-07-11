//go:build !linux && !darwin

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func openWorkloadArtifact(root, relative string) (*os.File, error) {
	current := root
	for _, component := range strings.Split(relative, "/") {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return nil, fmt.Errorf("inspect workload artifact path %q: %w", relative, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("workload artifact path %q must not contain symbolic links", relative)
		}
	}
	return os.Open(current)
}
