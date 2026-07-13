//go:build !darwin && !linux

package retainedprovider

import "os"

func validateOwner(os.FileInfo) error { return nil }
