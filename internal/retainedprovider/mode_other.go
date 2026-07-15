//go:build !darwin && !linux

package retainedprovider

import "os"

func validateStateMode(os.FileInfo) error { return nil }
