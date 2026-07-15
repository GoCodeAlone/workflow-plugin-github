//go:build !darwin && !linux

package retainedprovider

func syncDirectory(string) error { return nil }
