//go:build !linux

package main

func prepareRunnerProcessIsolation() error { return nil }

func cleanupDetachedRunnerProcesses() error { return nil }
