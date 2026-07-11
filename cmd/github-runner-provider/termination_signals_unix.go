//go:build unix

package main

import (
	"os"
	"syscall"
)

func providerTerminationSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}
