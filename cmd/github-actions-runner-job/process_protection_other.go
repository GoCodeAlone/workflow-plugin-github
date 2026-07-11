//go:build !linux

package main

func protectProviderProcessSecrets() error {
	return nil
}
