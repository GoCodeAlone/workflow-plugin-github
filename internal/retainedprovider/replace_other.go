//go:build !linux

package retainedprovider

import "errors"

func replaceProcess(string, []string, []string) error {
	return errors.New("retained foreground execution is supported only on Linux")
}
