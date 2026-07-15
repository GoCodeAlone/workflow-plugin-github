//go:build darwin || linux

package retainedprovider

import (
	"fmt"
	"os"
)

func validateStateMode(info os.FileInfo) error {
	if info.Mode().Perm() != 0o600 {
		return fmt.Errorf("state file mode must be 0600")
	}
	return nil
}
