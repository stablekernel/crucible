//go:build !windows

package durable

import (
	"fmt"
	"os"
)

// syncDir fsyncs a directory so a rename or create within it is durable across a
// crash. After an atomic rename the file's bytes are fsync'd (by writeAtomic), but
// the directory entry that records the rename also needs to be flushed to survive a
// crash on POSIX systems; opening the directory and calling Sync achieves this.
// Windows does not support opening a directory handle for sync (FlushFileBuffers on
// a directory-backed handle returns Access Denied), so the Windows build uses a
// no-op instead — the file rename plus the file's own fsync are sufficient for
// Windows crash durability.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("crucible/durable: opening dir %q to sync: %w", dir, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("crucible/durable: syncing dir %q: %w", dir, err)
	}
	return nil
}
