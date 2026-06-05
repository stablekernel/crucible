//go:build windows

package durable

// syncDir is a no-op on Windows. Opening a directory handle and calling
// FlushFileBuffers (Sync) on it returns "Access is denied" on Windows. The
// directory-entry durability POSIX needs from this call is covered on Windows by
// the file rename combined with the file's own fsync already performed in
// writeAtomic, so skipping the directory sync is correct for Windows crash
// durability and avoids the access-denied failure.
func syncDir(string) error { return nil }
