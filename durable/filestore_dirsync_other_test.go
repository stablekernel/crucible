//go:build !windows

package durable

import (
	"testing"
)

// TestSyncDir_NonexistentPathErrors confirms syncDir surfaces an error when the
// directory cannot be opened, rather than silently returning nil. This covers the
// os.Open error branch that a valid FileStore path never hits.
func TestSyncDir_NonexistentPathErrors(t *testing.T) {
	if err := syncDir("/nonexistent/path/that/cannot/exist/for/this/test"); err == nil {
		t.Fatal("syncDir on a nonexistent path returned nil, want an error")
	}
}
