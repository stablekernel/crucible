package durable

import (
	"testing"
)

// TestCheckFormat_HoldsRecordsToTheCheckpointFormat pins what a Store that drops
// Record.Format meets. checkFormat fails on a Record of a lower format than its
// checkpoint's, and accepts a higher one, as every version since Record.Format has.
func TestCheckFormat_HoldsRecordsToTheCheckpointFormat(t *testing.T) {
	if err := checkFormat(1, &Record{Format: 2}); err != nil {
		t.Errorf("checkFormat(1, a Record of format 2) = %v, want nil", err)
	}
	if err := checkFormat(2, &Record{Format: 1}); err == nil {
		t.Errorf("checkFormat(2, a Record of format 1) = nil, want the Store-lost-field error")
	}
}
