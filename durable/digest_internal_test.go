package durable

import "testing"

// TestRecordDigest_Golden pins the digest format. A retried Append after an upgrade is
// compared against digests already on disk, so this value must not change.
func TestRecordDigest_Golden(t *testing.T) {
	got, err := recordDigest(Record{Step: 1, Event: []byte(`"go"`)})
	if err != nil {
		t.Fatalf("recordDigest: %v", err)
	}
	if want := "b56f3a37e67187ccd02a5b465d0c6533fb4ef737b6dd135641c3d34c38e491b7"; got != want {
		t.Fatalf("recordDigest = %s, want %s", got, want)
	}
}
