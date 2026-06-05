package cluster

import (
	"math"
	"testing"
	"time"
)

// TestBackoffDelay_ClampsOnUncappedOverflow verifies that with no max set (max==0)
// a large restart count with factor>1 does not overflow the time.Duration
// conversion into a negative or wrapped delay: the result is clamped to
// math.MaxInt64. Without the clamp, float64(initial)*factor^n exceeds the int64
// range and time.Duration(d) is undefined, which would fire a backoff immediately
// or never.
func TestBackoffDelay_ClampsOnUncappedOverflow(t *testing.T) {
	pol := backoffPolicy{maxRestarts: 1000, initial: time.Second, max: 0, factor: 2.0}
	// 2^60 seconds is far beyond math.MaxInt64 nanoseconds.
	got := backoffDelay(pol, 60)
	if got != time.Duration(math.MaxInt64) {
		t.Fatalf("uncapped overflow delay = %d, want clamped to MaxInt64 (%d)", got, int64(math.MaxInt64))
	}
	if got < 0 {
		t.Fatalf("delay must never be negative, got %d", got)
	}
}

// TestBackoffDelay_RespectsExplicitMax confirms a set max caps growth well below
// the overflow clamp, so the clamp only governs the uncapped case.
func TestBackoffDelay_RespectsExplicitMax(t *testing.T) {
	pol := backoffPolicy{maxRestarts: 1000, initial: time.Second, max: 5 * time.Second, factor: 2.0}
	if got := backoffDelay(pol, 60); got != 5*time.Second {
		t.Fatalf("capped delay = %v, want 5s", got)
	}
}

// TestBackoffDelay_FloorsAtInitial confirms a shrinking factor never produces a
// delay below initial.
func TestBackoffDelay_FloorsAtInitial(t *testing.T) {
	pol := backoffPolicy{maxRestarts: 10, initial: time.Second, max: 10 * time.Second, factor: 0.5}
	if got := backoffDelay(pol, 5); got != time.Second {
		t.Fatalf("floored delay = %v, want 1s", got)
	}
}
