// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// erroringOutlet fails its first failN Sink calls with a distinct error each,
// then succeeds. It is not a BatchOutlet, so the reservoir takes the per-payload
// dispatch path where errors must be joined.
type erroringOutlet struct {
	mu     sync.Mutex
	errs   []error
	called int
}

func (o *erroringOutlet) Sink(_ context.Context, _ any) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	i := o.called
	o.called++
	if i < len(o.errs) {
		return o.errs[i]
	}
	return nil
}

// TestReservoirNonBatchJoinsAllErrors verifies the per-payload dispatch path
// joins every error rather than keeping only the last one.
func TestReservoirNonBatchJoinsAllErrors(t *testing.T) {
	t.Parallel()

	err1 := errors.New("first failed")
	err2 := errors.New("second failed")
	out := &erroringOutlet{errs: []error{err1, err2}}
	r := Reservoir(out, WithBatchSize(0), WithBatchInterval(0)).(*reservoir)

	_ = r.Sink(context.Background(), "a")
	_ = r.Sink(context.Background(), "b")
	err := r.Flush(context.Background())
	if err == nil {
		t.Fatal("Flush() = nil, want a joined error")
	}
	if !errors.Is(err, err1) {
		t.Errorf("joined error does not contain the first error: %v", err)
	}
	if !errors.Is(err, err2) {
		t.Errorf("joined error does not contain the second error: %v", err)
	}
}

// TestReservoirSinkAfterShutdownPassesThrough verifies a payload sunk after
// Shutdown is dispatched synchronously to inner rather than stranded in the
// buffer with no loop left to flush it.
func TestReservoirSinkAfterShutdownPassesThrough(t *testing.T) {
	t.Parallel()

	bucket := NewBucket()
	r := Reservoir(bucket, WithBatchSize(100), WithBatchInterval(0)).(*reservoir)
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if err := r.Sink(context.Background(), "late"); err != nil {
		t.Fatalf("Sink() after Shutdown error = %v", err)
	}
	all := bucket.All()
	if len(all) != 1 || all[0] != "late" {
		t.Fatalf("inner has %v, want the post-shutdown payload delivered immediately", all)
	}
}

// TestReservoirSinkAfterShutdownSurfacesError verifies the pass-through path
// still returns inner's error to the caller.
func TestReservoirSinkAfterShutdownSurfacesError(t *testing.T) {
	t.Parallel()

	boom := errors.New("inner down")
	out := &erroringOutlet{errs: []error{boom}}
	r := Reservoir(out, WithBatchSize(100), WithBatchInterval(0)).(*reservoir)
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := r.Sink(context.Background(), "late"); !errors.Is(err, boom) {
		t.Fatalf("Sink() after Shutdown = %v, want %v", err, boom)
	}
}
