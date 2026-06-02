// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestManifoldSinkFansOutToAll(t *testing.T) {
	t.Parallel()

	b1, b2 := sink.NewBucket(), sink.NewBucket()
	m := sink.NewManifold(sink.WithOutlets(b1)).Attach(b2)
	m.Sink(context.Background(), "p")

	if len(b1.All()) != 1 || len(b2.All()) != 1 {
		t.Fatalf("fan-out reached b1=%d b2=%d, want 1 each", len(b1.All()), len(b2.All()))
	}
}

func TestManifoldSinkContinuesAfterFailure(t *testing.T) {
	t.Parallel()

	var firstCalled, thirdCalled bool
	failing := sink.OutletFunc(func(context.Context, any) error { return errors.New("boom") })
	first := sink.OutletFunc(func(context.Context, any) error { firstCalled = true; return nil })
	third := sink.OutletFunc(func(context.Context, any) error { thirdCalled = true; return nil })

	m := sink.NewManifold().Attach(first, failing, third)
	m.Sink(context.Background(), "p") // must not panic; must reach third

	if !firstCalled || !thirdCalled {
		t.Fatalf("first=%v third=%v, want both true (a failing outlet must not stop the others)", firstCalled, thirdCalled)
	}
}

func TestManifoldSinkSkipsUnregisteredSilently(t *testing.T) {
	t.Parallel()

	skip := sink.OutletFunc(func(context.Context, any) error { return sink.ErrUnregistered })
	m := sink.NewManifold().Attach(skip)
	m.Sink(context.Background(), "p") // ErrUnregistered must not be treated as a failure (no panic, no error path)
}

type flushOutlet struct {
	*sink.Bucket
	flushed int
	err     error
}

func (f *flushOutlet) Flush(context.Context) error { f.flushed++; return f.err }

type shutdownOutlet struct {
	*sink.Bucket
	shutdowns int
}

func (s *shutdownOutlet) Shutdown(context.Context) error { s.shutdowns++; return nil }

func TestManifoldFlushCallsFlushers(t *testing.T) {
	t.Parallel()

	f := &flushOutlet{Bucket: sink.NewBucket()}
	m := sink.NewManifold().Attach(sink.NewBucket(), f)
	if err := m.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}
	if f.flushed != 1 {
		t.Fatalf("flusher called %d times, want 1", f.flushed)
	}
}

func TestManifoldFlushJoinsErrors(t *testing.T) {
	t.Parallel()

	want := errors.New("flush failed")
	f := &flushOutlet{Bucket: sink.NewBucket(), err: want}
	m := sink.NewManifold().Attach(f)
	if err := m.Flush(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Flush() = %v, want %v", err, want)
	}
}

func TestManifoldShutdownFlushesThenShutsDown(t *testing.T) {
	t.Parallel()

	f := &flushOutlet{Bucket: sink.NewBucket()}
	s := &shutdownOutlet{Bucket: sink.NewBucket()}
	m := sink.NewManifold().Attach(f, s)
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if f.flushed != 1 || s.shutdowns != 1 {
		t.Fatalf("flushed=%d shutdowns=%d, want 1 each", f.flushed, s.shutdowns)
	}
}

func TestManifoldCloseDelegatesToShutdown(t *testing.T) {
	t.Parallel()

	s := &shutdownOutlet{Bucket: sink.NewBucket()}
	m := sink.NewManifold().Attach(s)
	if err := m.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if s.shutdowns != 1 {
		t.Fatalf("shutdowns = %d, want 1", s.shutdowns)
	}
}
