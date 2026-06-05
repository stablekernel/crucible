// SPDX-License-Identifier: Apache-2.0

package sinktest

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

// flushOutlet is a conforming Outlet whose Flush behavior is configurable, so
// the harness's Flusher branch can be driven into both its error and panic
// arms.
type flushOutlet struct {
	flushErr   error
	flushPanic bool
}

func (o *flushOutlet) Sink(_ context.Context, _ any) error { return sink.ErrUnregistered }

func (o *flushOutlet) Flush(context.Context) error {
	if o.flushPanic {
		panic("flush boom")
	}
	return o.flushErr
}

// shutdownOutlet is a conforming Outlet whose Shutdown behavior is configurable.
type shutdownOutlet struct {
	shutdownErr   error
	shutdownPanic bool
}

func (o *shutdownOutlet) Sink(_ context.Context, _ any) error { return sink.ErrUnregistered }

func (o *shutdownOutlet) Shutdown(context.Context) error {
	if o.shutdownPanic {
		panic("shutdown boom")
	}
	return o.shutdownErr
}

func TestCheckFlusherReportsError(t *testing.T) {
	t.Parallel()

	boom := errors.New("dirty flush")
	errs := checkOutlet(func() sink.Outlet { return &flushOutlet{flushErr: boom} })
	if !containsErr(errs, "Flush on a clean outlet") {
		t.Fatalf("checkOutlet did not flag a flush error; got %v", errs)
	}
}

func TestCheckFlusherRecoversPanic(t *testing.T) {
	t.Parallel()

	errs := checkOutlet(func() sink.Outlet { return &flushOutlet{flushPanic: true} })
	if !containsErr(errs, "Flush panicked") {
		t.Fatalf("checkOutlet did not report a flush panic; got %v", errs)
	}
}

func TestCheckShutdownerReportsError(t *testing.T) {
	t.Parallel()

	boom := errors.New("dirty shutdown")
	errs := checkOutlet(func() sink.Outlet { return &shutdownOutlet{shutdownErr: boom} })
	if !containsErr(errs, "Shutdown returned") {
		t.Fatalf("checkOutlet did not flag a shutdown error; got %v", errs)
	}
}

func TestCheckShutdownerRecoversPanic(t *testing.T) {
	t.Parallel()

	errs := checkOutlet(func() sink.Outlet { return &shutdownOutlet{shutdownPanic: true} })
	if !containsErr(errs, "Shutdown panicked") {
		t.Fatalf("checkOutlet did not report a shutdown panic; got %v", errs)
	}
}

func TestCheckOutletRejectsNilOutlet(t *testing.T) {
	t.Parallel()

	errs := checkOutlet(func() sink.Outlet { return nil })
	if !containsErr(errs, "nil Outlet") {
		t.Fatalf("checkOutlet did not flag a nil Outlet; got %v", errs)
	}
}

func containsErr(errs []error, substr string) bool {
	for _, e := range errs {
		if e != nil && contains(e.Error(), substr) {
			return true
		}
	}
	return false
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
