// SPDX-License-Identifier: Apache-2.0

package sinktest

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestOutletConformancePassesForBucket(t *testing.T) {
	t.Parallel()
	OutletConformance(t, func() sink.Outlet { return sink.NewBucket() })
}

func TestCheckOutletAcceptsConformingOutlet(t *testing.T) {
	t.Parallel()
	if errs := checkOutlet(func() sink.Outlet { return sink.NewBucket() }); len(errs) != 0 {
		t.Fatalf("Bucket reported %d violations: %v", len(errs), errs)
	}
}

func TestCheckOutletRejectsBrokenOutlet(t *testing.T) {
	t.Parallel()

	// Broken: returns a non-skip error for a payload it should skip.
	broken := func() sink.Outlet {
		return sink.OutletFunc(func(context.Context, any) error { return errors.New("nope") })
	}
	if errs := checkOutlet(broken); len(errs) == 0 {
		t.Fatal("checkOutlet accepted a broken outlet; want at least one violation")
	}
}

func TestCheckOutletRejectsNilFactory(t *testing.T) {
	t.Parallel()
	if errs := checkOutlet(nil); len(errs) == 0 {
		t.Fatal("checkOutlet(nil) reported no error")
	}
}

func TestConformanceCoversFlusherAndShutdowner(t *testing.T) {
	t.Parallel()
	// A Reservoir is an Outlet + Flusher + Shutdowner, exercising those branches.
	OutletConformance(t, func() sink.Outlet {
		return sink.Reservoir(sink.NewBucket(), sink.WithBatchInterval(0))
	})
}
