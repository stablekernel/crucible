// SPDX-License-Identifier: Apache-2.0

// Package sinktest provides a conformance harness for sink.Outlet
// implementations. Destination authors call OutletConformance to verify their
// outlet honors the contract: it skips unknown payloads rather than failing
// them, is safe for concurrent use, and — when it also implements Flusher or
// Shutdowner — those drain cleanly and idempotently.
package sinktest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

// probe is a payload type no real registry knows about, used to exercise the
// unknown-payload contract: a conforming Outlet either accepts it (nil) or skips
// it (sink.ErrUnregistered), never returning some other error and never
// panicking.
type probe struct{ _ int }

// OutletConformance runs the sink.Outlet contract checks against outlets built
// by factory, reporting any violation on t. factory must return a fresh,
// independent Outlet on each call. It is safe to call with any Outlet
// implementation and is the recommended gate for every destination.
func OutletConformance(t *testing.T, factory func() sink.Outlet) {
	t.Helper()
	for _, err := range checkOutlet(factory) {
		t.Error(err)
	}
}

// checkOutlet runs the contract checks and returns the violations found, so the
// harness's own tests can assert it accepts a conforming outlet and rejects a
// broken one without needing to fabricate a *testing.T.
func checkOutlet(factory func() sink.Outlet) []error {
	if factory == nil {
		return []error{errors.New("sinktest: factory is nil")}
	}
	var errs []error

	if o := factory(); o == nil {
		return []error{errors.New("sinktest: factory returned a nil Outlet")}
	}

	errs = append(errs, checkUnknownPayload(factory())...)
	errs = append(errs, checkConcurrentSink(factory())...)
	errs = append(errs, checkFlusher(factory())...)
	errs = append(errs, checkShutdowner(factory())...)
	return errs
}

func checkUnknownPayload(o sink.Outlet) (errs []error) {
	defer func() {
		if r := recover(); r != nil {
			errs = append(errs, fmt.Errorf("sinktest: Sink panicked on an unknown payload: %v", r))
		}
	}()
	err := o.Sink(context.Background(), probe{})
	if err != nil && !errors.Is(err, sink.ErrUnregistered) {
		errs = append(errs, fmt.Errorf("sinktest: Sink on an unknown payload returned %v; want nil or ErrUnregistered", err))
	}
	return errs
}

func checkConcurrentSink(o sink.Outlet) (errs []error) {
	defer func() {
		if r := recover(); r != nil {
			errs = append(errs, fmt.Errorf("sinktest: concurrent Sink panicked: %v", r))
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = o.Sink(context.Background(), probe{})
		}()
	}
	wg.Wait()
	return errs
}

func checkFlusher(o sink.Outlet) (errs []error) {
	f, ok := o.(sink.Flusher)
	if !ok {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			errs = append(errs, fmt.Errorf("sinktest: Flush panicked: %v", r))
		}
	}()
	if err := f.Flush(context.Background()); err != nil {
		errs = append(errs, fmt.Errorf("sinktest: Flush on a clean outlet returned %v; want nil", err))
	}
	if err := f.Flush(context.Background()); err != nil { // idempotent
		errs = append(errs, fmt.Errorf("sinktest: second Flush returned %v; want nil (idempotent)", err))
	}
	return errs
}

func checkShutdowner(o sink.Outlet) (errs []error) {
	s, ok := o.(sink.Shutdowner)
	if !ok {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			errs = append(errs, fmt.Errorf("sinktest: Shutdown panicked: %v", r))
		}
	}()
	if err := s.Shutdown(context.Background()); err != nil {
		errs = append(errs, fmt.Errorf("sinktest: Shutdown returned %v; want nil", err))
	}
	if err := s.Shutdown(context.Background()); err != nil { // idempotent
		errs = append(errs, fmt.Errorf("sinktest: second Shutdown returned %v; want nil (idempotent)", err))
	}
	return errs
}
