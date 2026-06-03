// SPDX-License-Identifier: Apache-2.0

package memsource

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
)

// Harness drives a [source.Hopper] against an in-memory [Inlet] and exposes
// assertion helpers over the resulting [Ledger]. It is the one-call test entry
// point: queue messages, Run a handler, and assert outcomes — no broker, no
// goroutine bookkeeping in the test.
//
// Construct with [NewHarness]; it registers a t.Cleanup that closes the Hopper,
// so a test never leaks the consume loop.
type Harness struct {
	tb     testing.TB
	inlet  *Inlet
	hopper *source.Hopper
}

// NewHarness builds a Harness around a fresh [Inlet] and a [source.Hopper]
// configured with opts. Pass msgs to pre-queue them. The Hopper is closed via
// t.Cleanup.
func NewHarness(tb testing.TB, opts []source.Option, msgs ...Msg) *Harness {
	tb.Helper()
	return NewHarnessWith(tb, opts, nil, msgs...)
}

// NewHarnessWith is [NewHarness] with extra inlet options, e.g. [WithBatched] to
// exercise the engine's whole-batch fetch path or [WithClock] for a deterministic
// settle clock. inletOpts is applied after the pre-queued msgs.
func NewHarnessWith(tb testing.TB, opts []source.Option, inletOpts []Option, msgs ...Msg) *Harness {
	tb.Helper()
	all := append([]Option{WithMessages(msgs...)}, inletOpts...)
	in := New(all...)
	hp := source.New(opts...)
	tb.Cleanup(func() { _ = hp.Close() })
	return &Harness{tb: tb, inlet: in, hopper: hp}
}

// Inlet returns the underlying in-memory inlet, for queueing more messages mid
// run or reading its ledger directly.
func (h *Harness) Inlet() *Inlet { return h.inlet }

// Hopper returns the underlying Hopper, for direct Run/Close control.
func (h *Harness) Hopper() *source.Hopper { return h.hopper }

// Ledger returns the settle ledger the run records outcomes on.
func (h *Harness) Ledger() *Ledger { return h.inlet.Ledger() }

// Run drives the queued messages through handler and blocks until every queued
// message has been settled, then returns. It closes the inlet's subscription so
// the Hopper drains cleanly once the queue empties, and fails the test on an
// unexpected run error.
//
// Run uses a bounded timeout (defaulting to 5s) so a stuck handler fails the
// test rather than hanging the suite; override it with [RunFor].
func (h *Harness) Run(handler source.Handler) {
	h.tb.Helper()
	h.RunFor(5*time.Second, handler)
}

// RunFor is [Run] with an explicit timeout.
func (h *Harness) RunFor(timeout time.Duration, handler source.Handler) {
	h.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sub, err := h.inlet.Subscribe(ctx, source.SubscribeConfig{})
	if err != nil {
		h.tb.Fatalf("memsource: Subscribe failed: %v", err)
	}
	// Close the subscription so that, once the queued messages are settled, Next
	// returns ErrDrained and Run exits on its own.
	_ = sub.Close()

	if err := h.hopper.Run(ctx, sub, handler); err != nil {
		h.tb.Fatalf("memsource: Hopper.Run returned %v", err)
	}
}

// RunBatch drives the queued messages through bh in batch mode, the batch analog
// of [Run]. The Hopper must have been built with [source.WithBatch] for batching
// to take effect; otherwise every batch holds one message. It closes the inlet's
// subscription so the Hopper drains once the queue empties, flushing any partial
// final batch, and fails the test on an unexpected run error. It uses a bounded
// 5s timeout; use [RunBatchFor] to override.
func (h *Harness) RunBatch(bh source.BatchHandler) {
	h.tb.Helper()
	h.RunBatchFor(5*time.Second, bh)
}

// RunBatchFor is [RunBatch] with an explicit timeout.
func (h *Harness) RunBatchFor(timeout time.Duration, bh source.BatchHandler) {
	h.tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sub, err := h.inlet.Subscribe(ctx, source.SubscribeConfig{})
	if err != nil {
		h.tb.Fatalf("memsource: Subscribe failed: %v", err)
	}
	_ = sub.Close()

	if err := h.hopper.RunBatch(ctx, sub, bh); err != nil {
		h.tb.Fatalf("memsource: Hopper.RunBatch returned %v", err)
	}
}

// AssertCounts fails the test unless the recorded settlements match want exactly.
func (h *Harness) AssertCounts(want Counts) {
	h.tb.Helper()
	got := h.Ledger().Counts()
	if got != want {
		h.tb.Fatalf("settle counts = %+v, want %+v", got, want)
	}
}

// AssertSettled fails the test unless exactly n messages were settled.
func (h *Harness) AssertSettled(n int) {
	h.tb.Helper()
	if got := h.Ledger().Len(); got != n {
		h.tb.Fatalf("settled %d messages, want %d", got, n)
	}
}
