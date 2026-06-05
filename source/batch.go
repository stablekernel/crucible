// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/stablekernel/crucible/telemetry"
)

// ReceiveBatch subscribes to in with cfg and drives the resulting subscription
// with bh in batch mode, the batch analog of [Hopper.Receive]. It closes the
// subscription on return. The Hopper must have been constructed with [WithBatch];
// without it a batch run still works but every batch holds a single message.
func (hp *Hopper) ReceiveBatch(ctx context.Context, in Inlet, cfg SubscribeConfig, bh BatchHandler) error {
	sub, err := in.Subscribe(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Close() }()
	return hp.runBatch(ctx, sub, bh)
}

// RunBatch drives sub with bh in batch mode, the batch analog of [Hopper.Run].
// Within each ordered lane it accumulates messages up to the configured size (see
// [WithBatch]) or until the max-wait elapses, invokes bh once per accumulated
// slice, and settles each message by its corresponding [Result]. Per-key ordering
// and the [WithMaxInFlight] bound hold exactly as in the per-message path: a lane
// never overlaps two batches, and every message in a batch is settled before the
// lane accepts more.
//
// RunBatch honors a [Batched] subscription when present, fetching whole batches
// from the backend and regrouping them by key into lanes; otherwise it
// accumulates per message under the size/max-wait policy. Lifecycle, drain, and
// error semantics match [Hopper.Run]: a clean drain or context cancellation
// returns nil, a fetch error propagates, and a partial batch buffered when the
// run drains is flushed before exit. RunBatch does not close sub.
func (hp *Hopper) RunBatch(ctx context.Context, sub Subscription, bh BatchHandler) error {
	return hp.runBatch(ctx, sub, bh)
}

// batchSize resolves the per-lane batch size, defaulting to 1 when batch mode was
// not configured (each message is its own batch).
func (hp *Hopper) batchSize() int {
	if hp.batch.size > 0 {
		return hp.batch.size
	}
	return 1
}

// batchClock resolves the injected clock, defaulting to time.Now.
func (hp *Hopper) batchClock() func() time.Time {
	if hp.batch.now != nil {
		return hp.batch.now
	}
	return time.Now
}

// runBatch mirrors run but accumulates per-lane and dispatches to a BatchHandler.
// The fetch loop reserves an in-flight slot per message (the backpressure bound
// is per message, not per batch), routes each message to its ordered lane, and a
// lane goroutine buffers messages until the size or max-wait bound trips, then
// hands the slice to the handler and settles each result.
func (hp *Hopper) runBatch(ctx context.Context, sub Subscription, bh BatchHandler) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-hp.closed:
			cancel()
		case <-stop:
		}
	}()

	var inFlight chan struct{}
	if hp.maxInFlight > 0 {
		inFlight = make(chan struct{}, hp.maxInFlight)
	}
	// dispatchSem bounds how many lanes run their BatchHandler concurrently,
	// mirroring the per-message concurrency bound (at most N handler executions in
	// parallel). A lane buffers freely but acquires a token around each dispatch,
	// so distinct keys can all have live lanes while only N dispatch at once.
	dispatchSem := make(chan struct{}, hp.concurrency)

	var (
		mu      sync.Mutex
		lanes   = make(map[uint64]*batchLane)
		laneWG  sync.WaitGroup
		runErr  error
		errOnce sync.Once
	)

	size := hp.batchSize()
	clock := hp.batchClock()

	// laneFor returns the ordered lane for key, starting its goroutine on first
	// use. One goroutine per key, exactly like the per-message path; the
	// cross-key parallelism bound is applied at dispatch time, not creation.
	laneFor := func(key uint64) *batchLane {
		mu.Lock()
		defer mu.Unlock()
		if l, ok := lanes[key]; ok {
			return l
		}
		l := &batchLane{
			queue:    make(chan *delivery, 1),
			size:     size,
			maxWait:  hp.batch.maxWait,
			now:      clock,
			inFlight: inFlight,
			dispatch: dispatchSem,
		}
		lanes[key] = l
		laneWG.Add(1)
		go func() {
			defer laneWG.Done()
			l.run(runCtx, hp, bh)
		}()
		return l
	}

	defer func() {
		// Drain: close every lane queue so each lane flushes its buffered messages
		// and exits, then wait.
		mu.Lock()
		for _, l := range lanes {
			close(l.queue)
		}
		mu.Unlock()
		laneWG.Wait()
	}()

	// batched is the whole-batch fetch path when the subscription advertises it.
	batched, _ := sub.(Batched)

	for {
		msgs, err := hp.fetchBatch(runCtx, sub, batched, size)
		if err != nil {
			if errors.Is(err, ErrDrained) || errors.Is(err, context.Canceled) || runCtx.Err() != nil {
				return runErr
			}
			errOnce.Do(func() { runErr = err })
			return runErr
		}

		for _, msg := range msgs {
			// Reserve the backpressure slot per message (not per fetch) so a
			// NextBatch larger than maxInFlight cannot deadlock: slots free as
			// lanes dispatch, and a full window blocks here before routing more.
			if inFlight != nil {
				select {
				case inFlight <- struct{}{}:
				case <-runCtx.Done():
					return runErr
				}
			}

			hp.received.Add(runCtx, 1)
			hp.reportLag(runCtx, sub)

			l := laneFor(hp.laneKey(msg))
			select {
			case l.queue <- &delivery{msg: msg, sub: sub}:
			case <-runCtx.Done():
				if inFlight != nil {
					<-inFlight
				}
				return runErr
			}
		}
	}
}

// fetchBatch pulls the next group of messages, using the [Batched] capability
// when present (one NextBatch call yields up to size messages, settled per
// message by the lane) and otherwise a single [Subscription.Next]. It does not
// reserve backpressure slots; the routing loop reserves one per message so a
// NextBatch larger than the in-flight window cannot deadlock.
func (hp *Hopper) fetchBatch(ctx context.Context, sub Subscription, batched Batched, size int) ([]Message, error) {
	if batched != nil {
		msgs, err := batched.NextBatch(ctx, size)
		if err != nil {
			return nil, err
		}
		return msgs, nil
	}
	msg, err := sub.Next(ctx)
	if err != nil {
		return nil, err
	}
	return []Message{msg}, nil
}

// batchLane is a single ordered worker that buffers deliveries and dispatches
// them to a BatchHandler in groups. It mirrors the per-message lane but holds a
// buffer and a max-wait timer so a slow-filling lane still flushes on time.
type batchLane struct {
	queue    chan *delivery
	size     int
	maxWait  time.Duration
	now      func() time.Time
	inFlight chan struct{}
	dispatch chan struct{} // cross-key concurrency bound, acquired around a flush
}

// run drives the lane: it buffers deliveries, flushing when the buffer reaches
// size, when the max-wait since the first buffered message elapses, or when the
// queue closes (run drain). It is the single point where ordering within a key is
// preserved — one batch at a time, in arrival order.
func (l *batchLane) run(ctx context.Context, hp *Hopper, bh BatchHandler) {
	var buf []*delivery

	// timer fires maxWait after the first message lands in an empty buffer. It is
	// created stopped and reset on each first-message; a zero/negative maxWait
	// disables it entirely (flush on size or close only).
	var timerC <-chan time.Time
	var timer *time.Timer
	if l.maxWait > 0 {
		timer = time.NewTimer(l.maxWait)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()
	}
	armed := false

	flush := func() {
		if len(buf) == 0 {
			return
		}
		// Acquire a dispatch token so at most concurrency lanes run their handler
		// at once. If the run is shutting down, proceed without the token: the
		// buffered messages must still be settled so none are stranded.
		acquired := false
		select {
		case l.dispatch <- struct{}{}:
			acquired = true
		case <-ctx.Done():
		}
		hp.dispatchBatch(ctx, bh, buf)
		if acquired {
			<-l.dispatch
		}
		// Release the per-message in-flight slot held for every message in the
		// batch now that all are settled. The lane slot is held for the lane's
		// whole lifetime and released when its goroutine exits, not here.
		for range buf {
			if l.inFlight != nil {
				<-l.inFlight
			}
		}
		buf = nil
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			armed = false
			timerC = nil
		}
	}

	for {
		select {
		case d, ok := <-l.queue:
			if !ok {
				// Queue closed: flush the partial batch and exit.
				flush()
				return
			}
			buf = append(buf, d)
			if l.maxWait > 0 && !armed {
				timer.Reset(l.maxWait)
				timerC = timer.C
				armed = true
			}
			if len(buf) >= l.size {
				flush()
			}
		case <-timerC:
			armed = false
			timerC = nil
			flush()
		}
	}
}

// dispatchBatch decodes each buffered message, invokes the batch handler with the
// decoded slice, settles each message by its corresponding result, and releases
// the per-message in-flight and lane slots. A decode failure terminates that
// message as poison and excludes it from the handler call, so a bad payload never
// poisons its lane-mates. A result-count mismatch settles what it can and
// terminates the remainder as poison.
func (hp *Hopper) dispatchBatch(ctx context.Context, bh BatchHandler, buf []*delivery) {
	ctx, span := hp.tracer.Start(
		ctx, "source.batch",
		telemetry.String("source.name", hp.name),
		telemetry.Int("source.batch_size", len(buf)),
	)
	defer span.End()
	hp.batches.Add(ctx, 1)

	// Decode every message; a decode failure is settled as poison immediately and
	// dropped from the slice handed to the handler.
	msgs := make([]Message, 0, len(buf))
	live := make([]*delivery, 0, len(buf))
	for _, d := range buf {
		dec, err := hp.decodeFor(d.msg)
		if err != nil {
			span.RecordError(err)
			hp.settle(ctx, span, d, Term(err))
			continue
		}
		msgs = append(msgs, dec)
		live = append(live, d)
	}

	if len(live) == 0 {
		return
	}

	results := bh(ctx, msgs)

	for i, d := range live {
		var r Result
		switch {
		case i < len(results):
			r = results[i]
		default:
			// The handler returned too few results: terminate the unmatched
			// message as poison rather than silently acking or stranding it.
			r = Term(ErrBatchResultCount)
			span.RecordError(ErrBatchResultCount)
		}
		if r.Err != nil {
			span.RecordError(r.Err)
		}
		hp.settle(ctx, span, d, r)
	}

	// Surface a count mismatch loudly rather than letting it pass. An under-count
	// already terminated each unmatched message as poison above; an over-count
	// (more results than messages) silently discards the extra results, so record
	// the sentinel here too so the discard is visible in traces, not swallowed.
	if len(results) != len(live) {
		span.RecordError(ErrBatchResultCount)
		span.SetStatus(telemetry.StatusError, "batch result count mismatch")
	}
}
