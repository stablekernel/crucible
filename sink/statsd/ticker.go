// SPDX-License-Identifier: Apache-2.0

package statsd

import "time"

// ticker is the seam the background flush loop schedules against. The real
// implementation wraps time.Ticker; white-box tests inject a hand-driven one so
// they can step the loop deterministically without sleeping.
type ticker interface {
	// C returns the channel that fires on each tick.
	C() <-chan time.Time
	// Stop releases the ticker's resources.
	Stop()
}

// realTicker adapts time.Ticker to the ticker seam. It is the production tick
// source; the injected clock stamps each tick so the same clock the rest of the
// Aggregator reads also drives observable tick times.
type realTicker struct {
	t    *time.Ticker
	c    chan time.Time
	stop chan struct{}
	done chan struct{}
	now  func() time.Time
}

func newRealTicker(interval time.Duration, now func() time.Time) ticker {
	r := &realTicker{
		t:    time.NewTicker(interval),
		c:    make(chan time.Time, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
		now:  now,
	}
	go r.run()
	return r
}

func (r *realTicker) run() {
	defer close(r.done)
	for {
		select {
		case <-r.stop:
			return
		case <-r.t.C:
			select {
			case r.c <- r.now():
			default:
			}
		}
	}
}

func (r *realTicker) C() <-chan time.Time { return r.c }

func (r *realTicker) Stop() {
	r.t.Stop()
	close(r.stop)
	<-r.done
}
