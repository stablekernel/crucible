package state_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

var fakeEpoch = time.Unix(0, 0)

// fired reports whether ch holds a value, without blocking, and returns it.
func fired(ch <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		return time.Time{}, false
	}
}

// TestFakeClock_AfterWaitsForAdvance checks that After fires only once the clock
// reaches its deadline: not with no advance, not one second short, and then
// exactly once, with the deadline as its value.
func TestFakeClock_AfterWaitsForAdvance(t *testing.T) {
	clk := state.NewFakeClock(fakeEpoch)
	ch := clk.After(5 * time.Second)

	if _, ok := fired(ch); ok {
		t.Fatal("After(5s) fired with no advance")
	}
	clk.Advance(4 * time.Second)
	if _, ok := fired(ch); ok {
		t.Fatal("After(5s) fired after Advance(4s)")
	}
	clk.Advance(time.Second)
	got, ok := fired(ch)
	if !ok {
		t.Fatal("After(5s) did not fire once the clock reached 5s")
	}
	if want := fakeEpoch.Add(5 * time.Second); !got.Equal(want) {
		t.Fatalf("After value: want %v, got %v", want, got)
	}
	clk.Advance(time.Hour)
	if _, ok := fired(ch); ok {
		t.Fatal("After fired a second time")
	}
}

// TestFakeClock_AfterSingleAdvance checks that one Advance of exactly the
// duration fires the channel.
func TestFakeClock_AfterSingleAdvance(t *testing.T) {
	clk := state.NewFakeClock(fakeEpoch)
	ch := clk.After(5 * time.Second)
	clk.Advance(5 * time.Second)
	if _, ok := fired(ch); !ok {
		t.Fatal("After(5s) did not fire after Advance(5s)")
	}
}

// TestFakeClock_AfterNonPositiveFiresAtOnce checks that After(0) and a negative
// duration fire immediately with the current instant, as time.After does.
func TestFakeClock_AfterNonPositiveFiresAtOnce(t *testing.T) {
	clk := state.NewFakeClock(fakeEpoch)
	clk.Advance(3 * time.Second)
	for _, d := range []time.Duration{0, -time.Second} {
		got, ok := fired(clk.After(d))
		if !ok {
			t.Fatalf("After(%v) did not fire at once", d)
		}
		if want := fakeEpoch.Add(3 * time.Second); !got.Equal(want) {
			t.Fatalf("After(%v) value: want %v, got %v", d, want, got)
		}
	}
}

// TestFakeClock_AfterFiresInDeadlineOrder checks that one large Advance fires
// every waiter it reaches and none past it. Each channel receives its own
// deadline, not the clock's new time, so a host that orders what fired by value
// sees the deadlines in order, ties included.
func TestFakeClock_AfterFiresInDeadlineOrder(t *testing.T) {
	clk := state.NewFakeClock(fakeEpoch)
	waiters := []struct {
		d  time.Duration
		ch <-chan time.Time
	}{
		{9 * time.Second, clk.After(9 * time.Second)},
		{2 * time.Second, clk.After(2 * time.Second)},
		{5 * time.Second, clk.After(5 * time.Second)},
		{5 * time.Second, clk.After(5 * time.Second)},
	}
	never := clk.After(time.Minute)

	clk.Advance(10 * time.Second)

	for _, w := range waiters {
		got, ok := fired(w.ch)
		if !ok {
			t.Fatalf("After(%v) did not fire after Advance(10s)", w.d)
		}
		if want := fakeEpoch.Add(w.d); !got.Equal(want) {
			t.Fatalf("After(%v) value: want %v, got %v", w.d, want, got)
		}
	}
	if _, ok := fired(never); ok {
		t.Fatal("After(1m) fired after Advance(10s)")
	}
	clk.Advance(50 * time.Second)
	if _, ok := fired(never); !ok {
		t.Fatal("After(1m) did not fire once the clock reached 1m")
	}
}

// TestFakeClock_ConcurrentAfterAndAdvance arms waiters from many goroutines
// while others advance the clock, for the race detector. Every waiter armed must
// fire once the clock has moved past all deadlines.
func TestFakeClock_ConcurrentAfterAndAdvance(t *testing.T) {
	const arms, advances = 64, 16
	clk := state.NewFakeClock(fakeEpoch)

	chans := make([]<-chan time.Time, arms)
	var wg sync.WaitGroup
	for i := range arms {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chans[i] = clk.After(time.Duration(i+1) * time.Millisecond)
		}()
	}
	for range advances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			clk.Advance(time.Millisecond)
			_ = clk.Now()
		}()
	}
	wg.Wait()

	// The last waiter armed may have started as late as the final advance, so
	// move past the longest deadline from there.
	clk.Advance(time.Duration(arms+1) * time.Millisecond)
	for i, ch := range chans {
		if _, ok := fired(ch); !ok {
			t.Fatalf("waiter %d did not fire", i)
		}
	}
}
