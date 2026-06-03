// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
	"github.com/stablekernel/crucible/telemetry"
)

// recMeter is a minimal recording telemetry.Meter for asserting that the Hopper
// emits the expected counters and lag gauge.
type recMeter struct {
	mu       sync.Mutex
	counters map[string]int64
	gauges   map[string]float64
}

func newRecMeter() *recMeter {
	return &recMeter{counters: map[string]int64{}, gauges: map[string]float64{}}
}

func (m *recMeter) Counter(name string, _ ...telemetry.InstrumentOption) telemetry.Counter {
	return &recCounter{m: m, name: name}
}

func (m *recMeter) Histogram(string, ...telemetry.InstrumentOption) telemetry.Histogram {
	return recHistogram{}
}

func (m *recMeter) Gauge(name string, _ ...telemetry.InstrumentOption) telemetry.Gauge {
	return &recGauge{m: m, name: name}
}

func (m *recMeter) count(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[name]
}

func (m *recMeter) gauge(name string) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.gauges[name]
	return v, ok
}

type recCounter struct {
	m    *recMeter
	name string
}

func (c *recCounter) Add(_ context.Context, n int64, _ ...telemetry.Attr) {
	c.m.mu.Lock()
	defer c.m.mu.Unlock()
	c.m.counters[c.name] += n
}

type recHistogram struct{}

func (recHistogram) Record(context.Context, float64, ...telemetry.Attr) {}

type recGauge struct {
	m    *recMeter
	name string
}

func (g *recGauge) Record(_ context.Context, v float64, _ ...telemetry.Attr) {
	g.m.mu.Lock()
	defer g.m.mu.Unlock()
	g.m.gauges[g.name] = v
}

func TestHopper_Counters(t *testing.T) {
	t.Parallel()
	meter := newRecMeter()
	h := memsource.NewHarness(
		t,
		[]source.Option{source.WithMeter(meter)},
		memsource.Msg{Key: "a"},
		memsource.Msg{Key: "b"},
		memsource.Msg{Key: "c"},
		memsource.Msg{Key: "d"},
	)
	i := 0
	h.Run(func(context.Context, source.Message) source.Result {
		i++
		switch i {
		case 1:
			return source.Ack()
		case 2:
			return source.Nak(errors.New("retry"))
		case 3:
			return source.Term(errors.New("poison"))
		default:
			return source.Skip()
		}
	})

	if got := meter.count("source.received"); got != 4 {
		t.Errorf("source.received = %d, want 4", got)
	}
	if got := meter.count("source.acked"); got != 1 {
		t.Errorf("source.acked = %d, want 1", got)
	}
	if got := meter.count("source.nak"); got != 1 {
		t.Errorf("source.nak = %d, want 1", got)
	}
	if got := meter.count("source.term"); got != 1 {
		t.Errorf("source.term = %d, want 1", got)
	}
	if got := meter.count("source.dropped"); got != 1 {
		t.Errorf("source.dropped = %d, want 1", got)
	}
}

func TestHopper_LagGaugeFromReporter(t *testing.T) {
	t.Parallel()
	meter := newRecMeter()
	hp := source.New(source.WithMeter(meter))
	t.Cleanup(func() { _ = hp.Close() })

	sub := &lagSub{
		msgs: []source.Message{lagMsg{}},
		lag:  7,
	}
	_ = sub.Close() // drains after the one message settles
	if err := hp.Run(context.Background(), sub, func(context.Context, source.Message) source.Result {
		return source.Ack()
	}); err != nil {
		t.Fatalf("Run err = %v", err)
	}
	if v, ok := meter.gauge("source.lag"); !ok || v != 7 {
		t.Fatalf("source.lag gauge = %v (present=%v), want 7", v, ok)
	}
}

// lagSub is a Subscription that also implements LagReporter, yielding a fixed
// queue once then draining.
type lagSub struct {
	mu     sync.Mutex
	msgs   []source.Message
	lag    int64
	closed bool
}

func (s *lagSub) Next(ctx context.Context) (source.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.msgs) > 0 {
		m := s.msgs[0]
		s.msgs = s.msgs[1:]
		return m, nil
	}
	if s.closed {
		return nil, source.ErrDrained
	}
	return nil, context.Canceled
}
func (s *lagSub) Settle(context.Context, source.Message, source.Result) error { return nil }
func (s *lagSub) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *lagSub) Lag(context.Context) (int64, error) { return s.lag, nil }

type lagMsg struct{}

func (lagMsg) Key() []byte             { return nil }
func (lagMsg) Value() []byte           { return nil }
func (lagMsg) Headers() source.Headers { return nil }
func (lagMsg) Subject() string         { return "lag" }
func (lagMsg) PartitionKey() string    { return "" }
func (lagMsg) Cursor() source.Cursor   { return testCursor("lag") }
func (lagMsg) As(any) bool             { return false }

var _ source.LagReporter = (*lagSub)(nil)
