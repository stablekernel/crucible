// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"context"
	"errors"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	kafkasink "github.com/stablekernel/crucible/sink/kafka"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeProducer is a hand-rolled Producer implementation — no broker, no
// mockery. It records every produced record and can inject an error.
type fakeProducer struct {
	calls []produceCall
	err   error
}

type produceCall struct {
	topic string
	key   []byte
	value []byte
}

func (f *fakeProducer) Produce(_ context.Context, topic string, key, value []byte) error {
	f.calls = append(f.calls, produceCall{topic: topic, key: key, value: value})
	if f.err != nil {
		return f.err
	}
	return nil
}

type orderPlaced struct{ ID string }

func newOutlet(p kafkasink.Producer) csink.Outlet {
	reg := kafkasink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlaced) csink.Op[kafkasink.Producer] {
		return kafkasink.Produce("orders", []byte(o.ID), []byte("placed"))
	})
	return kafkasink.New(p, reg)
}

func TestProducePublishesRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        csink.Op[kafkasink.Producer]
		wantTopic string
		wantKey   []byte
		wantValue []byte
	}{
		{
			name:      "keyed record",
			op:        kafkasink.Produce("orders", []byte("A-1"), []byte("placed")),
			wantTopic: "orders",
			wantKey:   []byte("A-1"),
			wantValue: []byte("placed"),
		},
		{
			name:      "unkeyed record",
			op:        kafkasink.Produce("events", nil, []byte("ping")),
			wantTopic: "events",
			wantKey:   nil,
			wantValue: []byte("ping"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &fakeProducer{}
			if err := tt.op.Apply(context.Background(), p); err != nil {
				t.Fatalf("Apply() error = %v", err)
			}
			if len(p.calls) != 1 {
				t.Fatalf("Produce calls = %d, want 1", len(p.calls))
			}
			got := p.calls[0]
			if got.topic != tt.wantTopic {
				t.Errorf("topic = %q, want %q", got.topic, tt.wantTopic)
			}
			if string(got.key) != string(tt.wantKey) {
				t.Errorf("key = %q, want %q", got.key, tt.wantKey)
			}
			if string(got.value) != string(tt.wantValue) {
				t.Errorf("value = %q, want %q", got.value, tt.wantValue)
			}
		})
	}
}

func TestSinkRoutesPayloadToProducer(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{}
	if err := newOutlet(p).Sink(context.Background(), orderPlaced{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if len(p.calls) != 1 || p.calls[0].topic != "orders" {
		t.Fatalf("Produce calls = %+v, want one orders record", p.calls)
	}
	if string(p.calls[0].key) != "A-1" {
		t.Fatalf("key = %q, want A-1", p.calls[0].key)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	type other struct{}
	err := newOutlet(&fakeProducer{}).Sink(context.Background(), other{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
}

func TestProduceErrorWrapped(t *testing.T) {
	t.Parallel()

	boom := errors.New("broker unavailable")
	err := newOutlet(&fakeProducer{err: boom}).Sink(context.Background(), orderPlaced{ID: "A-2"})
	if !errors.Is(err, boom) {
		t.Fatalf("Sink() = %v, want wrapped %v", err, boom)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "kafka" {
		t.Fatalf("recovered = %+v, want *sink.Error{Outlet:kafka, Phase:apply}", se)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeProducer{}) })
}
