// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeSyncer is a hand-rolled produceSyncer: it records the record it received
// and returns a canned result, so the *kgo.Client adapter is covered without a
// broker.
type fakeSyncer struct {
	got *kgo.Record
	err error
}

func (f *fakeSyncer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	if len(rs) > 0 {
		f.got = rs[0]
	}
	return kgo.ProduceResults{{Record: rs[0], Err: f.err}}
}

func TestClientProducerProduceSuccess(t *testing.T) {
	t.Parallel()

	f := &fakeSyncer{}
	p := clientProducer{client: f}
	if err := p.Produce(context.Background(), "topic", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}
	if f.got == nil || f.got.Topic != "topic" || string(f.got.Key) != "k" || string(f.got.Value) != "v" {
		t.Fatalf("record = %+v, want topic/k/v", f.got)
	}
}

func TestClientProducerProduceError(t *testing.T) {
	t.Parallel()

	boom := errors.New("broker rejected")
	p := clientProducer{client: &fakeSyncer{err: boom}}
	if err := p.Produce(context.Background(), "topic", nil, []byte("v")); !errors.Is(err, boom) {
		t.Fatalf("Produce() = %v, want %v", err, boom)
	}
}

func TestNewProducerWrapsClient(t *testing.T) {
	t.Parallel()
	// NewProducer takes the concrete *kgo.Client; a nil client is fine to wrap
	// here because we never call through it.
	if NewProducer(nil) == nil {
		t.Fatal("NewProducer returned nil")
	}
}
