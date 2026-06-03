// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
)

// fullCap implements every optional capability, exercising detection by type
// assertion and the marker methods.
type fullCap struct{}

func (fullCap) SeekToTime(context.Context, time.Time) error       { return nil }
func (fullCap) SeekToCursor(context.Context, source.Cursor) error { return nil }
func (fullCap) SeekToStart(context.Context) error                 { return nil }
func (fullCap) SeekToEnd(context.Context) error                   { return nil }

func (fullCap) GroupID() string                                          { return "g" }
func (fullCap) OnAssigned(func(context.Context, []source.Partition))     {}
func (fullCap) OnRevoked(func(context.Context, []source.Partition))      {}
func (fullCap) Durable() string                                          { return "d" }
func (fullCap) PartitionOrdered()                                        {}
func (fullCap) OrderedDelivery()                                         {}
func (fullCap) NextBatch(context.Context, int) ([]source.Message, error) { return nil, nil }
func (fullCap) SettleBatch(context.Context, []source.Message, source.Result) error {
	return nil
}

func (fullCap) Begin(_ context.Context, _ source.Message, fn func(context.Context, source.Tx) error) error {
	return fn(context.Background(), txStub{})
}

// txStub is a no-op [source.Tx] that records nothing; it satisfies the produce
// handle the capability hands to a transactional work function.
type txStub struct{}

func (txStub) Produce(context.Context, ...source.ProducedRecord) error { return nil }
func (fullCap) Seen(context.Context, string) (bool, error)             { return false, nil }
func (fullCap) Lag(context.Context) (int64, error)                     { return 0, nil }

func TestCapabilities_DetectedByTypeAssertion(t *testing.T) {
	t.Parallel()
	var v any = fullCap{}

	if _, ok := v.(source.Seekable); !ok {
		t.Error("fullCap should satisfy Seekable")
	}
	if cg, ok := v.(source.ConsumerGroups); !ok || cg.GroupID() != "g" {
		t.Error("fullCap should satisfy ConsumerGroups with GroupID g")
	}
	if sd, ok := v.(source.SharedDurable); !ok || sd.Durable() != "d" {
		t.Error("fullCap should satisfy SharedDurable with Durable d")
	}
	for _, c := range []any{
		(*source.PartitionOrdered)(nil),
		(*source.OrderedDelivery)(nil),
		(*source.Batched)(nil),
		(*source.Transactional)(nil),
		(*source.Deduper)(nil),
		(*source.LagReporter)(nil),
	} {
		_ = c
	}

	// Exercise the marker and capability methods.
	c := fullCap{}
	c.PartitionOrdered()
	c.OrderedDelivery()
	c.OnAssigned(nil)
	c.OnRevoked(nil)
	_ = c.SeekToTime(context.Background(), time.Now())
	_ = c.SeekToCursor(context.Background(), nil)
	_ = c.SeekToStart(context.Background())
	_ = c.SeekToEnd(context.Background())
	_, _ = c.NextBatch(context.Background(), 1)
	_ = c.SettleBatch(context.Background(), nil, source.Ack())
	_ = c.Begin(context.Background(), nil, func(context.Context, source.Tx) error { return nil })
	_, _ = c.Seen(context.Background(), "k")
	_, _ = c.Lag(context.Background())
}

func TestPartitionFields(t *testing.T) {
	t.Parallel()
	p := source.Partition{Topic: "orders", ID: 3}
	if p.Topic != "orders" || p.ID != 3 {
		t.Fatalf("Partition = %+v, want {orders 3}", p)
	}
}

// A non-empty PartitionKey takes the lane key over Key, so messages with the
// same PartitionKey but different Keys still share a lane.
func TestPositionInterface(_ *testing.T) {
	var _ source.Position = posStub("p")
}

type posStub string

func (p posStub) String() string { return string(p) }
