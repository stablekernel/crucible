// SPDX-License-Identifier: Apache-2.0

// Package kafka is a sink destination that publishes payloads to Apache Kafka.
// It depends only on the standard library, crucible/sink, and the pure-Go
// franz-go client. Register a transformer that turns each payload type into a
// [Produce] operation, then attach the result of [New] to a sink.Manifold.
//
// The destination talks to Kafka through a narrow [Producer] interface that
// declares only the single produce method this package calls. The real
// franz-go client is adapted onto that interface with [NewProducer], so tests
// drive the destination with a hand-rolled fake and never touch a broker.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package kafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"

	csink "github.com/stablekernel/crucible/sink"
)

// Producer is the narrow Kafka surface this destination needs: publish one
// record and report whether the broker accepted it. It is deliberately small
// so consumers can fake it in tests; [NewProducer] adapts a *kgo.Client onto
// it for production use.
type Producer interface {
	// Produce publishes a single record to topic with the given key and value,
	// returning once the broker has acknowledged it or the context is done. A
	// nil key produces an unkeyed record.
	Produce(ctx context.Context, topic string, key, value []byte) error
}

// produceSyncer is the single franz-go method the adapter calls. *kgo.Client
// satisfies it; narrowing the adapter to it keeps the bridge unit-testable with
// a hand-rolled fake.
type produceSyncer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// clientProducer adapts a produceSyncer (in production a *kgo.Client) onto the
// [Producer] interface. The franz-go client exposes ProduceSync, not Produce,
// so it cannot satisfy the interface structurally; this thin wrapper bridges
// the two.
type clientProducer struct {
	client produceSyncer
}

// NewProducer wraps a *kgo.Client as a [Producer]. The client must be
// configured with its seed brokers and any auth before being passed here; this
// package neither dials nor closes it. Closing the client remains the caller's
// responsibility.
func NewProducer(client *kgo.Client) Producer {
	return clientProducer{client: client}
}

// Produce publishes one record synchronously and returns the first produce
// error, if any.
func (p clientProducer) Produce(ctx context.Context, topic string, key, value []byte) error {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Produce returns an Op that publishes a single record to topic. It is the
// workhorse constructor: a registry maps each payload type to the Produce (or
// OpFunc) that publishes it. A nil key yields an unkeyed record; key and value
// are passed through to the broker unchanged.
func Produce(topic string, key, value []byte) csink.Op[Producer] {
	return csink.OpFunc[Producer](func(ctx context.Context, p Producer) error {
		return p.Produce(ctx, topic, key, value)
	})
}

// NewRegistry returns an empty registry of Op[Producer] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Producer]] {
	return csink.NewRegistry[csink.Op[Producer]]()
}

// New builds an Outlet that applies each payload's registered Op[Producer] to
// producer. The outlet is named "kafka" unless overridden with sink.WithName.
func New(producer Producer, reg *csink.Registry[csink.Op[Producer]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Producer](producer, reg, append([]csink.EmitterOption{csink.WithName("kafka")}, opts...)...)
}
