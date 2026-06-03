// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"time"
)

// This file declares the optional capability interfaces an [Inlet] or
// [Subscription] MAY implement. They are discovered by type assertion inside the
// [Hopper] — never assumed — so the core [Inlet] and [Subscription] interfaces
// stay the honest common path while backend-specific powers are surfaced without
// a lowest-common-denominator lie. Each interface notes which backends satisfy
// it; an adapter declares conformance with a compile-time assertion
// (var _ Seekable = (*myInlet)(nil)).

// Position is an opaque seek target for a [Seekable] subscription: a stream-local
// coordinate (a Kafka offset, a JetStream stream sequence) the backend produced
// and can resume from. Like a [Cursor] it is meaningful only within the stream
// that issued it and is not comparable across inlets or topics; unlike a Cursor,
// which marks where a delivered message sat, a Position is a request to resume
// delivery from a chosen point.
type Position interface {
	// String renders the position for logs and diagnostics. It carries no
	// semantics beyond being stable for a given coordinate.
	String() string
}

// Partition identifies one ordering domain within a topic on a partitioned
// backend (a Kafka topic/partition). It is the unit of assignment a
// [ConsumerGroups] subscription is granted or has revoked. Backends without
// partitions (JetStream) do not produce Partitions.
type Partition struct {
	// Topic is the topic the partition belongs to.
	Topic string
	// ID is the partition number within the topic.
	ID int32
}

// Seekable is a [Subscription] that can reposition its read cursor to replay or
// skip ahead: the basis for replay-driven state reconstruction. Seeking takes
// effect on the next [Subscription.Next]. Satisfied by Kafka (live SetOffsets)
// and JetStream (by recreating the consumer at the target). A backend that
// cannot reposition simply does not implement it.
type Seekable interface {
	// SeekToTime repositions delivery to the first message at or after t.
	SeekToTime(ctx context.Context, t time.Time) error
	// SeekToCursor repositions delivery to resume from a previously observed
	// [Cursor] (re-delivering from just after it, per the backend's convention).
	SeekToCursor(ctx context.Context, c Cursor) error
	// SeekToStart repositions delivery to the earliest retained message.
	SeekToStart(ctx context.Context) error
	// SeekToEnd repositions delivery to the tail, skipping the backlog so only
	// messages produced after the seek are delivered.
	SeekToEnd(ctx context.Context) error
}

// ConsumerGroups is a [Subscription] that participates in competing-consumer
// rebalancing across a partitioned backend and exposes the assignment lifecycle
// so a consumer can drain and commit before partitions move. Satisfied by Kafka
// only. JetStream's durable consumer is the grouping analog but has no partitions
// and no assignment callbacks, so JetStream does NOT satisfy this — it satisfies
// [SharedDurable] instead.
type ConsumerGroups interface {
	// GroupID returns the consumer group the subscription belongs to.
	GroupID() string
	// OnAssigned registers a callback invoked when partitions are assigned to this
	// member, before their messages are delivered.
	OnAssigned(func(ctx context.Context, assigned []Partition))
	// OnRevoked registers a callback invoked before partitions are revoked, the
	// window in which the consumer drains in-flight work and commits.
	OnRevoked(func(ctx context.Context, revoked []Partition))
}

// SharedDurable is a [Subscription] backed by a named durable consumer that
// load-balances across processes without partition assignment: the
// competing-consumer analog on a backend that has no partitions. Satisfied by
// JetStream (a named durable consumer). Distinct from [ConsumerGroups] precisely
// because there are no partitions and no assignment lifecycle to observe.
type SharedDurable interface {
	// Durable returns the durable consumer name the subscription shares.
	Durable() string
}

// PartitionOrdered is a [Subscription] that guarantees per-partition delivery
// order: messages within a partition arrive in the order they were produced.
// Satisfied by Kafka. It is the structural guarantee the [Hopper] relies on to
// key its ordered lanes by partition.
type PartitionOrdered interface {
	// PartitionOrdered is a marker; its presence is the guarantee.
	PartitionOrdered()
}

// OrderedDelivery is a [Subscription] that guarantees total, single-stream
// delivery order at the cost of concurrency: every message arrives strictly in
// order on one logical flow. Satisfied by a JetStream OrderedConsumer. A Hopper
// driving such a subscription runs a single lane (no cross-key parallelism), so
// it is mutually exclusive with high concurrency.
type OrderedDelivery interface {
	// OrderedDelivery is a marker; its presence is the guarantee.
	OrderedDelivery()
}

// Batched is a [Subscription] that can yield and settle messages in batches,
// amortizing per-message overhead. The [Hopper] uses it when present to fetch and
// ack in groups; an unbatched subscription is driven one message at a time.
type Batched interface {
	// NextBatch returns up to limit messages, blocking for at least one, or
	// ctx.Err()/ErrDrained as [Subscription.Next] would.
	NextBatch(ctx context.Context, limit int) ([]Message, error)
	// SettleBatch applies r to every message in ms in one call.
	SettleBatch(ctx context.Context, ms []Message, r Result) error
}

// Transactional is a [Subscription] that can fence message settlement inside a
// transaction, so consume-process-produce is atomic (exactly-once into a sink).
// Satisfied by Kafka (EOS) only. JetStream has no equivalent and does NOT
// implement it — the capability is absent rather than faked.
type Transactional interface {
	// Begin starts a transaction; settlement of messages received during it is
	// committed or aborted atomically with the work done inside fn.
	Begin(ctx context.Context, fn func(ctx context.Context) error) error
}

// Deduper is a [Subscription] (or an inlet seam) that suppresses re-delivery of
// an already-processed message by an idempotency key. The no-op default is "no
// deduplication"; the state-machine bridge supplies the machine's state version
// as the key so redelivery is provably idempotent with no external store.
type Deduper interface {
	// Seen reports whether key has already been processed (and records it if not),
	// so the Hopper can skip a duplicate by acking without re-running the handler.
	Seen(ctx context.Context, key string) (bool, error)
}

// LagReporter is a [Subscription] that can report how far behind the tail it is,
// the headline health signal for a consumer. The [Hopper] feeds it into a lag
// gauge when present. Satisfied by backends that expose a high-water mark
// (Kafka end offsets, JetStream pending counts).
type LagReporter interface {
	// Lag returns the number of unconsumed messages between the committed position
	// and the stream tail, across all assigned partitions/subjects.
	Lag(ctx context.Context) (int64, error)
}
