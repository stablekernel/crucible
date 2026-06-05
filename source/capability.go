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
// amortizing per-message overhead. The [Hopper]'s batch mode (see
// [Hopper.RunBatch]) uses [Batched.NextBatch] to fetch whole batches from the
// backend when this capability is present; an unbatched subscription is fetched
// one message at a time.
//
// Settlement is a separate decision. A [BatchHandler] returns one [Result] per
// message, so the Hopper settles each message by its own Result through
// [Subscription.Settle] rather than collapsing a batch onto a single outcome —
// a slow handler that fails the third message of five must not nak the other
// four. [SettleBatch] is therefore a reserved seam the Hopper does not call: it
// is the one-call settle path for an adapter or a caller that already knows a
// whole batch shares one outcome (a uniform ack after a bulk commit), not a
// substitute for per-message settlement.
type Batched interface {
	// NextBatch returns up to limit messages, blocking for at least one, or
	// ctx.Err()/ErrDrained as [Subscription.Next] would.
	NextBatch(ctx context.Context, limit int) ([]Message, error)
	// SettleBatch applies the single result r to every message in ms in one call.
	// It is for a caller settling a uniform batch directly; the Hopper settles per
	// message (one Result each) and does not call it.
	SettleBatch(ctx context.Context, ms []Message, r Result) error
}

// Transactional is a [Subscription] that can fence message settlement and the
// records produced while processing inside one transaction, so a
// consume-process-produce cycle is atomic: the produced records and the consumed
// offsets are committed together or not at all (exactly-once into a sink).
// Satisfied by Kafka (EOS) only. JetStream has no equivalent and does NOT
// implement it — the capability is absent rather than faked.
//
// The contract is read-only on the message side: a transaction does not change
// how a delivered [Message] is settled (the engine still marks/commits the
// consumed offsets); it adds an atomic boundary that ties any records produced
// through the [Tx] to that settlement. Use it through the state-machine bridge
// (Drive) or directly: assert a subscription to Transactional, then run the
// produce side of the work inside [Transactional.Begin].
type Transactional interface {
	// Begin starts a transaction around processing the consumed message m and runs
	// fn inside it, handing fn a [Tx] to produce records on. If fn returns nil the
	// transaction commits: every record produced through tx is flushed AND m's
	// consumed offset is committed in one atomic unit, so the emitted records and
	// the ack of the input that produced them are exactly-once. If fn returns an
	// error, or the consumer is fenced by a rebalance, the transaction aborts: the
	// produced records are discarded and m's offset is not committed, so m is
	// redelivered. Begin returns fn's error on abort, or a non-nil error if the
	// commit itself failed.
	//
	// m must be a message this subscription delivered. Begin is the only settle
	// path for m when used transactionally: the caller does NOT also call
	// [Subscription.Settle] for m, because Begin commits its offset itself.
	Begin(ctx context.Context, m Message, fn func(ctx context.Context, tx Tx) error) error
}

// Tx is the backend-neutral produce handle a [Transactional.Begin] hands to its
// work function: records produced through it are buffered into the in-flight
// transaction and committed atomically with the consumed offsets, or discarded
// if the transaction aborts. It carries no vendor type; a power user who needs
// the native producer reaches it through the subscription's As escape hatch
// instead. A Tx is valid only for the duration of the Begin call that produced
// it; using it afterward is a programming error.
type Tx interface {
	// Produce enqueues records into the open transaction. They are not visible to
	// a read-committed consumer until the transaction commits. A non-nil error
	// means the produce failed (so the work function should return it, aborting the
	// transaction); a nil error means the records are buffered, not yet committed.
	Produce(ctx context.Context, records ...ProducedRecord) error
}

// ProducedRecord is a backend-neutral record emitted inside a [Tx]: a topic (or
// subject), an optional partitioning key, a value, and optional headers. It is
// the produce-side mirror of the read-only [Message] the consumer delivers, kept
// free of vendor types so the bridge can build it without importing an adapter.
type ProducedRecord struct {
	// Topic is the destination topic (Kafka) or subject the record is produced to.
	// It is required; an empty Topic is a programming error.
	Topic string
	// Key is the optional partitioning key. An empty key lets the backend choose a
	// partition.
	Key []byte
	// Value is the record payload.
	Value []byte
	// Headers are optional record headers carried alongside the value.
	Headers Headers
}

// Deduper suppresses re-delivery of an already-processed message by an
// idempotency key. It is a reserved seam, not a capability the [Hopper] consults
// on its own: the Hopper never type-asserts a subscription to Deduper and never
// calls [Deduper.Seen]. Deduplication is opt-in middleware — wire a Deduper into
// the source/idempotency middleware with its WithDeduper option (it adapts a
// Deduper to the middleware's store via FromDeduper) and add that middleware to
// the Hopper, or call Seen yourself from a handler. The no-op default, with no
// such middleware, is "no deduplication".
//
// Exactly-once into a statechart is provided separately by the source/statemachine
// bridge's version idempotency (a redelivered, already-applied event id is
// skipped against the persisted version); that path does not use this interface.
type Deduper interface {
	// Seen reports whether key has already been processed, recording it if not, so
	// a caller (the idempotency middleware, or a handler) can skip a duplicate by
	// acking without re-running the work.
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
