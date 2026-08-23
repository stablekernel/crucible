// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/source"
)

// poller is the narrow franz-go consume surface the subscription drives. The
// real *kgo.Client satisfies it; narrowing to it keeps the consume loop and
// settle logic unit-testable with a hand-rolled fake and no broker. The methods
// mirror the *kgo.Client signatures this package calls.
type poller interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	MarkCommitRecords(rs ...*kgo.Record)
	CommitMarkedOffsets(ctx context.Context) error
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	AllowRebalance()
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
	PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(topicPartitions map[string][]int32)
}

// Compile-time proof the real client satisfies the narrow seam.
var _ poller = (*kgo.Client)(nil)

// subscription is the franz-go-backed [source.Subscription]. The engine's fetch
// goroutine calls Next; worker goroutines call Settle concurrently. The
// subscription buffers a polled fetch and hands records out one at a time,
// re-polling only once a buffer is exhausted, which is where
// BlockRebalanceOnPoll's safe window opens and closes.
type subscription struct {
	client poller
	// transactSess is the EOS transaction session, present only when the inlet
	// was built with WithTransactional. It is narrowed to the transactor seam so
	// the begin/produce/end choreography is testable with a fake; nil means the
	// subscription is not transactional and Begin reports the capability absent.
	transactSess transactor
	group        string
	dlqTopic     string
	maxPoll      int

	mu       sync.Mutex
	buffer   []*kgo.Record
	inFlight int
	closed   bool
	// notify wakes a Next/NextBatch that is waiting (after Close) for the last
	// in-flight records to settle. Capacity one: sends are best-effort wakeups,
	// and every waiter re-checks the drain state under mu after waking. Guarded
	// by mu; created lazily by signal() so a zero-value subscription stays safe.
	notify chan struct{}

	// onAssignedFn/onRevokedFn are the engine-registered hooks the franz-go
	// rebalance trampolines forward to. Guarded by hookMu.
	hookMu       sync.Mutex
	onAssignedFn func(ctx context.Context, assigned []source.Partition)
	onRevokedFn  func(ctx context.Context, revoked []source.Partition)
}

// Next returns the next buffered record, polling the broker when the buffer is
// empty. It blocks until a record is available, returns ctx.Err() on
// cancellation, or [source.ErrDrained] once the subscription is closed and all
// delivered records are settled. After Close it never polls for new records:
// only records already fetched into the buffer are yielded, and once the buffer
// is empty Next blocks until the remaining in-flight records settle, then
// returns ErrDrained. Next is single-consumer (the engine's fetch loop).
func (s *subscription) Next(ctx context.Context) (source.Message, error) {
	for {
		if rec, ok := s.takeBuffered(); ok {
			return newMessage(rec), nil
		}

		s.mu.Lock()
		closed, drained := s.closed, s.inFlight == 0
		s.mu.Unlock()
		if closed && drained {
			return nil, source.ErrDrained
		}
		if closed {
			// Draining with records still in flight: hold here — do not poll —
			// until the last settle wakes us, or the caller gives up.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.signal():
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// A prior poll's records are all handed out: release any rebalance
		// blocked on the last poll before fetching again, so a pending
		// assignment change can proceed in the gap between batches.
		s.client.AllowRebalance()

		fetches := s.client.PollRecords(ctx, s.maxPoll)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			// Surface the first non-context fetch error; the engine decides
			// whether to retry the loop. Records fetched alongside an error are
			// deliberately discarded rather than buffered: none of them have
			// been delivered, so nothing can be settled or marked from them,
			// and the next successful poll refetches their offsets.
			for _, fe := range errs {
				if fe.Err != nil && fe.Err != context.Canceled && fe.Err != context.DeadlineExceeded {
					return nil, fmt.Errorf("source/kafka: poll %s[%d]: %w", fe.Topic, fe.Partition, fe.Err)
				}
			}
		}

		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		s.mu.Lock()
		s.buffer = recs
		s.mu.Unlock()
	}
}

// signal returns the wakeup channel for drain waiters, creating it on first
// use so a zero-value subscription stays safe.
func (s *subscription) signal() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notify == nil {
		s.notify = make(chan struct{}, 1)
	}
	return s.notify
}

// wake nudges any Next/NextBatch waiting for in-flight settles after Close.
// Best-effort: a coalesced wakeup is fine because every waiter re-checks the
// drain state after receiving. Call with s.mu held.
func (s *subscription) wake() {
	if s.notify != nil {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
}

// takeBuffered pops one record from the buffer and counts it in flight.
func (s *subscription) takeBuffered() (*kgo.Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buffer) == 0 {
		return nil, false
	}
	rec := s.buffer[0]
	s.buffer = s.buffer[1:]
	s.inFlight++
	return rec, true
}

// takeBatch pops up to limit records from the buffer and counts them in flight.
// It reports false only when the buffer is empty, so [NextBatch] can fall through
// to a poll.
func (s *subscription) takeBatch(limit int) ([]*kgo.Record, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buffer) == 0 {
		return nil, false
	}
	n := limit
	if n > len(s.buffer) {
		n = len(s.buffer)
	}
	recs := s.buffer[:n:n]
	s.buffer = s.buffer[n:]
	s.inFlight += n
	return recs, true
}

// NextBatch returns up to limit buffered records, polling the broker when the
// buffer is empty, satisfying [source.Batched]. franz-go already fetches in
// batches (PollRecords), so this exposes a poll's records as a group rather than
// draining them one at a time; the engine settles each through SettleBatch.
// After Close it never polls for new records: only already-buffered records are
// yielded, and once the buffer is empty it blocks until the remaining in-flight
// records settle, then returns [source.ErrDrained] — the same drain contract as
// [Next]. NextBatch is single-consumer, like Next.
func (s *subscription) NextBatch(ctx context.Context, limit int) ([]source.Message, error) {
	if limit < 1 {
		limit = 1
	}
	for {
		if recs, ok := s.takeBatch(limit); ok {
			msgs := make([]source.Message, len(recs))
			for i, rec := range recs {
				msgs[i] = newMessage(rec)
			}
			return msgs, nil
		}

		s.mu.Lock()
		closed, drained := s.closed, s.inFlight == 0
		s.mu.Unlock()
		if closed && drained {
			return nil, source.ErrDrained
		}
		if closed {
			// Draining with records still in flight: hold here — do not poll —
			// until the last settle wakes us, or the caller gives up.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.signal():
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		s.client.AllowRebalance()
		fetches := s.client.PollRecords(ctx, s.maxPoll)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err != nil && fe.Err != context.Canceled && fe.Err != context.DeadlineExceeded {
					return nil, fmt.Errorf("source/kafka: poll %s[%d]: %w", fe.Topic, fe.Partition, fe.Err)
				}
			}
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		s.mu.Lock()
		s.buffer = recs
		s.mu.Unlock()
	}
}

// SettleBatch applies r to every message in ms in one call, satisfying
// [source.Batched]. It settles each record through the same per-record path as
// [Settle] (mark, produce-to-DLQ, or no-op), returning the first error
// encountered while still attempting every message.
func (s *subscription) SettleBatch(ctx context.Context, ms []source.Message, r source.Result) error {
	var firstErr error
	for _, m := range ms {
		if err := s.Settle(ctx, m, r); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Settle applies r to a record previously returned by Next, translating the
// [source.Action] onto Kafka's commit/produce vocabulary per the ack model.
// Settle may be called from many worker goroutines and is safe for concurrent
// use.
func (s *subscription) Settle(ctx context.Context, m source.Message, r source.Result) error {
	defer s.settled()

	rec, ok := recordOf(m)
	if !ok {
		return fmt.Errorf("source/kafka: settle: %w", errNotKafkaMessage)
	}

	switch r.Action {
	case source.ActionAck:
		// Mark for commit; AutoCommitMarks advances the offset only past
		// processed records (commit-after-process). Drop (Skip) acks too.
		s.client.MarkCommitRecords(rec)
		return nil

	case source.ActionNak:
		// Redeliver: never mark the record. The partition is paused, re-seeked
		// to this record's offset so it (and anything after it) is fetched
		// again in this session, then resumed; a Requeue delay waits out the
		// pause, and a plain Nak re-seeks immediately (delay zero). Redelivery
		// across restarts/rebalances rides committed offsets, so a concurrent
		// ack that commits past this record can skip it after such a restart —
		// a documented best-effort divergence, not an in-session one.
		return s.requeueWithDelay(ctx, rec, r.Requeue)

	case source.ActionTerm:
		// Produce to the dead-letter topic, then mark so it is not re-read.
		if err := s.deadLetter(ctx, rec, r); err != nil {
			return err
		}
		s.client.MarkCommitRecords(rec)
		return nil

	case source.ActionInProgress:
		// Kafka has no per-message ack deadline to extend.
		return nil

	case source.ActionManual:
		// The handler settled the record itself via Message.As + the client.
		return nil

	default:
		return fmt.Errorf("source/kafka: settle: unknown action %q", r.Action)
	}
}

// settled decrements the in-flight count and wakes any drain waiter; deferred
// from Settle so it runs on every path, including errors, keeping the drain
// accounting honest.
func (s *subscription) settled() {
	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.wake()
	s.mu.Unlock()
}

// requeueWithDelay implements Nak redelivery: pause the record's partition so
// no further records are fetched from it, wait out the delay (zero for a plain
// Nak), re-seek delivery to the record's own offset so it is re-read, then
// resume the partition.
//
// Cost model, documented as best-effort by design: the delay blocks only the
// calling settle goroutine, but the pause head-of-line-blocks the whole
// partition for the delay's duration (Kafka has no native per-message
// redelivery delay), and records fetched before the seek but not yet yielded
// are still delivered ahead of the redelivered record. A concurrent ack on the
// same partition can also commit past the nacked offset before the re-seek
// lands; the commit advances the persisted position while the live fetch keeps
// reading from the seeked offset until the next rebalance or restart.
func (s *subscription) requeueWithDelay(ctx context.Context, rec *kgo.Record, d time.Duration) error {
	tp := map[string][]int32{rec.Topic: {rec.Partition}}
	s.client.PauseFetchPartitions(tp)
	defer s.client.ResumeFetchPartitions(tp)

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	// Re-seek to exactly this record's offset so it (and anything after it on
	// the partition) is delivered again; leave the leader epoch unset.
	s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		rec.Topic: {rec.Partition: {Offset: rec.Offset, Epoch: -1}},
	})
	return nil
}

// deadLetter produces rec to the configured dead-letter topic, copying the key,
// value, and original headers and appending diagnostic headers (the source
// topic/partition/offset and the rejection class and error). It fails if no DLQ
// topic was configured.
func (s *subscription) deadLetter(ctx context.Context, rec *kgo.Record, r source.Result) error {
	if s.dlqTopic == "" {
		return ErrNoDLQTopic
	}
	dlq := &kgo.Record{
		Topic:   s.dlqTopic,
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: dlqHeaders(rec, r),
	}
	if err := s.client.ProduceSync(ctx, dlq).FirstErr(); err != nil {
		return fmt.Errorf("source/kafka: dead-letter to %q: %w", s.dlqTopic, err)
	}
	return nil
}

// dlqHeaders builds the dead-letter record's headers: the original headers plus
// crucible-namespaced diagnostics so a parking-topic consumer can see where the
// record came from and why it was rejected.
func dlqHeaders(rec *kgo.Record, r source.Result) []kgo.RecordHeader {
	hs := make([]kgo.RecordHeader, 0, len(rec.Headers)+5)
	hs = append(hs, rec.Headers...)
	hs = append(hs,
		kgo.RecordHeader{Key: dlqHeaderSourceTopic, Value: []byte(rec.Topic)},
		kgo.RecordHeader{Key: dlqHeaderSourcePartition, Value: []byte(strconv.FormatInt(int64(rec.Partition), 10))},
		kgo.RecordHeader{Key: dlqHeaderSourceOffset, Value: []byte(strconv.FormatInt(rec.Offset, 10))},
		kgo.RecordHeader{Key: dlqHeaderClass, Value: []byte(r.Class.String())},
	)
	if r.Err != nil {
		hs = append(hs, kgo.RecordHeader{Key: dlqHeaderError, Value: []byte(r.Err.Error())})
	}
	return hs
}

const (
	dlqHeaderSourceTopic     = "crucible-source-topic"
	dlqHeaderSourcePartition = "crucible-source-partition"
	dlqHeaderSourceOffset    = "crucible-source-offset"
	dlqHeaderClass           = "crucible-class"
	dlqHeaderError           = "crucible-error"
)

// Close begins a graceful drain: from the moment it returns, Next/NextBatch
// stop fetching new records and yield only what was already buffered; once the
// in-flight records settle, Next returns [source.ErrDrained]. Marked offsets
// are committed best-effort so a clean shutdown does not re-read
// already-processed records. Close is idempotent.
func (s *subscription) Close() error {
	s.mu.Lock()
	already := s.closed
	s.closed = true
	s.wake()
	s.mu.Unlock()
	if already {
		return nil
	}
	// Best-effort commit of marked offsets so a clean shutdown does not re-read
	// already-processed records; the engine has stopped marking by now.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.client.CommitMarkedOffsets(ctx); err != nil {
		return fmt.Errorf("source/kafka: commit on close: %w", err)
	}
	return nil
}

// Compile-time assertions: the subscription satisfies the core interface and
// the capability interfaces this adapter honestly implements.
var (
	_ source.Subscription     = (*subscription)(nil)
	_ source.Seekable         = (*subscription)(nil)
	_ source.ConsumerGroups   = (*subscription)(nil)
	_ source.PartitionOrdered = (*subscription)(nil)
	_ source.LagReporter      = (*subscription)(nil)
	_ source.Transactional    = (*subscription)(nil)
	_ source.Batched          = (*subscription)(nil)
)
