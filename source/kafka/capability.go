// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/stablekernel/crucible/source"
)

// Sentinel errors the capability methods return so callers can branch with
// errors.Is rather than string-matching.
var (
	// errSeekUnavailable reports that a seek or lag call cannot reach the
	// franz-go client (the subscription was driven by a test poller that does
	// not satisfy the broker-request surface).
	errSeekUnavailable = errors.New("seek/lag requires the franz-go client")
	// errForeignCursor reports that SeekToCursor was given a cursor this adapter
	// did not issue.
	errForeignCursor = errors.New("cursor was not issued by this kafka inlet")
	// errNotTransactional reports that Begin was called on a subscription whose
	// inlet was not built with WithTransactional.
	errNotTransactional = errors.New("inlet is not transactional; build it with WithTransactional")
	// errTransactionAborted reports that End returned without committing despite
	// the work function succeeding (a broker-side abort).
	errTransactionAborted = errors.New("source/kafka: transaction aborted by broker")
)

// requester is the franz-go surface the seek/lag capabilities need beyond the
// poller seam: issuing a ListOffsets request to resolve timestamps and the
// stream tail, and reading committed offsets. *kgo.Client satisfies it.
type requester interface {
	Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error)
	CommittedOffsets() map[string]map[int32]kgo.EpochOffset
	GetConsumeTopics() []string
}

// asRequester recovers the requester behind the subscription's poller. The real
// client satisfies it; a fake poller in a unit test may not, in which case the
// seek/lag methods report they are unavailable rather than panicking.
func (s *subscription) asRequester() (requester, bool) {
	r, ok := s.client.(requester)
	return r, ok
}

// --- Seekable ---------------------------------------------------------------

// SeekToTime repositions every currently-assigned partition to the first record
// at or after t, taking effect on the next [Subscription.Next]. It resolves the
// timestamp to per-partition offsets with a ListOffsets request, then applies
// them with SetOffsets — the live-reposition path a group consumer supports
// without being recreated.
func (s *subscription) SeekToTime(ctx context.Context, t time.Time) error {
	r, ok := s.asRequester()
	if !ok {
		return fmt.Errorf("source/kafka: seek to time: %w", errSeekUnavailable)
	}
	offsets, err := listOffsets(ctx, r, s.assignedTopics(r), t.UnixMilli())
	if err != nil {
		return fmt.Errorf("source/kafka: seek to time: %w", err)
	}
	s.client.SetOffsets(offsets)
	return nil
}

// SeekToCursor repositions delivery to resume from c, re-delivering the record
// at the cursor's offset and everything after it. The cursor must be one this
// adapter issued (an offsetCursor); any other cursor is rejected.
func (s *subscription) SeekToCursor(_ context.Context, c source.Cursor) error {
	oc, ok := c.(offsetCursor)
	if !ok {
		return fmt.Errorf("source/kafka: seek to cursor: %w", errForeignCursor)
	}
	s.client.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		oc.topic: {oc.partition: {Offset: oc.offset, Epoch: -1}},
	})
	return nil
}

// SeekToStart repositions every assigned partition to its earliest retained
// record (logical offset -2).
func (s *subscription) SeekToStart(_ context.Context) error {
	return s.seekLogical(-2)
}

// SeekToEnd repositions every assigned partition to its tail (logical offset
// -1), skipping the backlog so only records produced after the seek are
// delivered.
func (s *subscription) SeekToEnd(_ context.Context) error {
	return s.seekLogical(-1)
}

// seekLogical applies a Kafka logical offset (-2 earliest, -1 latest) to every
// currently-assigned partition via SetOffsets.
func (s *subscription) seekLogical(logical int64) error {
	r, ok := s.asRequester()
	if !ok {
		return fmt.Errorf("source/kafka: seek: %w", errSeekUnavailable)
	}
	set := map[string]map[int32]kgo.EpochOffset{}
	for topic, parts := range r.CommittedOffsets() {
		set[topic] = map[int32]kgo.EpochOffset{}
		for p := range parts {
			set[topic][p] = kgo.EpochOffset{Offset: logical, Epoch: -1}
		}
	}
	if len(set) == 0 {
		return nil
	}
	s.client.SetOffsets(set)
	return nil
}

// assignedTopics reports the topics the consumer is currently assigned, the set
// SeekToTime resolves offsets across.
func (s *subscription) assignedTopics(r requester) []string {
	committed := r.CommittedOffsets()
	if len(committed) > 0 {
		topics := make([]string, 0, len(committed))
		for t := range committed {
			topics = append(topics, t)
		}
		return topics
	}
	return r.GetConsumeTopics()
}

// --- ConsumerGroups ---------------------------------------------------------

// GroupID returns the consumer group the subscription joined, or "" for a
// standalone subscription.
func (s *subscription) GroupID() string { return s.group }

// OnAssigned registers a callback invoked when partitions are assigned to this
// member, before their records are delivered. It is forwarded from franz-go's
// rebalance hook.
func (s *subscription) OnAssigned(fn func(ctx context.Context, assigned []source.Partition)) {
	s.hookMu.Lock()
	s.onAssignedFn = fn
	s.hookMu.Unlock()
}

// OnRevoked registers a callback invoked before partitions are revoked — the
// window in which the consumer drains in-flight work and commits. The adapter
// commits marked offsets after the callback returns, before releasing the
// partitions.
func (s *subscription) OnRevoked(fn func(ctx context.Context, revoked []source.Partition)) {
	s.hookMu.Lock()
	s.onRevokedFn = fn
	s.hookMu.Unlock()
}

// onAssigned is the franz-go trampoline: it maps the native assignment map onto
// neutral [source.Partition]s and forwards to the engine-registered hook.
func (s *subscription) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.hookMu.Lock()
	fn := s.onAssignedFn
	s.hookMu.Unlock()
	if fn != nil {
		fn(ctx, toPartitions(assigned))
	}
}

// onRevoked is the franz-go trampoline for a graceful revoke: forward to the
// engine hook so it drains, then commit marked offsets so processed records are
// not re-read after the partitions move. franz-go calls this synchronously
// inside the rebalance, so the commit completes before the partitions leave.
func (s *subscription) onRevoked(ctx context.Context, _ *kgo.Client, revoked map[string][]int32) {
	s.hookMu.Lock()
	fn := s.onRevokedFn
	s.hookMu.Unlock()
	if fn != nil {
		fn(ctx, toPartitions(revoked))
	}
	_ = s.client.CommitMarkedOffsets(ctx)
}

// onLost is the franz-go trampoline for an ungraceful revoke (the member lost
// the partitions without a chance to commit). It forwards to the revoke hook so
// the engine stops working them, but does not attempt a commit — the offsets
// are no longer this member's to commit.
func (s *subscription) onLost(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.hookMu.Lock()
	fn := s.onRevokedFn
	s.hookMu.Unlock()
	if fn != nil {
		fn(ctx, toPartitions(lost))
	}
}

// toPartitions maps a franz-go topic→partitions map onto neutral
// [source.Partition]s.
func toPartitions(m map[string][]int32) []source.Partition {
	var ps []source.Partition
	for topic, ids := range m {
		for _, id := range ids {
			ps = append(ps, source.Partition{Topic: topic, ID: id})
		}
	}
	return ps
}

// --- PartitionOrdered -------------------------------------------------------

// PartitionOrdered marks the subscription as guaranteeing per-partition order;
// its presence is the guarantee the engine keys its ordered lanes on.
func (s *subscription) PartitionOrdered() {}

// --- LagReporter ------------------------------------------------------------

// Lag reports the number of unconsumed records between the committed position
// and the stream tail across all assigned partitions. It resolves the tail with
// a ListOffsets request (timestamp -1) and subtracts the committed offsets.
func (s *subscription) Lag(ctx context.Context) (int64, error) {
	r, ok := s.asRequester()
	if !ok {
		return 0, fmt.Errorf("source/kafka: lag: %w", errSeekUnavailable)
	}
	committed := r.CommittedOffsets()
	if len(committed) == 0 {
		return 0, nil
	}
	topics := make([]string, 0, len(committed))
	for t := range committed {
		topics = append(topics, t)
	}
	ends, err := listOffsets(ctx, r, topics, -1)
	if err != nil {
		return 0, fmt.Errorf("source/kafka: lag: %w", err)
	}
	var lag int64
	for topic, parts := range ends {
		for p, end := range parts {
			c, ok := committed[topic][p]
			if !ok {
				continue
			}
			if d := end.Offset - c.Offset; d > 0 {
				lag += d
			}
		}
	}
	return lag, nil
}

// --- Transactional ----------------------------------------------------------

// Begin runs fn inside a Kafka producer transaction so consumed records settled
// during it are committed (or aborted) atomically with the produces fn performs
// — exactly-once consume-process-produce. It is available only when the inlet
// was built with [WithTransactional]; otherwise it reports the capability is
// absent rather than silently running fn without a transaction.
func (s *subscription) Begin(ctx context.Context, fn func(ctx context.Context) error) error {
	if s.transactSess == nil {
		return fmt.Errorf("source/kafka: transactional: %w", errNotTransactional)
	}
	if err := s.transactSess.Begin(); err != nil {
		return fmt.Errorf("source/kafka: begin transaction: %w", err)
	}
	fnErr := fn(ctx)
	commit := kgo.TryCommit
	if fnErr != nil {
		commit = kgo.TryAbort
	}
	committed, endErr := s.transactSess.End(ctx, commit)
	if endErr != nil {
		return fmt.Errorf("source/kafka: end transaction: %w", endErr)
	}
	if fnErr != nil {
		return fnErr
	}
	if !committed {
		return errTransactionAborted
	}
	return nil
}

// listOffsets resolves per-partition offsets for the given topics at timestamp
// ts (millis; -1 latest, -2 earliest) via a ListOffsets request, returning the
// EpochOffset map SetOffsets consumes. Partitions are discovered from the
// requester's committed offsets so the request targets exactly what the
// consumer holds.
func listOffsets(ctx context.Context, r requester, topics []string, ts int64) (map[string]map[int32]kgo.EpochOffset, error) {
	committed := r.CommittedOffsets()
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	for _, topic := range topics {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		for p := range committed[topic] {
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition = p
			rp.Timestamp = ts
			rp.CurrentLeaderEpoch = -1
			rt.Partitions = append(rt.Partitions, rp)
		}
		if len(rt.Partitions) > 0 {
			req.Topics = append(req.Topics, rt)
		}
	}
	if len(req.Topics) == 0 {
		return map[string]map[int32]kgo.EpochOffset{}, nil
	}

	resp, err := r.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	lr, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", resp)
	}

	out := map[string]map[int32]kgo.EpochOffset{}
	for _, t := range lr.Topics {
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, &kmsgError{code: p.ErrorCode, topic: t.Topic, partition: p.Partition}
			}
			if out[t.Topic] == nil {
				out[t.Topic] = map[int32]kgo.EpochOffset{}
			}
			out[t.Topic][p.Partition] = kgo.EpochOffset{Offset: p.Offset, Epoch: p.LeaderEpoch}
		}
	}
	return out, nil
}

// kmsgError reports a per-partition error code from a ListOffsets response.
type kmsgError struct {
	code      int16
	topic     string
	partition int32
}

func (e *kmsgError) Error() string {
	return fmt.Sprintf("list offsets %s[%d]: kafka error code %d", e.topic, e.partition, e.code)
}
