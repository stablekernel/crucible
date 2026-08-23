// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
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
	// errEmptyTopic reports that a ProducedRecord with no Topic was produced into
	// a transaction; the destination is required.
	errEmptyTopic = errors.New("source/kafka: produced record has no topic")
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
// without being recreated. Partitions are enumerated from committed offsets
// when present, else discovered from broker metadata, so a cold group (nothing
// committed yet) still seeks across its consume topics.
func (s *subscription) SeekToTime(ctx context.Context, t time.Time) error {
	r, ok := s.asRequester()
	if !ok {
		return fmt.Errorf("source/kafka: seek to time: %w", errSeekUnavailable)
	}
	parts, err := assignedPartitions(ctx, r)
	if err != nil {
		return fmt.Errorf("source/kafka: seek to time: %w", err)
	}
	offsets, err := listOffsets(ctx, r, parts, t.UnixMilli())
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
func (s *subscription) SeekToStart(ctx context.Context) error {
	return s.seekLogical(ctx, -2)
}

// SeekToEnd repositions every assigned partition to its tail (logical offset
// -1), skipping the backlog so only records produced after the seek are
// delivered.
func (s *subscription) SeekToEnd(ctx context.Context) error {
	return s.seekLogical(ctx, -1)
}

// seekLogical applies a Kafka logical offset (-2 earliest, -1 latest) to every
// currently-assigned partition via SetOffsets. Partitions are enumerated from
// committed offsets when present, else discovered from broker metadata for the
// consume topics — so a cold group (nothing committed) still seeks instead of
// silently no-oping.
func (s *subscription) seekLogical(ctx context.Context, logical int64) error {
	r, ok := s.asRequester()
	if !ok {
		return fmt.Errorf("source/kafka: seek: %w", errSeekUnavailable)
	}
	parts, err := assignedPartitions(ctx, r)
	if err != nil {
		return fmt.Errorf("source/kafka: seek: %w", err)
	}
	set := map[string]map[int32]kgo.EpochOffset{}
	for topic, ps := range parts {
		set[topic] = map[int32]kgo.EpochOffset{}
		for _, p := range ps {
			set[topic][p] = kgo.EpochOffset{Offset: logical, Epoch: -1}
		}
	}
	s.client.SetOffsets(set)
	return nil
}

// assignedPartitions enumerates the partitions the consumer works across:
// committed offsets where present per topic, broker metadata discovery for any
// consume topic without commits (a cold group), and the consume topics
// themselves when nothing at all has been committed.
func assignedPartitions(ctx context.Context, r requester) (map[string][]int32, error) {
	committed := r.CommittedOffsets()
	consume := r.GetConsumeTopics()
	if len(committed) == 0 && len(consume) == 0 {
		return map[string][]int32{}, nil
	}
	topics := make([]string, 0, len(committed)+len(consume))
	seen := map[string]bool{}
	add := func(ts []string) {
		for _, t := range ts {
			if t != "" && !seen[t] {
				seen[t] = true
				topics = append(topics, t)
			}
		}
	}
	for t := range committed {
		add([]string{t})
	}
	add(consume)

	out := make(map[string][]int32, len(topics))
	var discover []string
	for _, t := range topics {
		for p := range committed[t] {
			out[t] = append(out[t], p)
		}
		if len(out[t]) == 0 {
			discover = append(discover, t)
		}
	}
	if len(discover) > 0 {
		found, err := topicPartitions(ctx, r, discover)
		if err != nil {
			return nil, err
		}
		for t, ps := range found {
			out[t] = append(out[t], ps...)
		}
	}
	for _, t := range topics {
		if len(out[t]) == 0 {
			delete(out, t)
		}
	}
	return out, nil
}

// topicPartitions discovers a topic's partition ids from broker metadata via a
// MetadataRequest — the fallback that lets seek/lag act before the group's
// first commit. A per-topic or per-partition error code is returned as an
// error.
func topicPartitions(ctx context.Context, r requester, topics []string) (map[string][]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}
	resp, err := r.Request(ctx, req)
	if err != nil {
		return nil, err
	}
	mr, ok := resp.(*kmsg.MetadataResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", resp)
	}
	out := make(map[string][]int32, len(topics))
	for _, t := range mr.Topics {
		name := ""
		if t.Topic != nil {
			name = *t.Topic
		}
		if t.ErrorCode != 0 {
			return nil, &kmsgError{code: t.ErrorCode, topic: name}
		}
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				return nil, &kmsgError{code: p.ErrorCode, topic: name, partition: p.Partition}
			}
			out[name] = append(out[name], p.Partition)
		}
	}
	return out, nil
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
// and the stream tail across all partitions with a committed offset. It
// resolves the tail with a ListOffsets request (timestamp -1) and subtracts
// the committed offsets. Partitions with no commit yet are excluded — without
// a commit there is no baseline to measure from — and a cold group (no commits
// at all) reports [ErrNoCommittedOffsets] rather than a misleading 0.
func (s *subscription) Lag(ctx context.Context) (int64, error) {
	r, ok := s.asRequester()
	if !ok {
		return 0, fmt.Errorf("source/kafka: lag: %w", errSeekUnavailable)
	}
	committed := r.CommittedOffsets()
	if len(committed) == 0 {
		return 0, fmt.Errorf("source/kafka: lag: %w", ErrNoCommittedOffsets)
	}
	parts := map[string][]int32{}
	for t, ps := range committed {
		for p := range ps {
			parts[t] = append(parts[t], p)
		}
	}
	ends, err := listOffsets(ctx, r, parts, -1)
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

// transactor is the narrow franz-go transaction surface the EOS path drives: a
// *kgo.GroupTransactSession satisfies it. Narrowing to it keeps the begin →
// produce → end choreography unit-testable with a fake session and no broker.
// ProduceSync produces records into the open transaction; End commits or aborts,
// committing the consumed offsets atomically with the produced records when it
// commits.
type transactor interface {
	Begin() error
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	End(ctx context.Context, commit kgo.TransactionEndTry) (committed bool, err error)
}

// Compile-time proof the real session satisfies the narrow seam.
var _ transactor = (*kgo.GroupTransactSession)(nil)

// txHandle is the [source.Tx] the EOS path hands to a transactional work
// function: it produces records into the open transaction through the
// [transactor]. It buffers nothing of its own — franz-go holds the in-flight
// records until End commits or aborts them — so Produce is a direct, synchronous
// produce into the transaction whose error the work function propagates to
// trigger an abort. A txHandle is valid only for the Begin call that created it.
type txHandle struct {
	sess transactor
}

// Produce emits records into the open transaction. The neutral
// [source.ProducedRecord]s are mapped onto franz-go records and produced
// synchronously; a produce error is returned so the work function can abort the
// transaction by returning it. Produced records are not visible to a
// read-committed consumer until the transaction commits.
func (t txHandle) Produce(ctx context.Context, records ...source.ProducedRecord) error {
	if len(records) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(records))
	for _, pr := range records {
		if pr.Topic == "" {
			return fmt.Errorf("source/kafka: produce in transaction: %w", errEmptyTopic)
		}
		recs = append(recs, &kgo.Record{
			Topic:   pr.Topic,
			Key:     pr.Key,
			Value:   pr.Value,
			Headers: toRecordHeaders(pr.Headers),
		})
	}
	if err := t.sess.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("source/kafka: produce in transaction: %w", err)
	}
	return nil
}

// toRecordHeaders maps neutral [source.Headers] onto franz-go record headers.
func toRecordHeaders(hs source.Headers) []kgo.RecordHeader {
	if len(hs) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, len(hs))
	for i, h := range hs {
		out[i] = kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)}
	}
	return out
}

// Begin runs fn inside a Kafka producer transaction so the records fn produces
// through the handed [source.Tx] are committed (or aborted) atomically with the
// consumed offset of m — exactly-once consume-process-produce. The choreography
// is: begin the transaction, run fn (which produces emitted records through the
// txHandle), mark m's offset for commit on success, then End with TryCommit if fn
// succeeded or TryAbort if it failed. On commit, franz-go flushes the produced
// records and commits the group's marked offsets (including m's) in one unit; on
// abort it discards the produced records and does not commit m's offset, so m is
// redelivered.
//
// m's offset is marked only after fn succeeds and only inside the transaction, so
// a failed transform never advances the consumed position: the commit-after-
// process invariant holds, now atomically with the produce side. Begin is the
// full settle path for m; the caller must not also call [Subscription.Settle]
// for m. A direct Settle for m during an open Begin, or after Begin returned,
// is a caller error with undefined results — it is not detected or coordinated
// with the transaction: at best it duplicates an already-committed mark
// (harmless at-least-once), at worst it races End's commit/abort. fn must not
// panic: a panic is not recovered; it propagates to the caller of Begin,
// leaving the transaction open until a later End, the session closes, or a
// rebalance fences the producer (which aborts it).
//
// It is available only when the inlet was built with [WithTransactional];
// otherwise it reports the capability is absent rather than silently running fn
// without a transaction. A rebalance during the transaction fences the producer,
// so End reports the transaction did not commit and the work is retried after
// reassignment.
//
// Poison handling on a transactional subscription flows through Begin too:
// settling [source.Term] directly is rejected with [ErrTermInsideTransaction];
// every such attempt leaves the record unmarked, so a handler retrying Term in
// a loop simply sees the record again on its next fetch — route it through
// Begin instead. Inside fn, produce the rejected record to the dead-letter
// topic via the handed [source.Tx] and return nil, so the DLQ write and the
// consumed offset commit atomically. Begin is the only settle path for m when
// used transactionally: mixing a direct [Subscription.Settle] for m into an
// open Begin is a caller error and its behavior is undefined — the direct
// settle marks independently of the transaction, so it can double-advance the
// partition or race End's commit/abort.
func (s *subscription) Begin(ctx context.Context, m source.Message, fn func(ctx context.Context, tx source.Tx) error) error {
	if s.transactSess == nil {
		return fmt.Errorf("source/kafka: transactional: %w", errNotTransactional)
	}
	rec, ok := recordOf(m)
	if !ok {
		return fmt.Errorf("source/kafka: transactional begin: %w", errNotKafkaMessage)
	}
	defer s.settled()

	if err := s.transactSess.Begin(); err != nil {
		return fmt.Errorf("source/kafka: begin transaction: %w", err)
	}
	fnErr := fn(ctx, txHandle{sess: s.transactSess})
	if fnErr == nil {
		// Mark the consumed record only on success and inside the open
		// transaction, so End commits the produced records and this offset as one
		// unit. A failed transform leaves the offset unmarked, so m is redelivered.
		s.client.MarkCommitRecords(rec)
	}
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

// listOffsets resolves per-partition offsets for the given topic→partitions
// map at timestamp ts (millis; -1 latest, -2 earliest) via a ListOffsets
// request, returning the EpochOffset map SetOffsets consumes. The caller
// enumerates partitions (assignedPartitions), so a cold group's discovered
// partitions are included.
func listOffsets(ctx context.Context, r requester, parts map[string][]int32, ts int64) (map[string]map[int32]kgo.EpochOffset, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	for _, topic := range slices.Sorted(maps.Keys(parts)) {
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		for _, p := range parts[topic] {
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

// kmsgError reports a per-topic or per-partition error code from a Kafka
// protocol response (ListOffsets, Metadata).
type kmsgError struct {
	code      int16
	topic     string
	partition int32
}

func (e *kmsgError) Error() string {
	if e.topic == "" {
		return fmt.Sprintf("kafka error code %d", e.code)
	}
	return fmt.Sprintf("kafka error code %d on %s[%d]", e.code, e.topic, e.partition)
}
