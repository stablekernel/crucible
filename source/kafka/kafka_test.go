// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/source"
)

// fakePoller is a hand-rolled poller: it serves scripted fetches and records
// every mark, commit, produce, pause, resume, and set-offsets call, so the
// subscription's consume loop and settle logic are covered with no broker.
type fakePoller struct {
	mu sync.Mutex

	fetches []kgo.Fetches // queued fetch batches, served in order
	fetchIx int

	marked     []*kgo.Record
	committed  int
	produced   []*kgo.Record
	produceErr error
	paused     []map[string][]int32
	resumed    []map[string][]int32
	setOffsets []map[string]map[int32]kgo.EpochOffset
	allowed    int
}

func (f *fakePoller) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchIx < len(f.fetches) {
		fx := f.fetches[f.fetchIx]
		f.fetchIx++
		return fx
	}
	// No more scripted fetches: behave like a quiet broker by returning empty.
	// The caller passes a cancelable context so the loop terminates.
	select {
	case <-ctx.Done():
	case <-time.After(time.Millisecond):
	}
	return kgo.Fetches{}
}

func (f *fakePoller) MarkCommitRecords(rs ...*kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marked = append(f.marked, rs...)
}

func (f *fakePoller) CommitMarkedOffsets(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed++
	return nil
}

func (f *fakePoller) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.produced = append(f.produced, rs...)
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r, Err: f.produceErr}
	}
	return out
}

func (f *fakePoller) AllowRebalance() {
	f.mu.Lock()
	f.allowed++
	f.mu.Unlock()
}

func (f *fakePoller) SetOffsets(o map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	f.setOffsets = append(f.setOffsets, o)
	f.mu.Unlock()
}

func (f *fakePoller) PauseFetchPartitions(tp map[string][]int32) map[string][]int32 {
	f.mu.Lock()
	f.paused = append(f.paused, tp)
	f.mu.Unlock()
	return tp
}

func (f *fakePoller) ResumeFetchPartitions(tp map[string][]int32) {
	f.mu.Lock()
	f.resumed = append(f.resumed, tp)
	f.mu.Unlock()
}

func (f *fakePoller) markedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.marked)
}

func (f *fakePoller) producedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.produced)
}

// oneFetch wraps a single record into a one-partition fetch batch.
func oneFetch(rec *kgo.Record) kgo.Fetches {
	return kgo.Fetches{{
		Topics: []kgo.FetchTopic{{
			Topic: rec.Topic,
			Partitions: []kgo.FetchPartition{{
				Partition: rec.Partition,
				Records:   []*kgo.Record{rec},
			}},
		}},
	}}
}

func rec(topic string, partition int32, offset int64, key, value string) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte(key),
		Value:     []byte(value),
	}
}

func newSub(p poller) *subscription {
	return &subscription{client: p, group: "g", dlqTopic: "orders.DLQ"}
}

func TestNextYieldsRecordAsMessage(t *testing.T) {
	t.Parallel()

	r := rec("orders", 2, 41, "A-1", "placed")
	fp := &fakePoller{fetches: []kgo.Fetches{oneFetch(r)}}
	sub := newSub(fp)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m.Key()) != "A-1" || string(m.Value()) != "placed" {
		t.Errorf("key/value = %q/%q, want A-1/placed", m.Key(), m.Value())
	}
	if m.Subject() != "orders" {
		t.Errorf("Subject() = %q, want orders", m.Subject())
	}
	if m.PartitionKey() != "orders/2" {
		t.Errorf("PartitionKey() = %q, want orders/2", m.PartitionKey())
	}
	if got := m.Cursor().String(); got != "orders/2@41" {
		t.Errorf("Cursor() = %q, want orders/2@41", got)
	}

	var back *kgo.Record
	if !m.As(&back) || back != r {
		t.Errorf("As(**kgo.Record) did not recover the record")
	}
}

func TestSettleActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		result       source.Result
		wantMarked   int
		wantProduced int
	}{
		{"ack marks for commit", source.Ack(), 1, 0},
		{"skip (drop) acks", source.Skip(), 1, 0},
		{"nak does not mark", source.Nak(errors.New("transient")), 0, 0},
		{"term produces to dlq then marks", source.Term(errors.New("poison")), 1, 1},
		{"reject (invalid-for-state) dlqs then marks", source.Reject(errors.New("bad state")), 1, 1},
		{"in-progress is a no-op", source.InProgress(), 0, 0},
		{"manual is a no-op", source.Manual(), 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fp := &fakePoller{}
			sub := newSub(fp)
			m := newMessage(rec("orders", 0, 7, "k", "v"))

			if err := sub.Settle(context.Background(), m, tt.result); err != nil {
				t.Fatalf("Settle() error = %v", err)
			}
			if got := fp.markedCount(); got != tt.wantMarked {
				t.Errorf("marked = %d, want %d", got, tt.wantMarked)
			}
			if got := fp.producedCount(); got != tt.wantProduced {
				t.Errorf("produced = %d, want %d", got, tt.wantProduced)
			}
		})
	}
}

func TestSettleTermWithoutDLQTopicFails(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := &subscription{client: fp} // no dlqTopic configured
	m := newMessage(rec("orders", 0, 1, "k", "v"))

	err := sub.Settle(context.Background(), m, source.Term(errors.New("poison")))
	if !errors.Is(err, ErrNoDLQTopic) {
		t.Fatalf("Settle(term) = %v, want ErrNoDLQTopic", err)
	}
	if fp.markedCount() != 0 {
		t.Errorf("marked = %d, want 0 (must not commit a record it could not dead-letter)", fp.markedCount())
	}
}

func TestSettleDLQCarriesDiagnostics(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	orig := rec("orders", 3, 99, "A-1", "placed")
	orig.Headers = []kgo.RecordHeader{{Key: "trace", Value: []byte("t-1")}}
	m := newMessage(orig)

	boom := errors.New("schema mismatch")
	if err := sub.Settle(context.Background(), m, source.Term(boom)); err != nil {
		t.Fatalf("Settle(term) error = %v", err)
	}
	if len(fp.produced) != 1 {
		t.Fatalf("produced = %d records, want 1", len(fp.produced))
	}
	dlq := fp.produced[0]
	if dlq.Topic != "orders.DLQ" {
		t.Errorf("dlq topic = %q, want orders.DLQ", dlq.Topic)
	}
	if string(dlq.Key) != "A-1" || string(dlq.Value) != "placed" {
		t.Errorf("dlq key/value = %q/%q, want A-1/placed", dlq.Key, dlq.Value)
	}
	want := map[string]string{
		"trace":                  "t-1",
		dlqHeaderSourceTopic:     "orders",
		dlqHeaderSourcePartition: "3",
		dlqHeaderSourceOffset:    "99",
		dlqHeaderClass:           "poison",
		dlqHeaderError:           "schema mismatch",
	}
	got := map[string]string{}
	for _, h := range dlq.Headers {
		got[h.Key] = string(h.Value)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("dlq header %q = %q, want %q", k, got[k], v)
		}
	}
}

func TestSettleDLQProduceErrorPropagates(t *testing.T) {
	t.Parallel()

	boom := errors.New("broker down")
	fp := &fakePoller{produceErr: boom}
	sub := newSub(fp)
	m := newMessage(rec("orders", 0, 1, "k", "v"))

	err := sub.Settle(context.Background(), m, source.Term(errors.New("poison")))
	if !errors.Is(err, boom) {
		t.Fatalf("Settle(term) = %v, want wrapped %v", err, boom)
	}
	if fp.markedCount() != 0 {
		t.Errorf("marked = %d, want 0 (a failed dead-letter must not commit)", fp.markedCount())
	}
}

func TestSettleNakWithDelayPausesReseeksResumes(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	m := newMessage(rec("orders", 5, 88, "k", "v"))

	if err := sub.Settle(context.Background(), m, source.NakAfter(time.Millisecond, errors.New("retry"))); err != nil {
		t.Fatalf("Settle(nak-after) error = %v", err)
	}
	if len(fp.paused) != 1 || len(fp.resumed) != 1 {
		t.Fatalf("paused=%d resumed=%d, want 1/1", len(fp.paused), len(fp.resumed))
	}
	if len(fp.setOffsets) != 1 {
		t.Fatalf("setOffsets calls = %d, want 1 (re-seek to the record offset)", len(fp.setOffsets))
	}
	eo, ok := fp.setOffsets[0]["orders"][5]
	if !ok || eo.Offset != 88 {
		t.Errorf("re-seek offset = %+v, want offset 88 on orders/5", eo)
	}
	if fp.markedCount() != 0 {
		t.Errorf("marked = %d, want 0 (a nak never commits)", fp.markedCount())
	}
}

func TestSettleNakWithDelayHonorsContextCancel(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	m := newMessage(rec("orders", 0, 1, "k", "v"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := sub.Settle(ctx, m, source.NakAfter(time.Hour, errors.New("retry")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Settle(nak-after, canceled) = %v, want context.Canceled", err)
	}
	// The partition must still be resumed (deferred) so it is not left paused.
	if len(fp.resumed) != 1 {
		t.Errorf("resumed = %d, want 1 even on cancel", len(fp.resumed))
	}
}

func TestSettleForeignMessageFails(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{})
	err := sub.Settle(context.Background(), foreignMessage{}, source.Ack())
	if !errors.Is(err, errNotKafkaMessage) {
		t.Fatalf("Settle(foreign) = %v, want errNotKafkaMessage", err)
	}
}

func TestCloseDrainsAndCommits(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)

	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if fp.committed != 1 {
		t.Errorf("committed = %d, want 1 (commit marked offsets on close)", fp.committed)
	}
	// Idempotent: a second Close commits nothing more.
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if fp.committed != 1 {
		t.Errorf("committed = %d after second close, want 1", fp.committed)
	}

	// A closed, fully-settled subscription drains.
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Errorf("Next() after drain = %v, want ErrDrained", err)
	}
}

func TestNextRespectsContextCancel(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{} // no scripted fetches: Next blocks on the broker
	sub := newSub(fp)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := sub.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Next() = %v, want DeadlineExceeded", err)
	}
}

func TestNakDelayDoesNotMarkAcrossLane(t *testing.T) {
	t.Parallel()

	// A nak on offset 5 must not commit; an ack on 4 marks only 4. This guards
	// the per-partition high-water-mark reconciliation the spec calls out.
	fp := &fakePoller{}
	sub := newSub(fp)

	four := newMessage(rec("orders", 0, 4, "k", "v4"))
	five := newMessage(rec("orders", 0, 5, "k", "v5"))

	if err := sub.Settle(context.Background(), four, source.Ack()); err != nil {
		t.Fatalf("Settle(4, ack) error = %v", err)
	}
	if err := sub.Settle(context.Background(), five, source.Nak(errors.New("x"))); err != nil {
		t.Fatalf("Settle(5, nak) error = %v", err)
	}
	if fp.markedCount() != 1 {
		t.Fatalf("marked = %d, want exactly 1 (offset 4 only)", fp.markedCount())
	}
	if string(fp.marked[0].Value) != "v4" {
		t.Errorf("marked record = %q, want v4", fp.marked[0].Value)
	}
}

// foreignMessage is a source.Message from another inlet: its As never yields a
// *kgo.Record, so Settle must reject it.
type foreignMessage struct{}

func (foreignMessage) Key() []byte             { return nil }
func (foreignMessage) Value() []byte           { return nil }
func (foreignMessage) Headers() source.Headers { return nil }
func (foreignMessage) Subject() string         { return "other" }
func (foreignMessage) PartitionKey() string    { return "" }
func (foreignMessage) Cursor() source.Cursor   { return nil }
func (foreignMessage) As(any) bool             { return false }
