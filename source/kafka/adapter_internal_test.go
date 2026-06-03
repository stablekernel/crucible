// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/stablekernel/crucible/source"
)

func TestNewRequiresSeedBrokersOrClient(t *testing.T) {
	t.Parallel()

	if _, err := New(); !errors.Is(err, ErrNoSeedBrokers) {
		t.Fatalf("New() = %v, want ErrNoSeedBrokers", err)
	}
	if _, err := New(WithSeedBrokers("localhost:9092")); err != nil {
		t.Fatalf("New(seed) error = %v", err)
	}
}

func TestOptionsIgnoreEmptyValues(t *testing.T) {
	t.Parallel()

	cfg := config{}
	for _, o := range []Option{
		WithSeedBrokers("", ""),
		WithClientID(""),
		WithDLQTopic(""),
		WithSASL(nil),
		WithTLS(nil),
		WithBalancer(nil),
		WithMaxPollRecords(0),
		WithClient(nil),
		WithClientOptions(nil),
	} {
		o(&cfg)
	}
	if len(cfg.seedBrokers) != 0 || cfg.clientID != "" || cfg.dlqTopic != "" ||
		len(cfg.sasl) != 0 || cfg.tlsConfig != nil || len(cfg.balancers) != 0 ||
		cfg.maxPoll != 0 || cfg.client != nil || len(cfg.extraOpts) != 0 {
		t.Fatalf("empty options mutated config: %+v", cfg)
	}
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()

	cfg := config{}
	WithSeedBrokers("b1:9092", "b2:9092")(&cfg)
	WithClientID("crucible")(&cfg)
	WithDLQTopic("orders.DLQ")(&cfg)
	WithMaxPollRecords(128)(&cfg)
	WithBalancer(kgo.CooperativeStickyBalancer())(&cfg)
	WithTransactional("orders-eos-v1")(&cfg)

	if len(cfg.seedBrokers) != 2 || cfg.clientID != "crucible" || cfg.dlqTopic != "orders.DLQ" {
		t.Errorf("config = %+v, want seeds/clientID/dlq applied", cfg)
	}
	if cfg.maxPoll != 128 || len(cfg.balancers) != 1 || !cfg.transact || cfg.transactID != "orders-eos-v1" {
		t.Errorf("config = %+v, want maxPoll/balancer/transact applied", cfg)
	}
}

func TestWithTransactionalEmptyIDIgnored(t *testing.T) {
	t.Parallel()

	cfg := config{}
	WithTransactional("")(&cfg)
	if cfg.transact || cfg.transactID != "" {
		t.Errorf("WithTransactional(\"\") = %+v, want non-transactional", cfg)
	}
}

func TestSubscribeRejectsNoTopics(t *testing.T) {
	t.Parallel()

	in, err := New(WithSeedBrokers("localhost:9092"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := in.Subscribe(context.Background(), source.SubscribeConfig{}); err == nil {
		t.Fatal("Subscribe(no topics) = nil error, want failure")
	}
}

func TestInletAsBeforeClientBuilt(t *testing.T) {
	t.Parallel()

	in, _ := New(WithSeedBrokers("localhost:9092"))
	var c *kgo.Client
	if in.As(&c) {
		t.Error("As(**kgo.Client) = true before a client was built, want false")
	}
	if in.As(new(int)) {
		t.Error("As(*int) = true, want false for a non-matching target")
	}
}

func TestInletCloseWithInjectedClientIsNoOp(t *testing.T) {
	t.Parallel()

	// A nil injected client is ignored by WithClient, so build a real one to
	// prove Close does not close a caller-owned client. Use a client that never
	// dials by giving it a bogus seed; Close on it must not be called by us.
	client, err := kgo.NewClient(kgo.SeedBrokers("localhost:0"))
	if err != nil {
		t.Fatalf("kgo.NewClient error = %v", err)
	}
	defer client.Close()

	in, err := New(WithClient(client))
	if err != nil {
		t.Fatalf("New(WithClient) error = %v", err)
	}
	if in.ownsClient {
		t.Error("ownsClient = true for an injected client, want false")
	}
	if err := in.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil no-op", err)
	}
	var got *kgo.Client
	if !in.As(&got) || got != client {
		t.Error("As did not return the injected client")
	}
}

func TestMessageHeaderMapping(t *testing.T) {
	t.Parallel()

	r := rec("orders", 1, 10, "k", "v")
	r.Headers = []kgo.RecordHeader{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "trace", Value: []byte("t-9")},
	}
	m := newMessage(r)

	if got, ok := m.Headers().Get("content-type"); !ok || got != "application/json" {
		t.Errorf("Headers().Get(content-type) = %q,%v want application/json,true", got, ok)
	}
	if got, ok := m.Headers().Get("missing"); ok || got != "" {
		t.Errorf("Headers().Get(missing) = %q,%v want \"\",false", got, ok)
	}
}

func TestRecordOfRecoversRecord(t *testing.T) {
	t.Parallel()

	r := rec("t", 0, 0, "k", "v")
	if got, ok := recordOf(newMessage(r)); !ok || got != r {
		t.Errorf("recordOf(message) did not recover the record")
	}
	if _, ok := recordOf(foreignMessage{}); ok {
		t.Error("recordOf(foreign) = true, want false")
	}
	if _, ok := recordOf(nil); ok {
		t.Error("recordOf(nil) = true, want false")
	}
}

// --- capability coverage with a requester-capable fake -----------------------

// reqPoller embeds the fakePoller and additionally satisfies requester, so the
// seek and lag capabilities are exercised without a broker. It serves a canned
// ListOffsets response and a fixed committed-offset map.
type reqPoller struct {
	fakePoller
	committedMap  map[string]map[int32]kgo.EpochOffset
	listResp      *kmsg.ListOffsetsResponse
	listErr       error
	consumeTopics []string
}

func (r *reqPoller) Request(_ context.Context, _ kmsg.Request) (kmsg.Response, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.listResp, nil
}

func (r *reqPoller) CommittedOffsets() map[string]map[int32]kgo.EpochOffset {
	return r.committedMap
}

func (r *reqPoller) GetConsumeTopics() []string { return r.consumeTopics }

func TestSeekToCursorSetsOffset(t *testing.T) {
	t.Parallel()

	rp := &reqPoller{}
	sub := newSub(rp)
	c := offsetCursor{topic: "orders", partition: 2, offset: 50}

	if err := sub.SeekToCursor(context.Background(), c); err != nil {
		t.Fatalf("SeekToCursor() error = %v", err)
	}
	if len(rp.setOffsets) != 1 || rp.setOffsets[0]["orders"][2].Offset != 50 {
		t.Fatalf("setOffsets = %+v, want orders/2@50", rp.setOffsets)
	}
}

func TestSeekToCursorRejectsForeignCursor(t *testing.T) {
	t.Parallel()

	sub := newSub(&reqPoller{})
	if err := sub.SeekToCursor(context.Background(), stringCursor("x")); !errors.Is(err, errForeignCursor) {
		t.Fatalf("SeekToCursor(foreign) = %v, want errForeignCursor", err)
	}
}

func TestSeekLogicalStartAndEnd(t *testing.T) {
	t.Parallel()

	rp := &reqPoller{committedMap: map[string]map[int32]kgo.EpochOffset{
		"orders": {0: {Offset: 5}, 1: {Offset: 9}},
	}}
	sub := newSub(rp)

	if err := sub.SeekToStart(context.Background()); err != nil {
		t.Fatalf("SeekToStart() error = %v", err)
	}
	if err := sub.SeekToEnd(context.Background()); err != nil {
		t.Fatalf("SeekToEnd() error = %v", err)
	}
	if len(rp.setOffsets) != 2 {
		t.Fatalf("setOffsets calls = %d, want 2", len(rp.setOffsets))
	}
	if rp.setOffsets[0]["orders"][0].Offset != -2 {
		t.Errorf("start offset = %d, want -2", rp.setOffsets[0]["orders"][0].Offset)
	}
	if rp.setOffsets[1]["orders"][1].Offset != -1 {
		t.Errorf("end offset = %d, want -1", rp.setOffsets[1]["orders"][1].Offset)
	}
}

func TestSeekToTimeResolvesOffsets(t *testing.T) {
	t.Parallel()

	resp := &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{
		Topic: "orders",
		Partitions: []kmsg.ListOffsetsResponseTopicPartition{
			{Partition: 0, Offset: 12, LeaderEpoch: 3},
		},
	}}}
	rp := &reqPoller{
		committedMap: map[string]map[int32]kgo.EpochOffset{"orders": {0: {Offset: 1}}},
		listResp:     resp,
	}
	sub := newSub(rp)

	if err := sub.SeekToTime(context.Background(), time.Unix(1000, 0)); err != nil {
		t.Fatalf("SeekToTime() error = %v", err)
	}
	if len(rp.setOffsets) != 1 || rp.setOffsets[0]["orders"][0].Offset != 12 {
		t.Fatalf("setOffsets = %+v, want orders/0@12", rp.setOffsets)
	}
}

func TestSeekToTimePropagatesPartitionError(t *testing.T) {
	t.Parallel()

	resp := &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{
		Topic:      "orders",
		Partitions: []kmsg.ListOffsetsResponseTopicPartition{{Partition: 0, ErrorCode: 6}},
	}}}
	rp := &reqPoller{
		committedMap: map[string]map[int32]kgo.EpochOffset{"orders": {0: {}}},
		listResp:     resp,
	}
	sub := newSub(rp)

	if err := sub.SeekToTime(context.Background(), time.Now()); err == nil {
		t.Fatal("SeekToTime() = nil, want a partition error")
	}
}

func TestSeekWithoutRequesterReportsUnavailable(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{}) // plain poller, no requester
	if err := sub.SeekToStart(context.Background()); !errors.Is(err, errSeekUnavailable) {
		t.Errorf("SeekToStart() = %v, want errSeekUnavailable", err)
	}
	if _, err := sub.Lag(context.Background()); !errors.Is(err, errSeekUnavailable) {
		t.Errorf("Lag() = %v, want errSeekUnavailable", err)
	}
}

func TestLagComputesTailMinusCommitted(t *testing.T) {
	t.Parallel()

	resp := &kmsg.ListOffsetsResponse{Topics: []kmsg.ListOffsetsResponseTopic{{
		Topic: "orders",
		Partitions: []kmsg.ListOffsetsResponseTopicPartition{
			{Partition: 0, Offset: 100},
			{Partition: 1, Offset: 50},
		},
	}}}
	rp := &reqPoller{
		committedMap: map[string]map[int32]kgo.EpochOffset{
			"orders": {0: {Offset: 90}, 1: {Offset: 50}},
		},
		listResp: resp,
	}
	sub := newSub(rp)

	lag, err := sub.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if lag != 10 {
		t.Errorf("Lag() = %d, want 10 (100-90 + 50-50)", lag)
	}
}

func TestConsumerGroupHooksMapPartitions(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{})
	if sub.GroupID() != "g" {
		t.Errorf("GroupID() = %q, want g", sub.GroupID())
	}

	var assigned, revoked []source.Partition
	sub.OnAssigned(func(_ context.Context, ps []source.Partition) { assigned = ps })
	sub.OnRevoked(func(_ context.Context, ps []source.Partition) { revoked = ps })

	sub.onAssigned(context.Background(), nil, map[string][]int32{"orders": {0, 1}})
	sub.onRevoked(context.Background(), nil, map[string][]int32{"orders": {0}})

	if len(assigned) != 2 {
		t.Errorf("assigned = %d partitions, want 2", len(assigned))
	}
	if len(revoked) != 1 || revoked[0] != (source.Partition{Topic: "orders", ID: 0}) {
		t.Errorf("revoked = %+v, want orders/0", revoked)
	}
}

func TestOnRevokedCommitsMarkedOffsets(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	sub.onRevoked(context.Background(), nil, map[string][]int32{"orders": {0}})
	if fp.committed != 1 {
		t.Errorf("committed = %d on revoke, want 1 (drain-and-commit)", fp.committed)
	}
}

func TestOnLostDoesNotCommit(t *testing.T) {
	t.Parallel()

	fp := &fakePoller{}
	sub := newSub(fp)
	var revoked []source.Partition
	sub.OnRevoked(func(_ context.Context, ps []source.Partition) { revoked = ps })

	sub.onLost(context.Background(), nil, map[string][]int32{"orders": {0}})
	if fp.committed != 0 {
		t.Errorf("committed = %d on lost, want 0 (offsets are no longer ours)", fp.committed)
	}
	if len(revoked) != 1 {
		t.Errorf("lost did not forward to the revoke hook")
	}
}

func TestBeginWithoutTransactionalFails(t *testing.T) {
	t.Parallel()

	sub := newSub(&fakePoller{})
	m := newMessage(rec("orders", 0, 0, "k", "v"))
	err := sub.Begin(context.Background(), m, func(context.Context, source.Tx) error { return nil })
	if !errors.Is(err, errNotTransactional) {
		t.Fatalf("Begin() = %v, want errNotTransactional", err)
	}
}

// fakeTransactor is a hand-rolled [transactor] that records the begin/produce/end
// call sequence so the EOS choreography is asserted with no broker. It returns a
// scripted commit/abort outcome and optional errors at each step.
type fakeTransactor struct {
	mu sync.Mutex

	calls     []string        // ordered record of begin/produce/end
	produced  [][]*kgo.Record // records per ProduceSync call
	beginErr  error
	produceTr error
	endErr    error
	committed bool
}

func (f *fakeTransactor) Begin() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "begin")
	return f.beginErr
}

func (f *fakeTransactor) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "produce")
	f.produced = append(f.produced, rs)
	out := make(kgo.ProduceResults, len(rs))
	for i, r := range rs {
		out[i] = kgo.ProduceResult{Record: r, Err: f.produceTr}
	}
	return out
}

func (f *fakeTransactor) End(_ context.Context, commit kgo.TransactionEndTry) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if commit == kgo.TryCommit {
		f.calls = append(f.calls, "end-commit")
	} else {
		f.calls = append(f.calls, "end-abort")
	}
	return f.committed, f.endErr
}

func newTxSub(ft *fakeTransactor) (*subscription, *fakePoller) {
	fp := &fakePoller{}
	return &subscription{client: fp, group: "g", dlqTopic: "orders.DLQ", transactSess: ft}, fp
}

func TestBeginCommitsOnSuccessAfterProduce(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{committed: true}
	sub, fp := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))

	err := sub.Begin(context.Background(), m, func(ctx context.Context, tx source.Tx) error {
		return tx.Produce(ctx, source.ProducedRecord{
			Topic:   "orders.out",
			Key:     []byte("A-1"),
			Value:   []byte("emitted"),
			Headers: source.Headers{{Key: "message-id", Value: "evt-1"}},
		})
	})
	if err != nil {
		t.Fatalf("Begin() error = %v, want nil", err)
	}
	// The consumed record is marked (inside the tx, before End-commit) so its
	// offset commits atomically with the produced record.
	if got := fp.markedCount(); got != 1 {
		t.Errorf("marked = %d, want 1 (consumed offset marked on commit)", got)
	}
	if got, want := ft.calls, []string{"begin", "produce", "end-commit"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
	if len(ft.produced) != 1 || len(ft.produced[0]) != 1 {
		t.Fatalf("produced = %#v, want one record in one ProduceSync", ft.produced)
	}
	pr := ft.produced[0][0]
	if pr.Topic != "orders.out" || string(pr.Key) != "A-1" || string(pr.Value) != "emitted" {
		t.Errorf("produced record = %+v, want orders.out/A-1/emitted", pr)
	}
	if len(pr.Headers) != 1 || pr.Headers[0].Key != "message-id" || string(pr.Headers[0].Value) != "evt-1" {
		t.Errorf("produced headers = %+v, want message-id=evt-1", pr.Headers)
	}
}

func TestBeginAbortsAndReturnsWorkError(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{}
	sub, fp := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))
	boom := errors.New("handler boom")

	err := sub.Begin(context.Background(), m, func(ctx context.Context, tx source.Tx) error {
		_ = tx.Produce(ctx, source.ProducedRecord{Topic: "orders.out", Value: []byte("v")})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Begin() = %v, want the work error", err)
	}
	// On abort the consumed record is NOT marked, so the input is redelivered.
	if got := fp.markedCount(); got != 0 {
		t.Errorf("marked = %d on abort, want 0 (offset not committed)", got)
	}
	if got, want := ft.calls, []string{"begin", "produce", "end-abort"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v (abort on work error)", got, want)
	}
}

func TestBeginAbortsWhenProduceFails(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{produceTr: errors.New("produce rejected")}
	sub, fp := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))

	err := sub.Begin(context.Background(), m, func(ctx context.Context, tx source.Tx) error {
		// A failed produce is returned by the work function, aborting the tx.
		return tx.Produce(ctx, source.ProducedRecord{Topic: "orders.out", Value: []byte("v")})
	})
	if err == nil {
		t.Fatal("Begin() = nil, want the produce error")
	}
	if got := fp.markedCount(); got != 0 {
		t.Errorf("marked = %d on produce failure, want 0", got)
	}
	if got, want := ft.calls, []string{"begin", "produce", "end-abort"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

func TestBeginReportsBrokerAbortOnSuccessfulWork(t *testing.T) {
	t.Parallel()

	// Work succeeds but End reports the transaction did not commit (a fence/abort
	// by the broker, e.g. a rebalance): Begin surfaces errTransactionAborted so
	// the caller treats the cycle as not-committed and the input is redelivered.
	ft := &fakeTransactor{committed: false}
	sub, _ := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))

	err := sub.Begin(context.Background(), m, func(context.Context, source.Tx) error { return nil })
	if !errors.Is(err, errTransactionAborted) {
		t.Fatalf("Begin() = %v, want errTransactionAborted", err)
	}
	if got, want := ft.calls, []string{"begin", "end-commit"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v", got, want)
	}
}

func TestBeginPropagatesBeginError(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{beginErr: errors.New("cannot begin")}
	sub, _ := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))
	ran := false
	err := sub.Begin(context.Background(), m, func(context.Context, source.Tx) error {
		ran = true
		return nil
	})
	if err == nil {
		t.Fatal("Begin() = nil, want the begin error")
	}
	if ran {
		t.Error("work function ran despite a begin failure")
	}
}

func TestBeginRejectsForeignMessage(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{committed: true}
	sub, _ := newTxSub(ft)
	err := sub.Begin(context.Background(), foreignMessage{}, func(context.Context, source.Tx) error { return nil })
	if !errors.Is(err, errNotKafkaMessage) {
		t.Fatalf("Begin(foreign) = %v, want errNotKafkaMessage", err)
	}
}

func TestTxProduceRejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{committed: true}
	sub, _ := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))
	err := sub.Begin(context.Background(), m, func(ctx context.Context, tx source.Tx) error {
		return tx.Produce(ctx, source.ProducedRecord{Value: []byte("no topic")})
	})
	if !errors.Is(err, errEmptyTopic) {
		t.Fatalf("Begin() = %v, want errEmptyTopic", err)
	}
}

func TestTxProduceNoRecordsIsNoOp(t *testing.T) {
	t.Parallel()

	ft := &fakeTransactor{committed: true}
	sub, _ := newTxSub(ft)
	m := newMessage(rec("orders", 0, 7, "A-1", "placed"))
	err := sub.Begin(context.Background(), m, func(ctx context.Context, tx source.Tx) error {
		return tx.Produce(ctx) // no records
	})
	if err != nil {
		t.Fatalf("Begin() error = %v, want nil", err)
	}
	if got, want := ft.calls, []string{"begin", "end-commit"}; !equalStrings(got, want) {
		t.Fatalf("call order = %v, want %v (no produce for an empty batch)", got, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPartitionOrderedMarker(t *testing.T) {
	t.Parallel()
	// The marker method is the guarantee; calling it must be a harmless no-op.
	newSub(&fakePoller{}).PartitionOrdered()
}

func TestBaseOptsAssembly(t *testing.T) {
	t.Parallel()

	in, err := New(
		WithSeedBrokers("b1:9092"),
		WithClientID("crucible"),
		WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
		WithClientOptions(kgo.FetchMaxBytes(1024)),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	// baseOpts assembles seed/clientID/tls/extra without dialing; we only assert
	// it produces a non-empty option slice and is callable.
	if got := in.baseOpts(); len(got) < 4 {
		t.Errorf("baseOpts() = %d options, want >= 4 (seed, clientID, tls, extra)", len(got))
	}
}

func TestConsumeOptsAddsGroupAndMarkCommit(t *testing.T) {
	t.Parallel()

	in, _ := New(WithSeedBrokers("b1:9092"), WithBalancer(kgo.CooperativeStickyBalancer()))
	opts := in.consumeOpts(source.SubscribeConfig{Topics: []string{"orders"}, Group: "g"})
	// The exact options are opaque (franz-go Opt is a func); assert the call
	// produced a superset of the base options (group + balancer + mark-commit
	// + block-rebalance + consume-topics on top of the base seed).
	if len(opts) <= len(in.baseOpts()) {
		t.Errorf("consumeOpts() = %d options, want more than baseOpts()", len(opts))
	}
}

func TestSubscribeBuildsClientAndAs(t *testing.T) {
	t.Parallel()

	// A non-routable seed lets the client be built without a successful dial;
	// Subscribe must still return a live subscription and own the client.
	in, err := New(WithSeedBrokers("localhost:0"), WithDLQTopic("orders.DLQ"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })

	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{
		Topics: []string{"orders"},
		Group:  "g",
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if sub == nil {
		t.Fatal("Subscribe() returned a nil subscription")
	}
	if !in.ownsClient {
		t.Error("ownsClient = false after building a client, want true")
	}
	var c *kgo.Client
	if !in.As(&c) || c == nil {
		t.Error("As(**kgo.Client) did not return the built client")
	}
}

// stringCursor is a cursor from a different inlet, used to prove SeekToCursor
// rejects a foreign cursor type.
type stringCursor string

func (c stringCursor) String() string { return string(c) }
