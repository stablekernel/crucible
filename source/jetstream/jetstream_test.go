// SPDX-License-Identifier: Apache-2.0

package jetstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	njs "github.com/nats-io/nats.go/jetstream"

	"github.com/stablekernel/crucible/source"
)

// --- fakes: a jetstream seam with no live server -----------------------------

// fakeMsg implements njs.Msg, recording which settle method was invoked.
type fakeMsg struct {
	data    []byte
	subject string
	headers gonats.Header
	seq     uint64
	metaErr error

	mu         sync.Mutex
	acked      bool
	doubleAck  bool
	naked      bool
	nakDelay   time.Duration
	termed     bool
	termReason string
	inProgress bool
	settleErr  error
}

func (m *fakeMsg) Metadata() (*njs.MsgMetadata, error) {
	if m.metaErr != nil {
		return nil, m.metaErr
	}
	return &njs.MsgMetadata{Sequence: njs.SequencePair{Stream: m.seq}}, nil
}
func (m *fakeMsg) Data() []byte           { return m.data }
func (m *fakeMsg) Headers() gonats.Header { return m.headers }
func (m *fakeMsg) Subject() string        { return m.subject }
func (m *fakeMsg) Reply() string          { return "" }

func (m *fakeMsg) Ack() error { m.mu.Lock(); defer m.mu.Unlock(); m.acked = true; return m.settleErr }

func (m *fakeMsg) DoubleAck(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.doubleAck = true
	m.acked = true
	return m.settleErr
}

func (m *fakeMsg) Nak() error { m.mu.Lock(); defer m.mu.Unlock(); m.naked = true; return m.settleErr }

func (m *fakeMsg) NakWithDelay(d time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.naked = true
	m.nakDelay = d
	return m.settleErr
}

func (m *fakeMsg) InProgress() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgress = true
	return m.settleErr
}

func (m *fakeMsg) Term() error { m.mu.Lock(); defer m.mu.Unlock(); m.termed = true; return m.settleErr }

func (m *fakeMsg) TermWithReason(r string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.termed = true
	m.termReason = r
	return m.settleErr
}

// fakeIter implements njs.MessagesContext over a fixed slice of messages.
type fakeIter struct {
	msgs    []njs.Msg
	i       int
	stopped bool
}

func (it *fakeIter) Next(_ ...njs.NextOpt) (njs.Msg, error) {
	if it.stopped || it.i >= len(it.msgs) {
		return nil, njs.ErrMsgIteratorClosed
	}
	m := it.msgs[it.i]
	it.i++
	return m, nil
}
func (it *fakeIter) Stop()  { it.stopped = true }
func (it *fakeIter) Drain() { it.stopped = true }

// fakeConsumer embeds njs.Consumer (nil) and overrides only Messages and Info.
type fakeConsumer struct {
	njs.Consumer
	iter        *fakeIter
	info        *njs.ConsumerInfo
	infoErr     error
	messagesErr error
}

func (c *fakeConsumer) Messages(_ ...njs.PullMessagesOpt) (njs.MessagesContext, error) {
	if c.messagesErr != nil {
		return nil, c.messagesErr
	}
	return c.iter, nil
}

func (c *fakeConsumer) Info(context.Context) (*njs.ConsumerInfo, error) {
	if c.infoErr != nil {
		return nil, c.infoErr
	}
	return c.info, nil
}

// fakeJS implements the jsAPI seam, recording the configs it was asked for.
type fakeJS struct {
	cons         *fakeConsumer
	createErr    error
	orderedErr   error
	lastCfg      njs.ConsumerConfig
	lastOrdered  njs.OrderedConsumerConfig
	createCount  int
	orderedCount int
}

func (f *fakeJS) CreateOrUpdateConsumer(_ context.Context, _ string, cfg njs.ConsumerConfig) (njs.Consumer, error) {
	f.createCount++
	f.lastCfg = cfg
	if f.createErr != nil {
		return nil, f.createErr
	}
	return f.cons, nil
}

func (f *fakeJS) OrderedConsumer(_ context.Context, _ string, cfg njs.OrderedConsumerConfig) (njs.Consumer, error) {
	f.orderedCount++
	f.lastOrdered = cfg
	if f.orderedErr != nil {
		return nil, f.orderedErr
	}
	return f.cons, nil
}

func newFakeJS(msgs ...njs.Msg) *fakeJS {
	return &fakeJS{cons: &fakeConsumer{iter: &fakeIter{msgs: msgs}, info: &njs.ConsumerInfo{}}}
}

// newSub builds an Inlet over a fake seam and opens a subscription.
func newSub(t *testing.T, js jsAPI, opts ...Option) source.Subscription {
	t.Helper()
	base := []Option{WithStream("ORDERS"), WithDurable("workers"), withJetStream(js)}
	in, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

// --- New / option validation -------------------------------------------------

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{name: "missing stream", opts: []Option{WithURL("nats://x")}, wantErr: "WithStream is required"},
		{
			name:    "url and conn mutually exclusive",
			opts:    []Option{WithStream("S"), WithURL("nats://x"), WithConn(&gonats.Conn{})},
			wantErr: "mutually exclusive",
		},
		{name: "no transport", opts: []Option{WithStream("S")}, wantErr: "WithURL or WithConn is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tt.opts...)
			if err == nil || !errContains(err, tt.wantErr) {
				t.Fatalf("New() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestNew_WithJetStreamSeam_NoDial(t *testing.T) {
	t.Parallel()
	in, err := New(WithStream("ORDERS"), withJetStream(newFakeJS()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := in.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

// --- Next: message mapping ---------------------------------------------------

func TestNext_MapsMessage(t *testing.T) {
	t.Parallel()
	hdr := gonats.Header{KeyHeader: []string{"cust-7"}, "X-Trace": []string{"abc"}}
	js := newFakeJS(&fakeMsg{data: []byte("payload"), subject: "orders.placed", headers: hdr, seq: 42})
	sub := newSub(t, js)

	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m.Key()) != "cust-7" {
		t.Errorf("Key() = %q, want cust-7 (from KeyHeader)", m.Key())
	}
	if string(m.Value()) != "payload" {
		t.Errorf("Value() = %q, want payload", m.Value())
	}
	if m.Subject() != "orders.placed" {
		t.Errorf("Subject() = %q, want orders.placed", m.Subject())
	}
	if m.PartitionKey() != "" {
		t.Errorf("PartitionKey() = %q, want empty (JetStream has no partitions)", m.PartitionKey())
	}
	if got := m.Cursor().String(); got != "42" {
		t.Errorf("Cursor() = %q, want 42", got)
	}
	if v, ok := m.Headers().Get("X-Trace"); !ok || v != "abc" {
		t.Errorf("Headers().Get(X-Trace) = %q,%v want abc,true", v, ok)
	}
	var jm njs.Msg
	if !m.As(&jm) {
		t.Errorf("As(*njs.Msg) = false, want true")
	}
}

func TestNext_KeyFallsBackToSubject(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{data: []byte("x"), subject: "orders.shipped"})
	sub := newSub(t, js)
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m.Key()) != "orders.shipped" {
		t.Errorf("Key() = %q, want orders.shipped (subject fallback)", m.Key())
	}
}

func TestNext_DrainedOnIteratorClosed(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	sub := newSub(t, js)
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	_, err := sub.Next(context.Background())
	if !errors.Is(err, source.ErrDrained) {
		t.Fatalf("second Next() = %v, want ErrDrained", err)
	}
}

func TestNext_ContextCanceled(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	sub := newSub(t, js)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(canceled) = %v, want context.Canceled", err)
	}
}

func TestNext_CreateConsumerError(t *testing.T) {
	t.Parallel()
	js := &fakeJS{createErr: errors.New("boom")}
	sub := newSub(t, js)
	if _, err := sub.Next(context.Background()); err == nil || !errContains(err, "create consumer") {
		t.Fatalf("Next() = %v, want create-consumer error", err)
	}
}

func TestNext_BuildsExplicitAckConsumerConfig(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	in, err := New(
		WithStream("ORDERS"), WithDurable("workers"), withJetStream(js),
		WithAckWait(7*time.Second), WithMaxDeliver(3), WithMaxAckPending(99),
		WithFilterSubjects("orders.>"),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{})
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	c := js.lastCfg
	if c.AckPolicy != njs.AckExplicitPolicy {
		t.Errorf("AckPolicy = %v, want AckExplicitPolicy", c.AckPolicy)
	}
	if c.Durable != "workers" || c.AckWait != 7*time.Second || c.MaxDeliver != 3 || c.MaxAckPending != 99 {
		t.Errorf("config mismatch: %+v", c)
	}
	if len(c.FilterSubjects) != 1 || c.FilterSubjects[0] != "orders.>" {
		t.Errorf("FilterSubjects = %v, want [orders.>]", c.FilterSubjects)
	}
}

func TestSubscribe_TopicsAndGroupOverride(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	in, _ := New(WithStream("ORDERS"), WithDurable("default"), WithFilterSubjects("a.>"), withJetStream(js))
	sub, _ := in.Subscribe(context.Background(), source.SubscribeConfig{
		Topics: []string{"b.>", "c.>"}, Group: "override-durable",
	})
	t.Cleanup(func() { _ = sub.Close() })
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if js.lastCfg.Durable != "override-durable" {
		t.Errorf("Durable = %q, want override-durable", js.lastCfg.Durable)
	}
	if len(js.lastCfg.FilterSubjects) != 2 {
		t.Errorf("FilterSubjects = %v, want 2 from Topics", js.lastCfg.FilterSubjects)
	}
	if sd, ok := sub.(source.SharedDurable); !ok || sd.Durable() != "override-durable" {
		t.Errorf("Durable() = %v,%v want override-durable", sd, ok)
	}
}

// --- Settle: action -> ack vocabulary ----------------------------------------

func TestSettle_ActionMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result source.Result
		check  func(t *testing.T, m *fakeMsg)
	}{
		{
			name:   "ack uses double-ack",
			result: source.Ack(),
			check:  func(t *testing.T, m *fakeMsg) { mustTrue(t, "doubleAck", m.doubleAck) },
		},
		{
			name:   "nak immediate",
			result: source.Nak(errors.New("transient")),
			check:  func(t *testing.T, m *fakeMsg) { mustTrue(t, "naked", m.naked); mustZero(t, m.nakDelay) },
		},
		{
			name:   "nak with delay",
			result: source.NakAfter(5*time.Second, errors.New("transient")),
			check: func(t *testing.T, m *fakeMsg) {
				mustTrue(t, "naked", m.naked)
				if m.nakDelay != 5*time.Second {
					t.Errorf("nakDelay = %v, want 5s", m.nakDelay)
				}
			},
		},
		{
			name:   "term with reason carries error",
			result: source.Term(errors.New("bad data")),
			check: func(t *testing.T, m *fakeMsg) {
				mustTrue(t, "termed", m.termed)
				if m.termReason != "bad data" {
					t.Errorf("termReason = %q, want bad data", m.termReason)
				}
			},
		},
		{
			name:   "reject (invalid for state) terms",
			result: source.Reject(errors.New("wrong state")),
			check:  func(t *testing.T, m *fakeMsg) { mustTrue(t, "termed", m.termed) },
		},
		{
			name:   "in progress extends deadline",
			result: source.InProgress(),
			check:  func(t *testing.T, m *fakeMsg) { mustTrue(t, "inProgress", m.inProgress) },
		},
		{
			name:   "manual is a no-op",
			result: source.Manual(),
			check: func(t *testing.T, m *fakeMsg) {
				if m.acked || m.naked || m.termed || m.inProgress {
					t.Errorf("manual settled the message: %+v", m)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fm := &fakeMsg{subject: "s", data: []byte("a")}
			js := newFakeJS(fm)
			sub := newSub(t, js)
			m, err := sub.Next(context.Background())
			if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if err := sub.Settle(context.Background(), m, tt.result); err != nil {
				t.Fatalf("Settle() error = %v", err)
			}
			tt.check(t, fm)
		})
	}
}

func TestSettle_AckErrorWrapped(t *testing.T) {
	t.Parallel()
	boom := errors.New("server unreachable")
	fm := &fakeMsg{subject: "s", data: []byte("a"), settleErr: boom}
	sub := newSub(t, newFakeJS(fm))
	m, _ := sub.Next(context.Background())
	err := sub.Settle(context.Background(), m, source.Ack())
	if !errors.Is(err, boom) || !errContains(err, "ack") {
		t.Fatalf("Settle() = %v, want wrapped ack error", err)
	}
}

// --- capabilities: seek (replay-by-recreate) ---------------------------------

func TestSeek_RecreatesConsumerWithStartPolicy(t *testing.T) {
	t.Parallel()
	seekTime := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name   string
		seek   func(s source.Seekable) error
		verify func(t *testing.T, cfg njs.ConsumerConfig)
	}{
		{
			name: "to time",
			seek: func(s source.Seekable) error { return s.SeekToTime(context.Background(), seekTime) },
			verify: func(t *testing.T, cfg njs.ConsumerConfig) {
				if cfg.DeliverPolicy != njs.DeliverByStartTimePolicy || cfg.OptStartTime == nil {
					t.Errorf("want DeliverByStartTime, got %+v", cfg)
				}
			},
		},
		{
			name: "to cursor",
			seek: func(s source.Seekable) error { return s.SeekToCursor(context.Background(), seqCursor(10)) },
			verify: func(t *testing.T, cfg njs.ConsumerConfig) {
				if cfg.DeliverPolicy != njs.DeliverByStartSequencePolicy || cfg.OptStartSeq != 11 {
					t.Errorf("want DeliverByStartSequence seq=11, got %+v", cfg)
				}
			},
		},
		{
			name: "to start",
			seek: func(s source.Seekable) error { return s.SeekToStart(context.Background()) },
			verify: func(t *testing.T, cfg njs.ConsumerConfig) {
				if cfg.DeliverPolicy != njs.DeliverAllPolicy {
					t.Errorf("want DeliverAll, got %v", cfg.DeliverPolicy)
				}
			},
		},
		{
			name: "to end",
			seek: func(s source.Seekable) error { return s.SeekToEnd(context.Background()) },
			verify: func(t *testing.T, cfg njs.ConsumerConfig) {
				if cfg.DeliverPolicy != njs.DeliverNewPolicy {
					t.Errorf("want DeliverNew, got %v", cfg.DeliverPolicy)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
			sub := newSub(t, js)
			sk, ok := sub.(source.Seekable)
			if !ok {
				t.Fatal("subscription is not Seekable")
			}
			if err := sk.SeekToStart(context.Background()); err != nil { // prime an iterator
				t.Fatalf("prime seek error = %v", err)
			}
			if _, err := sub.Next(context.Background()); err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			before := js.createCount
			if err := tt.seek(sk); err != nil {
				t.Fatalf("seek error = %v", err)
			}
			// next Next rebuilds the consumer at the new position
			_, _ = sub.Next(context.Background())
			if js.createCount <= before {
				t.Errorf("createCount = %d, want > %d (consumer-recreate)", js.createCount, before)
			}
			tt.verify(t, js.lastCfg)
		})
	}
}

func TestSeek_OnClosedSubscription(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS())
	_ = sub.Close()
	sk := sub.(source.Seekable)
	if err := sk.SeekToStart(context.Background()); !errors.Is(err, errSeekClosed) {
		t.Fatalf("SeekToStart(closed) = %v, want errSeekClosed", err)
	}
}

func TestSeekToCursor_RejectsForeignCursor(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS())
	sk := sub.(source.Seekable)
	if err := sk.SeekToCursor(context.Background(), foreignCursor{}); err == nil {
		t.Fatal("SeekToCursor(foreign) = nil, want error")
	}
}

type foreignCursor struct{}

func (foreignCursor) String() string { return "foreign" }

// --- capabilities: ordered delivery ------------------------------------------

func TestOrderedDelivery_UsesOrderedConsumer(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	sub := newSub(t, js)
	od, ok := sub.(source.OrderedDelivery)
	if !ok {
		t.Fatal("subscription is not OrderedDelivery")
	}
	od.OrderedDelivery()
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if js.orderedCount != 1 {
		t.Errorf("orderedCount = %d, want 1 (OrderedConsumer path)", js.orderedCount)
	}
	if js.createCount != 0 {
		t.Errorf("createCount = %d, want 0 (durable path not used)", js.createCount)
	}
}

// --- capabilities: lag -------------------------------------------------------

func TestLag_ReportsPendingPlusAckPending(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	js.cons.info = &njs.ConsumerInfo{NumPending: 5, NumAckPending: 2}
	sub := newSub(t, js)
	if _, err := sub.Next(context.Background()); err != nil { // create the consumer
		t.Fatalf("Next() error = %v", err)
	}
	lr := sub.(source.LagReporter)
	got, err := lr.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if got != 7 {
		t.Errorf("Lag() = %d, want 7", got)
	}
}

func TestLag_ZeroBeforeConsumerCreated(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS())
	got, err := sub.(source.LagReporter).Lag(context.Background())
	if err != nil || got != 0 {
		t.Fatalf("Lag() = %d,%v want 0,nil before consumer created", got, err)
	}
}

// --- Close idempotency -------------------------------------------------------

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS(&fakeMsg{subject: "s", data: []byte("a")}))
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next() after Close = %v, want ErrDrained", err)
	}
}

// --- misc coverage -----------------------------------------------------------

func TestWithPullMaxMessages_OpensIterator(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a")})
	sub := newSub(t, js, WithPullMaxMessages(16))
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
}

func TestInlet_As_ReturnsConn(t *testing.T) {
	t.Parallel()
	conn := &gonats.Conn{}
	in, err := New(WithStream("ORDERS"), WithConn(conn), withJetStream(newFakeJS()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var got *gonats.Conn
	if !in.As(&got) || got != conn {
		t.Fatalf("As(**gonats.Conn) did not return the conn")
	}
	var wrong *int
	if in.As(&wrong) {
		t.Fatalf("As(**int) = true, want false")
	}
}

func TestSettle_UnknownAction(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS(&fakeMsg{subject: "s", data: []byte("a")}))
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Result{Action: source.Action(99)}); err == nil {
		t.Fatal("Settle(unknown action) = nil, want error")
	}
}

func TestSettle_NonJetStreamMessage(t *testing.T) {
	t.Parallel()
	sub := newSub(t, newFakeJS())
	if err := sub.Settle(context.Background(), foreignMsg{}, source.Ack()); err == nil {
		t.Fatal("Settle(foreign message) = nil, want error")
	}
}

// foreignMsg is a source.Message that is not backed by a jetstream.Msg.
type foreignMsg struct{}

func (foreignMsg) Key() []byte             { return nil }
func (foreignMsg) Value() []byte           { return nil }
func (foreignMsg) Headers() source.Headers { return nil }
func (foreignMsg) Subject() string         { return "" }
func (foreignMsg) PartitionKey() string    { return "" }
func (foreignMsg) Cursor() source.Cursor   { return seqCursor(0) }
func (foreignMsg) As(any) bool             { return false }

func TestNext_OpenIteratorError(t *testing.T) {
	t.Parallel()
	js := newFakeJS()
	js.cons.messagesErr = errors.New("iterator boom")
	sub := newSub(t, js)
	if _, err := sub.Next(context.Background()); err == nil || !errContains(err, "open iterator") {
		t.Fatalf("Next() = %v, want open-iterator error", err)
	}
}

func TestMetadataError_LeavesZeroCursor(t *testing.T) {
	t.Parallel()
	js := newFakeJS(&fakeMsg{subject: "s", data: []byte("a"), metaErr: errors.New("no meta")})
	sub := newSub(t, js)
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if m.Cursor().String() != "0" {
		t.Errorf("Cursor() = %q, want 0 on metadata error", m.Cursor().String())
	}
}

// --- helpers -----------------------------------------------------------------

func errContains(err error, sub string) bool {
	return err != nil && strContains(err.Error(), sub)
}

func strContains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return len(sub) == 0
}

func mustTrue(t *testing.T, name string, v bool) {
	t.Helper()
	if !v {
		t.Errorf("%s = false, want true", name)
	}
}

func mustZero(t *testing.T, d time.Duration) {
	t.Helper()
	if d != 0 {
		t.Errorf("delay = %v, want 0", d)
	}
}
