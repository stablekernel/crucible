// SPDX-License-Identifier: Apache-2.0

package redis

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/stablekernel/crucible/source"
)

// --- fake: a Redis Streams seam with no live server -------------------------

// fakeClient implements the Client seam over in-memory state, recording the
// calls a test asserts against. It is safe for concurrent Settle.
type fakeClient struct {
	mu sync.Mutex

	// read returns successive batches of entries; one batch per XReadGroup call,
	// then goredis.Nil to model an idle block window.
	readBatches [][]goredis.XMessage
	readIdx     int
	readErr     error

	groupCreated bool
	createErr    error

	acked     []string
	ackErr    error
	added     []*goredis.XAddArgs
	addErr    error
	rangeOut  []goredis.XMessage
	rangeErr  error
	xlen      int64
	xlenErr   error
	groups    []goredis.XInfoGroup
	groupsErr error

	pendingOut []goredis.XPendingExt
	pendingErr error
	claimOut   []goredis.XMessage
	claimErr   error

	lastReadArgs  *goredis.XReadGroupArgs
	lastClaimArgs *goredis.XClaimArgs
}

func (f *fakeClient) XGroupCreateMkStream(ctx context.Context, _, _, _ string) *goredis.StatusCmd {
	cmd := goredis.NewStatusCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groupCreated = true
	if f.createErr != nil {
		cmd.SetErr(f.createErr)
	}
	return cmd
}

func (f *fakeClient) XReadGroup(ctx context.Context, a *goredis.XReadGroupArgs) *goredis.XStreamSliceCmd {
	cmd := goredis.NewXStreamSliceCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReadArgs = a
	if f.readErr != nil {
		cmd.SetErr(f.readErr)
		return cmd
	}
	if f.readIdx >= len(f.readBatches) {
		cmd.SetErr(goredis.Nil)
		return cmd
	}
	batch := f.readBatches[f.readIdx]
	f.readIdx++
	stream := a.Streams[0]
	cmd.SetVal([]goredis.XStream{{Stream: stream, Messages: batch}})
	return cmd
}

func (f *fakeClient) XAck(ctx context.Context, _, _ string, ids ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ackErr != nil {
		cmd.SetErr(f.ackErr)
		return cmd
	}
	f.acked = append(f.acked, ids...)
	cmd.SetVal(int64(len(ids)))
	return cmd
}

func (f *fakeClient) XPendingExt(ctx context.Context, a *goredis.XPendingExtArgs) *goredis.XPendingExtCmd {
	cmd := goredis.NewXPendingExtCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pendingErr != nil {
		cmd.SetErr(f.pendingErr)
		return cmd
	}
	cmd.SetVal(f.pendingOut)
	return cmd
}

func (f *fakeClient) XClaim(ctx context.Context, a *goredis.XClaimArgs) *goredis.XMessageSliceCmd {
	cmd := goredis.NewXMessageSliceCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastClaimArgs = a
	if f.claimErr != nil {
		cmd.SetErr(f.claimErr)
		return cmd
	}
	// Honor the requested IDs: XClaim only reassigns the entries it is asked for,
	// so a test that holds an entry back (its requeue floor not yet passed) sees
	// it excluded from the claim arguments and so from the returned set.
	if len(a.Messages) > 0 {
		want := make(map[string]bool, len(a.Messages))
		for _, id := range a.Messages {
			want[id] = true
		}
		out := make([]goredis.XMessage, 0, len(f.claimOut))
		for _, e := range f.claimOut {
			if want[e.ID] {
				out = append(out, e)
			}
		}
		cmd.SetVal(out)
		return cmd
	}
	cmd.SetVal(f.claimOut)
	return cmd
}

func (f *fakeClient) XAdd(ctx context.Context, a *goredis.XAddArgs) *goredis.StringCmd {
	cmd := goredis.NewStringCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		cmd.SetErr(f.addErr)
		return cmd
	}
	f.added = append(f.added, a)
	cmd.SetVal("dlq-1")
	return cmd
}

func (f *fakeClient) XRange(ctx context.Context, _, _, _ string) *goredis.XMessageSliceCmd {
	cmd := goredis.NewXMessageSliceCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rangeErr != nil {
		cmd.SetErr(f.rangeErr)
		return cmd
	}
	cmd.SetVal(f.rangeOut)
	return cmd
}

func (f *fakeClient) XLen(ctx context.Context, _ string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.xlenErr != nil {
		cmd.SetErr(f.xlenErr)
		return cmd
	}
	cmd.SetVal(f.xlen)
	return cmd
}

func (f *fakeClient) XInfoGroups(ctx context.Context, _ string) *goredis.XInfoGroupsCmd {
	cmd := goredis.NewXInfoGroupsCmd(ctx, "")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.groupsErr != nil {
		cmd.SetErr(f.groupsErr)
		return cmd
	}
	cmd.SetVal(f.groups)
	return cmd
}

// entry is a terse XMessage constructor for tests.
func entry(id string, fields map[string]any) goredis.XMessage {
	return goredis.XMessage{ID: id, Values: fields}
}

// newSub builds an Inlet over a fake client and opens a subscription on
// "orders". WithBlock is short so an idle read returns promptly in tests.
func newSub(t *testing.T, c Client, opts ...Option) (*subscription, *Inlet) {
	t.Helper()
	base := []Option{
		WithClient(c), WithGroup("workers"), WithConsumer("w-1"),
		WithBlock(10 * time.Millisecond),
	}
	in, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	s, err := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: []string{"orders"}})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s.(*subscription), in
}

// --- New / option validation ------------------------------------------------

func TestNew_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		opts    []Option
		wantErr string
	}{
		{name: "missing group", opts: []Option{WithAddr("x:6379"), WithConsumer("c")}, wantErr: "WithGroup is required"},
		{name: "missing consumer", opts: []Option{WithAddr("x:6379"), WithGroup("g")}, wantErr: "WithConsumer is required"},
		{
			name:    "addr and client mutually exclusive",
			opts:    []Option{WithGroup("g"), WithConsumer("c"), WithAddr("x:6379"), WithClient(&fakeClient{})},
			wantErr: "mutually exclusive",
		},
		{name: "no transport", opts: []Option{WithGroup("g"), WithConsumer("c")}, wantErr: "WithAddr or WithClient is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tt.opts...)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("New() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestNew_DefaultsApplied(t *testing.T) {
	t.Parallel()
	in, err := New(WithClient(&fakeClient{}), WithGroup("g"), WithConsumer("c"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if in.cfg.block != defaultBlock || in.cfg.count != defaultCount || in.cfg.minIdle != defaultMinIdle {
		t.Errorf("defaults not applied: %+v", in.cfg)
	}
}

func TestSubscribe_RequiresExactlyOneStream(t *testing.T) {
	t.Parallel()
	in, _ := New(WithClient(&fakeClient{}), WithGroup("g"), WithConsumer("c"))
	for _, topics := range [][]string{nil, {"a", "b"}} {
		if _, err := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: topics}); err == nil {
			t.Errorf("Subscribe(%v) = nil, want error", topics)
		}
	}
}

// --- Next: message mapping --------------------------------------------------

func TestNext_MapsEntry(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{
		entry("1526919030474-55", map[string]any{
			ValueField: "payload", KeyHeader: "cust-7", "trace": "abc",
		}),
	}}}
	sub, _ := newSub(t, c)

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
	if m.Subject() != "orders" {
		t.Errorf("Subject() = %q, want orders", m.Subject())
	}
	if m.PartitionKey() != "" {
		t.Errorf("PartitionKey() = %q, want empty (Redis has no partitions)", m.PartitionKey())
	}
	if got := m.Cursor().String(); got != "1526919030474-55" {
		t.Errorf("Cursor() = %q, want the entry ID", got)
	}
	if v, ok := m.Headers().Get("trace"); !ok || v != "abc" {
		t.Errorf("Headers().Get(trace) = %q,%v want abc,true", v, ok)
	}
	var xm goredis.XMessage
	if !m.As(&xm) || xm.ID != "1526919030474-55" {
		t.Errorf("As(*goredis.XMessage) failed, got %+v", xm)
	}
}

func TestNext_KeyFallsBackToStream(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{
		entry("1-0", map[string]any{ValueField: "x"}),
	}}}
	sub, _ := newSub(t, c)
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m.Key()) != "orders" {
		t.Errorf("Key() = %q, want orders (stream fallback)", m.Key())
	}
}

func TestNext_EnsuresGroupOnce(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{
		{entry("1-0", map[string]any{ValueField: "a"})},
		{entry("2-0", map[string]any{ValueField: "b"})},
	}}
	sub, _ := newSub(t, c)
	for i := 0; i < 2; i++ {
		if _, err := sub.Next(context.Background()); err != nil {
			t.Fatalf("Next() error = %v", err)
		}
	}
	if !c.groupCreated {
		t.Error("group was not created")
	}
}

func TestNext_GroupCreateBusyGroupIsIdempotent(t *testing.T) {
	t.Parallel()
	c := &fakeClient{
		createErr:   errors.New("BUSYGROUP Consumer Group name already exists"),
		readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}},
	}
	sub, _ := newSub(t, c)
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v, want BUSYGROUP tolerated", err)
	}
}

func TestNext_GroupCreateRealErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{createErr: errors.New("NOPERM")}
	sub, _ := newSub(t, c)
	if _, err := sub.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "create group") {
		t.Fatalf("Next() = %v, want create-group error", err)
	}
}

func TestNext_ContextCanceled(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Next(canceled) = %v, want context.Canceled", err)
	}
}

func TestNext_ReadErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readErr: errors.New("connection refused")}
	sub, _ := newSub(t, c)
	if _, err := sub.Next(context.Background()); err == nil || !strings.Contains(err.Error(), "read group") {
		t.Fatalf("Next() = %v, want read-group error", err)
	}
}

func TestNext_DrainedAfterClose(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	if err := sub.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next() after Close = %v, want ErrDrained", err)
	}
}

func TestNext_DrainsBufferAfterClose(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{
		entry("1-0", map[string]any{ValueField: "a"}),
		entry("2-0", map[string]any{ValueField: "b"}),
	}}}
	sub, _ := newSub(t, c)
	// Buffer the batch with one Next.
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	_ = sub.Close()
	// The second buffered entry still drains.
	if _, err := sub.Next(context.Background()); err != nil {
		t.Fatalf("buffered Next() after Close = %v, want the buffered entry", err)
	}
	if _, err := sub.Next(context.Background()); !errors.Is(err, source.ErrDrained) {
		t.Fatalf("Next() = %v, want ErrDrained once buffer empty", err)
	}
}

func TestSubscribe_GroupOverride(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}}}
	in, _ := New(WithClient(c), WithGroup("default"), WithConsumer("w-1"), WithBlock(10*time.Millisecond))
	s, _ := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: []string{"orders"}, Group: "override"})
	t.Cleanup(func() { _ = s.Close() })
	if _, err := s.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if c.lastReadArgs.Group != "override" {
		t.Errorf("read group = %q, want override", c.lastReadArgs.Group)
	}
	if sd, ok := s.(source.SharedDurable); !ok || sd.Durable() != "override" {
		t.Errorf("Durable() = %v,%v want override", sd, ok)
	}
}

// --- Settle: action -> Redis vocabulary -------------------------------------

func TestSettle_ActionMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		result source.Result
		check  func(t *testing.T, c *fakeClient)
	}{
		{
			name:   "ack calls XAck",
			result: source.Ack(),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.acked) != 1 || c.acked[0] != "1-0" {
					t.Errorf("acked = %v, want [1-0]", c.acked)
				}
			},
		},
		{
			name:   "nak leaves the entry pending (no ack, no dlq)",
			result: source.Nak(errors.New("transient")),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.acked) != 0 || len(c.added) != 0 {
					t.Errorf("nak settled: acked=%v added=%v, want neither", c.acked, c.added)
				}
			},
		},
		{
			name:   "term dead-letters then acks",
			result: source.Term(errors.New("bad data")),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.added) != 1 {
					t.Fatalf("added = %v, want one DLQ entry", c.added)
				}
				v := c.added[0].Values.(map[string]any)
				if v["crucible-dlq-error"] != "bad data" || v["crucible-dlq-class"] != "poison" {
					t.Errorf("DLQ metadata = %v", v)
				}
				if v["crucible-dlq-original-id"] != "1-0" {
					t.Errorf("DLQ original-id = %v, want 1-0", v["crucible-dlq-original-id"])
				}
				if len(c.acked) != 1 {
					t.Errorf("acked = %v, want the original acked after DLQ", c.acked)
				}
			},
		},
		{
			name:   "reject (invalid for state) dead-letters with its class",
			result: source.Reject(errors.New("wrong state")),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.added) != 1 {
					t.Fatalf("added = %v, want one DLQ entry", c.added)
				}
				v := c.added[0].Values.(map[string]any)
				if v["crucible-dlq-class"] != "invalid_for_state" {
					t.Errorf("DLQ class = %v, want invalid_for_state", v["crucible-dlq-class"])
				}
			},
		},
		{
			name:   "in progress is a no-op",
			result: source.InProgress(),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.acked) != 0 || len(c.added) != 0 {
					t.Errorf("in-progress settled: acked=%v added=%v", c.acked, c.added)
				}
			},
		},
		{
			name:   "manual is a no-op",
			result: source.Manual(),
			check: func(t *testing.T, c *fakeClient) {
				if len(c.acked) != 0 || len(c.added) != 0 {
					t.Errorf("manual settled: acked=%v added=%v", c.acked, c.added)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &fakeClient{readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}}}
			sub, _ := newSub(t, c, WithDLQStream("orders.dlq"))
			m, err := sub.Next(context.Background())
			if err != nil {
				t.Fatalf("Next() error = %v", err)
			}
			if err := sub.Settle(context.Background(), m, tt.result); err != nil {
				t.Fatalf("Settle() error = %v", err)
			}
			tt.check(t, c)
		})
	}
}

func TestSettle_TermWithoutDLQJustAcks(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}}}
	sub, _ := newSub(t, c) // no WithDLQStream
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Term(errors.New("x"))); err != nil {
		t.Fatalf("Settle() error = %v", err)
	}
	if len(c.added) != 0 {
		t.Errorf("added = %v, want no DLQ entry without WithDLQStream", c.added)
	}
	if len(c.acked) != 1 {
		t.Errorf("acked = %v, want the entry acked-and-dropped", c.acked)
	}
}

func TestSettle_AckErrorWrapped(t *testing.T) {
	t.Parallel()
	boom := errors.New("unreachable")
	c := &fakeClient{
		readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}},
		ackErr:      boom,
	}
	sub, _ := newSub(t, c)
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Ack()); !errors.Is(err, boom) {
		t.Fatalf("Settle() = %v, want wrapped ack error", err)
	}
}

func TestSettle_DLQErrorWrapped(t *testing.T) {
	t.Parallel()
	boom := errors.New("dlq down")
	c := &fakeClient{
		readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}},
		addErr:      boom,
	}
	sub, _ := newSub(t, c, WithDLQStream("dlq"))
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Term(errors.New("x"))); !errors.Is(err, boom) {
		t.Fatalf("Settle() = %v, want wrapped dlq error", err)
	}
}

func TestSettle_UnknownAction(t *testing.T) {
	t.Parallel()
	c := &fakeClient{readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}}}
	sub, _ := newSub(t, c)
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Result{Action: source.Action(99)}); err == nil {
		t.Fatal("Settle(unknown action) = nil, want error")
	}
}

func TestSettle_NonRedisMessage(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	if err := sub.Settle(context.Background(), foreignMsg{}, source.Ack()); err == nil {
		t.Fatal("Settle(foreign message) = nil, want error")
	}
}

// --- NakRedeliver: pending scan + claim -------------------------------------

func TestNakRedeliver_ClaimsAndBuffersPending(t *testing.T) {
	t.Parallel()
	c := &fakeClient{
		pendingOut: []goredis.XPendingExt{{ID: "1-0", Consumer: "other", RetryCount: 1}},
		claimOut:   []goredis.XMessage{entry("1-0", map[string]any{ValueField: "redelivered"})},
	}
	sub, _ := newSub(t, c)
	n, err := sub.NakRedeliver(context.Background(), 0)
	if err != nil {
		t.Fatalf("NakRedeliver() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("NakRedeliver() = %d, want 1", n)
	}
	if c.lastClaimArgs == nil || c.lastClaimArgs.MinIdle != defaultMinIdle {
		t.Errorf("claim MinIdle = %v, want default", c.lastClaimArgs)
	}
	// The claimed entry is buffered, so the next Next yields it.
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() after redeliver error = %v", err)
	}
	if string(m.Value()) != "redelivered" {
		t.Errorf("Value() = %q, want redelivered", m.Value())
	}
}

func TestNakRedeliver_NoPendingIsNoOp(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	n, err := sub.NakRedeliver(context.Background(), time.Second)
	if err != nil || n != 0 {
		t.Fatalf("NakRedeliver() = %d,%v want 0,nil", n, err)
	}
}

func TestNakRedeliver_PendingErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{pendingErr: errors.New("boom")}
	sub, _ := newSub(t, c)
	if _, err := sub.NakRedeliver(context.Background(), time.Second); err == nil {
		t.Fatal("NakRedeliver() = nil, want pending-scan error")
	}
}

func TestNakRedeliver_ClaimErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{
		pendingOut: []goredis.XPendingExt{{ID: "1-0"}},
		claimErr:   errors.New("boom"),
	}
	sub, _ := newSub(t, c)
	if _, err := sub.NakRedeliver(context.Background(), time.Second); err == nil {
		t.Fatal("NakRedeliver() = nil, want claim error")
	}
}

func TestNak_RequeueRaisesPerEntryFloor(t *testing.T) {
	t.Parallel()
	// A controllable clock lets the test advance time deterministically across
	// the per-entry requeue floor a Nak with a large Requeue raises.
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }

	c := &fakeClient{
		readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}},
		pendingOut:  []goredis.XPendingExt{{ID: "1-0", RetryCount: 1}},
		claimOut:    []goredis.XMessage{entry("1-0", map[string]any{ValueField: "a"})},
	}
	// minIdle small so only the per-entry Requeue floor gates redelivery.
	sub, _ := newSub(t, c, WithMinIdle(time.Millisecond), WithClock(clock))

	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	// Nak with a Requeue (10s) far larger than minIdle: the entry's floor is now+10s.
	if err := sub.Settle(context.Background(), m, source.NakAfter(10*time.Second, errors.New("retry"))); err != nil {
		t.Fatalf("Settle(nak) error = %v", err)
	}

	// Before the floor passes, NakRedeliver must hold the entry back.
	if n, err := sub.NakRedeliver(context.Background(), 0); err != nil || n != 0 {
		t.Fatalf("NakRedeliver() before floor = %d,%v want 0,nil", n, err)
	}

	// Advance past the floor: the entry is now eligible and is reclaimed.
	now = now.Add(11 * time.Second)
	n, err := sub.NakRedeliver(context.Background(), 0)
	if err != nil {
		t.Fatalf("NakRedeliver() after floor error = %v", err)
	}
	if n != 1 {
		t.Fatalf("NakRedeliver() after floor = %d, want 1", n)
	}
}

func TestNak_NoRequeueLeavesFloorUnraised(t *testing.T) {
	t.Parallel()
	// A plain Nak (no Requeue, or one below minIdle) raises no floor, so the
	// entry redelivers on the next scan governed by minIdle alone.
	c := &fakeClient{
		readBatches: [][]goredis.XMessage{{entry("1-0", map[string]any{ValueField: "a"})}},
		pendingOut:  []goredis.XPendingExt{{ID: "1-0", RetryCount: 1}},
		claimOut:    []goredis.XMessage{entry("1-0", map[string]any{ValueField: "a"})},
	}
	sub, _ := newSub(t, c, WithMinIdle(time.Millisecond))
	m, _ := sub.Next(context.Background())
	if err := sub.Settle(context.Background(), m, source.Nak(errors.New("retry"))); err != nil {
		t.Fatalf("Settle(nak) error = %v", err)
	}
	n, err := sub.NakRedeliver(context.Background(), 0)
	if err != nil || n != 1 {
		t.Fatalf("NakRedeliver() = %d,%v want 1,nil", n, err)
	}
}

// --- capabilities: seek (replay by entry ID) --------------------------------

func TestSeek_BuffersBacklog(t *testing.T) {
	t.Parallel()
	seekTime := time.UnixMilli(1526919030000).UTC()
	tests := []struct {
		name string
		seek func(s source.Seekable) error
	}{
		{name: "to start", seek: func(s source.Seekable) error { return s.SeekToStart(context.Background()) }},
		{name: "to time", seek: func(s source.Seekable) error { return s.SeekToTime(context.Background(), seekTime) }},
		{name: "to cursor", seek: func(s source.Seekable) error { return s.SeekToCursor(context.Background(), idCursor("5-0")) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := &fakeClient{rangeOut: []goredis.XMessage{
				entry("5-0", map[string]any{ValueField: "replayed"}),
			}}
			sub, _ := newSub(t, c)
			sk := source.Seekable(sub)
			if err := tt.seek(sk); err != nil {
				t.Fatalf("seek error = %v", err)
			}
			m, err := sub.Next(context.Background())
			if err != nil {
				t.Fatalf("Next() after seek error = %v", err)
			}
			if string(m.Value()) != "replayed" {
				t.Errorf("Value() = %q, want replayed from backlog", m.Value())
			}
		})
	}
}

func TestSeekToEnd_SkipsBacklog(t *testing.T) {
	t.Parallel()
	c := &fakeClient{
		rangeOut:    []goredis.XMessage{entry("1-0", map[string]any{ValueField: "old"})},
		readBatches: [][]goredis.XMessage{{entry("9-0", map[string]any{ValueField: "new"})}},
	}
	sub, _ := newSub(t, c)
	// Pre-load a backlog via a SeekToStart, then SeekToEnd must clear it.
	if err := sub.SeekToStart(context.Background()); err != nil {
		t.Fatalf("SeekToStart() error = %v", err)
	}
	if err := sub.SeekToEnd(context.Background()); err != nil {
		t.Fatalf("SeekToEnd() error = %v", err)
	}
	m, err := sub.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if string(m.Value()) != "new" {
		t.Errorf("Value() = %q, want new (backlog skipped)", m.Value())
	}
}

func TestSeek_OnClosedSubscription(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	_ = sub.Close()
	if err := sub.SeekToStart(context.Background()); !errors.Is(err, errSeekClosed) {
		t.Fatalf("SeekToStart(closed) = %v, want errSeekClosed", err)
	}
	if err := sub.SeekToEnd(context.Background()); !errors.Is(err, errSeekClosed) {
		t.Fatalf("SeekToEnd(closed) = %v, want errSeekClosed", err)
	}
}

func TestSeekToCursor_RejectsForeignCursor(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	if err := sub.SeekToCursor(context.Background(), foreignCursor{}); err == nil {
		t.Fatal("SeekToCursor(foreign) = nil, want error")
	}
}

func TestSeek_RangeErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{rangeErr: errors.New("boom")}
	sub, _ := newSub(t, c)
	if err := sub.SeekToStart(context.Background()); err == nil {
		t.Fatal("SeekToStart() = nil, want range error")
	}
}

// --- capabilities: lag ------------------------------------------------------

func TestLag_PrefersGroupLag(t *testing.T) {
	t.Parallel()
	c := &fakeClient{
		groups: []goredis.XInfoGroup{{Name: "workers", Lag: 12}},
		xlen:   999,
	}
	sub, _ := newSub(t, c)
	got, err := sub.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if got != 12 {
		t.Errorf("Lag() = %d, want 12 (group lag)", got)
	}
}

func TestLag_GroupCaughtUpReportsZero(t *testing.T) {
	t.Parallel()
	// A caught-up group reports Lag 0 from XINFO GROUPS. The reporter must return
	// that zero, not fall through to XLEN and report the full stream length.
	c := &fakeClient{
		groups: []goredis.XInfoGroup{{Name: "workers", Lag: 0}},
		xlen:   999,
	}
	sub, _ := newSub(t, c)
	got, err := sub.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if got != 0 {
		t.Errorf("Lag() = %d, want 0 (group caught up)", got)
	}
}

func TestLag_FallsBackToXLen(t *testing.T) {
	t.Parallel()
	c := &fakeClient{groupsErr: errors.New("no groups info"), xlen: 7}
	sub, _ := newSub(t, c)
	got, err := sub.Lag(context.Background())
	if err != nil {
		t.Fatalf("Lag() error = %v", err)
	}
	if got != 7 {
		t.Errorf("Lag() = %d, want 7 (XLen fallback)", got)
	}
}

func TestLag_XLenErrorPropagates(t *testing.T) {
	t.Parallel()
	c := &fakeClient{groupsErr: errors.New("x"), xlenErr: errors.New("boom")}
	sub, _ := newSub(t, c)
	if _, err := sub.Lag(context.Background()); err == nil {
		t.Fatal("Lag() = nil, want XLen error")
	}
}

// --- Inlet As / Close -------------------------------------------------------

func TestInlet_As_ReturnsClientSeam(t *testing.T) {
	t.Parallel()
	fc := &fakeClient{}
	in, err := New(WithClient(fc), WithGroup("g"), WithConsumer("c"))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	var got Client
	if !in.As(&got) || got != fc {
		t.Fatalf("As(*Client) did not return the client")
	}
	var wrong *int
	if in.As(&wrong) {
		t.Fatal("As(**int) = true, want false")
	}
	// A fake is not a *redis.Client, so the concrete escape hatch misses.
	var rc *goredis.Client
	if in.As(&rc) {
		t.Fatal("As(**redis.Client) = true for a fake client, want false")
	}
}

func TestInlet_Close_LeavesCallerClientOpen(t *testing.T) {
	t.Parallel()
	fc := &closableClient{fakeClient: &fakeClient{}}
	in, _ := New(WithClient(fc), WithGroup("g"), WithConsumer("c"))
	if err := in.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if fc.closed {
		t.Error("Close() shut down a caller-owned client; want it left open")
	}
}

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	sub, _ := newSub(t, &fakeClient{})
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

// --- helpers / foreign types ------------------------------------------------

// closableClient adds a recording Close to the fake so the ownership test can
// assert a caller-owned client is never shut down.
type closableClient struct {
	*fakeClient
	closed bool
}

func (c *closableClient) Close() error { c.closed = true; return nil }

type foreignCursor struct{}

func (foreignCursor) String() string { return "foreign" }

// foreignMsg is a source.Message not backed by a redis entry.
type foreignMsg struct{}

func (foreignMsg) Key() []byte             { return nil }
func (foreignMsg) Value() []byte           { return nil }
func (foreignMsg) Headers() source.Headers { return nil }
func (foreignMsg) Subject() string         { return "" }
func (foreignMsg) PartitionKey() string    { return "" }
func (foreignMsg) Cursor() source.Cursor   { return idCursor("") }
func (foreignMsg) As(any) bool             { return false }
