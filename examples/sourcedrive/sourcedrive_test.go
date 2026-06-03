// SPDX-License-Identifier: Apache-2.0

package sourcedrive_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stablekernel/crucible/examples/sourcedrive"
	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
	"github.com/stablekernel/crucible/source/statemachine"
)

// cmdMsg builds a JSON fulfillment command for key carrying op and the
// idempotency id the exactly-once dedup reads.
func cmdMsg(key, id, op string) memsource.Msg {
	body, _ := json.Marshal(sourcedrive.Command{Op: op})
	return memsource.Msg{
		Key:   key,
		Value: body,
		Headers: source.Headers{
			{Key: "content-type", Value: "application/json"},
			{Key: statemachine.DefaultEventIDHeader, Value: id},
		},
	}
}

// runMessages drives msgs through a freshly seeded fulfillment with an in-memory
// inlet, returning the fulfillment and the settle ledger so a test asserts both
// the durable state and the per-message disposition with no broker.
func runMessages(t *testing.T, seed func(*sourcedrive.Fulfillment), msgs ...memsource.Msg) (*sourcedrive.Fulfillment, *memsource.Ledger) {
	t.Helper()
	f := sourcedrive.NewFulfillment()
	if seed != nil {
		seed(f)
	}

	inlet := memsource.New(memsource.WithMessages(msgs...))
	hopper := source.New(source.WithName("sourcedrive-test"))
	t.Cleanup(func() { _ = hopper.Close() })

	ctx := context.Background()
	sub, err := inlet.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"fulfillment"}})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// Close the subscription so the Hopper drains once the queue empties.
	_ = sub.Close()

	if err := hopper.Run(ctx, sub, f.Handler); err != nil {
		t.Fatalf("hopper run: %v", err)
	}
	return f, inlet.Ledger()
}

// TestRun_HappyPath_DrivesAndPersists consumes one funded pay command and proves
// the instance advanced to shipped, the transition persisted, and the message
// acked — the consume → decode → Fire → persist → ack loop end to end.
func TestRun_HappyPath_DrivesAndPersists(t *testing.T) {
	const key = "ship-1"
	f, ledger := runMessages(t,
		func(f *sourcedrive.Fulfillment) {
			if err := f.Seed(context.Background(), key, true); err != nil {
				t.Fatalf("seed: %v", err)
			}
		},
		cmdMsg(key, "evt-1", "pay"),
	)

	if got := ledger.Counts(); got != (memsource.Counts{Acked: 1}) {
		t.Fatalf("counts = %+v, want one ack", got)
	}

	rec, ok, err := f.Store.Load(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if rec.Snapshot.Current != "shipped" {
		t.Fatalf("state = %q, want shipped", rec.Snapshot.Current)
	}
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2", rec.Version)
	}
}

// TestRun_Redelivery_IsIdempotentNoOpAck delivers the same event id twice: the
// first applies the transition, the second is a no-op ack keyed on the persisted
// state version, never re-firing or advancing the version.
func TestRun_Redelivery_IsIdempotentNoOpAck(t *testing.T) {
	const key = "ship-1"
	f, ledger := runMessages(t,
		func(f *sourcedrive.Fulfillment) {
			if err := f.Seed(context.Background(), key, true); err != nil {
				t.Fatalf("seed: %v", err)
			}
		},
		cmdMsg(key, "evt-1", "pay"),
		cmdMsg(key, "evt-1", "pay"),
	)

	if got := ledger.Counts(); got != (memsource.Counts{Acked: 1, Dropped: 1}) {
		t.Fatalf("counts = %+v, want one ack and one drop", got)
	}

	rec, _, _ := f.Store.Load(context.Background(), key)
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2 (redelivery must not advance)", rec.Version)
	}
}

// TestRun_IllegalEventForState_IsTerm fires events the current state can never
// accept and proves both shapes terminate as poison-by-state (Term,
// InvalidForState) rather than nak for redelivery, leaving the state untouched.
func TestRun_IllegalEventForState_IsTerm(t *testing.T) {
	tests := []struct {
		name  string
		funds bool
		op    string
	}{
		{name: "guard rejection (unfunded pay)", funds: false, op: "pay"},
		{name: "no transition (deliver from pending)", funds: true, op: "deliver"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			const key = "ship-1"
			f, ledger := runMessages(t,
				func(f *sourcedrive.Fulfillment) {
					if err := f.Seed(context.Background(), key, tc.funds); err != nil {
						t.Fatalf("seed: %v", err)
					}
				},
				cmdMsg(key, "evt-1", tc.op),
			)

			if got := ledger.Counts(); got != (memsource.Counts{Term: 1}) {
				t.Fatalf("counts = %+v, want one term", got)
			}
			entries := ledger.Entries()
			if len(entries) != 1 || entries[0].Result.Class != source.InvalidForState {
				t.Fatalf("entries = %+v, want one invalid_for_state term", entries)
			}

			rec, _, _ := f.Store.Load(context.Background(), key)
			if rec.Version != 1 || rec.Snapshot.Current != "pending" {
				t.Fatalf("state = %q v%d, want pending v1 (unchanged)", rec.Snapshot.Current, rec.Version)
			}
		})
	}
}

// TestRun_UndecodablePayload_IsTermPoison proves a body that cannot decode into
// a Command terminates as poison (a route failure), never retried.
func TestRun_UndecodablePayload_IsTermPoison(t *testing.T) {
	const key = "ship-1"
	bad := memsource.Msg{
		Key:   key,
		Value: []byte("{not json"),
		Headers: source.Headers{
			{Key: "content-type", Value: "application/json"},
			{Key: statemachine.DefaultEventIDHeader, Value: "evt-1"},
		},
	}
	_, ledger := runMessages(t,
		func(f *sourcedrive.Fulfillment) {
			if err := f.Seed(context.Background(), key, true); err != nil {
				t.Fatalf("seed: %v", err)
			}
		},
		bad,
	)

	if got := ledger.Counts(); got != (memsource.Counts{Term: 1}) {
		t.Fatalf("counts = %+v, want one term", got)
	}
	if got := ledger.Entries()[0].Result.Class; got != source.Poison {
		t.Fatalf("class = %v, want poison", got)
	}
}

// TestRun_MissingKey_IsTermPoison proves a message with no shipment key is
// unroutable and terminates as poison, covering the empty-key route guard.
func TestRun_MissingKey_IsTermPoison(t *testing.T) {
	keyless := memsource.Msg{
		Value: func() []byte { b, _ := json.Marshal(sourcedrive.Command{Op: "pay"}); return b }(),
		Headers: source.Headers{
			{Key: "content-type", Value: "application/json"},
			{Key: statemachine.DefaultEventIDHeader, Value: "evt-1"},
		},
	}
	_, ledger := runMessages(t, nil, keyless)

	if got := ledger.Counts(); got != (memsource.Counts{Term: 1}) {
		t.Fatalf("counts = %+v, want one term", got)
	}
	if got := ledger.Entries()[0].Result.Class; got != source.Poison {
		t.Fatalf("class = %v, want poison", got)
	}
}

// TestRun_DrivesInletThroughHopper exercises the exported [sourcedrive.Run]
// against an in-memory inlet: it queues one funded pay command, runs the consume
// loop in a goroutine, and cancels the context once the message has settled. Run
// returns nil on the graceful cancellation, and the instance has advanced to
// shipped, proving the package-level driving entrypoint end to end without a broker.
func TestRun_DrivesInletThroughHopper(t *testing.T) {
	const (
		key   = "ship-1"
		topic = "fulfillment"
		group = "sourcedrive-run"
	)

	f := sourcedrive.NewFulfillment()
	if err := f.Seed(context.Background(), key, true); err != nil {
		t.Fatalf("seed: %v", err)
	}

	inlet := memsource.New(memsource.WithMessages(cmdMsg(key, "evt-1", "pay")))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- sourcedrive.Run(ctx, nil, inlet, f, []string{topic}, group)
	}()

	// Cancel once the message has settled so the blocking consume loop exits
	// gracefully (Run returns nil on context cancellation). A deadline guards
	// against a hang if the message never settles.
	waitForSettle(t, inlet.Ledger(), 1)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not return after context cancellation")
	}

	if got := inlet.Ledger().Counts(); got != (memsource.Counts{Acked: 1}) {
		t.Fatalf("counts = %+v, want one ack", got)
	}
	rec, ok, err := f.Store.Load(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if rec.Snapshot.Current != "shipped" {
		t.Fatalf("state = %q, want shipped", rec.Snapshot.Current)
	}
}

// TestRun_SubscribeError surfaces a subscribe failure as a wrapped error from Run.
func TestRun_SubscribeError(t *testing.T) {
	f := sourcedrive.NewFulfillment()
	sentinel := errors.New("boom")
	inlet := failingInlet{err: sentinel}

	err := sourcedrive.Run(context.Background(), nil, inlet, f, []string{"t"}, "g")
	if !errors.Is(err, sentinel) {
		t.Fatalf("Run() error = %v, want wrapping %v", err, sentinel)
	}
}

// TestRunKafka_NoBrokersErrors proves RunKafka returns a wrapped inlet-construction
// error when no seed brokers are configured, covering the option assembly and the
// error path without touching a broker.
func TestRunKafka_NoBrokersErrors(t *testing.T) {
	cfg := sourcedrive.KafkaConfig{
		Topic:    "fulfillment",
		Group:    "g",
		ClientID: "sourcedrive",
		DLQTopic: "fulfillment.DLQ",
	}
	err := sourcedrive.RunKafka(context.Background(), nil, cfg)
	if err == nil {
		t.Fatal("RunKafka() error = nil, want inlet construction error")
	}
}

// TestRunKafka_CanceledContextReturnsCleanly drives RunKafka with an
// already-canceled context against a dead local broker: the inlet constructs and
// the consume loop exits immediately on the canceled context, so RunKafka returns
// without error. It covers the success path (inlet construction through Run)
// without a live broker. No connection is awaited because the context is dead.
func TestRunKafka_CanceledContextReturnsCleanly(t *testing.T) {
	cfg := sourcedrive.KafkaConfig{
		Brokers:  []string{"127.0.0.1:0"},
		Topic:    "fulfillment",
		Group:    "g",
		ClientID: "sourcedrive",
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- sourcedrive.RunKafka(ctx, nil, cfg) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunKafka() error = %v, want nil on canceled context", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunKafka() did not return on canceled context")
	}
}

// TestSplitBrokers tabulates the comma-list parsing the cmd entrypoint relies on.
func TestSplitBrokers(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "single", in: "localhost:9092", want: []string{"localhost:9092"}},
		{name: "multiple", in: "a:9092,b:9092", want: []string{"a:9092", "b:9092"}},
		{name: "trims spaces", in: " a:9092 , b:9092 ", want: []string{"a:9092", "b:9092"}},
		{name: "drops empties", in: "a:9092,,", want: []string{"a:9092"}},
		{name: "all empty", in: " , ", want: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sourcedrive.SplitBrokers(tc.in); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("SplitBrokers(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

// waitForSettle blocks until the ledger has recorded at least n settlements or a
// deadline elapses, so a consume-loop test can cancel deterministically rather
// than racing a fixed sleep.
func waitForSettle(t *testing.T, ledger *memsource.Ledger, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for ledger.Len() < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d settlement(s); saw %d", n, ledger.Len())
		}
		time.Sleep(time.Millisecond)
	}
}

// failingInlet is a source.Inlet whose Subscribe always fails, to drive Run's
// subscribe-error path.
type failingInlet struct{ err error }

func (f failingInlet) Subscribe(context.Context, source.SubscribeConfig) (source.Subscription, error) {
	return nil, f.err
}

func (failingInlet) Close() error { return nil }
