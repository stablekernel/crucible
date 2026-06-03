// SPDX-License-Identifier: Apache-2.0

package sourcedrive_test

import (
	"context"
	"encoding/json"
	"testing"

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
