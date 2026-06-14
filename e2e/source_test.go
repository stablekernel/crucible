// SPDX-License-Identifier: Apache-2.0

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/memsource"
	"github.com/stablekernel/crucible/source/statemachine"
	"github.com/stablekernel/crucible/state"
)

// This whole-stack test wires the ingress seam end to end with no broker: an
// in-memory source.Inlet feeds a source.Hopper, the Hopper drives a crucible
// statechart through the source/statemachine bridge, and a statemachine.MemStore
// persists each transition before the message is acked. It is the
// message → decode → route → Fire → persist → ack loop running in the workspace,
// proving the bridge composes the three modules (source, statemachine, state)
// through the Store seam without any core importing another.

// shipment is the entity a fulfillment instance advances. Funds gates the pay
// transition so a guard rejection (invalid-for-state) is reachable. Stage
// records the lifecycle state reached; CurrentStateFn reads it so a shipment
// cast from an in-flight value resumes at its real state, not at pending.
type shipment struct {
	Funds bool   `json:"funds"`
	Stage string `json:"stage"`
}

// currentStage derives a shipment's current state for CurrentStateFn. A nil
// entity (a fresh cast with no record) or an empty Stage starts at pending;
// otherwise the recorded stage is honored so resume lands on the real state.
func currentStage(s *shipment) string {
	if s == nil || s.Stage == "" {
		return "pending"
	}
	return s.Stage
}

// shipEvent is the decoded inbound command carried in each message's JSON body.
// The bridge router projects it to the statechart's event name.
type shipEvent struct {
	// Op is the event to fire: "pay" advances pending → shipped.
	Op string `json:"op"`
}

// fulfillmentMachine forges the shipment lifecycle:
//
//	pending --pay[funded]--> shipped --deliver--> delivered
//
// pay is guarded by funded so an unfunded pay is rejected as invalid-for-state,
// and deliver from pending has no transition, exercising the two state-aware
// rejection shapes.
func fulfillmentMachine() *state.Machine[string, string, *shipment] {
	return state.ForgeFor[*shipment]("fulfillment").
		Guard("funded", func(ctx state.GuardCtx[*shipment]) bool {
			return ctx.Entity != nil && ctx.Entity.Funds
		}).
		State("pending").
		State("shipped").
		State("delivered").
		Initial("pending").
		CurrentStateFn(currentStage).
		Transition("pending").On("pay").GoTo("shipped").When("funded").
		Transition("shipped").On("deliver").GoTo("delivered").
		Quench(state.Strict())
}

// shipRegistry decodes JSON shipEvent bodies; the router decodes through it so
// the codec union is honestly checked against the wire payload.
func shipRegistry() *source.Registry {
	return source.NewRegistry().
		Register("application/json", source.NewJSONCodec[shipEvent]())
}

// routeShipment resolves the instance key from the message key and decodes the
// event op from the JSON body. A decode/route failure returns an error, which
// Drive treats as poison (Term).
func routeShipment(reg *source.Registry) statemachine.Router[string, string] {
	return func(m source.Message) (string, string, error) {
		ev, err := source.DecodeTyped[shipEvent](reg, m)
		if err != nil {
			return "", "", fmt.Errorf("decode shipment event: %w", err)
		}
		key := string(m.Key())
		if key == "" {
			return "", "", fmt.Errorf("shipment event missing key")
		}
		return key, ev.Op, nil
	}
}

// seedShipment persists a version-1 pending instance for key with the given
// funding, so the guarded pay path is exercised from a known state.
func seedShipment(t *testing.T, m *state.Machine[string, string, *shipment], store *statemachine.MemStore[string, string, string, *shipment], key string, funds bool) {
	t.Helper()
	inst := m.Cast(&shipment{Funds: funds}, state.WithInitialState("pending"))
	rec := statemachine.Record[string, string, *shipment]{Snapshot: inst.Snapshot(), Version: 1}
	if err := store.Save(context.Background(), key, rec, 0); err != nil {
		t.Fatalf("seed %q: %v", key, err)
	}
}

// payMsg builds a JSON "pay" command for key, tagged with a content type and an
// idempotency id so redelivery dedup keys on the id.
func payMsg(key, id string) memsource.Msg {
	return cmdMsg(key, id, "pay")
}

// cmdMsg builds a JSON command message for key carrying op, the id header the
// exactly-once dedup reads, and the JSON content type.
func cmdMsg(key, id, op string) memsource.Msg {
	body, _ := json.Marshal(shipEvent{Op: op})
	return memsource.Msg{
		Key:   key,
		Value: body,
		Headers: source.Headers{
			{Key: "content-type", Value: "application/json"},
			{Key: statemachine.DefaultEventIDHeader, Value: id},
		},
	}
}

// TestE2E_SourceDrivesStatechartToDurableTransition is the happy path: a single
// funded shipment, paid by one message, advances pending → shipped, persists the
// transition, and acks — the full ingress loop with the durable transition tied
// to the ack.
func TestE2E_SourceDrivesStatechartToDurableTransition(t *testing.T) {
	m := fulfillmentMachine()
	store := statemachine.NewMemStore[string, string, string, *shipment]()
	reg := shipRegistry()
	const key = "ship-1"
	seedShipment(t, m, store, key, true)

	drive := statemachine.Drive[string, string, *shipment](m, store, routeShipment(reg))

	h := memsource.NewHarness(t, nil, payMsg(key, "evt-1"))
	h.Run(drive)

	h.AssertCounts(memsource.Counts{Acked: 1})

	rec, ok, err := store.Load(context.Background(), key)
	if err != nil || !ok {
		t.Fatalf("load after ack: ok=%v err=%v", ok, err)
	}
	if rec.Snapshot.Current != "shipped" {
		t.Fatalf("state = %q, want shipped", rec.Snapshot.Current)
	}
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2 (seed 1 + 1 transition)", rec.Version)
	}
	if rec.LastEventID != "evt-1" {
		t.Fatalf("lastEventID = %q, want evt-1", rec.LastEventID)
	}
}

// TestE2E_SourceRedeliveryIsIdempotentNoOpAck redelivers an already-applied
// event id through the full Hopper path: the bridge keys dedup on the persisted
// state version, so the second delivery is a no-op ack (classified Drop) that
// never re-fires the transition or advances the version. Exactly-once into the
// machine, no external dedup store.
func TestE2E_SourceRedeliveryIsIdempotentNoOpAck(t *testing.T) {
	m := fulfillmentMachine()
	store := statemachine.NewMemStore[string, string, string, *shipment]()
	reg := shipRegistry()
	const key = "ship-1"
	seedShipment(t, m, store, key, true)

	drive := statemachine.Drive[string, string, *shipment](m, store, routeShipment(reg))

	// Same id twice: the first applies the transition, the second is a no-op ack.
	h := memsource.NewHarness(t, nil, payMsg(key, "evt-1"), payMsg(key, "evt-1"))
	h.Run(drive)

	// One real ack (the applied transition) and one drop (the deduped redelivery).
	h.AssertCounts(memsource.Counts{Acked: 1, Dropped: 1})

	rec, _, _ := store.Load(context.Background(), key)
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2 (redelivery must not advance)", rec.Version)
	}
	if rec.Snapshot.Current != "shipped" {
		t.Fatalf("state = %q, want shipped", rec.Snapshot.Current)
	}
}

// TestE2E_SourceIllegalEventForStateIsTermNotRetried fires an event that is
// illegal for the current state through the full Hopper path. Two shapes:
//
//   - a guard rejection (unfunded pay) — the transition exists but its guard
//     fails;
//   - a no-transition (deliver from pending) — no declared transition at all.
//
// Both must terminate (Term, classified InvalidForState), not nak for redelivery:
// retrying an event the state can never accept would loop forever. The persisted
// state stays untouched.
func TestE2E_SourceIllegalEventForStateIsTermNotRetried(t *testing.T) {
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
			m := fulfillmentMachine()
			store := statemachine.NewMemStore[string, string, string, *shipment]()
			reg := shipRegistry()
			const key = "ship-1"
			seedShipment(t, m, store, key, tc.funds)

			drive := statemachine.Drive[string, string, *shipment](m, store, routeShipment(reg))

			h := memsource.NewHarness(t, nil, cmdMsg(key, "evt-1", tc.op))
			h.Run(drive)

			// Termed, never nak'd: a state-invalid event is poison-by-state.
			h.AssertCounts(memsource.Counts{Term: 1})

			entries := h.Ledger().Entries()
			if len(entries) != 1 {
				t.Fatalf("settled %d, want 1", len(entries))
			}
			res := entries[0].Result
			if res.Class != source.InvalidForState {
				t.Fatalf("class = %v, want invalid_for_state", res.Class)
			}

			rec, _, _ := store.Load(context.Background(), key)
			if rec.Version != 1 {
				t.Fatalf("version = %d, want 1 (rejection must not persist)", rec.Version)
			}
			if rec.Snapshot.Current != "pending" {
				t.Fatalf("state = %q, want pending (unchanged)", rec.Snapshot.Current)
			}
		})
	}
}
