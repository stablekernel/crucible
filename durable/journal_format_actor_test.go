package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests pin the journal format bump that made an actor's done-data decode
// through WithEventDataDecoder: a delivery an older version journaled, at format 4 or
// lower, replays its done-data as that version replayed it — in JSON's generic form,
// through no decoder and no type check — and this version's Records, at format 5,
// carry the done-data's Go type and decode through the decoder. The three decoder
// rows (a failing decoder, one that returns another type, one that returns the actor's
// own) mutation-check the older formats' row the way
// TestJournalFormat_OlderServiceResult_ReplaysAsItDid does: if a change made replay
// decode an older Record's done-data through the decoder, or hold it to a type, the
// first two rows would fail Recover on journals the older version recovered.

// TestJournalFormat_OlderActorDoneData_ReplaysAsItDid: an actor's done-data an older
// version journaled, at format 0 (the 0.1.0 release), 1, 2, 3 or 4 (each before
// done-data was decoded through WithEventDataDecoder), replays as that version
// replayed it: in JSON's generic form, through no decoder and no type check. A Runner
// whose decoder fails for the onDone event, or returns another type, or returns the
// actor's own type, still recovers it, and the replayed Assign sees the map the older
// version's replay gave it. A delivery this version records, at format 5, decodes
// through the decoder (see TestActor_DoneData_SameTypeLiveAndAfterRecover), and its
// entry carries the done-data's Go type.
func TestJournalFormat_OlderActorDoneData_ReplaysAsItDid(t *testing.T) {
	decoders := []struct {
		name string
		opt  durable.Option[string, string, outcomeCtx]
	}{
		{"decoder fails", childDoneDecoder(func([]byte) (any, error) { return nil, errors.New("decoder refused") })},
		{"decoder returns another type", childDoneDecoder(durable.DecodeAs[scnAdmin])},
		{"decoder returns the actor's type", childDoneDecoder(durable.DecodeAs[order])},
	}
	for _, format := range []int{0, 1, 2, 3, 4} {
		for _, dc := range decoders {
			t.Run(dc.name+", format "+string(rune('0'+format)), func(t *testing.T) {
				ctx := context.Background()
				var runs int64
				behavior := outputChild(&runs, order{ID: 7})
				palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior})
				// The olderWriter stamps the format an older version wrote on every
				// Record and checkpoint. The done-data bytes this version records are
				// the bytes the older version's actorEntry journaled too — json.Marshal
				// of the actor's output — and the older version recorded no type, which
				// a Record of its format ignores anyway.
				store := &olderWriter{MemStore: durable.NewMemStore(durable.WithHistory()), format: format}
				h, err := durable.NewRunner(collectorMachine(), store, palette).Start(ctx, "collect-1", outcomeCtx{}, state.WithInitialState("supervising"))
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				ref, ok := h.ActorRef(state.ActorID("collector", "supervising", 0))
				if !ok {
					t.Fatal("no actor ref for the spawned child")
				}
				if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
					t.Fatalf("DeliverToActor: %v", err)
				}
				_, recs, err := store.Load(ctx, "collect-1")
				if err != nil || len(recs) != 1 || recs[0].Format != format {
					t.Fatalf("the journal holds %+v (err %v), want one Record of format %d", recs, err, format)
				}

				rh, err := durable.Recover(ctx, collectorMachine(), store, "collect-1", palette, dc.opt)
				if err != nil {
					t.Fatalf("Recover: %v; the older version recovered this journal", err)
				}
				if got := rh.Instance().Current(); got != "complete" {
					t.Fatalf("recovered in %q, want complete", got)
				}
				if got := atomic.LoadInt64(&runs); got != 1 {
					t.Fatalf("Recover ran the actor %d times, want only the live run's once", got)
				}
				notes := rh.Instance().Entity().Notes
				if len(notes) != 1 {
					t.Fatalf("recovered notes %q, want the one the delivery settled", notes)
				}
				if notes[0] != "map[string]interface {}:map[ID:7]" {
					t.Fatalf("replay saw %s, want the map the older version's replay gave: "+
						"map[string]interface {}:map[ID:7]", describe(notes[0]))
				}
				if _, err := durable.StateAt(ctx, collectorMachine(), store, "collect-1", 0, palette, dc.opt); err != nil {
					t.Fatalf("StateAt: %v", err)
				}
			})
		}
	}
}

// TestJournalFormat_ThisVersionsActorEntry_CarriesTheType pins what a Record this
// version writes holds: the delivery's Record is of format 5, and its actor entry
// carries the done-data's Go type, so replay holds the decoder to it. An older
// version's entry, which the olderWriter's payload for the same delivery shows, has
// no type.
func TestJournalFormat_ThisVersionsActorEntry_CarriesTheType(t *testing.T) {
	ctx := context.Background()
	var runs int64
	behavior := outputChild(&runs, order{ID: 7})
	palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior})
	store := durable.NewMemStore(durable.WithHistory())
	h, err := durable.NewRunner(collectorMachine(), store, palette).Start(ctx, "collect-1", outcomeCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("collector", "supervising", 0))
	if !ok {
		t.Fatal("no actor ref for the spawned child")
	}
	if _, err = h.DeliverToActor(ctx, ref, "finish"); err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	_, recs, err := store.Load(ctx, "collect-1")
	if err != nil || len(recs) != 1 || recs[0].Format != 5 {
		t.Fatalf("the journal holds %+v (err %v), want one Record of format 5", recs, err)
	}
	typ, ok := recordedActorType(recs[0])
	if !ok || typ != "github.com/stablekernel/crucible/durable_test.order" {
		t.Fatalf("the actor entry's done-data type = %q, %v; want the actor output's own full name", typ, ok)
	}
}

// recordedActorType returns the Go type the first actor entry in rec says its
// done-data is, and whether the entry carries one.
func recordedActorType(rec durable.Record) (string, bool) {
	for _, e := range rec.Entries {
		if e.Kind != state.JournalActorMessage {
			continue
		}
		var mp struct {
			Type string `json:"type,omitempty"`
		}
		if err := json.Unmarshal(e.Payload, &mp); err != nil {
			return "", false
		}
		return mp.Type, mp.Type != ""
	}
	return "", false
}
