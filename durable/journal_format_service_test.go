package durable_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// olderWriter is a MemStore that stamps every Record and checkpoint it keeps with
// format, as a version that wrote that journal format did. The scnUser result these
// tests record needs no rewrite: an older version journaled json.Marshal of the value
// the ServiceFn returned, and for a scnUser those are the bytes this version records.
type olderWriter struct {
	*durable.MemStore
	format int
}

func (s *olderWriter) Append(ctx context.Context, id durable.InstanceID, rec durable.Record, opts ...durable.AppendOption) (int64, error) {
	rec.Format = s.format
	return s.MemStore.Append(ctx, id, rec, opts...)
}

func (s *olderWriter) Checkpoint(ctx context.Context, id durable.InstanceID, snapshot []byte, throughStep int, opts ...durable.CheckpointOption) error {
	var env map[string]json.RawMessage
	if err := json.Unmarshal(snapshot, &env); err != nil {
		return err
	}
	delete(env, "format")
	if s.format > 0 {
		env["format"] = json.RawMessage([]byte{byte('0' + s.format)})
	}
	b, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.MemStore.Checkpoint(ctx, id, b, throughStep, opts...)
}

// TestJournalFormat_OlderServiceResult_ReplaysAsItDid: a service result an older
// version journaled, at format 0 (the 0.1.0 release), 1, 2 or 3 (each before results
// were decoded through WithEventDataDecoder), replays as that version replayed it: in
// JSON's generic form, through no decoder and no type check. Formats 2 and 3 are the
// ones the versions just before this one write, so this version can't take either for
// its own. A Runner whose decoder covers onDone, and fails, returns another type or
// returns the service's own type, still recovers it, and the replayed Assign sees the
// map the older version's replay gave it. Records this version writes, at format 4,
// decode through the decoder (see
// TestServiceResult_RecordedResultThatWontDecode_RunsNoAssignOnReplay).
func TestJournalFormat_OlderServiceResult_ReplaysAsItDid(t *testing.T) {
	decoders := []struct {
		name string
		opt  durable.Option[string, string, svcCtx]
	}{
		{"decoder fails", failingDecoder()},
		{"decoder returns another type", decoderFor("done", durable.DecodeAs[scnAdmin])},
		{"decoder returns the service's type", decoderFor("done", durable.DecodeAs[scnUser])},
	}
	for _, format := range []int{0, 1, 2, 3} {
		for _, dc := range decoders {
			t.Run(dc.name+", format "+string(rune('0'+format)), func(t *testing.T) {
				ctx := context.Background()
				fn := returning(scnUser{ID: 7, Name: "ann"}, nil)
				a := &scnAssigns{}
				m := scnMachine(fn, a)
				store := &olderWriter{MemStore: durable.NewMemStore(durable.WithHistory()), format: format}
				reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
				h := scnStart(t, m, store, reg)
				mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
				_, recs, err := store.Load(ctx, "scn-1")
				if err != nil || len(recs) != 1 || recs[0].Format != format {
					t.Fatalf("the journal holds %+v (err %v), want one Record of format %d", recs, err, format)
				}

				rh, err := durable.Recover(ctx, m, store, "scn-1", reg, dc.opt)
				if err != nil {
					t.Fatalf("Recover: %v; the older version recovered this journal", err)
				}
				if got := rh.Instance().Current(); got != "ready" {
					t.Fatalf("recovered in %q, want ready", got)
				}
				if a.done != 2 {
					t.Fatalf("onDone's Assign ran %d times, want 2 (live, replay)", a.done)
				}
				if _, ok := a.seen[1].(map[string]any); !ok {
					t.Fatalf("replay saw %s, want the map[string]any the older version's replay gave", describe(a.seen[1]))
				}
				if _, err := durable.StateAt(ctx, m, store, "scn-1", 0, reg, dc.opt); err != nil {
					t.Fatalf("StateAt: %v", err)
				}
			})
		}
	}
}
