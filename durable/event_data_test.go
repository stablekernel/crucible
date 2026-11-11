package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

func TestRecover_FireWithEventData_FileStoreAndCheckpointTail(t *testing.T) {
	ctx := context.Background()
	m := quantityMachine()
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("qty-1")
	runner := durable.NewRunner(m, st, durable.WithCheckpointEvery[string, string, *runCtx](2))
	h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	for _, q := range []float64{3, 4, 5} { // the checkpoint lands on the second
		if _, err = h.Fire(ctx, "add", state.WithEventData(q)); err != nil {
			t.Fatalf("Fire: %v", err)
		}
	}
	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	recovered, err := durable.Recover(ctx, m, reopened, id)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if live, got := handleSnapshot(t, h), handleSnapshot(t, recovered); got != live {
		t.Fatalf("recovered instance differs from live:\n live: %s\n got:  %s", live, got)
	}
}

func TestStateAt_ReplaysEventData(t *testing.T) {
	ctx := context.Background()
	m := quantityMachine()
	store := durable.NewMemStore(durable.WithHistory())
	const id = durable.InstanceID("qty-1")
	h, err := durable.NewRunner(m, store).Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "add", state.WithEventData(3.0)); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	view, err := durable.StateAt(ctx, m, store, id, 0)
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	got, err := state.MarshalSnapshot(view.Snapshot())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if live := handleSnapshot(t, h); string(got) != live {
		t.Fatalf("view differs from live:\n live: %s\n got:  %s", live, got)
	}
}

func TestFire_EventDataWithNoJSONForm_RefusedBeforeTheKernelRuns(t *testing.T) {
	ctx := context.Background()
	m := quantityMachine()
	store := durable.NewMemStore()
	const id = durable.InstanceID("qty-1")
	h, err := durable.NewRunner(m, store).Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	before := handleSnapshot(t, h)
	if _, err = h.Fire(ctx, "add", state.WithEventData(make(chan int))); err == nil {
		t.Fatal("Fire with an unencodable payload succeeded")
	}
	if got := handleSnapshot(t, h); got != before {
		t.Fatalf("refused Fire moved the instance:\n before: %s\n after:  %s", before, got)
	}
	if n := tailLen(t, store, id); n != 0 {
		t.Fatalf("records = %d, want 0", n)
	}
}

func TestFire_NilEventData_ReachesReplay(t *testing.T) {
	ctx := context.Background()
	var seen []any
	m := state.ForgeFor[*runCtx]("nil-data").
		Reducer("note", func(in state.AssignCtx[*runCtx]) *runCtx {
			seen = append(seen, in.Event)
			return in.Entity
		}).
		State("open").
		Initial("open").
		Transition("open").On("add").GoTo("open").Assign("note").
		Quench()
	store := durable.NewMemStore()
	const id = durable.InstanceID("nil-1")
	h, err := durable.NewRunner(m, store).Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "add", state.WithEventData(nil)); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if _, err = durable.Recover(ctx, m, store, id); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(seen) != 2 || seen[0] != nil || seen[1] != nil {
		t.Fatalf("assign saw %v, want nil live and nil on replay", seen)
	}
}

// A Fire with no payload keeps today's on-disk Record, so existing journals and
// their digests don't change shape.
func TestRecord_NoEventData_OmittedFromJSON(t *testing.T) {
	b, err := json.Marshal(durable.Record{Step: 1})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(b), "EventData") {
		t.Fatalf("record JSON carries EventData: %s", b)
	}
}

// payloadMachine records what each Fire's Assign saw, so a test can compare the live
// run with a replay.
func payloadMachine(seen *[]any) *state.Machine[string, string, *runCtx] {
	return state.ForgeFor[*runCtx]("payload").
		Reducer("note", func(in state.AssignCtx[*runCtx]) *runCtx {
			*seen = append(*seen, in.Event)
			return in.Entity
		}).
		State("open").
		Initial("open").
		Transition("open").On("add").GoTo("open").Assign("note").
		Quench()
}

// TestFire_EventData_LiveEqualsReplay proves a payload reaches Assign as the same
// value and type live and after Recover: decoded by the Runner's EventDataDecoder, or
// in JSON's own form by default.
func TestFire_EventData_LiveEqualsReplay(t *testing.T) {
	type order struct{ Qty int }
	tests := []struct {
		name    string
		payload any
		decode  durable.EventDataDecoder[string]
		want    string
	}{
		{"an int", 3, decodeAll[int], "int:3"},
		{"a large int keeps its precision", int64(1<<53 + 1), decodeAll[int64], "int64:9007199254740993"},
		{"a struct", order{Qty: 2}, decodeAll[order], "durable_test.order:{2}"},
		{"a map by default", map[string]any{"qty": 2.0}, nil, "map[string]interface {}:map[qty:2]"},
		{"a nil payload stays nil under a typed decoder", nil, decodeAll[int], "<nil>:<nil>"},
		{"an error", errors.New("card declined"), nil, "*errors.errorString:card declined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var seen []any
			m := payloadMachine(&seen)
			store := durable.NewMemStore()
			const id = durable.InstanceID("p-1")
			opt := durable.WithEventDataDecoder[string, string, *runCtx](tt.decode)
			h, err := durable.NewRunner(m, store, opt).Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "add", state.WithEventData(tt.payload)); err != nil {
				t.Fatalf("Fire: %v", err)
			}
			if _, err = durable.Recover(ctx, m, store, id, opt); err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if len(seen) != 2 {
				t.Fatalf("assign ran %d times, want 2", len(seen))
			}
			for i, v := range seen {
				if got := fmt.Sprintf("%T:%v", v, v); got != tt.want {
					t.Fatalf("assign %d saw %s, want %s", i, got, tt.want)
				}
			}
		})
	}
}

// decodeAll decodes every event's payload as a T.
func decodeAll[T any](_ string, data []byte) (any, error) { return durable.DecodeAs[T](data) }

// TestFire_EventData_DecodedAsAnotherType_IsRefused proves a payload the decoder
// returns as a different type than the caller passed refuses the Fire before the
// kernel runs, so Assign never sees a map where the caller passed a struct.
func TestFire_EventData_DecodedAsAnotherType_IsRefused(t *testing.T) {
	type order struct{ Qty int }
	tests := []struct {
		name    string
		payload any
		decode  durable.EventDataDecoder[string]
	}{
		{"a struct by default", order{Qty: 2}, nil},
		{"an int by default", 3, nil},
		{"a struct the decoder reads as a pointer", order{Qty: 2}, decodeAll[*order]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var seen []any
			store := durable.NewMemStore()
			h, err := durable.NewRunner(payloadMachine(&seen), store, durable.WithEventDataDecoder[string, string, *runCtx](tt.decode)).
				Start(ctx, "p-1", &runCtx{}, state.WithInitialState("open"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			_, err = h.Fire(ctx, "add", state.WithEventData(tt.payload))
			if !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("Fire err = %v, want ErrEventDataEncoding", err)
			}
			if len(seen) != 0 {
				t.Fatalf("assign ran %d times, want 0", len(seen))
			}
			if _, tail, err := store.Load(ctx, "p-1"); err != nil || len(tail) != 0 {
				t.Fatalf("Load: tail %d, err %v; want nothing recorded", len(tail), err)
			}
		})
	}
}

// TestFire_EventData_DecoderGetsTheEvent proves the decoder is called with the event
// the payload rode with, live and on replay.
func TestFire_EventData_DecoderGetsTheEvent(t *testing.T) {
	ctx := context.Background()
	var seen []any
	var events []string
	decode := func(ev string, data []byte) (any, error) {
		events = append(events, ev)
		return durable.DecodeAs[int](data)
	}
	m := payloadMachine(&seen)
	store := durable.NewMemStore()
	opt := durable.WithEventDataDecoder[string, string, *runCtx](decode)
	h, err := durable.NewRunner(m, store, opt).Start(ctx, "p-1", &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "add", state.WithEventData(7)); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if _, err = durable.Recover(ctx, m, store, "p-1", opt); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if fmt.Sprint(events) != "[add add]" {
		t.Fatalf("decoder saw events %v, want [add add]", events)
	}
}

// TestFire_NoEventData_AssignSeesTheEvent proves a Fire with no payload keeps the
// event in AssignCtx.Event.
func TestFire_NoEventData_AssignSeesTheEvent(t *testing.T) {
	ctx := context.Background()
	var seen []any
	h, err := durable.NewRunner(payloadMachine(&seen), durable.NewMemStore()).
		Start(ctx, "p-1", &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "add"); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if len(seen) != 1 || seen[0] != "add" {
		t.Fatalf("assign saw %v, want the event", seen)
	}
}

// panicMarshaler panics while encoding.
type panicMarshaler struct{}

func (panicMarshaler) MarshalJSON() ([]byte, error) { panic("boom") }

// nilError panics on a nil receiver, as an error with a pointer receiver can.
type nilError struct{ msg string }

func (e *nilError) Error() string { return e.msg }

// TestFire_ErrorEventData_InvalidUTF8_LiveEqualsReplay proves an error message reads
// the same live and from a reopened FileStore, whose JSON replaces invalid UTF-8.
func TestFire_ErrorEventData_InvalidUTF8_LiveEqualsReplay(t *testing.T) {
	ctx := context.Background()
	var seen []any
	m := payloadMachine(&seen)
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("p-1")
	h, err := durable.NewRunner(m, st).Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "add", state.WithEventData(errors.New("bad\xffbyte"))); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, err = durable.Recover(ctx, m, reopened, id); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(seen) != 2 || seen[0].(error).Error() != seen[1].(error).Error() {
		t.Fatalf("live and replay saw %q", seen)
	}
}

// TestFire_EventData_Refused proves a payload that can't be recorded refuses the Fire
// before the kernel runs, and the Handle stays usable.
func TestFire_EventData_Refused(t *testing.T) {
	tooBig, ok := new(big.Int).SetString("1"+strings.Repeat("0", 400), 10)
	if !ok {
		t.Fatal("SetString")
	}
	tests := []struct {
		name    string
		payload any
	}{
		{"no JSON form", make(chan int)},
		{"a number no float64 holds", tooBig},
		{"over the limit", strings.Repeat("x", 100)},
		{"an error over the limit", errors.New(strings.Repeat("x", 100))},
		{"an error over the limit once encoded", errors.New(strings.Repeat("<", 20))},
		{"a typed-nil error", (*nilError)(nil)},
		{"a panicking marshaler", panicMarshaler{}},
		{"a nil embedded marshaler", struct{ *time.Time }{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			var seen []any
			store := durable.NewMemStore()
			runner := durable.NewRunner(payloadMachine(&seen), store,
				durable.WithMaxEventDataBytes[string, string, *runCtx](64))
			const id = durable.InstanceID("p-1")
			h, err := runner.Start(ctx, id, &runCtx{}, state.WithInitialState("open"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err = h.Fire(ctx, "add", state.WithEventData(tt.payload)); !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("Fire = %v, want ErrEventDataEncoding", err)
			}
			if len(seen) != 0 {
				t.Fatalf("assign ran on a refused Fire: %v", seen)
			}
			if _, err = h.Fire(ctx, "add", state.WithEventData(1.0)); err != nil {
				t.Fatalf("Fire after the refusal: %v", err)
			}
		})
	}
}

// TestFire_EventData_LeavesTheCallersOptionsAlone proves the decoded option doesn't
// land in spare capacity of the caller's slice.
func TestFire_EventData_LeavesTheCallersOptionsAlone(t *testing.T) {
	ctx := context.Background()
	var seen []any
	h, err := durable.NewRunner(payloadMachine(&seen), durable.NewMemStore()).
		Start(ctx, "p-1", &runCtx{}, state.WithInitialState("open"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	opts := make([]state.FireOption, 1, 2)
	opts[0] = state.WithEventData("one")
	if _, err = h.Fire(ctx, "add", opts...); err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if spare := opts[:2][1]; spare != nil {
		t.Fatal("Fire wrote into the caller's slice")
	}
}

// TestFileStore_RecordOverTheLineLimit_IsRefused proves a Record Load couldn't read back
// is refused, leaving the instance readable.
func TestFileStore_RecordOverTheLineLimit_IsRefused(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("p-1")
	if _, err = st.Append(ctx, id, durable.Record{Step: 0}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	big := durable.Record{Step: 1, EventData: make([]byte, 13<<20)}
	if _, err = st.Append(ctx, id, big); !errors.Is(err, durable.ErrRecordTooLarge) {
		t.Fatalf("Append = %v, want ErrRecordTooLarge", err)
	}
	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, tail, err := reopened.Load(ctx, id); err != nil || len(tail) != 1 {
		t.Fatalf("Load = (%d records, %v), want (1, nil)", len(tail), err)
	}
}

// TestFileStore_LargeRecordWithSnapshot_StaysReadable proves a Record over the line
// limit only because of its Snapshot is kept without it, so Load still works if the
// checkpoint never lands.
func TestFileStore_LargeRecordWithSnapshot_StaysReadable(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("p-1")
	rec := durable.Record{Step: 0, Event: []byte(`"go"`), Snapshot: make([]byte, 13<<20)}
	if _, err = st.Append(ctx, id, rec); err != nil {
		t.Fatalf("Append: %v", err)
	}
	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_, tail, err := reopened.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(tail) != 1 || string(tail[0].Event) != `"go"` || len(tail[0].Snapshot) != 0 {
		t.Fatalf("tail = %d records, want the step without its Snapshot", len(tail))
	}
	if _, err = reopened.Append(ctx, id, rec); err != nil {
		t.Fatalf("retry of the same Record: %v", err)
	}
}

// TestFileStore_LargeSnapshotRecord_SurvivesCompaction proves compaction doesn't rewrite
// the Snapshot an oversized Record dropped, which would leave a line Load can't read.
func TestFileStore_LargeSnapshotRecord_SurvivesCompaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	const id = durable.InstanceID("p-1")
	for step := range 4 {
		if _, err = st.Append(ctx, id, durable.Record{Step: step}); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if _, err = st.Append(ctx, id, durable.Record{Step: 4, Snapshot: make([]byte, 13<<20)}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err = st.Checkpoint(ctx, id, []byte("{}"), 2); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	reopened, err := durable.NewFileStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if _, tail, err := reopened.Load(ctx, id); err != nil || len(tail) != 2 {
		t.Fatalf("Load = (%d records, %v), want (2, nil)", len(tail), err)
	}
}
