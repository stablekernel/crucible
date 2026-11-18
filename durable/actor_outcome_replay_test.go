package durable_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// This file pins issue #256: an actor's done data and its error must replay
// exactly as the live run delivered them, the way service results do since #235
// and #258. The parent's onDone / onError Assign folds what it saw — the payload's
// Go type and form — into the context, so a recovered instance's context can be
// compared against the live one: the same type live and after Recover, the same
// error message live and after Recover.

// order is a done-data value with a concrete Go type, the shape whose live/replay
// asymmetry the issue reports: live the parent's Assign sees an order, after
// Recover a map.
type order struct{ ID int }

// outcomeCtx folds each settled actor's routed outcome as the parent's Assign saw
// it: the payload's Go type and fmt form, an error's message quoted so a UTF-8
// replacement shows as an escape, so live and recovered contexts compare exactly.
type outcomeCtx struct {
	Notes []string `json:"notes"`
}

// collectorMachine is a single-actor parent whose onDone / onError Assigns record
// what the actor's outcome looked like from AssignCtx.Event: its Go type and value,
// so the recorded notes say whether the live run and a replay delivered the same
// type. For an error the note quotes the message, so invalid UTF-8 and its U+FFFD
// replacement differ visibly.
func collectorMachine() *state.Machine[string, string, outcomeCtx] {
	return state.ForgeFor[outcomeCtx]("collector").
		Reducer("recordDone", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			c := in.Entity
			c.Notes = append(c.Notes, fmt.Sprintf("%T:%v", in.Event, in.Event))
			return c
		}).
		Reducer("recordFail", func(in state.AssignCtx[outcomeCtx]) outcomeCtx {
			c := in.Entity
			if e, ok := in.Event.(error); ok {
				c.Notes = append(c.Notes, fmt.Sprintf("%T:%q", e, e.Error()))
			} else {
				c.Notes = append(c.Notes, fmt.Sprintf("%T:%v", in.Event, in.Event))
			}
			return c
		}).
		Actor("spawnChild").
		State("supervising").InvokeActor("spawnChild", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childFail")).
		State("complete").Final().
		State("failed").Final().
		Initial("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordDone").
		Transition("supervising").On("childFail").GoTo("failed").Assign("recordFail").
		Quench()
}

// outputChild builds an actor behavior whose completion output is output, a fixed
// Go value: the child is working -(finish)-> done(final), so the actor settles with
// output when delivered "finish".
func outputChild(runs *int64, output any) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		atomic.AddInt64(runs, 1)
		child := state.ForgeFor[outcomeCtx]("child").
			State("working").
			State("done").Final().
			Initial("working").
			Transition("working").On("finish").GoTo("done").
			Quench()
		inst := child.Cast(outcomeCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, func(*state.Instance[string, string, outcomeCtx]) any {
			return output
		}), nil
	}
}

// utf8FailChild builds an actor behavior that fails with an error whose message
// holds invalid UTF-8: the child's final state runs an action that returns such an
// error, so the child's Fire fails and the actor settles through onError carrying
// it.
func utf8FailChild(runs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		atomic.AddInt64(runs, 1)
		child := state.ForgeFor[outcomeCtx]("child").
			Action("boom", func(state.ActionCtx[outcomeCtx]) (state.Effect, error) {
				return nil, errors.New("boom-\xff")
			}).
			State("working").
			State("done").Final().OnEntry("boom").
			Initial("working").
			Transition("working").On("finish").GoTo("done").
			Quench()
		inst := child.Cast(outcomeCtx{}, state.WithInitialState[string]("working"))
		return state.NewActor(inst, nil), nil
	}
}

// panickingErr is an error whose Error method panics on a nil receiver: the shape
// a typed nil settles with.
type panickingErr struct{ msg string }

func (e *panickingErr) Error() string { return e.msg }

// panickingChild builds an actor behavior backed by a bespoke ActorInstance whose
// FireErr reports a typed nil (*panickingErr)(nil): the actor settles through
// onError carrying an error whose Error method panics, as a host's own
// ActorInstance can produce.
func panickingChild(runs *int64) state.ActorBehavior {
	return func(map[string]any) (state.ActorInstance, error) {
		atomic.AddInt64(runs, 1)
		return &panickingInstance{}, nil
	}
}

// panickingInstance is a bespoke ActorInstance whose every Fire fails with a typed
// nil error, so the ActorSystem settles it as a failure carrying an error whose
// Error method panics.
type panickingInstance struct{}

func (p *panickingInstance) DeliverFire(context.Context, any) (bool, any) { return true, nil }
func (p *panickingInstance) ChildEffects() []state.Effect                 { return nil }
func (p *panickingInstance) Output() any                                  { return nil }
func (p *panickingInstance) FireErr() error                               { return (*panickingErr)(nil) }

// liveOutcomeRun starts a collector instance, delivers "finish" to its spawned
// child (settling it through onDone or onError), and returns the notes the parent's
// Assigns recorded live and the store and id for recovery. A delivery that fails
// the Handle reports the error instead.
func liveOutcomeRun(t *testing.T, behavior state.ActorBehavior, opt durable.Option[string, string, outcomeCtx]) ([]string, durable.InstanceID, durable.Store, error) {
	t.Helper()
	ctx := context.Background()
	m := collectorMachine()
	store := durable.NewMemStore()
	id := durable.InstanceID("collect-1")

	opts := []durable.Option[string, string, outcomeCtx]{
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior}),
	}
	if opt != nil {
		opts = append(opts, opt)
	}
	h, err := durable.NewRunner(m, store, opts...).Start(ctx, id, outcomeCtx{}, state.WithInitialState("supervising"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	ref, ok := h.ActorRef(state.ActorID("collector", "supervising", 0))
	if !ok {
		t.Fatalf("no actor ref for spawned child")
	}
	if _, derr := h.DeliverToActor(ctx, ref, "finish"); derr != nil {
		return nil, id, store, derr
	}
	return h.Instance().Entity().Notes, id, store, nil
}

// recoveredNotes recovers the instance and returns the notes its Assigns recorded
// on replay.
func recoveredNotes(t *testing.T, behavior state.ActorBehavior, opt durable.Option[string, string, outcomeCtx], store durable.Store, id durable.InstanceID) []string {
	t.Helper()
	ctx := context.Background()
	opts := []durable.Option[string, string, outcomeCtx]{
		durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior}),
	}
	if opt != nil {
		opts = append(opts, opt)
	}
	h, err := durable.Recover(ctx, collectorMachine(), store, id, opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	return h.Instance().Entity().Notes
}

// TestActor_DoneData_SameTypeLiveAndAfterRecover pins the issue's first effect: an
// actor that finishes with a concrete Go value — an order, an int64 — delivers it
// through the onDone event's WithEventDataDecoder, so the parent's Assign sees the
// same type live and after Recover. With a decoder both see the registered type;
// without one both see JSON's generic form, as a service result does.
func TestActor_DoneData_SameTypeLiveAndAfterRecover(t *testing.T) {
	tests := []struct {
		name   string
		output any
		decode durable.EventDataDecoder[string]
		want   string
	}{
		{"an order with a decoder", order{ID: 7}, decodeAll[order], "durable_test.order:{7}"},
		{"an int64 with a decoder", int64(5), decodeAll[int64], "int64:5"},
		{"no decoder", order{ID: 7}, nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var runs int64
			behavior := outputChild(&runs, tt.output)
			var opt durable.Option[string, string, outcomeCtx]
			if tt.decode != nil {
				opt = durable.WithEventDataDecoder[string, string, outcomeCtx](tt.decode)
			}
			live, id, store, err := liveOutcomeRun(t, behavior, opt)
			if err != nil {
				t.Fatalf("DeliverToActor: %v", err)
			}
			if got := atomic.LoadInt64(&runs); got != 1 {
				t.Fatalf("live run instantiated the actor %d times, want 1", got)
			}
			rec := recoveredNotes(t, behavior, opt, store, id)
			if got := atomic.LoadInt64(&runs); got != 1 {
				t.Fatalf("recovery re-ran the actor: run count %d, want 1", got)
			}
			if len(live) != 1 || len(rec) != 1 {
				t.Fatalf("live notes %q, recovered notes %q, want one each", live, rec)
			}
			if live[0] != rec[0] {
				t.Fatalf("the parent's Assign saw different done data live and after Recover:\n live=%s\n  rec=%s", live[0], rec[0])
			}
			if tt.want != "" && rec[0] != tt.want {
				t.Fatalf("after Recover the parent's Assign saw %s, want %s", rec[0], tt.want)
			}
		})
	}
}

// TestActor_Error_InvalidUTF8_LiveEqualsRecover pins the issue's second effect: an
// actor error whose message holds invalid UTF-8 reaches the parent's onError Assign
// with the same message live and after Recover, normalized the way a Store keeps
// it, not U+FFFD-replaced on one path only.
func TestActor_Error_InvalidUTF8_LiveEqualsRecover(t *testing.T) {
	var runs int64
	behavior := utf8FailChild(&runs)
	live, id, store, err := liveOutcomeRun(t, behavior, nil)
	if err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	rec := recoveredNotes(t, behavior, nil, store, id)
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("recovery re-ran the failing actor: run count %d, want 1", got)
	}
	if len(live) != 1 || len(rec) != 1 {
		t.Fatalf("live notes %q, recovered notes %q, want one each", live, rec)
	}
	if live[0] != rec[0] {
		t.Fatalf("the parent's Assign saw different error messages live and after Recover:\n live=%s\n  rec=%s", live[0], rec[0])
	}
}

// TestActor_Error_APanickingErrorMethod_LiveEqualsRecover pins the issue's third
// effect: an actor error whose Error method panics — a typed nil — settles with a
// message saying so, normalized, and the parent's onError Assign sees the same
// message live and after Recover, rather than the panic failing the Handle.
func TestActor_Error_APanickingErrorMethod_LiveEqualsRecover(t *testing.T) {
	var runs int64
	behavior := panickingChild(&runs)
	live, id, store, err := liveOutcomeRun(t, behavior, nil)
	if err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	rec := recoveredNotes(t, behavior, nil, store, id)
	if got := atomic.LoadInt64(&runs); got != 1 {
		t.Fatalf("recovery re-ran the failing actor: run count %d, want 1", got)
	}
	if len(live) != 1 || len(rec) != 1 {
		t.Fatalf("live notes %q, recovered notes %q, want one each", live, rec)
	}
	if live[0] != rec[0] {
		t.Fatalf("the parent's Assign saw different error messages live and after Recover:\n live=%s\n  rec=%s", live[0], rec[0])
	}
}

// TestActor_DoneData_AWrongDecoderOnReplayNamesTheDoneData pins the wording of the
// error a Recover's decoder fails an actor's recorded done-data with, held to the
// recorded type: it names the done-data, as the live settle's errors do, not a
// "service result", so a host reading it knows which outcome wouldn't decode.
func TestActor_DoneData_AWrongDecoderOnReplayNamesTheDoneData(t *testing.T) {
	ctx := context.Background()
	var runs int64
	behavior := outputChild(&runs, order{ID: 7})
	_, id, store, err := liveOutcomeRun(t, behavior, childDoneDecoder(durable.DecodeAs[order]))
	if err != nil {
		t.Fatalf("DeliverToActor: %v", err)
	}
	palette := durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior})
	_, rerr := durable.Recover(ctx, collectorMachine(), store, id, palette, childDoneDecoder(durable.DecodeAs[scnAdmin]))
	if !errors.Is(rerr, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover with a decoder for another type = %v, want ErrEventDataEncoding", rerr)
	}
	if !strings.Contains(rerr.Error(), "actor done-data decodes as") {
		t.Fatalf("the replay error %q doesn't name the actor's done-data as the outcome that wouldn't decode", rerr)
	}
}

// endsDecoderFor is endsDecoder for a chosen decoder of childDone's done-data, so a
// test can hand kid's onDone a decoder that fails or returns another type.
func childDoneDecoder(d func([]byte) (any, error)) durable.Option[string, string, outcomeCtx] {
	return durable.WithEventDataDecoder[string, string, outcomeCtx](func(e string, data []byte) (any, error) {
		if e == "childDone" {
			return d(data)
		}
		return durable.DecodeAs[any](data)
	})
}

// TestActor_DoneDataThatWontDecode_IsNeverParked pins that an actor outcome has no
// parked form, as a service result that won't decode does: the settle aborts before it
// fires anything, so no Assign ran and the parent stays where it was; DeliverToActor
// fails the Handle with ErrHandleFailed wrapping ErrEventDataEncoding (a done-data
// with no JSON form fails it without the encoding error) and records nothing, so a
// Recover runs the actor again and the delivery settles once a decoder decodes it.
func TestActor_DoneDataThatWontDecode_IsNeverParked(t *testing.T) {
	cases := []struct {
		name     string
		opt      durable.Option[string, string, outcomeCtx] // the live run's decoder
		fixed    durable.Option[string, string, outcomeCtx] // the recovered run's
		output   any
		encoding bool // ErrHandleFailed wraps ErrEventDataEncoding
	}{
		{"the decoder fails", childDoneDecoder(func([]byte) (any, error) { return nil, errors.New("decoder refused") }), childDoneDecoder(durable.DecodeAs[order]), order{ID: 7}, true},
		{"the decoder returns another type", childDoneDecoder(durable.DecodeAs[scnAdmin]), childDoneDecoder(durable.DecodeAs[order]), order{ID: 7}, true},
		{"the output has no JSON form", nil, nil, make(chan int), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var runs int64
			behavior := outputChild(&runs, tc.output)
			store := durable.NewMemStore()
			id := durable.InstanceID("collect-1")
			runOpts := func(o durable.Option[string, string, outcomeCtx]) []durable.Option[string, string, outcomeCtx] {
				opts := []durable.Option[string, string, outcomeCtx]{
					durable.WithActorPalette[string, string, outcomeCtx](map[string]state.ActorBehavior{"spawnChild": behavior}),
				}
				if o != nil {
					opts = append(opts, o)
				}
				return opts
			}
			h, err := durable.NewRunner(collectorMachine(), store, runOpts(tc.opt)...).
				Start(ctx, id, outcomeCtx{}, state.WithInitialState("supervising"))
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			ref, ok := h.ActorRef(state.ActorID("collector", "supervising", 0))
			if !ok {
				t.Fatal("no actor ref for the spawned child")
			}
			_, derr := h.DeliverToActor(ctx, ref, "finish")
			if !errors.Is(derr, durable.ErrHandleFailed) {
				t.Fatalf("DeliverToActor = %v, want ErrHandleFailed: the settle aborted and nothing was recorded", derr)
			}
			if got := errors.Is(derr, durable.ErrEventDataEncoding); got != tc.encoding {
				t.Fatalf("errors.Is(%v, ErrEventDataEncoding) = %v, want %v", derr, got, tc.encoding)
			}
			if got := h.Instance().Current(); got != "supervising" {
				t.Fatalf("the parent reached %q, want supervising: the aborted settle fired nothing", got)
			}
			if notes := h.Instance().Entity().Notes; len(notes) != 0 {
				t.Fatalf("the aborted settle ran Assigns: %q", notes)
			}
			if _, recs, err := store.Load(ctx, id); err != nil || len(recs) != 0 {
				t.Fatalf("the journal holds %d Records (err %v), want none: nothing was recorded", len(recs), err)
			}

			// A recovered Handle runs the actor again, and the delivery settles once its
			// done-data decodes. A done-data with no JSON form aborts again, as it must.
			rh, err := durable.Recover(ctx, collectorMachine(), store, id, runOpts(tc.fixed)...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := atomic.LoadInt64(&runs); got != 2 {
				t.Fatalf("kid instantiated %d times, want 2: Recover ran it again", got)
			}
			ref, ok = rh.ActorRef(state.ActorID("collector", "supervising", 0))
			if !ok {
				t.Fatal("no actor ref on the recovered Handle")
			}
			_, derr = rh.DeliverToActor(ctx, ref, "finish")
			if tc.fixed == nil {
				if !errors.Is(derr, durable.ErrHandleFailed) {
					t.Fatalf("the redelivered unencodable done-data = %v, want ErrHandleFailed again", derr)
				}
				if _, recs, err := store.Load(ctx, id); err != nil || len(recs) != 0 {
					t.Fatalf("the journal holds %d Records (err %v), want still none", len(recs), err)
				}
				return
			}
			if derr != nil {
				t.Fatalf("the redelivered done-data with a fixed decoder: %v", derr)
			}
			notes := rh.Instance().Entity().Notes
			want := "durable_test.order:{7}"
			if len(notes) != 1 || notes[0] != want {
				t.Fatalf("the settled redelivery's notes = %q, want [%s]", notes, want)
			}
		})
	}
}
