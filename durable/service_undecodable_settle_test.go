package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// outOfRangeResult is a service result whose MarshalJSON emits a number beyond
// float64, so its JSON form encodes but won't decode.
type outOfRangeResult struct{}

func (outOfRangeResult) MarshalJSON() ([]byte, error) { return []byte("1e400"), nil }

// undecodableProbe counts what a settle with a result that won't decode drives: the
// onDone and onError Assigns, with the type of the event each saw, the actor the
// onDone state spawns, and the domain effects dispatched.
type undecodableProbe struct {
	doneAssigns, errorAssigns int
	doneEventType             string
	errorMessage              string
	spawns                    int
	dispatched                int
}

// undecodableMachine invokes fetch in loading. Its onDone transition runs an Assign,
// emits a notify effect, and enters ready, which spawns child; its onError runs an
// Assign and enters failed. With assertString, the onDone Assign asserts its event is
// a string unchecked, as a reducer that trusts its service's result type does.
func undecodableMachine(fn state.ServiceFn[svcCtx], p *undecodableProbe, assertString bool) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("undecodable").
		Service("fetch", fn).
		Actor("child").
		Action("notify", func(state.ActionCtx[svcCtx]) (state.Effect, error) {
			return notifyEffect{To: "ops@example.com"}, nil
		}).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			p.doneAssigns++
			p.doneEventType = fmt.Sprintf("%T", in.Event)
			c := in.Entity
			if assertString {
				c.Notes = append(c.Notes, in.Event.(string))
			}
			return c
		}).
		Reducer("noteError", func(in state.AssignCtx[svcCtx]) svcCtx {
			p.errorAssigns++
			if e, ok := in.Event.(error); ok {
				p.errorMessage = e.Error()
			}
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done"), state.WithInvokeOnError("fail")).
		State("ready").InvokeActor("child").
		State("failed").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("ready").Assign("noteDone").Do("notify").
		Transition("loading").On("fail").GoTo("failed").Assign("noteError").
		Quench()
}

func undecodableOptions(fn state.ServiceFn[svcCtx], p *undecodableProbe) []durable.Option[string, string, svcCtx] {
	return []durable.Option[string, string, svcCtx]{
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithActorPalette[string, string, svcCtx](map[string]state.ActorBehavior{
			"child": func(map[string]any) (state.ActorInstance, error) {
				p.spawns++
				return &reentryGreeter{sent: true}, nil
			},
		}),
		durable.WithEffectHandler[string, string, svcCtx](func(context.Context, string, state.Effect) error {
			p.dispatched++
			return nil
		}),
	}
}

// TestRunService_UndecodableResult_ParksBeforeAnyAssign pins what a ServiceFn result
// whose JSON form won't decode does, and one with no JSON form at all. A result that
// encodes but won't decode is parked: RunService returns ErrEventDataEncoding, not
// ErrHandleFailed, before the onDone or onError Assign runs, so the onDone state spawns
// no actor and no effect dispatches. The journal holds one Record for the park, and
// each later RunService records nothing more. A recovered Handle has the service in
// flight and the result still parked, and its ServiceFn doesn't run again. A result
// with no JSON form fails the Handle with ErrHandleFailed, also before any Assign, and
// records nothing, so after Recover the ServiceFn runs again. A reducer that asserts
// its event's type unchecked never sees the result, so it can't panic on it.
func TestRunService_UndecodableResult_ParksBeforeAnyAssign(t *testing.T) {
	ctx := context.Background()
	id := state.InvokeID("undecodable", "loading", 0)
	start := func(t *testing.T, result any, assertString bool) (*undecodableProbe, *int, *state.Machine[string, string, svcCtx], *durable.MemStore, []durable.Option[string, string, svcCtx], *durable.Handle[string, string, svcCtx]) {
		t.Helper()
		calls := new(int)
		fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { *calls++; return result, nil }
		p := &undecodableProbe{}
		m := undecodableMachine(fn, p, assertString)
		st := durable.NewMemStore()
		opts := undecodableOptions(fn, p)
		h, err := durable.NewRunner(m, st, opts...).Start(ctx, "i1", svcCtx{}, state.WithInitialState("loading"))
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		return p, calls, m, st, opts, h
	}
	untouched := func(t *testing.T, p *undecodableProbe) {
		t.Helper()
		if p.doneAssigns != 0 || p.errorAssigns != 0 || p.spawns != 0 || p.dispatched != 0 {
			t.Fatalf("onDone Assign ran %d times, onError %d, child spawned %d, %d dispatched; want none of them",
				p.doneAssigns, p.errorAssigns, p.spawns, p.dispatched)
		}
	}
	journal := func(t *testing.T, st *durable.MemStore, want int) {
		t.Helper()
		if _, recs, err := st.Load(ctx, "i1"); err != nil || len(recs) != want {
			t.Fatalf("journal holds %d Records (err %v), want %d", len(recs), err, want)
		}
	}
	parked := map[string]any{
		"json.Number":        json.Number("1e400"),
		"custom MarshalJSON": outOfRangeResult{},
		"json.RawMessage":    json.RawMessage("1e400"),
	}
	for name, result := range parked {
		t.Run(name, func(t *testing.T) {
			p, calls, m, st, opts, h := start(t, result, false)
			_, _, err := h.RunService(ctx, id)
			if !errors.Is(err, durable.ErrEventDataEncoding) || errors.Is(err, durable.ErrHandleFailed) {
				t.Fatalf("RunService = %v, want ErrEventDataEncoding and not ErrHandleFailed", err)
			}
			untouched(t, p)
			journal(t, st, 1)
			if _, _, err = h.RunService(ctx, id); !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("second RunService = %v, want ErrEventDataEncoding", err)
			}
			journal(t, st, 1)
			recovered, err := durable.Recover(ctx, m, st, "i1", opts...)
			if err != nil {
				t.Fatalf("Recover: %v", err)
			}
			if got := recovered.Instance().Current(); got != "loading" {
				t.Fatalf("recovered in %q, want loading", got)
			}
			if _, _, err = recovered.RunService(ctx, id); !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("RunService after Recover = %v, want ErrEventDataEncoding", err)
			}
			untouched(t, p)
			if *calls != 1 {
				t.Fatalf("ServiceFn ran %d times, want once: a parked result doesn't run it again", *calls)
			}
		})
	}
	t.Run("won't encode", func(t *testing.T) {
		p, calls, m, st, opts, h := start(t, make(chan int), false)
		if _, _, err := h.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
			t.Fatalf("RunService = %v, want ErrHandleFailed", err)
		}
		untouched(t, p)
		journal(t, st, 0)
		recovered, err := durable.Recover(ctx, m, st, "i1", opts...)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		if _, _, err = recovered.RunService(ctx, id); !errors.Is(err, durable.ErrHandleFailed) {
			t.Fatalf("RunService after Recover = %v, want ErrHandleFailed again", err)
		}
		untouched(t, p)
		if *calls != 2 {
			t.Fatalf("ServiceFn ran %d times, want twice: nothing was recorded, so it runs again after Recover", *calls)
		}
	})
	t.Run("unchecked assertion never sees the result", func(t *testing.T) {
		p, _, _, st, _, h := start(t, json.Number("1e400"), true)
		var panicked any
		var err error
		func() {
			defer func() { panicked = recover() }()
			_, _, err = h.RunService(ctx, id)
		}()
		if panicked != nil || !errors.Is(err, durable.ErrEventDataEncoding) {
			t.Fatalf("RunService panicked %v, returned %v; want ErrEventDataEncoding and no panic", panicked, err)
		}
		untouched(t, p)
		if got := h.Instance().Current(); got != "loading" {
			t.Fatalf("live instance in %q, want loading", got)
		}
		journal(t, st, 1)
	})
}
