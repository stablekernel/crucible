package state_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// These tests pin the load half of the declared-outcome flags: a machine or
// effect serialized by an older producer — before Invocation.HasOnDone /
// HasOnError and SpawnActor.HasDone / HasError existed — never wrote the flags,
// but it also never left an outcome event set without declaring it, so a load
// back-fills the flag for an outcome that is present and non-zero and a reloaded
// definition keeps the outcomes it declared. A new producer's serialization
// round-trips byte-identically still, and an unset outcome — the zero event —
// stays unrouted.

// oldInvokeReg is the host palette for oldInvokeParent's reducers, so a test can
// rehydrate the machine through Provide with its Assigns bound.
func oldInvokeReg() *state.Registry[*trec] {
	return state.NewRegistry[*trec]().
		Reducer("recordDone", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if o, ok := in.Event.(string); ok {
				e.notes = append(e.notes, "result:"+o)
			}
			return e
		}).
		Reducer("recordFail", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if err, ok := in.Event.(error); ok {
				e.notes = append(e.notes, "error:"+err.Error())
			}
			return e
		})
}

// oldInvokeParent builds a parent whose supervising state invokes a child-machine
// actor with the given outcome options. Its transition on the zero event ("")
// catches any misroute of an unset outcome: the pre-flags kernel fired the zero
// event for an outcome nothing declared, and a load that back-fills too eagerly
// would bring that back.
func oldInvokeParent(name string, opts ...state.InvokeOption) *state.Machine[string, string, *trec] {
	return state.ForgeFor[*trec](name).
		Reducer("recordDone", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if o, ok := in.Event.(string); ok {
				e.notes = append(e.notes, "result:"+o)
			}
			return e
		}).
		Reducer("recordFail", func(in state.AssignCtx[*trec]) *trec {
			e := in.Entity
			if err, ok := in.Event.(error); ok {
				e.notes = append(e.notes, "error:"+err.Error())
			}
			return e
		}).
		State("idle").
		State("supervising").InvokeActor("child", opts...).
		State("complete").
		State("failed").
		State("oops").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").Assign("recordDone").
		Transition("supervising").On("childErr").GoTo("failed").Assign("recordFail").
		Transition("supervising").On("").GoTo("oops").
		Quench()
}

// stripOutcomeFlags ages a serialized machine to what an older producer wrote,
// removing the hasOnDone / hasOnError keys that producer never emitted. It fails
// the test when there is nothing to strip, so a machine that stopped serializing
// the flags can't silently pass as an aged one.
func stripOutcomeFlags(t *testing.T, raw []byte) []byte {
	t.Helper()
	if !strings.Contains(string(raw), `"hasOnDone":true`) && !strings.Contains(string(raw), `"hasOnError":true`) {
		t.Fatal("the serialized machine carried no outcome flags to strip")
	}
	aged := strings.ReplaceAll(string(raw), `"hasOnDone":true,`, "")
	aged = strings.ReplaceAll(aged, `,"hasOnDone":true`, "")
	aged = strings.ReplaceAll(aged, `"hasOnError":true,`, "")
	aged = strings.ReplaceAll(aged, `,"hasOnError":true`, "")
	if strings.Contains(aged, "hasOnDone") || strings.Contains(aged, "hasOnError") {
		t.Fatalf("the flag strip left keys behind: %s", aged)
	}
	return []byte(aged)
}

// TestInvoke_OldJSONKeepsADeclaredOnDone proves the back-fill on load: a machine
// serialized by an older producer — one that wrote the declared onDone / onError
// events but no flags — reloads with those outcomes still declared, so the
// invoked actor's completion routes through onDone. Without the back-fill the
// flags decode as unset and the child's success settles as the fire-and-forget
// no-op an outcome nothing declared is, losing the declared routing.
func TestInvoke_OldJSONKeepsADeclaredOnDone(t *testing.T) {
	ctx := context.Background()
	m := oldInvokeParent("agedone", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr"))
	raw, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	aged := stripOutcomeFlags(t, raw)

	ir, err := state.LoadFromJSON[string, string, *trec](aged)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(oldInvokeReg()).Quench()
	parent := m2.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", settleChildBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID(m2.Name(), "supervising", 0)
	if !sys.DeliverByID(ctx, id, "finish") {
		t.Fatal("DeliverByID returned false; the actor was running")
	}
	if parent.Current() != "complete" {
		t.Fatalf("parent = %q, want complete: the older machine's JSON lost its declared onDone", parent.Current())
	}
	if got := strings.Join(parent.Entity().notes, ","); got != "result:payload" {
		t.Fatalf("notes = %q, want the declared onDone carrying the child's payload", got)
	}
}

// TestInvoke_OldJSONWithNoOnDoneStaysUnrouted pins the other side of the
// back-fill: an older machine that declared no outcome wrote the zero events (an
// unset outcome leaves OnDone / OnError at the zero event, which the tags emit),
// and the zero event back-fills nothing, so the reloaded invoke stays unrouted —
// the child's success settles as the fire-and-forget no-op, the zero event never
// fires, and nothing escalates.
func TestInvoke_OldJSONWithNoOnDoneStaysUnrouted(t *testing.T) {
	ctx := context.Background()
	m := oldInvokeParent("agednone")
	raw, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	// A machine that declares neither outcome emits no flag keys, so its bytes
	// are already the older producer's shape.
	if strings.Contains(string(raw), "hasOnDone") || strings.Contains(string(raw), "hasOnError") {
		t.Fatalf("a machine declaring neither outcome serialized flags: %s", raw)
	}

	ir, err := state.LoadFromJSON[string, string, *trec](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(oldInvokeReg()).Quench()
	parent := m2.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", settleChildBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID(m2.Name(), "supervising", 0)
	if !sys.DeliverByID(ctx, id, "finish") {
		t.Fatal("DeliverByID returned false; the actor was running")
	}
	if parent.Current() != "supervising" {
		t.Fatalf("parent = %q, want supervising: an unset outcome must route nothing (and never the zero event)", parent.Current())
	}
	if sys.IsRunning(id) || sys.Running() != 0 {
		t.Fatalf("the unrouted completion left the actor running: IsRunning=%v Running=%d", sys.IsRunning(id), sys.Running())
	}
	if esc := sys.LastEscalation(); esc != nil {
		t.Fatalf("an unrouted success escalated: %+v", esc)
	}
}

// TestInvoke_FlagRoundTripStaysByteIdentical pins the forward half of the
// back-fill: a new producer's serialization round-trips byte-identically through
// ToJSON -> LoadFromJSON -> Provide -> Quench -> ToJSON, declared outcomes and
// unset ones alike, so the flags add no round-trip drift of their own.
func TestInvoke_FlagRoundTripStaysByteIdentical(t *testing.T) {
	cases := []struct {
		name string
		opts []state.InvokeOption
	}{
		{"both outcomes", []state.InvokeOption{state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")}},
		{"onDone only", []state.InvokeOption{state.WithInvokeOnDone("childDone")}},
		{"onError only", []state.InvokeOption{state.WithInvokeOnError("childErr")}},
		{"neither", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := oldInvokeParent("rtflags", tc.opts...)
			raw1, err := m.ToJSON(state.WithoutSrcPos())
			if err != nil {
				t.Fatalf("ToJSON err = %v", err)
			}
			ir, err := state.LoadFromJSON[string, string, *trec](raw1)
			if err != nil {
				t.Fatalf("LoadFromJSON err = %v", err)
			}
			m2 := ir.Provide(oldInvokeReg()).Quench()
			raw2, err := m2.ToJSON(state.WithoutSrcPos())
			if err != nil {
				t.Fatalf("ToJSON (round 2) err = %v", err)
			}
			if string(raw1) != string(raw2) {
				t.Fatalf("round-trip diverged:\n%s\n--- vs ---\n%s", raw1, raw2)
			}
		})
	}
}

// TestSpawnActor_OldEffectJSONKeepsADeclaredOnDone proves the same back-fill for
// the SpawnActor effect payload: an envelope an older producer journaled carries
// the declared outcome events and no flags, and decoding it back-fills HasDone /
// HasError, so a host replaying that journal routes the spawned actor's
// completion through the onDone its spawn declared. The decoded spawn is driven
// end-to-end: absorbed into a system, its child completed, the parent routed.
func TestSpawnActor_OldEffectJSONKeepsADeclaredOnDone(t *testing.T) {
	ctx := context.Background()
	reg := state.NewEffectRegistry()
	env := state.EffectEnvelope{
		Kind:    state.EffectKindSpawnActor,
		Payload: json.RawMessage(`{"id":"aged","src":{"name":"child"},"onDone":"childDone","onError":"childErr","state":"supervising"}`),
	}
	eff, err := reg.Unmarshal(env)
	if err != nil {
		t.Fatalf("registry Unmarshal err = %v", err)
	}
	sp, ok := eff.(*state.SpawnActor)
	if !ok {
		t.Fatalf("decoded %T, want *SpawnActor", eff)
	}
	if !sp.HasDone || !sp.HasError {
		t.Fatalf("decoded SpawnActor = %+v, want the declared outcomes back-filled into HasDone and HasError", *sp)
	}

	m := oldInvokeParent("agedspawn", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr"))
	parent := m.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", settleChildBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	sys.Absorb(ctx, []state.Effect{*sp})
	if !sys.DeliverByID(ctx, "aged", "finish") {
		t.Fatal("DeliverByID returned false; the aged actor was running")
	}
	if parent.Current() != "complete" {
		t.Fatalf("parent = %q, want complete: the older effect's JSON lost its declared onDone", parent.Current())
	}
	if got := strings.Join(parent.Entity().notes, ","); got != "result:payload" {
		t.Fatalf("notes = %q, want the declared onDone carrying the child's payload", got)
	}
}

// TestInvoke_ZeroValuedOnErrorRoundTripsAsDeclared pins #251 at the IR level: an
// onError deliberately declared as the zero event — the first constant of an iota
// enum is the everyday case — serializes with its hasOnError flag set and reloads
// as declared, distinct from an invocation that declared none, whose flag stays
// unset however the zero event reads on the wire. The distinction holds on the
// wire and in behavior: the reloaded declared-zero onError routes the failure
// through the transition keyed on the zero event, while a reload that declared
// none escalates instead.
func TestInvoke_ZeroValuedOnErrorRoundTripsAsDeclared(t *testing.T) {
	ctx := context.Background()
	m := oldInvokeParent("zeroerr", state.WithInvokeOnError(""))
	raw1, err := m.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON err = %v", err)
	}
	if !strings.Contains(string(raw1), `"hasOnError":true`) {
		t.Fatalf("the serialized machine lost the declared zero-valued onError: %s", raw1)
	}
	ir, err := state.LoadFromJSON[string, string, *trec](raw1)
	if err != nil {
		t.Fatalf("LoadFromJSON err = %v", err)
	}
	m2 := ir.Provide(oldInvokeReg()).Quench()
	raw2, err := m2.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON (round 2) err = %v", err)
	}
	if string(raw1) != string(raw2) {
		t.Fatalf("round-trip diverged:\n%s\n--- vs ---\n%s", raw1, raw2)
	}

	// The reloaded declaration still routes: a child that cannot start fails through
	// the declared zero event, which the parent's transition on "" catches.
	parent := m2.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sys := state.NewActorSystem(parent).Register("child", failingChildBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if parent.Current() != "oops" {
		t.Fatalf("parent = %q, want oops: the reloaded zero-valued onError stopped routing", parent.Current())
	}
	if esc := sys.LastEscalation(); esc != nil {
		t.Fatalf("a declared zero-valued onError escalated: %+v", esc)
	}

	// The control: an invocation that declared no onError escalates the same
	// failure instead, so the two remain distinct after a round trip.
	m3 := oldInvokeParent("zeroerr-none")
	rawNone, err := m3.ToJSON(state.WithoutSrcPos())
	if err != nil {
		t.Fatalf("ToJSON (none) err = %v", err)
	}
	if strings.Contains(string(rawNone), "hasOnError") {
		t.Fatalf("a machine declaring no onError serialized the flag: %s", rawNone)
	}
	irNone, err := state.LoadFromJSON[string, string, *trec](rawNone)
	if err != nil {
		t.Fatalf("LoadFromJSON (none) err = %v", err)
	}
	m4 := irNone.Provide(oldInvokeReg()).Quench()
	parentNone := m4.Cast(&trec{}, state.WithInitialState[string]("idle"))
	sysNone := state.NewActorSystem(parentNone).Register("child", failingChildBehavior())
	resNone := parentNone.Fire(ctx, "start")
	sysNone.Absorb(ctx, resNone.Effects)
	if parentNone.Current() != "supervising" {
		t.Fatalf("parent = %q, want supervising: an undeclared onError must route nothing", parentNone.Current())
	}
	if esc := sysNone.LastEscalation(); esc == nil {
		t.Fatal("an undeclared onError failed without escalating")
	}
}

// TestSpawnActor_OldEffectJSONWithNoOutcomeStaysUnset pins the other side of the
// effect back-fill: an older producer writing an unset outcome left either the
// zero event (a static invoke boxes its machine's zero event into the type-erased
// field) or no key at all (a dynamic spawn's omitted option), and neither
// back-fills, so the decoded spawn routes nothing.
func TestSpawnActor_OldEffectJSONWithNoOutcomeStaysUnset(t *testing.T) {
	reg := state.NewEffectRegistry()
	cases := []struct {
		name    string
		payload string
	}{
		{"a static invoke's zero events", `{"id":"a1","src":{"name":"child"},"onDone":"","onError":"","state":"supervising"}`},
		{"a dynamic spawn's omitted options", `{"id":"a2","src":{"name":"child"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eff, err := reg.Unmarshal(state.EffectEnvelope{
				Kind:    state.EffectKindSpawnActor,
				Payload: json.RawMessage(tc.payload),
			})
			if err != nil {
				t.Fatalf("registry Unmarshal err = %v", err)
			}
			sp, ok := eff.(*state.SpawnActor)
			if !ok {
				t.Fatalf("decoded %T, want *SpawnActor", eff)
			}
			if sp.HasDone || sp.HasError {
				t.Fatalf("decoded SpawnActor = %+v, want both outcomes unset", *sp)
			}
		})
	}
}

// TestSpawnActor_EffectRoundTripStaysByteIdentical pins the forward half of the
// effect back-fill: a new producer's SpawnActor envelope round-trips through the
// registry byte-identically, a declared outcome (of any event, the zero one
// included), an unset one, and a spawn that declared neither alike.
func TestSpawnActor_EffectRoundTripStaysByteIdentical(t *testing.T) {
	reg := state.NewEffectRegistry()
	spawns := []state.SpawnActor{
		{ID: "a1", Src: state.Ref{Name: "child"}, OnDone: "childDone", HasDone: true, OnError: "childErr", HasError: true, State: "supervising"},
		// A declared outcome of the zero event itself: the flag, not the event's
		// value, says it was declared, and the flag is on the wire.
		{ID: "a2", Src: state.Ref{Name: "child"}, OnDone: "", HasDone: true, State: "supervising"},
		// The failure-side twin: a declared zero-valued onError, distinct on the wire
		// from an unset one (a3).
		{ID: "a5", Src: state.Ref{Name: "child"}, OnError: "", HasError: true, State: "supervising"},
		// An unset outcome: the zero event a static invoke boxes, no flag.
		{ID: "a3", Src: state.Ref{Name: "child"}, OnDone: "", OnError: "", State: "supervising"},
		// A dynamic spawn that declared neither: no events at all.
		{ID: "a4", Src: state.Ref{Name: "child"}},
	}
	for _, sp := range spawns {
		env, err := state.MarshalEffect(sp)
		if err != nil {
			t.Fatalf("MarshalEffect err = %v", err)
		}
		b1, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("marshal envelope err = %v", err)
		}
		var back state.EffectEnvelope
		if uerr := json.Unmarshal(b1, &back); uerr != nil {
			t.Fatalf("unmarshal envelope err = %v", uerr)
		}
		eff, err := reg.Unmarshal(back)
		if err != nil {
			t.Fatalf("registry Unmarshal err = %v", err)
		}
		env2, err := state.MarshalEffect(*eff.(*state.SpawnActor))
		if err != nil {
			t.Fatalf("re-marshal err = %v", err)
		}
		b2, err := json.Marshal(env2)
		if err != nil {
			t.Fatalf("re-marshal envelope err = %v", err)
		}
		if string(b1) != string(b2) {
			t.Fatalf("SpawnActor envelope round-trip diverged:\n%s\n--- vs ---\n%s", b1, b2)
		}
	}
}
