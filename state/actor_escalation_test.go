package state_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// panicChildBehavior returns an ActorBehavior whose child machine panics when it
// handles the "boom" event (an action that panics on entry to a "blow" state),
// exercising the ActorSystem's panic recovery + escalation. On "finish" it still
// completes normally, so the same child serves both the panic and the happy path.
func panicChildBehavior() state.ActorBehavior {
	cm := state.Forge[string, string, *childEntity]("child").
		Action("boom", func(state.ActionCtx[*childEntity]) (state.Effect, error) {
			panic("child blew up")
		}).
		State("working").
		State("blow").OnEntry("boom").
		State("done").Final().
		Initial("working").
		Transition("working").On("boom").GoTo("blow").
		Transition("working").On("finish").GoTo("done").
		Quench()
	return func(map[string]any) (state.ActorInstance, error) {
		inst := cm.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}
}

// escActorID is the stable id the no-onError parents spawn their child under.
const escActorID = "ff-child"

// noErrorParent builds a parent that dynamically SPAWNS a child actor with NO
// onError event wired (a fire-and-forget child). When that child fails the runtime
// has no onError to route, so the G3 default must escalate the failure to the parent
// observably rather than swallow it. The child is spawned on the "start" event.
func noErrorParent() *state.Machine[string, string, *trec] {
	return state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").
		State("complete").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Spawn("child", escActorID).
		Quench()
}

// startSupervising casts the parent, builds a system registered with behavior, and
// drives it into the supervising state with one live, no-onError child actor. It
// returns the system, the parent instance, and the spawned actor's id.
func startSupervising(t *testing.T, m *state.Machine[string, string, *trec], behavior state.ActorBehavior) (*state.ActorSystem[string, string, *trec], *state.Instance[string, string, *trec], string) {
	t.Helper()
	parent := m.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", behavior)
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	if sys.Running() != 1 {
		t.Fatalf("running actors = %d, want 1 after start", sys.Running())
	}
	return sys, parent, escActorID
}

// TestActorEscalation_ChildError_NoOnError_EscalatesToParent asserts the G3 default:
// a child failure with NO onError wired escalates to the parent as a typed,
// observable ActorEscalation rather than being silently swallowed.
func TestActorEscalation_ChildError_NoOnError_EscalatesToParent(t *testing.T) {
	sys, parent, id := startSupervising(t, noErrorParent(), childBehavior())
	ctx := context.Background()

	cause := errors.New("child failed")
	if _, ok := sys.SettleError(ctx, id, cause); ok {
		t.Fatal("SettleError routed an onError, but the parent declared none")
	}

	// The parent never transitioned (no onError handler), but the failure is NOT
	// lost: it escalated and is observable on the system.
	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising (no onError transition)", parent.Current())
	}
	esc := sys.LastEscalation()
	if esc == nil {
		t.Fatal("LastEscalation = nil; child failure was swallowed, not escalated")
	}
	if esc.ActorID != id {
		t.Fatalf("escalation ActorID = %q, want %q", esc.ActorID, id)
	}
	if !errors.Is(esc, cause) {
		t.Fatalf("escalation does not wrap the cause: %v", esc)
	}
}

// TestActorEscalation_Typed asserts the escalation is discoverable with errors.As
// and unwraps to its cause, so a host can branch on it as a typed failure.
func TestActorEscalation_Typed(t *testing.T) {
	sys, _, id := startSupervising(t, noErrorParent(), childBehavior())
	ctx := context.Background()

	sentinel := errors.New("typed cause")
	sys.SettleError(ctx, id, sentinel)

	var esc *state.ActorEscalation
	if !errors.As(sys.LastEscalation(), &esc) {
		t.Fatal("errors.As did not match *ActorEscalation")
	}
	if esc.Src != "child" {
		t.Fatalf("escalation Src = %q, want child", esc.Src)
	}
	if !errors.Is(esc, sentinel) {
		t.Fatalf("errors.Is(esc, sentinel) = false; Unwrap broken: %v", esc)
	}
	// The rendered message names the failed actor and the wrapped cause.
	if msg := esc.Error(); !strings.Contains(msg, id) || !strings.Contains(msg, sentinel.Error()) {
		t.Fatalf("escalation message %q omits actor id or cause", msg)
	}
}

// TestActorEscalation_Handler asserts a registered EscalationHandler receives the
// escalation and may react (here, by recording it), in addition to the always-on
// record + inspect.
func TestActorEscalation_Handler(t *testing.T) {
	sys, _, id := startSupervising(t, noErrorParent(), childBehavior())
	ctx := context.Background()

	var got *state.ActorEscalation
	sys.WithEscalationHandler(func(_ context.Context, esc *state.ActorEscalation) {
		got = esc
	})

	cause := errors.New("handled escalation")
	sys.SettleError(ctx, id, cause)

	if got == nil {
		t.Fatal("EscalationHandler was not invoked")
	}
	if !errors.Is(got, cause) {
		t.Fatalf("handler escalation does not wrap cause: %v", got)
	}
}

// TestActorEscalation_ChildPanic_NoOnError_Escalates asserts a child that PANICS
// while stepping is recovered (never crashing the host driver) and escalates to the
// parent as a typed failure when no onError is wired.
func TestActorEscalation_ChildPanic_NoOnError_Escalates(t *testing.T) {
	sys, parent, id := startSupervising(t, noErrorParent(), panicChildBehavior())
	ctx := context.Background()

	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no ref for actor %q", id)
	}
	// Deliver the event that drives the child into a panicking action. The driver
	// must not panic; the failure escalates instead.
	sys.Deliver(ctx, ref, "boom")

	if parent.Current() != "supervising" {
		t.Fatalf("parent state = %q, want supervising", parent.Current())
	}
	esc := sys.LastEscalation()
	if esc == nil {
		t.Fatal("child panic was swallowed; LastEscalation = nil")
	}
	var pErr *state.ErrActorPanic
	if !errors.As(esc, &pErr) {
		t.Fatalf("escalation cause is not *ErrActorPanic: %v", esc)
	}
	if pErr.ActorID != id {
		t.Fatalf("panic ActorID = %q, want %q", pErr.ActorID, id)
	}
	if msg := pErr.Error(); !strings.Contains(msg, id) || !strings.Contains(msg, "child blew up") {
		t.Fatalf("panic message %q omits actor id or recovered value", msg)
	}
	if sys.IsRunning(id) {
		t.Fatal("panicked actor still running; should be settled done")
	}
}

// TestActorEscalation_WithOnError_HandledLocally asserts the unchanged path: when an
// onError IS wired, the child failure routes to the parent's onError transition and
// does NOT escalate.
func TestActorEscalation_WithOnError_HandledLocally(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").InvokeActor("child", "childDone", "childErr").
		State("errored").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childErr").GoTo("errored").
		Quench()

	parent := m.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := state.ActorID("parent", "supervising", 0)

	if _, ok := sys.SettleError(ctx, id, errors.New("boom")); !ok {
		t.Fatal("SettleError did not route the wired onError")
	}
	if parent.Current() != "errored" {
		t.Fatalf("parent state = %q, want errored (onError handled locally)", parent.Current())
	}
	if sys.LastEscalation() != nil {
		t.Fatalf("failure escalated despite a wired onError: %v", sys.LastEscalation())
	}
}

// TestActorEscalation_Inspector_ObservesEscalation asserts the escalation surfaces
// to a wired inspector as an InspectActor event with the ActorEscalated phase, so a
// failure is never lost to a silent code path.
func TestActorEscalation_Inspector_ObservesEscalation(t *testing.T) {
	parent := noErrorParent().Cast(&trec{}, state.WithInitialState("idle"))
	rec := &recordingInspector{}
	sys := state.NewActorSystem(parent).Register("child", childBehavior()).WithActorInspector(rec)
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)
	id := escActorID

	sys.SettleError(ctx, id, errors.New("observe me"))

	var sawEscalated bool
	for _, ev := range rec.events {
		if ev.Kind == state.InspectActor && ev.ActorPhase == state.ActorEscalated && ev.ActorID == id {
			sawEscalated = true
		}
	}
	if !sawEscalated {
		t.Fatal("inspector saw no ActorEscalated event for the failed actor")
	}
}

// TestActorEscalation_Nested_ClimbsToGrandparent asserts a grandchild failure with
// no onError climbs the supervision chain: it escalates from the grandchild to its
// parent actor (the middle) and is observed at each level (grandchild ->
// middle -> root), so a deeply-nested crash never vanishes.
func TestActorEscalation_Nested_ClimbsToGrandparent(t *testing.T) {
	const grandID = "grand-1"
	const greatID = "great-1"

	// Great-grandchild: a trivial leaf actor.
	great := childMachine()
	greatBehavior := func(map[string]any) (state.ActorInstance, error) {
		inst := great.Cast(&childEntity{}, state.WithInitialState("working"))
		return state.NewActor(inst, nil), nil
	}
	// Grandchild dynamically spawns the great-grandchild with no onError wired.
	grand := state.Forge[string, string, *childEntity]("grand").
		State("run").
		Initial("run").
		Transition("run").On("spawnGreat").GoTo("run").Spawn("great", greatID).
		Quench()
	grandBehavior := func(map[string]any) (state.ActorInstance, error) {
		inst := grand.Cast(&childEntity{}, state.WithInitialState("run"))
		return state.NewActor(inst, nil), nil
	}
	// Middle dynamically spawns the grandchild with no onError wired, so a descendant
	// failure escalates and climbs rather than routing into an onError.
	middle := state.Forge[string, string, *childEntity]("middle").
		State("run").
		State("end").Final().
		Initial("run").
		Transition("run").On("spawnGrand").GoTo("run").Spawn("grand", grandID).
		Transition("run").On("stop").GoTo("end").
		Quench()
	middleBehavior := func(map[string]any) (state.ActorInstance, error) {
		inst := middle.Cast(&childEntity{}, state.WithInitialState("run"))
		return state.NewActor(inst, nil), nil
	}

	parent := parentInvokeMachineWith(nil).Cast(&trec{}, state.WithInitialState("idle"))
	rec := &recordingInspector{}
	sys := state.NewActorSystem(parent).
		Register("child", middleBehavior).
		Register("grand", grandBehavior).
		Register("great", greatBehavior).
		WithActorInspector(rec)
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)

	middleID := state.ActorID("parent", "supervising", 0)
	if !sys.IsRunning(middleID) {
		t.Fatalf("middle actor %q not running", middleID)
	}
	// Build the chain: middle -> grand -> great.
	mref, _ := sys.Ref(middleID)
	sys.Deliver(ctx, mref, "spawnGrand")
	if !sys.IsRunning(grandID) {
		t.Fatalf("grandchild %q not running; nested spawn failed", grandID)
	}
	gref, _ := sys.Ref(grandID)
	sys.Deliver(ctx, gref, "spawnGreat")
	if !sys.IsRunning(greatID) {
		t.Fatalf("great-grandchild %q not running; deep spawn failed", greatID)
	}

	// Fail the great-grandchild with no onError: the failure must climb the whole
	// chain great -> grand -> middle, observed at every level.
	sys.SettleError(ctx, greatID, errors.New("great-grandchild crashed"))

	var sawGreat, sawGrand, sawMiddle bool
	for _, ev := range rec.events {
		if ev.Kind != state.InspectActor || ev.ActorPhase != state.ActorEscalated {
			continue
		}
		switch ev.ActorID {
		case greatID:
			sawGreat = true
		case grandID:
			sawGrand = true
		case middleID:
			sawMiddle = true
		}
	}
	if !sawGreat {
		t.Fatal("no escalation observed at the great-grandchild level")
	}
	if !sawGrand {
		t.Fatal("escalation did not climb to the grandchild")
	}
	if !sawMiddle {
		t.Fatal("escalation did not climb to the middle (root child) actor")
	}
}

// TestActorRef_Opacity_ResolvedThroughSystem pins the L7 ActorRef-opacity lock: a
// ref is an opaque structured handle resolved through the ActorSystem API (Ref /
// RefBySystemID), never a raw index or positional handle. The ref survives a
// round-trip through the system (resolve by id, then by systemId) yielding the same
// stable identity, and is NOT constructable as a meaningful positional index.
func TestActorRef_Opacity_ResolvedThroughSystem(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").
		InvokeActor("child", "childDone", "childErr", state.WithSystemID("supervisor")).
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Quench()

	parent := m.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("child", childBehavior())
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)

	id := state.ActorID("parent", "supervising", 0)
	byID, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("Ref(%q) not resolved", id)
	}
	bySys, ok := sys.RefBySystemID("supervisor")
	if !ok {
		t.Fatal("RefBySystemID(supervisor) not resolved")
	}
	// Both resolution paths funnel to the same opaque structured handle.
	if byID != bySys {
		t.Fatalf("ref identity diverged across resolution paths: %+v vs %+v", byID, bySys)
	}
	if byID.ID != id || byID.SystemID != "supervisor" || byID.Src != "child" {
		t.Fatalf("ref is not the expected structured handle: %+v", byID)
	}

	// A hand-constructed ref carrying ONLY the structured id still resolves through
	// the system (the holder treats it opaquely; resolution is the system's job, not
	// a positional index into a slice).
	if !sys.Deliver(ctx, state.ActorRef{ID: id}, "finish") {
		t.Fatal("system did not resolve a structured ref by its id")
	}

	// Resolution is registry-keyed, not positional: a ref whose id names no
	// registered actor resolves to nothing, and addressing one is a safe no-op — a
	// holder can never reach an actor by guessing a slice position.
	if _, ok := sys.Ref("no-such-actor"); ok {
		t.Fatal("a foreign id resolved; resolution is acting positionally")
	}
	if sys.Deliver(ctx, state.ActorRef{ID: "no-such-actor"}, "finish") {
		t.Fatal("delivery to an unregistered ref succeeded; ref is not opaque")
	}
}

// TestActorEscalation_UnboundSrc_NoOnError_Escalates asserts a spawn whose Src is
// unregistered, with NO onError wired, escalates rather than hanging or swallowing.
func TestActorEscalation_UnboundSrc_NoOnError_Escalates(t *testing.T) {
	m := state.Forge[string, string, *trec]("parent").
		State("idle").
		State("supervising").
		Initial("idle").
		CurrentStateFn(func(*trec) string { return "idle" }).
		Transition("idle").On("start").GoTo("supervising").
		Spawn("missing", "missing-1"). // no onError wired
		Quench()

	parent := m.Cast(&trec{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent) // "missing" never registered
	ctx := context.Background()
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)

	esc := sys.LastEscalation()
	if esc == nil {
		t.Fatal("unbound-src spawn failure was swallowed; LastEscalation = nil")
	}
	var unbound *state.ErrUnboundActor
	if !errors.As(esc, &unbound) {
		t.Fatalf("escalation cause is not *ErrUnboundActor: %v", esc)
	}
}
