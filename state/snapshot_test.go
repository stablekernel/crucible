package state_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
)

// snapCtx is a JSON-marshalable instance context for snapshot round-trip tests:
// exported fields so encoding/json (the default snapshot codec) captures it
// losslessly.
type snapCtx struct {
	Count int      `json:"count"`
	Notes []string `json:"notes"`
}

// flatSnapMachine builds a flat three-state machine whose transitions mutate the
// context, so a snapshot taken mid-run captures both configuration and context.
func flatSnapMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx]("flow").
		Action("bump", func(c state.ActionCtx[*snapCtx]) (state.Effect, error) {
			c.Entity.Count++
			c.Entity.Notes = append(c.Entity.Notes, "bumped")
			return nil, nil
		}).
		State("idle").
		State("active").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("active").Do("bump").
		Transition("active").On("finish").GoTo("done").Do("bump").
		Quench()
}

// hsmSnapMachine builds a hierarchical machine with a compound "running" state so
// a snapshot of a nested configuration round-trips the spine.
func hsmSnapMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx]("hsm").
		State("off").
		SuperState("running").
		Initial("warmup").
		SubState("warmup").On("ready").GoTo("steady").
		SubState("steady").
		EndSuperState().
		Initial("off").
		Transition("off").On("power").GoTo("running").
		Quench()
}

// parallelSnapMachine builds a parallel "on" state with two regions so a snapshot
// of a multi-leaf configuration round-trips every region's active leaf.
func parallelSnapMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx]("par").
		State("off").
		SuperState("on").
		Region("A").
		Initial("a1").
		SubState("a1").On("aNext").GoTo("a2").
		SubState("a2").
		EndRegion().
		Region("B").
		Initial("b1").
		SubState("b1").On("bNext").GoTo("b2").
		SubState("b2").
		EndRegion().
		EndSuperState().
		Initial("off").
		Transition("off").On("start").GoTo("on").
		Quench()
}

// TestSnapshot_PreservesConfigurationContextHistory captures a flat instance
// mid-run and asserts the snapshot holds its configuration, context, and history.
func TestSnapshot_PreservesConfigurationContextHistory(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"), state.WithUnboundedHistory[string]())
	ctx := context.Background()
	inst.Fire(ctx, "go")

	snap := inst.Snapshot()
	if snap.Machine != "flow" {
		t.Fatalf("machine: want flow, got %q", snap.Machine)
	}
	if snap.Current != "active" {
		t.Fatalf("current: want active, got %q", snap.Current)
	}
	if !reflect.DeepEqual(snap.Configuration, []string{"active"}) {
		t.Fatalf("configuration: want [active], got %v", snap.Configuration)
	}
	if snap.Context.Count != 1 || len(snap.Context.Notes) != 1 {
		t.Fatalf("context not captured: %+v", snap.Context)
	}
	if len(snap.Traces) != 1 {
		t.Fatalf("traces: want 1, got %d", len(snap.Traces))
	}
	if snap.Status != state.StatusRunning {
		t.Fatalf("status: want running, got %v", snap.Status)
	}
}

// TestSnapshot_StatusDone reports StatusDone once the configuration is final.
func TestSnapshot_StatusDone(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	ctx := context.Background()
	inst.Fire(ctx, "go")
	inst.Fire(ctx, "finish")

	snap := inst.Snapshot()
	if snap.Status != state.StatusDone {
		t.Fatalf("status: want done, got %v", snap.Status)
	}
}

// TestSnapshot_JSONRoundTrip serializes a snapshot to JSON and back, asserting the
// configuration, context, and history survive verbatim.
func TestSnapshot_JSONRoundTrip(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	ctx := context.Background()
	inst.Fire(ctx, "go")

	snap := inst.Snapshot()
	b, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Current != snap.Current {
		t.Fatalf("current: want %q, got %q", snap.Current, back.Current)
	}
	if !reflect.DeepEqual(back.Configuration, snap.Configuration) {
		t.Fatalf("configuration: want %v, got %v", snap.Configuration, back.Configuration)
	}
	if back.Context == nil || back.Context.Count != 1 {
		t.Fatalf("context not round-tripped: %+v", back.Context)
	}
	if len(back.Traces) != len(snap.Traces) {
		t.Fatalf("traces: want %d, got %d", len(snap.Traces), len(back.Traces))
	}
}

// TestRestore_ResumesIdenticallyWithoutReentry restores a flat instance from a
// snapshot and asserts it fires identically from that point (behavioral identity)
// and that no entry action re-ran on restore.
func TestRestore_ResumesIdenticallyWithoutReentry(t *testing.T) {
	m := flatSnapMachine()
	ctx := context.Background()

	// Original: advance to active, then finish.
	orig := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	orig.Fire(ctx, "go")
	snap := orig.Snapshot()
	contextAtSnapshot := *snap.Context

	// Restore at the snapshot and continue.
	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	// No entry action re-ran: context is unchanged from the snapshot.
	if !reflect.DeepEqual(*restored.Entity(), contextAtSnapshot) {
		t.Fatalf("restore re-ran actions: want %+v, got %+v", contextAtSnapshot, *restored.Entity())
	}
	if restored.Current() != "active" {
		t.Fatalf("restored current: want active, got %q", restored.Current())
	}

	// Behavioral identity: firing "finish" on both yields the same state + context.
	origRes := orig.Fire(ctx, "finish")
	restoredRes := restored.Fire(ctx, "finish")
	if orig.Current() != restored.Current() {
		t.Fatalf("divergent state: orig=%q restored=%q", orig.Current(), restored.Current())
	}
	if !reflect.DeepEqual(*orig.Entity(), *restored.Entity()) {
		t.Fatalf("divergent context: orig=%+v restored=%+v", *orig.Entity(), *restored.Entity())
	}
	if origRes.Err != nil || restoredRes.Err != nil {
		t.Fatalf("fire errors: orig=%v restored=%v", origRes.Err, restoredRes.Err)
	}
}

// TestRestore_HierarchicalRoundTrips snapshots a nested configuration and restores
// it, asserting the spine and subsequent firing match the original.
func TestRestore_HierarchicalRoundTrips(t *testing.T) {
	m := hsmSnapMachine()
	ctx := context.Background()
	orig := m.Cast(&snapCtx{}, state.WithInitialState("off"))
	orig.Fire(ctx, "power") // -> running -> warmup

	snap := orig.Snapshot()
	if snap.Current != "warmup" {
		t.Fatalf("current: want warmup, got %q", snap.Current)
	}
	b, _ := json.Marshal(snap)
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	restored, err := m.Restore(back)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	orig.Fire(ctx, "ready")
	restored.Fire(ctx, "ready")
	if orig.Current() != "steady" || restored.Current() != "steady" {
		t.Fatalf("divergent: orig=%q restored=%q", orig.Current(), restored.Current())
	}
}

// TestRestore_ParallelRoundTrips snapshots a multi-region configuration and
// restores it, asserting every region's leaf and subsequent firing match.
func TestRestore_ParallelRoundTrips(t *testing.T) {
	m := parallelSnapMachine()
	ctx := context.Background()
	orig := m.Cast(&snapCtx{}, state.WithInitialState("off"))
	orig.Fire(ctx, "start") // -> on -> {a1, b1}
	orig.Fire(ctx, "aNext") // -> {a2, b1}

	snap := orig.Snapshot()
	if len(snap.Configuration) != 2 {
		t.Fatalf("configuration: want 2 leaves, got %v", snap.Configuration)
	}
	b, _ := json.Marshal(snap)
	var back state.Snapshot[string, string, *snapCtx]
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	restored, err := m.Restore(back)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !reflect.DeepEqual(restored.Configuration(), orig.Configuration()) {
		t.Fatalf("configuration: want %v, got %v", orig.Configuration(), restored.Configuration())
	}
	// Behavioral identity in the second region.
	orig.Fire(ctx, "bNext")
	restored.Fire(ctx, "bNext")
	if !reflect.DeepEqual(restored.Configuration(), orig.Configuration()) {
		t.Fatalf("post-fire divergence: orig=%v restored=%v", orig.Configuration(), restored.Configuration())
	}
}

// TestRestore_RejectsWrongMachine returns a typed *SnapshotError when a snapshot's
// machine does not match the target.
func TestRestore_RejectsWrongMachine(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	snap := inst.Snapshot()
	snap.Machine = "other"

	_, err := m.Restore(snap)
	var se *state.SnapshotError
	if !errors.As(err, &se) {
		t.Fatalf("want *SnapshotError, got %v", err)
	}
}

// TestRestore_RejectsUnknownConfigurationLeaf returns a typed *SnapshotError when a
// configuration leaf is not a declared state.
func TestRestore_RejectsUnknownConfigurationLeaf(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	snap := inst.Snapshot()
	snap.Configuration = []string{"nonexistent"}

	_, err := m.Restore(snap)
	var se *state.SnapshotError
	if !errors.As(err, &se) {
		t.Fatalf("want *SnapshotError, got %v", err)
	}
}

// afterSnapMachine builds a machine whose "waiting" state declares an `after`
// delayed transition, so a snapshot's ResumeEffects re-arms the pending timer.
func afterSnapMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx]("timer").
		State("idle").
		State("waiting").After(50 * time.Millisecond).On("elapsed").GoTo("done").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("begin").GoTo("waiting").
		Quench()
}

// TestResumeEffects_ReArmsPendingTimer asserts a restored instance whose
// configuration owns an `after` transition re-arms the timer via ResumeEffects,
// and that the snapshot's Pending inventory lists it.
func TestResumeEffects_ReArmsPendingTimer(t *testing.T) {
	m := afterSnapMachine()
	ctx := context.Background()
	orig := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	orig.Fire(ctx, "begin") // -> waiting (arms the after timer)

	snap := orig.Snapshot()
	wantID := state.ScheduleID("timer", "waiting", 0)
	found := false
	for _, id := range snap.Pending.Timers {
		if id == wantID {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending timers %v missing %q", snap.Pending.Timers, wantID)
	}

	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	effects := restored.ResumeEffects()
	var armed bool
	for _, eff := range effects {
		if sa, ok := eff.(state.ScheduleAfter); ok && sa.ID == wantID {
			armed = true
		}
	}
	if !armed {
		t.Fatalf("ResumeEffects %v did not re-arm timer %q", effects, wantID)
	}
}

// TestResumeEffects_ReArmsPendingService asserts a restored instance whose
// configuration owns an `invoke` re-arms the service via ResumeEffects.
func TestResumeEffects_ReArmsPendingService(t *testing.T) {
	m := state.Forge[string, string, *snapCtx]("svc").
		Service("fetch", func(context.Context, state.ServiceCtx[*snapCtx]) (any, error) { return nil, nil }).
		State("idle").
		State("loading").Invoke("fetch", state.WithInvokeOnDone("ok"), state.WithInvokeOnError("fail")).
		State("ready").
		Initial("idle").
		Transition("idle").On("start").GoTo("loading").
		Transition("loading").On("ok").GoTo("ready").
		Quench()
	ctx := context.Background()
	orig := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	orig.Fire(ctx, "start") // -> loading (arms the service)

	snap := orig.Snapshot()
	wantID := state.InvokeID("svc", "loading", 0)
	if len(snap.Pending.Services) == 0 || snap.Pending.Services[0] != wantID {
		t.Fatalf("pending services %v missing %q", snap.Pending.Services, wantID)
	}
	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	var armed bool
	for _, eff := range restored.ResumeEffects() {
		if ss, ok := eff.(state.StartService); ok && ss.ID == wantID {
			armed = true
		}
	}
	if !armed {
		t.Fatalf("ResumeEffects did not re-arm service %q", wantID)
	}
}

// TestSnapshot_ContextCodec serializes a snapshot whose context is not directly
// JSON-marshalable through a custom ContextCodec and round-trips it.
func TestSnapshot_ContextCodec(t *testing.T) {
	m := flatSnapMachine()
	inst := m.Cast(&snapCtx{Count: 7}, state.WithInitialState("idle"))
	snap := inst.Snapshot()

	codec := snapCodec{}
	b, err := state.MarshalSnapshot(snap, state.WithContextCodec[*snapCtx](codec))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	back, err := state.UnmarshalSnapshot[string, string, *snapCtx](b, state.WithContextCodec[*snapCtx](codec))
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Context == nil || back.Context.Count != 7 {
		t.Fatalf("codec round-trip lost context: %+v", back.Context)
	}
	restored, err := m.Restore(back)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if restored.Entity().Count != 7 {
		t.Fatalf("restored context: want 7, got %d", restored.Entity().Count)
	}
}

// snapChildCtx is a JSON-marshalable child-actor context for actor-tree snapshot
// tests.
type snapChildCtx struct {
	Steps int `json:"steps"`
}

// snapChildMachine builds a flat child machine that advances idle -> mid -> done,
// bumping a JSON-marshalable step counter, so its snapshot round-trips.
func snapChildMachine() *state.Machine[string, string, *snapChildCtx] {
	return state.Forge[string, string, *snapChildCtx]("snapchild").
		Action("step", func(c state.ActionCtx[*snapChildCtx]) (state.Effect, error) {
			c.Entity.Steps++
			return nil, nil
		}).
		State("idle").
		State("mid").
		State("done").Final().
		Initial("idle").
		Transition("idle").On("advance").GoTo("mid").Do("step").
		Transition("mid").On("finish").GoTo("done").Do("step").
		Quench()
}

// snapParentMachine builds a parent that invokes the snap child actor.
func snapParentMachine() *state.Machine[string, string, *snapCtx] {
	return state.Forge[string, string, *snapCtx]("snapparent").
		State("idle").
		State("supervising").InvokeActor("snapchild", state.WithInvokeOnDone("childDone"), state.WithInvokeOnError("childErr")).
		State("complete").
		Initial("idle").
		Transition("idle").On("start").GoTo("supervising").
		Transition("supervising").On("childDone").GoTo("complete").
		Quench()
}

// TestActorSystem_SnapshotRestoresChildActor snapshots a parent's spawned child
// actor recursively, restores it into a fresh system, and asserts the restored
// child resumes mid-run (its step counter preserved) and fires identically.
func TestActorSystem_SnapshotRestoresChildActor(t *testing.T) {
	ctx := context.Background()
	m := snapParentMachine()

	childBehavior := func() state.ActorBehavior {
		cm := snapChildMachine()
		return func(input map[string]any) (state.ActorInstance, error) {
			inst := cm.Cast(&snapChildCtx{}, state.WithInitialState("idle"))
			return state.NewActor(inst, nil), nil
		}
	}

	parent := m.Cast(&snapCtx{}, state.WithInitialState("idle"))
	sys := state.NewActorSystem(parent).Register("snapchild", childBehavior())
	res := parent.Fire(ctx, "start")
	sys.Absorb(ctx, res.Effects)

	id := state.ActorID(m.Name(), "supervising", 0)
	ref, ok := sys.Ref(id)
	if !ok {
		t.Fatalf("no actor ref for id %q", id)
	}
	// Advance the child once so its restored state is mid-run with a step recorded.
	if !sys.Deliver(ctx, ref, "advance") {
		t.Fatal("deliver advance failed")
	}

	actorSnaps, err := sys.SnapshotActors()
	if err != nil {
		t.Fatalf("SnapshotActors: %v", err)
	}
	if len(actorSnaps) != 1 {
		t.Fatalf("want 1 actor snapshot, got %d", len(actorSnaps))
	}

	// Restore into a fresh parent + system.
	parent2 := m.Cast(&snapCtx{}, state.WithInitialState("supervising"))
	sys2 := state.NewActorSystem(parent2).Register("snapchild", childBehavior())
	if err := sys2.RestoreActors(ctx, actorSnaps); err != nil {
		t.Fatalf("RestoreActors: %v", err)
	}
	if sys2.Running() != 1 {
		t.Fatalf("restored running actors = %d, want 1", sys2.Running())
	}
	ref2, ok := sys2.Ref(id)
	if !ok {
		t.Fatalf("restored actor missing for id %q", id)
	}
	// Resumed mid-run: delivering "finish" completes the child and routes childDone,
	// landing the restored parent in complete.
	if !sys2.Deliver(ctx, ref2, "finish") {
		t.Fatal("deliver finish to restored actor failed")
	}
	if parent2.Current() != "complete" {
		t.Fatalf("restored parent state = %q, want complete", parent2.Current())
	}
}

// snapCodec is a trivial ContextCodec over *snapCtx, exercising the WithContextCodec
// hook. It delegates to encoding/json but proves the codec path is wired.
type snapCodec struct{}

func (snapCodec) Encode(c *snapCtx) ([]byte, error) { return json.Marshal(c) }

func (snapCodec) Decode(b []byte) (*snapCtx, error) {
	var c snapCtx
	if len(b) == 0 {
		return &c, nil
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
