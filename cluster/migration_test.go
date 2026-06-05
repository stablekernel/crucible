package cluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/cluster"
	"github.com/stablekernel/crucible/state"
)

// migEnt is the migrated instance's context.
type migEnt struct {
	Step int `json:"step"`
}

// migSource is the machine an instance runs on the source node: a, b, c with
// a --go--> b.
func migSource() *state.Machine[string, string, *migEnt] {
	return state.Forge[string, string, *migEnt]("mig").
		State("a").
		State("b").
		State("c").
		Initial("a").
		Transition("a").On("go").GoTo("b").
		Quench()
}

// migAdditive adds a state d — a backward-compatible (additive) evolution.
func migAdditive() *state.Machine[string, string, *migEnt] {
	return state.Forge[string, string, *migEnt]("mig").
		State("a").
		State("b").
		State("c").
		State("d").
		Initial("a").
		Transition("a").On("go").GoTo("b").
		Quench()
}

// migBreaking removes state c — a breaking evolution.
func migBreaking() *state.Machine[string, string, *migEnt] {
	return state.Forge[string, string, *migEnt]("mig").
		State("a").
		State("b").
		Initial("a").
		Transition("a").On("go").GoTo("b").
		Quench()
}

// capturedInB drives a fresh source instance to state b and captures it.
func capturedInB(t *testing.T) cluster.Checkpoint {
	t.Helper()
	ctx := context.Background()
	inst := migSource().Cast(&migEnt{Step: 1}, state.WithInitialState("a"))
	inst.Fire(ctx, "go")
	if inst.Current() != "b" {
		t.Fatalf("source instance state = %q, want b", inst.Current())
	}
	sys := state.NewActorSystem(inst)
	cp, err := cluster.Capture(inst, sys, migSource())
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	return cp
}

// TestMigration_RoundTrip captures an instance on the source machine and restores
// it on the target (same machine), reaching the identical configuration and
// context without re-running the transition.
func TestMigration_RoundTrip(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)

	// The checkpoint is wire-shippable: round-trip it through JSON as a transport
	// would.
	raw, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	var shipped cluster.Checkpoint
	if err = json.Unmarshal(raw, &shipped); err != nil {
		t.Fatalf("unmarshal checkpoint: %v", err)
	}

	inst, _, err := cluster.Restore(ctx, shipped, migSource())
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if inst.Current() != "b" {
		t.Fatalf("restored state = %q, want b", inst.Current())
	}
	if inst.Entity().Step != 1 {
		t.Fatalf("restored context Step = %d, want 1", inst.Entity().Step)
	}
}

// TestMigration_AdditiveTargetAllowed lets an instance migrate onto an additively
// evolved machine.
func TestMigration_AdditiveTargetAllowed(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)
	inst, _, err := cluster.Restore(ctx, cp, migAdditive())
	if err != nil {
		t.Fatalf("Restore onto additive target: %v", err)
	}
	if inst.Current() != "b" {
		t.Fatalf("restored state = %q, want b", inst.Current())
	}
}

// TestMigration_BreakingTargetRefused refuses to migrate onto a machine whose
// definition changed in a backward-incompatible way.
func TestMigration_BreakingTargetRefused(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)
	_, _, err := cluster.Restore(ctx, cp, migBreaking())
	if !errors.Is(err, cluster.ErrIncompatibleMigration) {
		t.Fatalf("Restore onto breaking target err = %v, want ErrIncompatibleMigration", err)
	}
}

// TestMigration_Restore_BadMachineIR reports an error when the captured machine IR
// cannot be decoded, rather than silently proceeding with no compatibility gate.
func TestMigration_Restore_BadMachineIR(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)
	cp.MachineIR = json.RawMessage(`{not valid ir`)

	_, _, err := cluster.Restore(ctx, cp, migSource())
	if err == nil {
		t.Fatal("Restore with a corrupt machine IR must error")
	}
	if errors.Is(err, cluster.ErrIncompatibleMigration) {
		t.Fatalf("a decode failure must not masquerade as an incompatible-migration refusal: %v", err)
	}
}

// TestMigration_Restore_BadSnapshot reports an error when the captured snapshot
// cannot be unmarshaled, after the compatibility gate has passed.
func TestMigration_Restore_BadSnapshot(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)
	cp.Snapshot = json.RawMessage(`{"current": 12345}`) // wrong shape for the snapshot

	_, _, err := cluster.Restore(ctx, cp, migSource())
	if err == nil {
		t.Fatal("Restore with a corrupt snapshot must error")
	}
}

// TestMigration_Restore_BadActors reports an error when a captured actor entry
// cannot be decoded, rather than restoring a partial actor tree.
func TestMigration_Restore_BadActors(t *testing.T) {
	ctx := context.Background()
	cp := capturedInB(t)
	cp.Actors = map[string]json.RawMessage{"w-bad": json.RawMessage(`{not an actor`)}

	_, _, err := cluster.Restore(ctx, cp, migSource(), cluster.WithActorBehaviors(map[string]state.ActorBehavior{
		"child": childBehavior(),
	}))
	if err == nil {
		t.Fatal("Restore with a corrupt actor entry must error")
	}
}

// TestMigration_Capture_Marshalable confirms Capture succeeds on a well-formed
// instance and that the captured snapshot, IR, and actor tree are all valid JSON
// (the marshal/serialize/snapshot success paths the error branches guard).
func TestMigration_Capture_Marshalable(t *testing.T) {
	cp := capturedInB(t)
	if !json.Valid(cp.Snapshot) {
		t.Error("captured Snapshot is not valid JSON")
	}
	if !json.Valid(cp.MachineIR) {
		t.Error("captured MachineIR is not valid JSON")
	}
	for id, raw := range cp.Actors {
		if !json.Valid(raw) {
			t.Errorf("captured actor %q is not valid JSON", id)
		}
	}
}

// TestMigration_ActorsMove confirms a migrated instance carries its running actors.
func TestMigration_ActorsMove(t *testing.T) {
	ctx := context.Background()
	inst := migSource().Cast(&migEnt{Step: 2}, state.WithInitialState("a"))
	sys := state.NewActorSystem(inst).Register("child", childBehavior())
	// Spawn an actor directly so the snapshot carries it.
	sys.Absorb(ctx, []state.Effect{state.SpawnActor{ID: "w-mig", Src: state.Ref{Name: "child"}}})
	if sys.Running() != 1 {
		t.Fatalf("source Running() = %d, want 1", sys.Running())
	}

	cp, err := cluster.Capture(inst, sys, migSource())
	if err != nil {
		t.Fatalf("Capture: %v", err)
	}
	_, sys2, err := cluster.Restore(ctx, cp, migSource(), cluster.WithActorBehaviors(map[string]state.ActorBehavior{
		"child": childBehavior(),
	}))
	if err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if _, ok := sys2.Ref("w-mig"); !ok {
		t.Fatal("migrated actor w-mig not present on the target system")
	}
}
