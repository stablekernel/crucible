package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/evolution"
)

// ErrIncompatibleMigration is returned by Restore when the target node's machine
// definition differs from the source's in a backward-incompatible (breaking) way,
// so resuming the instance there would misinterpret its persisted configuration.
var ErrIncompatibleMigration = errors.New("cluster: target machine is a breaking change from the source")

// Checkpoint is a migratable capture of a running instance: its kernel snapshot,
// its actor tree, and the source machine's IR so the target can gate the move on
// schema compatibility. Every field is JSON, so a Checkpoint ships over any
// transport as bytes.
type Checkpoint struct {
	// Snapshot is the marshaled kernel Snapshot of the migrating instance.
	Snapshot json.RawMessage `json:"snapshot"`
	// Actors is the instance's actor tree as SnapshotActors produced it, keyed by
	// actor id; empty when the instance runs no actors.
	Actors map[string]json.RawMessage `json:"actors,omitempty"`
	// MachineIR is the source machine's serialized definition, diffed against the
	// target machine to gate the migration on backward compatibility.
	MachineIR json.RawMessage `json:"machineIR"`
}

// Capture snapshots a running instance, its actor tree, and its machine definition
// into a Checkpoint ready to ship to another node. It is a pure read: it neither
// fires the instance nor mutates any actor.
//
// # Consistency boundary
//
// The instance snapshot and the actor-tree snapshot are read as two separate
// operations, not under one combined lock, so Capture does not by itself produce a
// point-in-time-consistent pair. The caller must ensure no Fire or actor delivery
// runs against the instance for the duration of the call — that is what "quiescent"
// means here. Captured under concurrent firing, the snapshot and the actor tree may
// reflect different instants and Restore could rebuild a tree that does not match
// the kernel configuration. Quiesce the instance (stop driving it) before Capture.
func Capture[S comparable, E comparable, C any](inst *state.Instance[S, E, C], sys *state.ActorSystem[S, E, C], machine *state.Machine[S, E, C]) (Checkpoint, error) {
	snap, err := state.MarshalSnapshot(inst.Snapshot())
	if err != nil {
		return Checkpoint{}, fmt.Errorf("cluster: marshal snapshot: %w", err)
	}
	ir, err := machine.ToJSON()
	if err != nil {
		return Checkpoint{}, fmt.Errorf("cluster: serialize machine: %w", err)
	}
	var actors map[string]json.RawMessage
	if sys != nil {
		if actors, err = sys.SnapshotActors(); err != nil {
			return Checkpoint{}, fmt.Errorf("cluster: snapshot actors: %w", err)
		}
	}
	return Checkpoint{Snapshot: snap, Actors: actors, MachineIR: ir}, nil
}

type restoreConfig struct {
	behaviors map[string]state.ActorBehavior
}

// RestoreOption configures a Restore. New capabilities arrive as additional
// options, so the signature never breaks.
type RestoreOption func(*restoreConfig)

// WithActorBehaviors registers the child-machine behaviors the target node binds
// before its actor tree is rebuilt, keyed by the src ref name — exactly the
// palette the source registered. An actor whose src is absent here is skipped.
func WithActorBehaviors(behaviors map[string]state.ActorBehavior) RestoreOption {
	return func(c *restoreConfig) { c.behaviors = behaviors }
}

// Restore rebuilds a captured instance on machine and reconstructs its actor tree,
// gating the move on schema compatibility: if machine is a breaking change from
// the source definition the Checkpoint carries, it refuses with
// ErrIncompatibleMigration rather than resume an instance against a definition
// that would misread its state. An additive (or identical) target is allowed. It
// returns the resumed instance and its actor system; the instance is resumed in
// place (no entry actions re-run), and a host re-arms timers/services by absorbing
// the instance's ResumeEffects through its drivers.
func Restore[S comparable, E comparable, C any](ctx context.Context, cp Checkpoint, machine *state.Machine[S, E, C], opts ...RestoreOption) (*state.Instance[S, E, C], *state.ActorSystem[S, E, C], error) {
	var cfg restoreConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	targetIR, err := machine.ToJSON()
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: serialize target machine: %w", err)
	}
	report, err := evolution.DiffJSON[S, E, C](cp.MachineIR, targetIR)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: diff machines for migration: %w", err)
	}
	if report.Breaking() {
		return nil, nil, fmt.Errorf("%w: %s", ErrIncompatibleMigration, report)
	}

	snap, err := state.UnmarshalSnapshot[S, E, C](cp.Snapshot)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: unmarshal snapshot: %w", err)
	}
	inst, err := machine.Restore(snap)
	if err != nil {
		return nil, nil, fmt.Errorf("cluster: restore instance: %w", err)
	}

	sys := state.NewActorSystem(inst)
	for src, behavior := range cfg.behaviors {
		sys.Register(src, behavior)
	}
	if len(cp.Actors) > 0 {
		if err := sys.RestoreActors(ctx, cp.Actors); err != nil {
			return nil, nil, fmt.Errorf("cluster: restore actors: %w", err)
		}
	}
	return inst, sys, nil
}
