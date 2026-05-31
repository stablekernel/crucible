package state

import (
	"context"
	"encoding/json"
)

// This file extends deep persistence to the actor tree: an ActorSystem snapshots
// its running child actors recursively and restores them, so a parent snapshot
// can carry its spawned children's state (the actor tree is persisted
// invoked and spawned actors). It is the actor-model analog of Instance.Snapshot
// / Machine.Restore, layered on the same ActorSystem that already runs the actors
// — so it stays on the documented host-driver harness and the kernel's Fire step
// stays pure.
//
// # Depth of actor-tree persistence
//
// The synchronous/test path is implemented end to end: SnapshotActors walks every
// live actor, captures each child's own runtime state through the Snapshotter
// interface (which an actorAdapter satisfies), and recurses through each actor's
// nested children; RestoreActors re-spawns each actor from the system's palette
// and restores its child instance state. What it deliberately does NOT capture is
// the per-actor MAILBOX backlog (queued-but-unprocessed envelopes) and the
// transient sender bookkeeping: a snapshot is taken at a quiescent point (mailboxes
// drained by Step), so there is no in-flight envelope to persist. An actor whose
// child machine is a non-Instance ActorInstance (a host's own ActorInstance that
// does not implement Snapshotter) is captured as a present-but-opaque entry and
// re-spawned fresh on restore rather than resumed; this is the only actor-tree
// depth deferred, and it is flagged on the entry's Resumed field.

// Snapshotter is implemented by an ActorInstance that can capture and reload its
// own runtime state as JSON, so an ActorSystem can persist it recursively. The
// actorAdapter (the standard wrapper for a child *Instance) satisfies it; a host's
// bespoke ActorInstance may implement it to participate in deep persistence, and an
// ActorInstance that does not is re-spawned fresh on restore rather than resumed.
type Snapshotter interface {
	// SnapshotJSON captures the actor's runtime state as JSON.
	SnapshotJSON() ([]byte, error)
	// RestoreJSON reloads the actor's runtime state from JSON produced by
	// SnapshotJSON, resuming the actor in place without re-running entry actions.
	RestoreJSON([]byte) error
}

// actorSnapshot is the per-actor envelope an ActorSystem records for one running
// child actor: its spawn coordinates (so RestoreActors can re-spawn it from the
// palette), its captured child state, whether that state is resumable, and the
// recursive snapshots of its own nested children.
type actorSnapshot struct {
	// ID, Src, SystemID, State, and Input are the spawn coordinates, mirroring the
	// SpawnActor effect that created the actor, so RestoreActors re-spawns it
	// identically before reloading its state.
	ID       string         `json:"id"`
	Src      string         `json:"src"`
	SystemID string         `json:"systemId,omitempty"`
	State    string         `json:"state,omitempty"`
	Input    map[string]any `json:"input,omitempty"`
	// OnDone / OnError carry the routing events as JSON so completion still routes
	// to the parent after restore.
	OnDone  json.RawMessage `json:"onDone,omitempty"`
	OnError json.RawMessage `json:"onError,omitempty"`
	// Child is the actor's own captured runtime state (from Snapshotter), or empty
	// when the actor does not implement Snapshotter.
	Child json.RawMessage `json:"child,omitempty"`
	// Resumed reports whether Child holds resumable state: true when the actor
	// implements Snapshotter and was captured, false when it will be re-spawned
	// fresh on restore (the only deferred actor-tree depth).
	Resumed bool `json:"resumed"`
	// Done reports whether the actor had already reached its final state and been
	// settled at snapshot time, so RestoreActors does not re-spawn a completed actor.
	Done bool `json:"done"`
	// Mailbox is the reserved slot for the actor's mailbox backlog (queued but
	// unprocessed envelopes). Empty at this version under the quiescence assumption
	// (mailboxes are drained by Step before a snapshot is taken), present so a future
	// distributed/async resume — where a node can crash mid-delivery — has a place to
	// carry the backlog without a format break. This reserves capacity for the
	// mailbox-loss gap documented above.
	Mailbox []json.RawMessage `json:"mailbox,omitempty"`
	// Children is the recursive snapshots of this actor's own spawned children.
	Children []actorSnapshot `json:"children,omitempty"`
}

// SnapshotJSON captures the wrapped child instance's full runtime state as JSON,
// satisfying Snapshotter so an ActorSystem can persist the actor recursively. The
// child resumes from exactly this state on RestoreJSON, without re-running entry
// actions.
func (a *actorAdapter[S, E, C]) SnapshotJSON() ([]byte, error) {
	snap := a.inst.Snapshot()
	return json.Marshal(snap)
}

// RestoreJSON reloads the wrapped child instance's runtime state from JSON,
// resuming the actor in place: it restores the child's configuration, context, and
// history onto a fresh instance of the same machine and swaps it in, without
// re-running entry actions.
func (a *actorAdapter[S, E, C]) RestoreJSON(b []byte) error {
	var snap Snapshot[S, E, C]
	if err := json.Unmarshal(b, &snap); err != nil {
		return &SnapshotError{Op: "unmarshal", Reason: "actor child decode failed: " + err.Error()}
	}
	restored, err := a.inst.machine.Restore(snap, WithRestoreClock[S](a.inst.clock))
	if err != nil {
		return err
	}
	a.inst = restored
	return nil
}

// SnapshotActors captures the runtime state of every live child actor the system
// runs, recursively (each actor's own spawned children are captured beneath it),
// as a JSON document keyed by actor id. It is the actor-tree companion to
// Instance.Snapshot: a host that persists a parent instance also calls
// SnapshotActors to persist the parent's spawned children, and stores the result
// under the parent snapshot's Actors map. It is a pure read of the system's actor
// registry and never fires or mutates an actor.
//
// Call it at a quiescent point (after draining mailboxes with Step), so no
// in-flight mailbox backlog is lost. An actor whose ActorInstance does not
// implement Snapshotter is recorded as present but not resumable (Resumed false)
// and is re-spawned fresh on RestoreActors.
func (s *ActorSystem[S, E, C]) SnapshotActors() (map[string]json.RawMessage, error) {
	s.mu.Lock()
	roots := make([]string, 0, len(s.actors))
	childSet := map[string]bool{}
	for _, ra := range s.actors {
		for _, c := range ra.children {
			childSet[c] = true
		}
	}
	for id := range s.actors {
		if !childSet[id] {
			roots = append(roots, id)
		}
	}
	s.mu.Unlock()

	out := map[string]json.RawMessage{}
	for _, id := range roots {
		snap, ok, err := s.snapshotActor(id)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		b, err := json.Marshal(snap)
		if err != nil {
			return nil, &SnapshotError{Op: "marshal", Reason: "actor encode failed: " + err.Error()}
		}
		out[id] = b
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// snapshotActor captures one actor and recurses through its children. ok is false
// when no actor is registered under id (a stale child reference).
func (s *ActorSystem[S, E, C]) snapshotActor(id string) (actorSnapshot, bool, error) {
	s.mu.Lock()
	ra, ok := s.actors[id]
	if !ok {
		s.mu.Unlock()
		return actorSnapshot{}, false, nil
	}
	snap := actorSnapshot{
		ID:       id,
		Src:      ra.ref.Src,
		SystemID: ra.ref.SystemID,
		State:    ra.state,
		Done:     ra.done,
	}
	if ra.hasDone {
		if b, err := json.Marshal(ra.onDone); err == nil {
			snap.OnDone = b
		}
	}
	if ra.hasError {
		if b, err := json.Marshal(ra.onError); err == nil {
			snap.OnError = b
		}
	}
	inst := ra.inst
	children := append([]string(nil), ra.children...)
	s.mu.Unlock()

	if sn, ok := inst.(Snapshotter); ok {
		b, err := sn.SnapshotJSON()
		if err != nil {
			return actorSnapshot{}, false, &SnapshotError{Op: "marshal", State: id, Reason: err.Error()}
		}
		snap.Child = b
		snap.Resumed = true
	}

	for _, c := range children {
		cs, cok, err := s.snapshotActor(c)
		if err != nil {
			return actorSnapshot{}, false, err
		}
		if cok {
			snap.Children = append(snap.Children, cs)
		}
	}
	return snap, true, nil
}

// RestoreActors re-establishes the system's child actors from the snapshots
// SnapshotActors produced, recursively: each actor is re-spawned from the system's
// palette under its original id, its captured state reloaded (resuming it in place
// without re-running entry actions) when it was resumable, and its nested children
// restored beneath it. A not-yet-done actor whose Src does not resolve against the
// palette is skipped (the host registered a different palette); a done actor is not
// re-spawned. Register the same child-machine behaviors before calling it, exactly
// as for the original Absorb.
//
// An actor recorded as not resumable (its ActorInstance did not implement
// Snapshotter) is re-spawned fresh rather than resumed — the one deferred
// actor-tree depth, flagged on the snapshot's Resumed field.
func (s *ActorSystem[S, E, C]) RestoreActors(ctx context.Context, actors map[string]json.RawMessage) error {
	for _, raw := range actors {
		var snap actorSnapshot
		if err := json.Unmarshal(raw, &snap); err != nil {
			return &SnapshotError{Op: "unmarshal", Reason: "actor decode failed: " + err.Error()}
		}
		if err := s.restoreActor(ctx, snap); err != nil {
			return err
		}
	}
	return nil
}

// restoreActor re-spawns one actor and recurses through its children.
func (s *ActorSystem[S, E, C]) restoreActor(ctx context.Context, snap actorSnapshot) error {
	if snap.Done {
		return nil
	}
	s.mu.Lock()
	behavior, ok := s.palette[snap.Src]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	inst, err := behavior(snap.Input)
	if err != nil {
		return &SnapshotError{Op: "restore", State: snap.ID, Reason: "actor re-spawn failed: " + err.Error()}
	}

	ra := &runningActor[E]{
		inst:  inst,
		ref:   ActorRef{ID: snap.ID, SystemID: snap.SystemID, Src: snap.Src},
		state: snap.State,
	}
	if len(snap.OnDone) > 0 {
		var ev E
		if json.Unmarshal(snap.OnDone, &ev) == nil {
			ra.onDone = ev
			ra.hasDone = true
		}
	}
	if len(snap.OnError) > 0 {
		var ev E
		if json.Unmarshal(snap.OnError, &ev) == nil {
			ra.onError = ev
			ra.hasError = true
		}
	}

	if snap.Resumed && len(snap.Child) > 0 {
		if sn, ok := inst.(Snapshotter); ok {
			if err := sn.RestoreJSON(snap.Child); err != nil {
				return err
			}
		}
	}

	s.mu.Lock()
	s.actors[snap.ID] = ra
	if snap.SystemID != "" {
		s.bySystem[snap.SystemID] = snap.ID
	}
	s.mu.Unlock()

	for _, c := range snap.Children {
		if err := s.restoreActor(ctx, c); err != nil {
			return err
		}
		s.mu.Lock()
		if parent, ok := s.actors[snap.ID]; ok {
			if _, live := s.actors[c.ID]; live {
				parent.children = append(parent.children, c.ID)
			}
		}
		s.mu.Unlock()
	}
	return nil
}
