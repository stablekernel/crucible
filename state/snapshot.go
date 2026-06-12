package state

import (
	"encoding/json"
	"fmt"
)

// This file defines deep persistence for a running Instance: capturing its full
// runtime state into a serializable Snapshot and restoring an Instance that
// resumes from exactly that point. It is the instance-runtime analog of the IR's
// ToJSON / LoadFromJSON, which persist the MACHINE DEFINITION; a Snapshot
// persists the INSTANCE's runtime state — its active configuration, context,
// recorded history, status, and the metadata needed to re-arm its pending timers,
// invoked services, and spawned actors.
//
// The model captures instance persistence: a restored instance resumes at its persisted
// configuration WITHOUT re-running entry actions (resume, not re-enter), and the
// host re-establishes the instance's pending invoked/spawned children and timers
// by absorbing the re-arm effects ResumeEffects emits — the same StartService /
// SpawnActor / ScheduleAfter effects the host already drives for Fire and the
// initial StartEffects. Fire stays pure: Snapshot is a read and Restore rebuilds
// the instance without firing.

// Status classifies a snapshotted instance's lifecycle. It mirrors the
// runtime status. StatusRunning is an instance still advancing; StatusDone is an
// instance whose active configuration is entirely final (every active leaf is a
// final state); StatusError is an instance the host settled as failed, carrying
// the error message on the snapshot.
type Status int

// Instance lifecycle statuses recorded on a Snapshot.
const (
	// StatusRunning is the default: the instance has not reached completion.
	StatusRunning Status = iota
	// StatusDone marks an instance whose whole active configuration is final.
	StatusDone
	// StatusError marks an instance the host explicitly failed; Snapshot.Error
	// carries the message.
	StatusError
)

// String renders a Status for diagnostics and stable JSON.
func (s Status) String() string {
	switch s {
	case StatusDone:
		return "done"
	case StatusError:
		return "error"
	default:
		return "running"
	}
}

// Snapshot is the serializable, deep runtime state of one Instance at a point in
// time. It captures the active configuration (all active leaves, in declaration
// order, plus the primary leaf), the recorded per-compound history (shallow and
// deep), the instance context, the lifecycle status and optional output/error,
// and the metadata of the pending timers, invoked services, and spawned actors so
// a host can re-arm them on restore. Child-actor snapshots are carried under
// Actors when an ActorSystem snapshots the instance's spawned children
// recursively.
//
// A Snapshot round-trips losslessly through JSON when the context type C is
// JSON-marshalable (the default requirement) or a context codec is supplied via
// WithContextCodec. The machine definition is NOT carried here — restore binds the
// snapshot back to a live Machine, exactly as Cast binds an entity — so a snapshot
// stays small and a definition change is detected at restore rather than silently
// absorbed.
type Snapshot[S comparable, E comparable, C any] struct {
	// Machine names the machine the snapshot was taken from. Restore rejects a
	// snapshot whose Machine does not match the target machine with a typed
	// *SnapshotError, so a snapshot is never restored against the wrong definition.
	Machine string `json:"machine"`

	// Current is the primary (first) active leaf — the back-compatible
	// "what state am I in?" answer, equal to Configuration[0].
	Current S `json:"current"`
	// Configuration is every currently-active leaf, in declaration order: length 1
	// for a flat or single-spine instance, length N when N parallel regions are
	// active. Restore activates exactly this configuration without re-entering it.
	Configuration []S `json:"configuration"`

	// Context is the instance's bound entity C at snapshot time. With the default
	// codec it must be JSON-marshalable; with WithContextCodec the supplied codec
	// owns its encoding. In JSON it is held as a raw message so the snapshot
	// envelope marshals once and the context decodes through the chosen codec.
	Context C `json:"-"`
	// ContextRaw is the JSON (or codec-encoded) form of Context, populated when the
	// snapshot is marshaled and consumed when it is unmarshaled. It is the wire
	// form of Context; callers read Context, not ContextRaw.
	ContextRaw json.RawMessage `json:"context,omitempty"`

	// HistoryShallow records each compound's last-active direct child, and
	// HistoryDeep each compound's last-active leaf configuration, for history
	// pseudo-state restoration. Both are restored verbatim so a history-targeted
	// transition after restore behaves identically to before the snapshot.
	HistoryShallow map[S]S   `json:"historyShallow,omitempty"`
	HistoryDeep    map[S][]S `json:"historyDeep,omitempty"`

	// Traces is the instance's recorded Fire history, preserved so History()
	// reports the same ordered traces after restore. Traces are stored in
	// chronological order (oldest first), as History() returns them, so a restored
	// ring-buffer instance resumes with its window already in order and HistHead 0.
	Traces []Trace `json:"traces,omitempty"`

	// HistLimit is the bounded-history retention cap (WithHistory(n)) the instance
	// was running under at snapshot time: the ring-buffer capacity. It is persisted
	// so a restored instance keeps the SAME bound rather than silently becoming an
	// instance with no live retention (frozen on the snapshot traces) or an
	// unbounded one. Zero means no bounded-ring retention was configured (the
	// instance was unbounded or had no retention); a positive value is the ring
	// capacity restored verbatim. The ring head is not serialized: Traces are stored
	// chronologically, so a restored full ring resumes with HistHead 0 (its oldest
	// entry at index 0).
	HistLimit int `json:"histLimit,omitempty"`

	// Status is the instance's lifecycle status at snapshot time. Output carries an
	// instance's completion output (when StatusDone) and Error a settled instance's
	// failure message (when StatusError); both are optional and host-supplied.
	Status Status          `json:"status"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`

	// Pending records the IDs/metadata of the timers, invoked services, and spawned
	// actors that were live for the active configuration, so a host can confirm
	// what ResumeEffects re-arms. It is descriptive: the authoritative re-arm is the
	// effect slice ResumeEffects returns, derived from the same configuration.
	Pending PendingRefs `json:"pending,omitempty"`

	// Actors carries the recursively-captured snapshots of the instance's spawned
	// child actors, keyed by actor id, when an ActorSystem snapshots the instance.
	// Each entry is an opaque per-child snapshot envelope a matching ActorSystem
	// restores. It is empty for an instance with no spawned children, or when only
	// the instance core (not the actor tree) is snapshotted.
	Actors map[string]json.RawMessage `json:"actors,omitempty"`

	// SnapshotVersion is the snapshot-format schema version of this envelope, so the
	// serialization contract can evolve with explicit, detectable versions. Snapshot
	// stamps it with CurrentSnapshotVersion; Restore validates it under the lenient
	// restore-version posture (accept within the current major, reject across a major
	// mismatch). A zero value is a pre-versioning snapshot and is treated as the
	// current version on restore.
	SnapshotVersion int `json:"snapshotVersion,omitempty"`
	// MachineVersion is the machine DEFINITION version (the IR Version) the snapshot
	// was taken from, stamped alongside the Machine name so a restored instance
	// self-identifies which version of the machine it belongs to — the precondition
	// for live migration. It is advisory by default at restore (recorded, surfaced,
	// not enforced) so version stamping is non-breaking; RejectMachineVersionMismatch
	// opts into strict rejection.
	MachineVersion string `json:"machineVersion,omitempty"`
	// MachineID is the machine definition id (the IR ID), carried alongside
	// MachineVersion so a migrator can resolve the source definition unambiguously.
	MachineID string `json:"machineId,omitempty"`

	// Journal is the reserved replay journal: the per-step record of external,
	// nondeterministic results (invoked-service done-output, actor messages, clock
	// reads, randomness) so a future deterministic replay returns the recorded value
	// rather than re-invoking the source. It is empty at this version under the
	// recording contract documented on JournalEntry; the runtime that populates and
	// consumes it is host-side. Reserved and optional: it round-trips empty and
	// populated.
	Journal []JournalEntry `json:"journal,omitempty"`

	// InFlightServices is the reserved slot for invoked services that were started
	// but not yet resolved at snapshot time (id + input + the OnDone/OnError routing
	// events), so a future distributed/async resume can re-establish them. Empty at
	// this version under the quiescence assumption; present so resume never needs a
	// new field.
	InFlightServices []InFlightService `json:"inFlightServices,omitempty"`
	// Mailboxes is the reserved slot for per-actor mailbox backlog (queued but
	// unprocessed envelopes), keyed by actor id, for a future distributed/async
	// resume where a node can crash mid-delivery. Empty at this version under the
	// quiescence assumption (mailboxes are drained at a snapshot point); present so a
	// backlog never needs a new field. This closes the documented mailbox-loss gap in
	// the actor-tree snapshot.
	Mailboxes map[string][]json.RawMessage `json:"mailboxes,omitempty"`
}

// CurrentSnapshotVersion is the snapshot-format schema version stamped by
// Snapshot and validated by Restore. It is the major.minor schema generation of
// the Snapshot envelope encoded as major*1000 + minor, so a single int both orders
// versions and exposes the major for the restore-version posture: a snapshot is
// restorable within the same major (snapshotMajor), and a major mismatch is
// rejected. Version 1 is (1*1000 + 0); a future additive field bumps the minor, a
// breaking change bumps the major.
const CurrentSnapshotVersion = 1 * snapshotMajorScale

// snapshotMajorScale is the multiplier separating the snapshot-version major from
// its minor in the single CurrentSnapshotVersion int.
const snapshotMajorScale = 1000

// snapshotMajor returns the major component of a snapshot-format version, used by
// the restore-version posture to accept within a major and reject across one.
func snapshotMajor(v int) int { return v / snapshotMajorScale }

// JournalKind classifies a JournalEntry's recorded nondeterministic result, so a
// replay routes each recorded value back to the source that produced it.
type JournalKind string

// JournalKind values, one per nondeterministic source the replay contract covers.
const (
	// JournalServiceResult records an invoked service's OnDone/OnError result
	// payload, correlated by its invocationID.
	JournalServiceResult JournalKind = "serviceResult"
	// JournalActorMessage records an actor message payload, correlated by the
	// actorInvocationID of the routed actor.
	JournalActorMessage JournalKind = "actorMessage"
	// JournalClockRead records a Clock.Now() reading consumed during a step.
	JournalClockRead JournalKind = "clockRead"
	// JournalRandom records a host randomness draw consumed during a step. Like a
	// service result, the recorded draw rides the shared JournalEntry.Payload field
	// (the structured JSON value the host consumed) — the variant needs no dedicated
	// backing field. Replay returns Payload verbatim so the draw resolves
	// identically, correlated by CorrelationID to the draw it stands in for.
	JournalRandom JournalKind = "random"
)

// JournalEntry records one external, nondeterministic resolution so a future
// deterministic replay returns the recorded value rather than re-invoking its
// source. It is the unit of the reserved Snapshot.Journal.
//
// The recording contract (locked here; the recording/replay runtime is host-side):
// any result that is NOT a pure function of (current configuration, context, event
// payload, machine definition) is nondeterministic and MUST be recordable as a
// JournalEntry so replay returns the recorded value. The nondeterministic sources
// are the invoked-service OnDone/OnError result payloads, actor message payloads,
// Clock.Now() reads, and host randomness — each correlated by a stable id reused
// from the effect that armed it (invocationID / actorInvocationID / scheduleID).
type JournalEntry struct {
	// Step is the Fire ordinal the result resolved at, indexing the instance's
	// recorded Traces, so replay applies the recorded value at the right step.
	Step int `json:"step"`
	// Kind classifies which nondeterministic source produced the result.
	Kind JournalKind `json:"kind"`
	// CorrelationID is the stable id of the source, reused from the arming effect
	// (invocationID / actorInvocationID / scheduleID), so replay matches the
	// recorded value to the resolution it stands in for.
	CorrelationID string `json:"correlationId,omitempty"`
	// Payload is the structured, JSON result the source produced (a service's
	// done-output, an actor message, or a JournalRandom draw), returned verbatim on
	// replay.
	Payload json.RawMessage `json:"payload,omitempty"`
	// ClockUnixNano is the recorded Clock.Now() reading (Unix nanoseconds) for a
	// JournalClockRead entry, returned on replay so time-dependent transitions
	// resolve identically.
	ClockUnixNano int64 `json:"clockUnixNano,omitempty"`
}

// InFlightService is the reserved record of an invoked service started but not yet
// resolved at snapshot time, so a future distributed/async resume can re-establish
// it. It mirrors the StartService effect's coordinates: the invocation id, the
// service src name, the input, and the OnDone/OnError routing event labels.
type InFlightService struct {
	// ID is the invocationID of the started service, the stable correlation id a
	// resolving JournalEntry reuses.
	ID string `json:"id"`
	// Src is the service src name (the registry key) the host re-starts.
	Src string `json:"src,omitempty"`
	// Input is the structured input the service was started with.
	Input json.RawMessage `json:"input,omitempty"`
	// OnDone and OnError are the routing event labels the host re-fires the result
	// through after the service resolves.
	OnDone  string `json:"onDone,omitempty"`
	OnError string `json:"onError,omitempty"`
}

// PendingRefs is the descriptive inventory of an instance's live timers, invoked
// services, and spawned actors at snapshot time, by stable ID. It mirrors what
// ResumeEffects re-arms; a host can assert on it or display it without replaying
// effects.
type PendingRefs struct {
	// Timers are the schedule IDs of the pending delayed (`after`) transitions
	// armed for the active configuration.
	Timers []string `json:"timers,omitempty"`
	// Services are the IDs of the invoked services running for the active
	// configuration.
	Services []string `json:"services,omitempty"`
	// Actors are the IDs of the child-machine actors invoked for the active
	// configuration.
	Actors []string `json:"actors,omitempty"`
}

// ContextCodec encodes and decodes an instance context C to and from bytes for a
// Snapshot, for a context type that is not directly JSON-marshalable (or needs a
// custom wire form). Encode is called by Snapshot.MarshalJSON; Decode by
// Snapshot.UnmarshalJSON. When no codec is supplied, the default codec marshals C
// with encoding/json, so C must be JSON-marshalable by default.
//
// ContextCodec is a FROZEN, host-implementable interface: its method set is
// LOCKED at v1.0 and no method will be added to it. Post-v1 capabilities ship as a
// SEPARATE optional interface discovered by type-asserting a ContextCodec value
// (the io.Reader/io.ReaderAt idiom), never by widening this one — so a host's
// codec keeps compiling across minor versions.
type ContextCodec[C any] interface {
	Encode(C) ([]byte, error)
	Decode([]byte) (C, error)
}

// jsonCodec is the default ContextCodec: it marshals and unmarshals C with
// encoding/json. It is used whenever no WithContextCodec is supplied, so the
// documented default requirement is that C is JSON-marshalable.
type jsonCodec[C any] struct{}

func (jsonCodec[C]) Encode(c C) ([]byte, error) { return json.Marshal(c) }

func (jsonCodec[C]) Decode(b []byte) (C, error) {
	var c C
	if len(b) == 0 {
		return c, nil
	}
	err := json.Unmarshal(b, &c)
	return c, err
}

// Snapshot captures the instance's full runtime state into a serializable
// Snapshot: the active configuration, recorded history, context, lifecycle
// status, and the IDs of the pending timers / services / actors armed for the
// active configuration. It is a pure read — it never fires, mutates the instance,
// or consults a clock — so Fire stays pure and a snapshot may be taken at any
// quiescent point between Fires.
//
// The returned Snapshot's Context holds the live entity value; serialize the
// whole snapshot with MarshalSnapshot (or json.Marshal once the default codec
// suffices) to obtain the wire form. Status is derived from the active
// configuration (StatusDone when the whole configuration is final, else
// StatusRunning); a host that tracks an explicit failure sets StatusError and
// Error on the returned snapshot before persisting.
func (i *Instance[S, E, C]) Snapshot() Snapshot[S, E, C] {
	cfg := i.Configuration()
	snap := Snapshot[S, E, C]{
		Machine:         i.machine.name,
		Current:         i.current,
		Configuration:   cfg,
		Context:         i.entity,
		HistoryShallow:  copyMap(i.historyShallow),
		HistoryDeep:     copyLeafMap(i.historyDeep),
		Traces:          i.History(),
		HistLimit:       i.histLimit,
		Status:          StatusRunning,
		SnapshotVersion: CurrentSnapshotVersion,
		MachineVersion:  i.machine.envelope.version,
		MachineID:       i.machine.envelope.id,
	}
	if i.InFinal() {
		snap.Status = StatusDone
	}
	snap.Pending = i.pendingRefs(cfg)
	return snap
}

// pendingRefs inventories the stable IDs of the timers, services, and actors
// armed for the configuration cfg, mirroring what ResumeEffects re-arms. It is a
// pure read of the machine definition against the active configuration.
func (i *Instance[S, E, C]) pendingRefs(cfg []S) PendingRefs {
	var p PendingRefs
	m := i.machine
	for _, s := range cfg {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ti := range n.state.Transitions {
			if n.state.Transitions[ti].After != nil {
				p.Timers = append(p.Timers, scheduleID(m.name, s, ti))
			}
		}
		for ix := range n.state.Invoke {
			inv := &n.state.Invoke[ix]
			if inv.Kind == ActorKindMachine {
				p.Actors = append(p.Actors, actorInvocationID(m.name, s, ix, inv))
			} else {
				p.Services = append(p.Services, invocationID(m.name, s, ix, inv))
			}
		}
	}
	return p
}

// ResumeEffects returns the re-arm effects a host absorbs after Restore to
// re-establish the instance's pending timers, invoked services, and spawned
// actors for its restored configuration: a ScheduleAfter per pending delayed
// transition, a StartService per invoked service, and a SpawnActor per
// child-machine actor invocation active in the configuration. It is the restore
// twin of StartEffects (which arms an initial Cast configuration) extended with
// the delayed-timer effects, so a restored instance
// re-establishes its invoked/spawned children. Like StartEffects it is a pure
// read of the configuration and emits no IO; route the effects through the same
// Scheduler / ServiceRunner / ActorSystem the host drives for Fire.
//
// Entry actions are NOT re-run: ResumeEffects emits only the lifecycle re-arm
// effects, never the states' OnEntry actions, so a restored instance resumes
// rather than re-enters.
func (i *Instance[S, E, C]) ResumeEffects() []Effect {
	var tr Trace
	cfg := i.Configuration()
	out := i.afterEffectsOnEntry(cfg, &tr)
	out = append(out, i.invokeEffectsOnEntry(cfg, &tr)...)
	out = append(out, i.actorEffectsOnEntry(cfg, &tr)...)
	return out
}

// Restore rebuilds a running Instance from snap, resuming at the snapshot's
// configuration, context, and recorded history WITHOUT re-running any entry
// actions (resume, not re-enter). The restored instance picks up at the
// persisted snapshot. The snapshot's Machine must match m's name, every configuration leaf
// must be a declared state, and the configuration must be non-empty; a violation
// returns a typed *SnapshotError. The restored instance is wired to the supplied
// clock (WithRestoreClock) or SystemClock by default, exactly as Cast wires it.
//
// After Restore, a host that drove timers/services/actors re-arms them by
// absorbing the instance's ResumeEffects through the same drivers it uses for
// Fire — Restore itself fires nothing and performs no IO, so Fire stays pure.
//
// A snapshot is pure data and carries no live observability seam, so a plain
// Restore re-attaches neither an Inspector nor a *slog.Logger and the restored
// instance is silent. Pass WithRestoreInspector / WithRestoreLogger to re-arm
// those seams, mirroring WithInspector / WithLogger at Cast.
func (m *Machine[S, E, C]) Restore(snap Snapshot[S, E, C], opts ...RestoreOption[S]) (*Instance[S, E, C], error) {
	if snap.Machine != m.name {
		return nil, &SnapshotError{
			Op:     "restore",
			Reason: fmt.Sprintf("snapshot machine %q does not match target machine %q", snap.Machine, m.name),
		}
	}

	rcfg := restoreConfig[S]{}
	for _, o := range opts {
		o(&rcfg)
	}

	// Lenient restore-version posture (frozen at v1): validate the snapshot-format
	// schema version. A zero version is a pre-versioning snapshot, treated as the
	// current version. A version within the current major is accept-and-upgrade; a
	// major mismatch (newer or older) is rejected with a typed *SnapshotVersionError.
	sv := snap.SnapshotVersion
	if sv == 0 {
		sv = CurrentSnapshotVersion
	}
	if sv < 0 || snapshotMajor(sv) != snapshotMajor(CurrentSnapshotVersion) {
		return nil, &SnapshotVersionError{
			Kind:    "snapshotFormat",
			Machine: m.name,
			Got:     fmt.Sprintf("%d", snap.SnapshotVersion),
			Want:    fmt.Sprintf("major %d", snapshotMajor(CurrentSnapshotVersion)),
			Reason:  "snapshot format major version is incompatible with this build",
		}
	}

	// Machine definition version is advisory by default (recorded, not enforced) so
	// version stamping is non-breaking; RejectMachineVersionMismatch opts into a
	// strict reject across a differing definition version.
	if rcfg.rejectMachineVersion && snap.MachineVersion != "" && snap.MachineVersion != m.envelope.version {
		return nil, &SnapshotVersionError{
			Kind:    "machineVersion",
			Machine: m.name,
			Got:     snap.MachineVersion,
			Want:    m.envelope.version,
			Reason:  "snapshot machine version does not match target machine version",
		}
	}

	cfg := snap.Configuration
	if len(cfg) == 0 {
		if _, ok := m.resolveNode(snap.Current); !ok {
			return nil, &SnapshotError{Op: "restore", Reason: "snapshot has empty configuration and unknown current state"}
		}
		cfg = []S{snap.Current}
	}
	for _, leaf := range cfg {
		if _, ok := m.resolveNode(leaf); !ok {
			return nil, &SnapshotError{
				Op:     "restore",
				State:  fmtState(leaf),
				Reason: fmt.Sprintf("configuration leaf %q is not a declared state of machine %q", fmtState(leaf), m.name),
			}
		}
	}

	clock := rcfg.clock
	if clock == nil {
		clock = systemClock{}
	}

	current := snap.Current
	if _, ok := m.resolveNode(current); !ok {
		current = cfg[0]
	}

	// Restore the bounded-history retention cap so a WithHistory(n) instance stays
	// bounded across the round-trip instead of silently becoming an instance with no
	// live retention (frozen on the snapshot traces) or an unbounded one. Bounded
	// retention implies full trace, mirroring the Cast-time elevation (a retained
	// trace must carry its rich fields); the unbounded restore option already
	// elevates the same way. The ring head is not persisted: snap.Traces are stored
	// chronologically, so a restored full ring resumes with histHead 0 — its oldest
	// entry at index 0, exactly what History() expects.
	histLimit := snap.HistLimit
	// An attached inspector or any history retention elevates the restored instance
	// to full trace, mirroring the Cast-time elevation. A re-attached logger alone
	// stays lite (it reads only always-present lite fields), exactly like WithLogger.
	traceFull := rcfg.traceFull || histLimit > 0 || rcfg.inspector != nil

	inst := &Instance[S, E, C]{
		machine:        m,
		entity:         snap.Context,
		current:        current,
		config:         append([]S(nil), cfg...),
		history:        append([]Trace(nil), snap.Traces...),
		historyShallow: copyMap(snap.HistoryShallow),
		historyDeep:    copyLeafMap(snap.HistoryDeep),
		clock:          clock,
		traceFull:      traceFull,
		histLimit:      histLimit,
		histUnbounded:  rcfg.histUnbounded,
		inspector:      rcfg.inspector,
		logger:         rcfg.logger,
	}
	return inst, nil
}

// MarshalSnapshot serializes snap to JSON, encoding its context through codec (or
// the default JSON codec when codec is nil). It is the explicit serialization
// entry point when a non-JSON-marshalable context needs a custom codec; for a
// JSON-marshalable context, json.Marshal(snap) works directly via the snapshot's
// own MarshalJSON.
func MarshalSnapshot[S comparable, E comparable, C any](snap Snapshot[S, E, C], opts ...SnapshotCodecOption[C]) ([]byte, error) {
	codec := resolveCodec(opts...)
	raw, err := codec.Encode(snap.Context)
	if err != nil {
		return nil, &SnapshotError{Op: "marshal", Reason: "context encode failed: " + err.Error(), Cause: err}
	}
	snap.ContextRaw = raw
	return json.Marshal(snapshotWire[S, E, C](snap))
}

// UnmarshalSnapshot deserializes a snapshot from JSON, decoding its context
// through codec (or the default JSON codec when codec is nil). It is the inverse
// of MarshalSnapshot; for a JSON-marshalable context, json.Unmarshal into a
// Snapshot works directly via the snapshot's own UnmarshalJSON.
func UnmarshalSnapshot[S comparable, E comparable, C any](b []byte, opts ...SnapshotCodecOption[C]) (Snapshot[S, E, C], error) {
	codec := resolveCodec(opts...)
	var wire snapshotWire[S, E, C]
	if err := json.Unmarshal(b, &wire); err != nil {
		return Snapshot[S, E, C]{}, &SnapshotError{Op: "unmarshal", Reason: err.Error(), Cause: err}
	}
	snap := Snapshot[S, E, C](wire)
	ctx, err := codec.Decode(snap.ContextRaw)
	if err != nil {
		return Snapshot[S, E, C]{}, &SnapshotError{Op: "unmarshal", Reason: "context decode failed: " + err.Error(), Cause: err}
	}
	snap.Context = ctx
	return snap, nil
}

// snapshotWire is Snapshot without the custom JSON methods, so MarshalSnapshot /
// UnmarshalSnapshot serialize the envelope (including the already-encoded
// ContextRaw) without re-entering the context codec through MarshalJSON.
type snapshotWire[S comparable, E comparable, C any] Snapshot[S, E, C]

// MarshalJSON serializes the snapshot, encoding its context with the default JSON
// codec. It is the convenient path for a JSON-marshalable context; a context that
// needs a custom codec is serialized with MarshalSnapshot(snap, WithContextCodec).
func (snap Snapshot[S, E, C]) MarshalJSON() ([]byte, error) {
	raw, err := json.Marshal(snap.Context)
	if err != nil {
		return nil, &SnapshotError{Op: "marshal", Reason: "context encode failed: " + err.Error(), Cause: err}
	}
	snap.ContextRaw = raw
	return json.Marshal(snapshotWire[S, E, C](snap))
}

// UnmarshalJSON deserializes the snapshot, decoding its context with the default
// JSON codec. The inverse of MarshalJSON.
func (snap *Snapshot[S, E, C]) UnmarshalJSON(b []byte) error {
	var wire snapshotWire[S, E, C]
	if err := json.Unmarshal(b, &wire); err != nil {
		return &SnapshotError{Op: "unmarshal", Reason: err.Error(), Cause: err}
	}
	*snap = Snapshot[S, E, C](wire)
	if len(snap.ContextRaw) == 0 {
		return nil
	}
	var ctx C
	if err := json.Unmarshal(snap.ContextRaw, &ctx); err != nil {
		return &SnapshotError{Op: "unmarshal", Reason: "context decode failed: " + err.Error(), Cause: err}
	}
	snap.Context = ctx
	return nil
}

// resolveCodec returns the supplied context codec or the default JSON codec.
func resolveCodec[C any](opts ...SnapshotCodecOption[C]) ContextCodec[C] {
	cfg := snapshotCodecConfig[C]{}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.codec != nil {
		return cfg.codec
	}
	return jsonCodec[C]{}
}

// copyMap returns a shallow copy of a state->state map, or nil for an empty
// source, so a snapshot never aliases the instance's live history maps.
func copyMap[S comparable](in map[S]S) map[S]S {
	if len(in) == 0 {
		return nil
	}
	out := make(map[S]S, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// copyLeafMap returns a deep copy of a state->[]state map, cloning each leaf
// slice so a snapshot never aliases the instance's live deep-history slices.
func copyLeafMap[S comparable](in map[S][]S) map[S][]S {
	if len(in) == 0 {
		return nil
	}
	out := make(map[S][]S, len(in))
	for k, v := range in {
		out[k] = append([]S(nil), v...)
	}
	return out
}
