package state

import (
	"context"
	"fmt"
	"time"
)

// This file defines the delayed-transition (`after`) scheduler contract: the
// effects the kernel emits so a host runtime can drive timed transitions, the
// clock seam used by drivers, and the host-driver model. The kernel itself never
// reads a clock and never sleeps — Fire stays pure. Entering a state that owns
// `after` transitions emits a ScheduleAfter effect per delayed edge; exiting that
// state emits a CancelScheduled effect per pending timer
// auto-cancel-on-exit); a host's runtime owns the real timer and re-fires the
// delayed event back through Fire when it elapses.

// ScheduleAfter is the effect the kernel emits when an instance enters a state
// that declares a delayed (`after`) transition. The host's runtime is expected
// to start a timer for Delay and, when it elapses, call Fire with Event. ID is
// stable per (instance, source state, delayed edge), so a later CancelScheduled
// with the same ID cancels exactly this timer.
//
// The kernel never starts the timer itself: it emits this as data alongside the
// transition's other effects, keeping Fire pure (no clock, no goroutine, no IO).
type ScheduleAfter struct {
	// ID identifies the pending timer. It is stable across the schedule/cancel
	// pair for one source state on one instance, so a host keys its timer table
	// by ID.
	ID string `json:"id"`
	// Delay is the wall-clock duration the host should wait before re-firing.
	Delay time.Duration `json:"delay"`
	// Event is the delayed event to feed back through Fire when Delay elapses.
	// It is the transition's On event, type-erased for the abstract effect
	// surface; a host driver built with NewScheduler keeps it typed.
	Event any `json:"event,omitempty"`
	// State names the source state whose entry scheduled this timer, for
	// diagnostics and host bookkeeping.
	State string `json:"state,omitempty"`
}

// CancelScheduled is the effect the kernel emits when an instance exits a state
// that had a pending delayed (`after`) timer, or when a Cancel action runs. The
// host cancels the timer registered under ID; canceling an unknown ID is a
// no-op. A state's `after` timers are
// auto-canceled when the state is exited before the delay elapses.
type CancelScheduled struct {
	// ID identifies the timer to cancel. It matches the ID of the ScheduleAfter
	// that armed it (auto-cancel-on-exit), or an ID supplied to Cancel.
	ID string `json:"id"`
}

// cancelBuiltinName is the reserved action ref name for the Cancel built-in. Like
// the stateIn guard built-in, it needs no host registration: the kernel handles
// it directly at Fire time, emitting a CancelScheduled effect from its params.
const cancelBuiltinName = "crucible.cancel"

// cancelIDParam is the params key carrying the schedule ID a Cancel built-in
// cancels.
const cancelIDParam = "id"

// isBuiltinAction reports whether a ref names a kernel built-in action that the
// host registry need not (and must not be required to) provide. Built-in actions
// are exempt from the unbound-ref lint at Quench and handled directly by
// evalAction.
func isBuiltinAction(name string) bool {
	return name == cancelBuiltinName || isActorBuiltinAction(name) || isCommBuiltinAction(name)
}

// evalBuiltinAction runs a kernel built-in action ref, returning its effect. It
// is called only for refs isBuiltinAction reports true for.
func evalBuiltinAction(a Ref) (Effect, error) {
	switch {
	case a.Name == cancelBuiltinName:
		id, _ := a.Params[cancelIDParam].(string)
		return CancelScheduled{ID: id}, nil
	case isActorBuiltinAction(a.Name):
		return evalActorBuiltinAction(a)
	case isCommBuiltinAction(a.Name):
		return evalCommBuiltinAction(a)
	default:
		return nil, &UnknownBuiltinError{Name: a.Name}
	}
}

// scheduleID builds the stable per-instance identifier for the delayed edge at
// index idx on source state `from`. The same (machine, from, idx) always yields
// the same ID within a process, so the schedule emitted on entry and the cancel
// emitted on exit pair up without per-instance bookkeeping in the kernel.
func scheduleID[S comparable](machine string, from S, idx int) string {
	return fmt.Sprintf("%s:%s:after:%d", machine, fmtState(from), idx)
}

// ScheduleID returns the stable schedule identifier the kernel assigns to the
// delayed (`after`) transition at index idx on source state `from` of machine
// `machine`. A host or test uses it to correlate a ScheduleAfter with a later
// Cancel, or to assert which timer a CancelScheduled targets.
func ScheduleID[S comparable](machine string, from S, idx int) string {
	return scheduleID(machine, from, idx)
}

// afterEffectsOnEntry returns the ScheduleAfter effects for every delayed
// (`after`) transition declared on the entered states, in entry order. It reads
// no clock and performs no IO: it only translates declared delays into schedule
// effects for the host to act on.
func (i *Instance[S, E, C]) afterEffectsOnEntry(entries []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range entries {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ti := range n.state.Transitions {
			t := &n.state.Transitions[ti]
			if t.After == nil {
				continue
			}
			id := scheduleID(m.name, s, ti)
			out = append(out, ScheduleAfter{
				ID:    id,
				Delay: *t.After,
				Event: t.On,
				State: fmtState(s),
			})
			tr.note("schedule." + id)
		}
	}
	return out
}

// lifecycleExits expands the structural exit cascade to cover every leaf actually
// leaving the active configuration. The cascade is computed from the primary leaf's
// ancestor chain, so when a transition exits a parallel superstate it names only one
// region's spine; the orthogonal regions' active leaves leave the configuration too.
// This adds any active configuration leaf that descends from an exited state but is
// not already in the exit set, so auto-cancel/auto-stop-on-exit reaches the sibling
// regions' armed timers, in-flight services, and running actors. The structural
// exits keep their order; the extra orthogonal leaves are appended after them.
func (i *Instance[S, E, C]) lifecycleExits(exits []S) []S {
	if len(i.config) <= 1 {
		return exits
	}
	exiting := make(map[S]bool, len(exits))
	for _, s := range exits {
		exiting[s] = true
	}
	// Copy the caller's slice before appending: out is grown with the orthogonal
	// leaves below, and aliasing exits here would let an append overwrite the
	// caller's backing array (or surprise a later caller that still holds exits).
	out := append([]S(nil), exits...)
	m := i.machine
	for _, leaf := range i.config {
		if exiting[leaf] {
			continue
		}
		for _, s := range exits {
			if leaf != s && m.isDescendant(leaf, s) {
				out = append(out, leaf)
				exiting[leaf] = true
				break
			}
		}
	}
	return out
}

// afterEffectsOnExit returns the CancelScheduled effects for every delayed
// (`after`) transition declared on the exited states, in exit order. Emitting a
// cancel for a state that may not have an armed timer is safe: the host treats an
// unknown ID as a no-op; this is auto-cancel-on-exit.
func (i *Instance[S, E, C]) afterEffectsOnExit(exits []S, tr *Trace) []Effect {
	var out []Effect
	m := i.machine
	for _, s := range exits {
		n, ok := m.resolveNode(s)
		if !ok {
			continue
		}
		for ti := range n.state.Transitions {
			t := &n.state.Transitions[ti]
			if t.After == nil {
				continue
			}
			id := scheduleID(m.name, s, ti)
			out = append(out, CancelScheduled{ID: id})
			tr.note("cancel." + id)
		}
	}
	return out
}

// Clock is the deterministic time seam used by host drivers (never by the
// kernel). A real host wires a wall-clock implementation; a test wires a fake
// clock so `after` machines are exercised deterministically. The kernel's Fire
// step never calls a Clock — only effect-consuming drivers do.
//
// Clock is a FROZEN, host-implementable interface: its method set is LOCKED at
// v1.0 and no method will be added to it. Post-v1 capabilities ship as a SEPARATE
// optional interface a driver discovers by type-asserting a Clock value (the
// io.Reader/io.ReaderAt idiom), never by widening this one — so a host's
// implementation keeps compiling across minor versions.
type Clock interface {
	// Now reports the current time.
	Now() time.Time
	// After returns a channel that receives once the duration elapses, mirroring
	// time.After. A driver selects on it to learn when a delayed event is due.
	After(d time.Duration) <-chan time.Time
}

// systemClock is the production Clock backed by the standard library.
type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// SystemClock returns the wall-clock Clock backed by the standard library, for a
// production host driver.
func SystemClock() Clock { return systemClock{} }

// keep context referenced for the driver contract's symmetry with Fire.
var _ = context.Background
