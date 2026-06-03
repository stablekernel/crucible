// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"fmt"
	"sort"

	"github.com/stablekernel/crucible/state"
)

// Conformance is the result of checking a router's (or codec's) accepted event
// union against a machine's event alphabet: the analyzable-consumption answer to
// "what does consuming into this machine actually accept, and what can it never
// trigger?". It is a value a caller asserts on in a build-time test or logs at
// load time.
type Conformance[E comparable] struct {
	// Alphabet is the machine's event alphabet: every event that appears on a
	// declared transition, across all states, substates, and parallel regions,
	// sorted for stable reporting.
	Alphabet []E
	// Accepted is the event union the router/codec declares it can produce, as
	// supplied to [CheckEvents].
	Accepted []E
	// Missing lists alphabet events the accepted union does NOT cover: events the
	// machine can act on but this consumer will never deliver. A non-empty Missing
	// is usually a gap — a transition no inbound message can ever drive.
	Missing []E
	// Unreachable lists accepted events that are NOT in the machine's alphabet: events
	// the consumer can deliver but no state can ever handle, so they would always be
	// rejected as invalid-for-state. A non-empty Unreachable is usually dead routing.
	Unreachable []E
}

// Exhaustive reports whether the accepted union exactly covers the machine's
// alphabet with nothing unreachable: every event the machine handles is
// deliverable, and every deliverable event is handled.
func (c Conformance[E]) Exhaustive() bool {
	return len(c.Missing) == 0 && len(c.Unreachable) == 0
}

// Err returns a non-nil error describing the gaps when the consumption is not
// [Conformance.Exhaustive], so a conformance test can fail with one check:
//
//	if err := statemachine.CheckEvents(m, accepted).Err(); err != nil {
//	    t.Fatal(err)
//	}
//
// It returns nil when exhaustive.
func (c Conformance[E]) Err() error {
	if c.Exhaustive() {
		return nil
	}
	return fmt.Errorf("statemachine: event union not exhaustive against machine alphabet: missing=%v unreachable=%v",
		c.Missing, c.Unreachable)
}

// CheckEvents validates that accepted — the event union a router or codec can
// produce — is exhaustive against machine's event alphabet, and reports both the
// alphabet events the union misses and the accepted events the machine can never
// handle (which would always be rejected as invalid-for-state). It reads the
// alphabet from the machine's serialized definition through the state module's IR
// (no private state access), so it works on any built or loaded [state.Machine].
//
// A machine whose definition cannot be serialized (an impossible state for a
// Quenched machine) yields an empty alphabet and reports every accepted event as
// unreachable, surfacing the problem rather than masking it.
func CheckEvents[K comparable, E comparable, C any](machine *state.Machine[K, E, C], accepted []E) Conformance[E] {
	alphabet := EventAlphabet(machine)

	alphaSet := make(map[E]struct{}, len(alphabet))
	for _, e := range alphabet {
		alphaSet[e] = struct{}{}
	}
	acceptSet := make(map[E]struct{}, len(accepted))
	for _, e := range accepted {
		acceptSet[e] = struct{}{}
	}

	var missing, unreachable []E
	for _, e := range alphabet {
		if _, ok := acceptSet[e]; !ok {
			missing = append(missing, e)
		}
	}
	// Preserve the caller's order for accepted, deduping.
	seen := make(map[E]struct{}, len(accepted))
	for _, e := range accepted {
		if _, dup := seen[e]; dup {
			continue
		}
		seen[e] = struct{}{}
		if _, ok := alphaSet[e]; !ok {
			unreachable = append(unreachable, e)
		}
	}

	return Conformance[E]{
		Alphabet:    alphabet,
		Accepted:    append([]E(nil), accepted...),
		Missing:     missing,
		Unreachable: unreachable,
	}
}

// EventAlphabet returns every event that appears on a declared transition of
// machine — across all states, nested substates, and parallel regions — sorted
// by its string form for stable output. It is the introspection [CheckEvents]
// builds on, exported so a caller can enumerate "what events can this machine
// ever act on" directly.
//
// It derives the alphabet from the machine's serialized IR, the public
// definition surface, so it never reaches into kernel internals.
func EventAlphabet[K comparable, E comparable, C any](machine *state.Machine[K, E, C]) []E {
	data, err := machine.ToJSON()
	if err != nil {
		return nil
	}
	ir, err := state.LoadFromJSON[K, E, C](data)
	if err != nil {
		return nil
	}

	set := make(map[E]struct{})
	var walk func(states []state.State[K, E, C])
	walk = func(states []state.State[K, E, C]) {
		for i := range states {
			s := &states[i]
			for j := range s.Transitions {
				set[s.Transitions[j].On] = struct{}{}
			}
			walk(s.Children)
			for r := range s.Regions {
				walk(s.Regions[r].States)
			}
		}
	}
	walk(ir.States)

	alphabet := make([]E, 0, len(set))
	for e := range set {
		alphabet = append(alphabet, e)
	}
	sort.Slice(alphabet, func(i, j int) bool {
		return fmt.Sprint(alphabet[i]) < fmt.Sprint(alphabet[j])
	})
	return alphabet
}
