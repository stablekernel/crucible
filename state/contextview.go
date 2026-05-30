package state

import "encoding/json"

// This file defines the contextView projection seam — the abstraction by which a
// machine's context C reaches a bound behavior at the invocation boundary. It is
// the input half of the data boundary, the read-side mirror of the effect
// envelope's output half.
//
// The seam exists so a behavior authored out-of-process (a different language, a
// sandboxed component, a remote service) can be handed a *serialized projection*
// of the context instead of a live Go value it could never receive. The in-process
// default is a zero-cost pass-through to the live entity: a Go binding reads the
// live value through Raw and never pays a marshaling cost. Only a binding that
// crosses a boundary consumes the serialized JSON projection through JSON.
//
// The view is read-only by construction: it exposes no mutator. Guards observe
// context through it and cannot change it; the action contract still returns
// effects-as-data and does not write context through this seam. Marshaling and
// concrete out-of-process transports are deliberately NOT built here — the seam is
// thin on purpose, reserving the projection point so adding a transport later is
// additive rather than a breaking change to the guard/action signatures.

// ContextView is the read-only projection of a machine's context C as it crosses
// the behavior-invocation boundary. Raw returns the live value for the in-process
// fast path; JSON returns the serialized wire form an out-of-process binding
// consumes. It carries no mutator — context is read-only at this seam.
type ContextView interface {
	// Raw returns the underlying context value. For the in-process projection it is
	// the live entity itself (a zero-cost pass-through); a binding that knows it is
	// in-process may type-assert it back to its concrete C.
	Raw() any
	// JSON returns the serialized projection of the context — the wire form an
	// out-of-process binding receives. The in-process projection marshals the live
	// value with the context codec on demand.
	JSON() ([]byte, error)
}

// inProcessView is the default ContextView: a pass-through to the live entity that
// marshals to JSON only when the serialized projection is explicitly requested.
type inProcessView[C any] struct {
	entity C
}

// newInProcessView wraps a live entity in the default pass-through projection.
func newInProcessView[C any](entity C) inProcessView[C] {
	return inProcessView[C]{entity: entity}
}

// Raw returns the live entity unchanged — the in-process fast path performs no
// copy and no marshaling.
func (v inProcessView[C]) Raw() any { return v.entity }

// JSON marshals the live entity to its serialized projection with encoding/json,
// matching the default ContextCodec wire form. A binding that crosses a process
// boundary consumes this; the in-process path never calls it.
func (v inProcessView[C]) JSON() ([]byte, error) { return json.Marshal(v.entity) }
