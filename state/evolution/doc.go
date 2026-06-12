// Package evolution classifies the difference between two versions of a state
// machine definition as additive (backward-compatible) or breaking, following
// the Crucible Evolution Guide.
//
// A machine definition is a schema. Renaming or removing a state, retargeting a
// transition, or moving the initial state breaks entities already persisted
// under the old definition; adding states, transitions, events, or optional
// metadata is safe. The guide maps these onto a deprecation lifecycle and a
// semantic-version bump: additive changes are minor, breaking changes are major.
//
// This package operates on the serializable [state.IR], which is the canonical,
// versioned snapshot of a machine (the committed machine.json). A consumer
// commits a golden IR and gates their machine changes in CI by diffing the live
// machine against it:
//
//	report, err := evolution.DiffJSON[State, Event, *Entity](goldenBytes, currentBytes)
//	if err != nil {
//		return err
//	}
//	if report.Breaking() {
//		return fmt.Errorf("breaking machine change requires a major version bump:\n%s", report)
//	}
//
// The package imports only [state] and the standard library, preserving the
// kernel's stdlib-only dependency stance.
//
// Stability: this package ships as part of the v1.0 release, but its API and its
// classification results are ADVISORY and are NOT covered by the v1.0
// frozen-contract compatibility guarantee. Both may change in a minor release as
// the differ's fidelity improves; do not treat a specific bump or change kind as
// a stable contract.
package evolution
