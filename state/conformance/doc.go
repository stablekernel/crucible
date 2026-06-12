// Package conformance proves that a state machine behaves correctly.
//
// It rests on the three pillars from the suite's conformance design:
//
//  1. Oracle comparison — the effects a machine's Fire produces are diffed
//     against a trusted reference implementation for the same input.
//  2. Golden scenarios — committed event sequences are replayed and the final
//     state, emitted effects, and trace are asserted.
//  3. Round-trip identity — a machine authored in Go and the same machine loaded
//     from JSON (then bound via Provide) are proven to behave identically.
//
// Scenarios are derived from the machine graph: GenerateScenarios enumerates the
// shortest event path to every reachable state by breadth-first search over the
// IR, mirroring the path-planning model so a small machine yields full coverage
// without hand-authored fixtures. A Scenario and the Trace a run produces are
// both first-class, JSON-serializable artifacts: scenarios can be committed as
// goldens under testdata and replayed in CI, and a captured run can be diffed
// against a committed expectation.
//
// The package depends only on the state kernel and the standard library, so it
// adds no third-party dependencies to a consumer that vendors it to prove its
// own machines correct.
//
// # Stability
//
// This package SHIPS as part of the v1.0 release, but it is ADVISORY: its API
// surface and its golden/assertion/trace schema shapes are NOT covered by the
// v1.0 frozen-contract guarantee and MAY change in a minor release. Depend on it
// to prove your machines correct, but pin your version and expect the
// scenario/trace/assertion shapes to evolve. The frozen v1.0 contract is the
// state kernel itself; conformance is the tooling that exercises it.
package conformance
