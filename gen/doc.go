// Package gen turns a state machine's intermediate representation into typed Go
// stub source — an "eject" codegen.
//
// Eject walks an [state.IR] and emits a single gofmt'd Go source file containing:
//
//   - a Context type synthesized from the IR's context schema (a struct when the
//     schema declares fields, otherwise a map[string]any alias);
//   - one panic-bodied stub per referenced behavior (guard, action, assign,
//     service), each typed to the exact engine signature with the synthesized
//     Context substituted for the machine's context type parameter; and
//   - a Provide function that registers every stub against a [state.Registry] by
//     its original IR name.
//
// The emitted file is a starting point a host fills in: every stub panics with a
// TODO until implemented, but the file compiles and its Provide type-checks
// against the real registry, so the wiring is proven before any behavior is
// written.
//
// Output is deterministic: names are sorted and the file is rendered from sorted
// slices, so ejecting the same IR twice yields byte-identical source.
package gen
