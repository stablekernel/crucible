// Package analysis performs static model-checking over a Quenched Crucible
// state machine. It reasons about a machine's serializable IR — the same graph
// the kernel runs and the visualizers render — and reports structural defects a
// human is unlikely to catch by eye: states that can never be entered,
// transitions that can never fire, ambiguous event handling, dead ends, and
// states that can never reach completion.
//
// The analysis is purely static. It never casts an instance, never fires an
// event, and never evaluates a guard or action — guards and actions are opaque
// host functions, referenced in the IR by name only, so their runtime truth is
// invisible to a static pass. Every check therefore reasons about the graph's
// shape, not its behavior, and the package documents where that boundary makes a
// finding exact versus heuristic.
//
// This is the analysis a code-first state-machine library cannot offer: because
// Crucible's canonical machine is a serializable IR rather than a tangle of
// closures, the whole transition graph is inspectable as data. The machine
// config is similarly serializable, but it ships no equivalent static
// model-checker; here the checks fall out almost for free from the IR plus the
// breadth-first reachability the kernel already uses for path planning.
//
// # Usage
//
//	report := analysis.Analyze(machine)
//	for _, f := range report.Findings {
//		fmt.Printf("%s [%s] %s\n", f.Severity, f.Kind, f.Message)
//	}
//
// Analyze accepts a [state.Machine] built either by the Forge DSL or loaded from
// JSON via LoadFromJSON+Provide; both yield the same IR, so both analyze
// identically. Restrict the pass to a subset of checks with [Only] or [Without].
//
// The package imports only [state] and the standard library, preserving the
// kernel's stdlib-only dependency stance.
//
// # Stability
//
// This package ships in the v1.0 release but is advisory: its API and the shape
// of its findings (the [Kind] set, [Finding] fields, and report semantics) are
// NOT covered by the v1.0 frozen-contract guarantee, which names only the
// kernel, IR, context, effect, and emission surfaces. The findings are
// heuristic-or-exact diagnostics, not a stable contract, and may change in a
// minor release.
package analysis
