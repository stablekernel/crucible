package expr_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// This file holds the Core↔CEL equivalence check — the block-merge conformance gate
// that proves the two expression tiers never disagree. It generates random Core
// guard trees over a fixed ContextSchema and random schema-conforming contexts,
// evaluates each tree two ways — through the kernel (Core, via a fired transition)
// and through the lowered CEL program (expr.Lower) — and asserts the verdicts match.
//
// The check is scoped to schema-attached nodes (FORK F): an unschema'd Core machine
// has quiet-false cross-category semantics with no CEL analog, so the guarantee is
// stated for schema-attached machines only. The one place Core and raw CEL diverge —
// int vs float comparison, where Core coerces to float64 and CEL is strict — is
// closed by the explicit double() casts Lower injects; this check is what proves that
// injection is correct and complete.

// fuzzCtx is the fixed context shape the equivalence check generates values for. Its
// fields span the numeric (int/float), string, bool, and duration categories so the
// generated compares exercise every Core coercion rule, including the int↔float case
// the cast injection must reconcile.
type fuzzCtx struct {
	I int           `json:"i"`
	J int           `json:"j"`
	F float64       `json:"f"`
	G float64       `json:"g"`
	S string        `json:"s"`
	B bool          `json:"b"`
	D time.Duration `json:"d"`
}

// fuzzSchema is the schema the equivalence check types its generated nodes against.
func fuzzSchema() state.ContextSchema { return state.SchemaOf[fuzzCtx]() }

// TestEquivalence_CoreEqualsLoweredCEL is the conformance gate. For a fixed number
// of seeded iterations it generates a random Core tree and a random context, evaluates
// the tree through the kernel and through the lowered CEL program, and asserts the two
// verdicts agree. A divergence fails the test with the offending tree, context, and
// the two verdicts, so any disagreement beyond the int↔float case the casts handle is
// surfaced loudly rather than papered over.
func TestEquivalence_CoreEqualsLoweredCEL(t *testing.T) {
	const iterations = 4000
	rng := rand.New(rand.NewSource(0xC0FFEE))
	for i := 0; i < iterations; i++ {
		node := genNode(rng, 0)
		ctx := genCtx(rng)
		assertEquivalent(t, node, ctx, i)
	}
}

// FuzzEquivalence_CoreEqualsLoweredCEL is the native-fuzz entry point: each fuzz seed
// drives the same generator and the same equivalence assertion, so `go test -fuzz`
// explores the space beyond the seeded iterations. It is not required by CI (the
// seeded test above is the gate) but lets a developer harden the check locally.
func FuzzEquivalence_CoreEqualsLoweredCEL(f *testing.F) {
	f.Add(int64(1))
	f.Add(int64(42))
	f.Fuzz(func(t *testing.T, seed int64) {
		rng := rand.New(rand.NewSource(seed))
		node := genNode(rng, 0)
		ctx := genCtx(rng)
		assertEquivalent(t, node, ctx, 0)
	})
}

// assertEquivalent evaluates node through Core and through lowered CEL and fails if
// the verdicts differ. It also asserts error-parity at the boundary: a node Core can
// evaluate must lower to a CEL program (a lowering/compile failure on an otherwise
// valid schema-typed node is itself a divergence).
func assertEquivalent(t *testing.T, node state.GuardNode[string], ctx fuzzCtx, iter int) {
	t.Helper()
	celVerdict, source, err := expr.EvalLowered(node, fuzzSchema(), ctx)
	if err != nil {
		t.Fatalf("iter %d: lower/eval failed for a schema-typed Core node: %v\nnode=%+v", iter, err, node)
	}
	coreVerdict := evalCoreNode(t, node, ctx)
	if coreVerdict != celVerdict {
		t.Fatalf("iter %d DIVERGENCE: core=%v cel=%v\nsource=%s\nnode=%+v\nctx=%+v",
			iter, coreVerdict, celVerdict, source, node, ctx)
	}
}

// evalCoreNode evaluates a Core guard tree through the kernel by firing a one-edge
// machine whose transition carries the node against a schema-attached context, and
// reports whether the transition was enabled. A schema is attached so the kernel
// type-checks at Quench, scoping the check to schema-attached semantics.
func evalCoreNode(t *testing.T, node state.GuardNode[string], ctx fuzzCtx) bool {
	t.Helper()
	m := state.ForgeFor[fuzzCtx]("eq").
		WithContextSchema(fuzzSchema()).
		State("from").
		Transition("from").On("go").GoTo("to").WhenExpr(node).
		State("to").
		Initial("from").
		Quench()
	inst := m.Cast(ctx, state.WithInitialState("from"))
	inst.Fire(context.Background(), "go")
	return inst.Current() == "to"
}
