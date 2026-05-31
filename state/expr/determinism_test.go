package expr_test

import (
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/expr"
)

// TestDeterminism_NondeterministicBuiltinsUnavailable asserts the rich env exposes
// no ambient or nondeterministic builtin: now/random-style references fail to
// compile, so a guard cannot read wall-clock time or a random source and is a pure
// function of its context. The deterministic timestamp/duration CONSTRUCTORS stay
// available (they are pure of their arguments) and are confirmed compilable.
func TestDeterminism_NondeterministicBuiltinsUnavailable(t *testing.T) {
	rejected := []string{
		`now() > timestamp("2020-01-01T00:00:00Z")`,
		`now > timestamp("2020-01-01T00:00:00Z")`,
		`rand() > 0.5`,
		`random() > 0.5`,
		`math.random() > 0.5`,
	}
	for _, src := range rejected {
		t.Run("reject/"+src, func(t *testing.T) {
			reg := state.NewRegistry[order]()
			_, err := expr.Guard[string](reg, "g", src, orderSchema())
			if err == nil {
				t.Fatalf("source %q should be rejected (nondeterministic/undeclared)", src)
			}
			if !strings.Contains(err.Error(), "undeclared") &&
				!strings.Contains(err.Error(), "no matching overload") {
				t.Fatalf("source %q rejected for the wrong reason: %v", src, err)
			}
		})
	}

	accepted := []string{
		`window > duration("1h")`,
		`total >= 0.0`,
	}
	for _, src := range accepted {
		t.Run("accept/"+src, func(t *testing.T) {
			reg := state.NewRegistry[order]()
			if _, err := expr.Guard[string](reg, "g", src, orderSchema()); err != nil {
				t.Fatalf("deterministic source %q should compile: %v", src, err)
			}
		})
	}
}

// TestDeterminism_RepeatedEvalIsStable asserts evaluating the same guard against the
// same context repeatedly yields the same verdict — a direct check that nothing in
// the eval path reads an ambient source.
func TestDeterminism_RepeatedEvalIsStable(t *testing.T) {
	e := sampleOrder()
	first := fireRich(t, `total > 40.0 && status == "paid"`, e)
	for i := 0; i < 32; i++ {
		if got := fireRich(t, `total > 40.0 && status == "paid"`, e); got != first {
			t.Fatalf("eval %d diverged: got %v, first %v", i, got, first)
		}
	}
}
