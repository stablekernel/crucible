package expr

import (
	"strings"
	"testing"

	"github.com/google/cel-go/common/decls"

	"github.com/stablekernel/crucible/state"
)

// TestCELEnvDeterminism introspects the real CEL environment the package builds
// (via newEnv, the same factory Guard uses) and asserts the function registry
// contains no nondeterministic builtin. The behavioral determinism tests in
// determinism_test.go prove that today's source-level references fail to compile;
// this test guards a different failure mode — a future cel-go bump that silently
// ADDS a nondeterministic builtin (a zero-arg now()/timestamp(), a random(), a
// uuid()) to the standard library. Such a builtin would slip past compile-rejection
// tests yet break the replay/guard determinism contract, so the registry itself is
// asserted clean.
//
// The denylist distinguishes the genuinely nondeterministic shape from the pure
// deterministic constructors that legitimately live in the StdLib: timestamp(string)
// and duration(string) take an argument and are pure functions of it, so they are
// allowed; a zero-argument time source (now(), timestamp()) reads the wall clock and
// is rejected.
func TestCELEnvDeterminism(t *testing.T) {
	// Build the real env through the package factory so the assertion tracks exactly
	// what Guard compiles against, including the StdLib option newEnv selects.
	env, err := newEnv(state.ContextSchema{})
	if err != nil {
		t.Fatalf("build real CEL env: %v", err)
	}

	fns := env.Functions()
	if len(fns) == 0 {
		t.Fatal("env exposed no function declarations; introspection API (env.Functions) returned empty — test cannot validate determinism")
	}

	// banned names are unconditionally nondeterministic: no overload shape makes them
	// pure, so any registration is a violation.
	banned := map[string]bool{
		"now":    true,
		"random": true,
		"rand":   true,
		"uuid":   true,
	}

	// timeSource names have a legitimate pure overload (a constructor taking an
	// argument) AND a hypothetical nondeterministic zero-arg overload that reads the
	// wall clock. Only the zero-arg shape is a violation.
	timeSource := map[string]bool{
		"now":       true,
		"timestamp": true,
	}

	for name, fn := range fns {
		lower := strings.ToLower(name)

		if banned[lower] {
			t.Errorf("nondeterministic function %q is registered in the CEL env; the determinism contract forbids it", name)
			continue
		}

		// Defense in depth: catch a "randomFoo"/"uuidv4" style nondeterministic
		// builtin that a future StdLib bump might add under a prefixed name.
		for _, p := range []string{"random", "rand", "uuid"} {
			if strings.HasPrefix(lower, p) {
				t.Errorf("function %q matches nondeterministic prefix %q; the determinism contract forbids ambient/random sources", name, p)
				break
			}
		}

		if timeSource[lower] {
			if zeroArgOverload(fn) {
				t.Errorf("time-source function %q exposes a zero-argument overload that reads the wall clock; the determinism contract forbids ambient time", name)
			}
		}
	}
}

// zeroArgOverload reports whether fn has any overload with no operands — the shape
// of a wall-clock reader such as now() or a zero-arg timestamp(). A member-function
// overload (receiver.fn()) is not ambient: its receiver supplies the value, so it is
// not counted.
func zeroArgOverload(fn *decls.FunctionDecl) bool {
	for _, o := range fn.OverloadDecls() {
		if o.IsMemberFunction() {
			continue
		}
		if len(o.ArgTypes()) == 0 {
			return true
		}
	}
	return false
}
