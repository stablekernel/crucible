package state

import (
	"context"
	"strings"
	"testing"
)

// ptrCtxEntity is a small struct used as a pointer context (*ptrCtxEntity) to
// exercise the soft pointer-context determinism diagnostic.
type ptrCtxEntity struct {
	at string
}

// valCtxEntity is a value context (non-pointer) used to assert the determinism
// diagnostic is NOT emitted for value contexts.
type valCtxEntity struct {
	at string
}

// hasDeterminismWarning reports whether diags carries the pointer-context
// determinism advisory, matched leniently on its message so other diagnostics in
// the slice do not affect the assertion. The advisory is emitted at diagInfo
// severity: it is surfaced for inspection but never escalated, even under Strict.
func hasDeterminismWarning(diags []Diagnostic) bool {
	for _, d := range diags {
		if d.Severity != diagInfo {
			continue
		}
		m := strings.ToLower(d.Message)
		if strings.Contains(m, "determinism") || strings.Contains(m, "pointer") {
			return true
		}
	}
	return false
}

// TestQuench_PointerContext_StrictDoesNotEscalate asserts that a pointer-C
// machine Quenches under Strict WITHOUT panicking (pointer context is a
// supported escape hatch, surfaced as an advisory diagnostic that Strict never
// promotes to an error) and that the advisory is still surfaced in Temper.
func TestQuench_PointerContext_StrictDoesNotEscalate(t *testing.T) {
	build := func() *Builder[string, string, *ptrCtxEntity] {
		return ForgeFor[*ptrCtxEntity]("ptr").
			State("a").
			State("b").
			Initial("a").
			CurrentStateFn(func(c *ptrCtxEntity) string { return c.at }).
			Transition("a").On("go").GoTo("b")
	}

	// The advisory is surfaced for inspection...
	if !hasDeterminismWarning(build().Temper()) {
		t.Fatalf("expected the pointer-context advisory to be surfaced, got %+v", build().Temper())
	}

	// ...but Strict does NOT escalate it to a panic.
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Quench(Strict()) panicked on the pointer-context advisory: %v", r)
			}
		}()
		_ = build().Quench(Strict())
	}()
}

// TestQuench_Strict_StillEscalatesGenuineWarning asserts that the fix did not
// neuter Strict: a genuine diagWarning (here, a missing CurrentStateFn) STILL
// escalates to a panic under Strict.
func TestQuench_Strict_StillEscalatesGenuineWarning(t *testing.T) {
	// Value context so the pointer advisory is absent; omit CurrentStateFn so the
	// only finding is the missing-CurrentStateFn warning.
	b := ForgeFor[valCtxEntity]("val").
		State("a").
		State("b").
		Initial("a").
		Transition("a").On("go").GoTo("b")

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Quench(Strict()) did not panic on a genuine warning (Strict was neutered)")
		}
	}()
	_ = b.Quench(Strict())
}

// TestTemper_PointerContext_EmitsDeterminismWarning asserts a pointer-C machine
// surfaces the soft determinism warning, still builds without Strict, and fires.
func TestTemper_PointerContext_EmitsDeterminismWarning(t *testing.T) {
	b := ForgeFor[*ptrCtxEntity]("ptr").
		State("a").
		State("b").
		Initial("a").
		CurrentStateFn(func(c *ptrCtxEntity) string { return c.at }).
		Transition("a").On("go").GoTo("b")

	diags := b.Temper()
	if !hasDeterminismWarning(diags) {
		t.Fatalf("expected a pointer-context determinism warning, got %+v", diags)
	}

	var m *Machine[string, string, *ptrCtxEntity]
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("plain Quench() panicked on a determinism warning: %v", r)
			}
		}()
		m = b.Quench()
	}()

	res := m.Cast(&ptrCtxEntity{at: "a"}).Fire(context.Background(), "go")
	if res.Err != nil {
		t.Fatalf("Fire returned error: %v", res.Err)
	}
	if res.NewState != "b" {
		t.Fatalf("expected new state %q, got %q", "b", res.NewState)
	}
}

// TestTemper_ValueContext_NoDeterminismWarning asserts a value-C machine does not
// emit the pointer-context determinism warning.
func TestTemper_ValueContext_NoDeterminismWarning(t *testing.T) {
	b := ForgeFor[valCtxEntity]("val").
		State("a").
		State("b").
		Initial("a").
		CurrentStateFn(func(c valCtxEntity) string { return c.at }).
		Transition("a").On("go").GoTo("b")

	if hasDeterminismWarning(b.Temper()) {
		t.Fatalf("did not expect a determinism warning for a value context, got %+v", b.Temper())
	}
}
