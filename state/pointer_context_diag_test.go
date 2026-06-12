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
// determinism warning, matched leniently on its message so other diagnostics in
// the slice do not affect the assertion.
func hasDeterminismWarning(diags []Diagnostic) bool {
	for _, d := range diags {
		if d.Severity != diagWarning {
			continue
		}
		m := strings.ToLower(d.Message)
		if strings.Contains(m, "determinism") || strings.Contains(m, "pointer") {
			return true
		}
	}
	return false
}

// TestTemper_PointerContext_EmitsDeterminismWarning asserts a pointer-C machine
// surfaces the soft determinism warning, still builds without Strict, and fires.
func TestTemper_PointerContext_EmitsDeterminismWarning(t *testing.T) {
	b := Forge[string, string, *ptrCtxEntity]("ptr").
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
	b := Forge[string, string, valCtxEntity]("val").
		State("a").
		State("b").
		Initial("a").
		CurrentStateFn(func(c valCtxEntity) string { return c.at }).
		Transition("a").On("go").GoTo("b")

	if hasDeterminismWarning(b.Temper()) {
		t.Fatalf("did not expect a determinism warning for a value context, got %+v", b.Temper())
	}
}
