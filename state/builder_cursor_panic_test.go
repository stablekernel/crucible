package state

import (
	"fmt"
	"strings"
	"testing"
)

// cursorEntity is a tiny value context for the builder-cursor footgun tests.
type cursorEntity struct {
	at string
}

// recoverContains runs fn and reports the recovered panic's string form, or "" if
// fn did not panic.
func recoverContains(t *testing.T, fn func()) (panicked bool, msg string) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			msg = fmt.Sprint(r)
		}
	}()
	fn()
	return false, ""
}

// newCursorBuilder returns a builder with a single declared transition cursor
// already closed by a trailing State() call, so a consumer method chained next
// must panic.
func newCursorBuilder() *Builder[string, string, cursorEntity] {
	// Transition(a).On(go).GoTo(b) opens then targets a cursor; the following
	// State("a") call closes it (declareState sets curTransition = nil), so any
	// cursor-consumer chained after State must panic.
	return ForgeFor[cursorEntity]("cursor").
		State("a").
		State("b").
		Initial("a").
		CurrentStateFn(func(c cursorEntity) string { return c.at }).
		Transition("a").On("go").GoTo("b").
		State("a")
}

// TestBuilderCursor_ConsumerAfterClosedCursor_Panics asserts each in-scope
// cursor-consumer method panics with an actionable message when chained after the
// cursor has been closed by State().
func TestBuilderCursor_ConsumerAfterClosedCursor_Panics(t *testing.T) {
	cases := []struct {
		method string
		call   func(b *Builder[string, string, cursorEntity])
	}{
		{"When", func(b *Builder[string, string, cursorEntity]) { b.When("g") }},
		{"Do", func(b *Builder[string, string, cursorEntity]) { b.Do("act") }},
		{"GoTo", func(b *Builder[string, string, cursorEntity]) { b.GoTo("b") }},
		{"Assign", func(b *Builder[string, string, cursorEntity]) { b.Assign("r") }},
		{"Raise", func(b *Builder[string, string, cursorEntity]) { b.Raise("e") }},
	}
	for _, tc := range cases {
		t.Run(tc.method, func(t *testing.T) {
			b := newCursorBuilder()
			panicked, msg := recoverContains(t, func() { tc.call(b) })
			if !panicked {
				t.Fatalf("%s after a closed cursor should panic, but it did not", tc.method)
			}
			if !strings.Contains(msg, "no open transition") {
				t.Fatalf("panic message should mention %q, got: %s", "no open transition", msg)
			}
			if !strings.Contains(msg, tc.method) {
				t.Fatalf("panic message should name the method %q, got: %s", tc.method, msg)
			}
		})
	}
}

// TestBuilderCursor_ValidChain_DoesNotPanic asserts a well-formed chain that
// keeps the cursor open through the consumer methods Tempers without panicking.
func TestBuilderCursor_ValidChain_DoesNotPanic(t *testing.T) {
	panicked, msg := recoverContains(t, func() {
		ForgeFor[cursorEntity]("ok").
			Guard("g", func(GuardCtx[cursorEntity]) bool { return true }).
			State("a").
			State("b").
			Initial("a").
			CurrentStateFn(func(c cursorEntity) string { return c.at }).
			Transition("a").On("go").GoTo("b").When("g").
			Temper()
	})
	if panicked {
		t.Fatalf("a valid open-cursor chain must not panic, got: %s", msg)
	}
}
