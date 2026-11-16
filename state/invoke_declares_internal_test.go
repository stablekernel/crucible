package state

import "testing"

// TestDeclaresOnError_NoPanicOnAnUncomparableEvent pins that declaresOnError, for an
// invocation loaded from JSON (no builder flag), doesn't panic when its event type is an
// interface holding an uncomparable value, as a JSON array decodes to, and reads such
// an onError as declared and the zero event as not.
func TestDeclaresOnError_NoPanicOnAnUncomparableEvent(t *testing.T) {
	loaded := Invocation[string, any, int]{OnError: []any{1.0}}
	if !loaded.declaresOnError() {
		t.Fatal("an onError holding a JSON array reads as not declared")
	}
	if zero := (Invocation[string, any, int]{}); zero.declaresOnError() {
		t.Fatal("a nil onError reads as declared")
	}
	if typed := (Invocation[string, int, int]{OnError: 0}); typed.declaresOnError() {
		t.Fatal("a loaded int onError of 0 reads as declared")
	}
	if built := (Invocation[string, int, int]{OnError: 0, hasOnError: true}); !built.declaresOnError() {
		t.Fatal("a built onError of 0 reads as not declared")
	}
}
