package symbolic

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestLitNumber_NumericForms asserts litNumber extracts a float64 from every
// supported numeric Go form a Literal.Value can carry — float64, int64, int, and
// int32 — and rejects a non-numeric value. The int forms arise from hand-built or
// cross-package literals, not just the int64 the Int constructor emits.
func TestLitNumber_NumericForms(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  float64
		ok    bool
	}{
		{"float64", float64(3.5), 3.5, true},
		{"int64", int64(7), 7, true},
		{"int", int(9), 9, true},
		{"int32", int32(11), 11, true},
		{"string-not-numeric", "nope", 0, false},
		{"bool-not-numeric", true, 0, false},
		{"nil", nil, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := litNumber(state.Literal{Value: tc.value})
			if ok != tc.ok {
				t.Fatalf("litNumber(%v) ok = %v, want %v", tc.value, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Fatalf("litNumber(%v) = %v, want %v", tc.value, got, tc.want)
			}
		})
	}
}
