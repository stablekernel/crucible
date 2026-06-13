package state_test

import (
	"testing"

	"github.com/stablekernel/crucible/state"
)

// This file pins the guard-expression inspection accessors tooling depends on:
//
//   - GuardNode.LeafRefs (guard.go:222): the named-ref leaves of a composite
//     guard, left-to-right, with the config-aware stateIn built-in omitted.
//   - GuardNode.StateInTargets (guard.go:227): the target states of every stateIn
//     leaf, left-to-right.
//
// Both walk an And/Or/Not tree mixing named-ref leaves and stateIn leaves.

// TestGuardNode_LeafRefs extracts the named-ref leaves of composite guard trees,
// asserting left-to-right order and that stateIn leaves (which carry no host ref)
// are omitted.
func TestGuardNode_LeafRefs(t *testing.T) {
	cases := []struct {
		name string
		expr state.GuardNode[string]
		want []string
	}{
		{
			name: "single named leaf",
			expr: state.Guard[string]("a"),
			want: []string{"a"},
		},
		{
			name: "stateIn leaf carries no ref",
			expr: state.StateIn("X"),
			want: nil,
		},
		{
			name: "And preserves left-to-right ref order",
			expr: state.And(state.Guard[string]("a"), state.Guard[string]("b")),
			want: []string{"a", "b"},
		},
		{
			name: "Or over nested And, stateIn omitted",
			expr: state.Or(
				state.And(state.Guard[string]("a"), state.StateIn("X")),
				state.Guard[string]("b"),
			),
			want: []string{"a", "b"},
		},
		{
			name: "Not wrapping a named leaf",
			expr: state.Not(state.Guard[string]("a")),
			want: []string{"a"},
		},
		{
			name: "deep mix of And/Or/Not and stateIn",
			expr: state.And(
				state.Guard[string]("a"),
				state.Or(state.StateIn("X"), state.Not(state.Guard[string]("b"))),
				state.Guard[string]("c"),
			),
			want: []string{"a", "b", "c"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr := tc.expr
			refs := expr.LeafRefs()
			got := make([]string, len(refs))
			for i, r := range refs {
				got[i] = r.Name
			}
			if !equalStrings(got, tc.want) {
				t.Fatalf("LeafRefs() names = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGuardNode_StateInTargets extracts the target states of every stateIn leaf in
// a composite guard, asserting left-to-right order and that named-ref leaves
// contribute nothing.
func TestGuardNode_StateInTargets(t *testing.T) {
	cases := []struct {
		name string
		expr state.GuardNode[string]
		want []string
	}{
		{
			name: "single stateIn",
			expr: state.StateIn("X"),
			want: []string{"X"},
		},
		{
			name: "named leaf has no stateIn target",
			expr: state.Guard[string]("a"),
			want: nil,
		},
		{
			name: "And preserves left-to-right stateIn order",
			expr: state.And(state.StateIn("X"), state.StateIn("Y")),
			want: []string{"X", "Y"},
		},
		{
			name: "Or over nested And/Not, named leaves omitted",
			expr: state.Or(
				state.And(state.Guard[string]("a"), state.StateIn("X")),
				state.Not(state.StateIn("Y")),
			),
			want: []string{"X", "Y"},
		},
		{
			name: "deep mix yields stateIn targets in spine order",
			expr: state.And(
				state.StateIn("X"),
				state.Or(state.Guard[string]("a"), state.StateIn("Y")),
				state.StateIn("Z"),
			),
			want: []string{"X", "Y", "Z"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			expr := tc.expr
			got := expr.StateInTargets()
			if !equalStrings(got, tc.want) {
				t.Fatalf("StateInTargets() = %v, want %v", got, tc.want)
			}
		})
	}
}

// equalStrings reports whether two string slices are element-wise equal, treating
// nil and empty as equal (an absence of refs/targets).
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
