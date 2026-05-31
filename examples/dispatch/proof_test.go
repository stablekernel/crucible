package dispatch

import (
	"testing"

	"github.com/stablekernel/crucible/examples/fooddelivery"
	"github.com/stablekernel/crucible/state"
)

// mustProve builds the order-saga model and proves it, failing the test on any
// construction or proof error. It returns the report for the caller to assert on.
func mustProve(t *testing.T) ProofReport {
	t.Helper()
	m, err := fooddelivery.NewModel()
	if err != nil {
		t.Fatalf("build model: %v", err)
	}
	report, err := Prove(m)
	if err != nil {
		t.Fatalf("prove model: %v", err)
	}
	return report
}

// TestProve_KeyStages_AllReachable confirms every key lifecycle stage is reachable.
// Reachability is exact (guard-agnostic), so each true verdict is a stage some real
// run can enter.
func TestProve_KeyStages_AllReachable(t *testing.T) {
	report := mustProve(t)

	want := []fooddelivery.Stage{
		fooddelivery.Active,
		fooddelivery.Delivered,
		fooddelivery.Canceled,
		fooddelivery.Rejected,
		fooddelivery.Overdue,
	}
	if len(report.Reachable) != len(want) {
		t.Fatalf("Reachable has %d entries, want %d: %v", len(report.Reachable), len(want), report.Reachable)
	}
	for _, s := range want {
		reachable, ok := report.Reachable[s.String()]
		if !ok {
			t.Errorf("Reachable missing entry for %s", s)
			continue
		}
		if !reachable {
			t.Errorf("stage %s is unreachable, want reachable", s)
		}
	}
}

// TestProve_Watchdog_MutuallyExclusive confirms the Watchdog region's OnTime and
// Overdue leaves are never simultaneously active: they are sequential leaves of one
// region (OnTime steps to Overdue on the SLA breach), so the mutual-exclusion
// invariant holds in every reachable configuration.
func TestProve_Watchdog_MutuallyExclusive(t *testing.T) {
	report := mustProve(t)
	if !report.WatchdogExclusive {
		t.Error("WatchdogExclusive = false, want true (OnTime and Overdue are sequential leaves)")
	}
}

// TestProve_Overlaps_Empty confirms the analyzer found no competing transitions it
// could not prove disjoint. The order saga's only composite guard — the Rich CEL
// "generousOrder" leaf composed via Or with the Core admit branch — sits on a single
// edge, not as two branches racing on the same source/event, so there is no
// same-source/same-event pair to report. The result is provably deterministic over
// the analyzable guarded choices.
func TestProve_Overlaps_Empty(t *testing.T) {
	report := mustProve(t)
	if len(report.Overlaps) != 0 {
		t.Errorf("Overlaps = %v, want none", report.Overlaps)
	}
}

// TestProve_Guards_NoDeadBranch confirms no transition guard is contradictory. The
// saga carries exactly one composite guard — the admit guard on the Authorized edge.
// The symbolic analyzer is conservative on the opaque Rich CEL "generousOrder" leaf:
// it treats it as satisfiable rather than risk a false dead-branch report, so the
// disjunction it heads is satisfiable. A satisfiable verdict here means the branch
// can fire; an unsatisfiable one would be a dead branch the proof rejects.
func TestProve_Guards_NoDeadBranch(t *testing.T) {
	report := mustProve(t)
	if len(report.Guards) != 1 {
		t.Fatalf("Guards has %d entries, want 1: %v", len(report.Guards), report.Guards)
	}
	g := report.Guards[0]
	if !g.Satisfiable {
		t.Errorf("guard %q is unsatisfiable (dead branch), want satisfiable", g.Guard)
	}
	if g.Guard == "" {
		t.Error("guard rendering is empty, want a readable expression")
	}
}

// TestProve_Sound confirms the aggregate verdict is sound: every key stage reachable,
// the Watchdog leaves mutually exclusive, and no guard a dead branch.
func TestProve_Sound(t *testing.T) {
	report := mustProve(t)
	if !report.Sound() {
		t.Errorf("Sound() = false, want true; report = %+v", report)
	}
}

// TestProofReport_Sound_FalseBranches exercises the failure arms of Sound so an
// unreachable stage, a violated invariant, and a dead guard each flip the verdict —
// the report's contract a release gate relies on.
func TestProofReport_Sound_FalseBranches(t *testing.T) {
	base := func() ProofReport {
		return ProofReport{
			Reachable:         map[string]bool{"Delivered": true},
			WatchdogExclusive: true,
			Guards:            []GuardVerdict{{Guard: "admit", Satisfiable: true}},
		}
	}

	if !base().Sound() {
		t.Fatal("base report should be sound")
	}

	tests := []struct {
		name   string
		mutate func(*ProofReport)
	}{
		{"unreachable stage", func(r *ProofReport) { r.Reachable["Delivered"] = false }},
		{"watchdog not exclusive", func(r *ProofReport) { r.WatchdogExclusive = false }},
		{"dead guard", func(r *ProofReport) { r.Guards[0].Satisfiable = false }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := base()
			tc.mutate(&r)
			if r.Sound() {
				t.Errorf("Sound() = true after %q, want false", tc.name)
			}
		})
	}
}

// TestRenderGuard_NodeShapes covers the guard-rendering arms the saga does not itself
// exercise — in-state, not, equality, membership, and the bare-op fallback — so the
// report renders every node shape readably.
func TestRenderGuard_NodeShapes(t *testing.T) {
	type node = state.GuardNode[fooddelivery.Stage]
	priority := state.Field[fooddelivery.Stage]("priority")
	tests := []struct {
		name string
		node node
		want string
	}{
		{"leaf", state.Guard[fooddelivery.Stage]("admit"), "admit"},
		{"stateIn", state.StateIn(fooddelivery.Active), "in(Active)"},
		{"not", state.Not(state.Guard[fooddelivery.Stage]("admit")), "not(admit)"},
		{"and", state.And(state.Guard[fooddelivery.Stage]("a"), state.Guard[fooddelivery.Stage]("b")), "and(a, b)"},
		{"or", state.Or(state.Guard[fooddelivery.Stage]("a"), state.Guard[fooddelivery.Stage]("b")), "or(a, b)"},
		{"equality", priority.Eq(state.Str[fooddelivery.Stage]("rush")), "priority eq rush"},
		{"membership", priority.In(state.Str[fooddelivery.Stage]("rush")), "priority in set"},
		{"less-than", priority.Lt(state.Int[fooddelivery.Stage](5)), "priority lt 5"},
		{"field", node{Op: state.GuardField, Path: "subtotal"}, "subtotal"},
		{"literal", node{Op: state.GuardLit, Lit: &state.Literal{Type: state.FloatParam, Value: 7}}, "7"},
		{"leaf-no-ref", node{Op: state.GuardLeaf}, "leaf"},
		{"stateIn-no-target", node{Op: state.GuardStateIn}, "in"},
		{"literal-no-lit", node{Op: state.GuardLit}, "lit"},
		{"fallback", node{Op: state.GuardOp("custom")}, "custom"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := renderGuard(tc.node); got != tc.want {
				t.Errorf("renderGuard(%s) = %q, want %q", tc.name, got, tc.want)
			}
		})
	}
}

// TestEffectiveGuard_Composition covers how a transition's named-ref guards and its
// composite GuardExpr fold into one effective guard: zero guards yield the empty
// node, a single guard passes through unwrapped, and several conjoin under and().
func TestEffectiveGuard_Composition(t *testing.T) {
	type tx = state.Transition[fooddelivery.Stage, fooddelivery.Signal, fooddelivery.Order]
	expr := state.Guard[fooddelivery.Stage]("expr")

	t.Run("none", func(t *testing.T) {
		got := effectiveGuard(tx{})
		if got.Op != "" || len(got.Children) != 0 || got.Ref != nil {
			t.Errorf("empty transition yielded %+v, want zero node", got)
		}
	})

	t.Run("single ref", func(t *testing.T) {
		got := effectiveGuard(tx{Guards: []state.Ref{{Name: "admit"}}})
		if renderGuard(got) != "admit" {
			t.Errorf("single ref rendered %q, want %q", renderGuard(got), "admit")
		}
	})

	t.Run("single expr", func(t *testing.T) {
		got := effectiveGuard(tx{GuardExpr: &expr})
		if renderGuard(got) != "expr" {
			t.Errorf("single expr rendered %q, want %q", renderGuard(got), "expr")
		}
	})

	t.Run("ref and expr", func(t *testing.T) {
		got := effectiveGuard(tx{Guards: []state.Ref{{Name: "admit"}}, GuardExpr: &expr})
		if want := "and(admit, expr)"; renderGuard(got) != want {
			t.Errorf("ref+expr rendered %q, want %q", renderGuard(got), want)
		}
	})
}
