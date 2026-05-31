package verify_test

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/verify"
)

// updateGolden rewrites the committed verify-report goldens when set, so a
// deliberate change to a fixture machine or to report rendering refreshes the
// goldens in one reviewable diff.
var updateGolden = flag.Bool("update-golden", false, "rewrite golden verify reports")

// goldenCase pins one machine's full verify report as a golden file. The report
// is deterministic (findings sorted by kind then state, witnesses minimal), so a
// committed golden catches any drift in reachability verdicts or witness paths.
type goldenCase struct {
	name    string
	machine *state.Machine[string, string, any]
	opts    []verify.Option
}

func goldenCases() []goldenCase {
	return []goldenCase{
		{name: "linear", machine: linearChain()},
		{name: "branching", machine: branching()},
		{name: "island", machine: withUnreachable()},
		{name: "parallel", machine: parallelMachine()},
		{
			name:    "reach_avoiding",
			machine: branchingAvoid(),
			opts: []verify.Option{
				verify.ReachAvoiding("goal", "hazard"), // satisfiable via the clean arm
				verify.ReachAvoiding("hazard", "calm"), // satisfiable: hazard is on its own arm
				verify.ReachAvoiding("calm", "start"),  // unsatisfiable: start gates every route
			},
		},
		{
			name:    "liveness_holds",
			machine: liveToGoal(),
			opts:    []verify.Option{verify.AlwaysEventually("done")},
		},
		{
			name:    "liveness_trap",
			machine: trapBeforeGoal(),
			opts:    []verify.Option{verify.AlwaysEventually("goal")},
		},
		{
			name:    "liveness_cycle",
			machine: zFreeCycle(),
			opts:    []verify.Option{verify.AlwaysEventually("goal")},
		},
		{
			name:    "invariant",
			machine: parallelMachine(),
			opts: []verify.Option{
				verify.CheckInvariant(
					verify.MutualExclusion("busy", "loud"), // violated: co-active
					verify.Implies("busy", "active"),       // holds: nested
					verify.NeverActive("offline"),          // violated: initial state
				),
			},
		},
	}
}

// TestGoldenReport diffs each machine's verify report against its committed
// golden. Run with -update-golden to refresh after an intended change. The
// report rendering is order-stable, so the diff is reproducible.
func TestGoldenReport(t *testing.T) {
	dir := filepath.Join("testdata", "report")
	if *updateGolden {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir golden dir: %v", err)
		}
	}
	for _, gc := range goldenCases() {
		t.Run(gc.name, func(t *testing.T) {
			got := verify.Verify(gc.machine, gc.opts...).String() + "\n"
			path := filepath.Join(dir, gc.name+".txt")
			if *updateGolden {
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run -update-golden to create): %v", err)
			}
			if got != string(want) {
				t.Errorf("report golden mismatch for %s; run with -update-golden if intended\n got:\n%s\nwant:\n%s", gc.name, got, want)
			}
		})
	}
}
