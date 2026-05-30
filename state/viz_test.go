package state_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// vizRender pairs a format extension with a renderer over a machine, so the same
// golden harness drives both Mermaid and DOT across every example machine.
type vizRender struct {
	machine string
	format  string // "mermaid" | "dot"
	render  func() string
}

// vizGoldens returns the full matrix of diagram goldens: each example machine
// (flat, hierarchical, parallel) in each format. These reuse the example
// machines defined for the IR goldens, keeping a single source of fixtures.
func vizGoldens() []vizRender {
	return []vizRender{
		{"document", "mermaid", func() string { return buildDocMachine().ToMermaid() }},
		{"document", "dot", func() string { return buildDocMachine().ToDOT() }},
		{"job", "mermaid", func() string { return buildJobMachine().ToMermaid() }},
		{"job", "dot", func() string { return buildJobMachine().ToDOT() }},
		{"worker", "mermaid", func() string { return buildWorkerMachine().ToMermaid() }},
		{"worker", "dot", func() string { return buildWorkerMachine().ToDOT() }},
	}
}

// vizExt maps a format name to its file extension.
func vizExt(format string) string {
	if format == "mermaid" {
		return ".mmd"
	}
	return ".dot"
}

// TestGoldenViz diffs each machine's Mermaid and DOT rendering against its
// committed golden. A change to a machine surfaces as a reviewable diagram diff
// in CI. Run with -update-golden to refresh after an intended change.
func TestGoldenViz(t *testing.T) {
	for _, g := range vizGoldens() {
		t.Run(g.machine+"_"+g.format, func(t *testing.T) {
			dir := filepath.Join("testdata", g.format)
			path := filepath.Join(dir, g.machine+vizExt(g.format))
			got := g.render()

			if *updateGolden {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					t.Fatalf("mkdir: %v", err)
				}
				if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
					t.Fatalf("write golden: %v", err)
				}
				return
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden (run -update-golden to create): %v", err)
			}
			if string(want) != got {
				t.Errorf("%s %s golden mismatch; run with -update-golden if intended\n--- got ---\n%s",
					g.machine, g.format, got)
			}
		})
	}
}

// TestToMermaid_RendersFromLoadedJSON asserts the renderers are pure functions
// of the IR: a machine round-tripped through JSON renders identically to the
// original forged machine.
func TestToMermaid_RendersFromLoadedJSON(t *testing.T) {
	orig := buildDocMachine()
	want := orig.ToMermaid()

	raw, err := orig.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON: %v", err)
	}
	ir, err := state.LoadFromJSON[DocState, DocEvent, *Document](raw)
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	reloaded := ir.Provide(docRegistry()).Quench()
	if got := reloaded.ToMermaid(); got != want {
		t.Errorf("Mermaid diverged after JSON round-trip\n--- want ---\n%s\n--- got ---\n%s", want, got)
	}
}

// TestToMermaid_StartAndFinalMarkers asserts the initial and final markers
// appear for a flat machine.
func TestToMermaid_StartAndFinalMarkers(t *testing.T) {
	out := buildJobMachine().ToMermaid()
	if !strings.Contains(out, "[*] --> Queued") {
		t.Errorf("missing initial marker for Queued:\n%s", out)
	}
	if !strings.Contains(out, "JobDone --> [*]") {
		t.Errorf("missing final marker for JobDone:\n%s", out)
	}
}

// TestToMermaid_GuardAnnotations asserts guards render as a bracketed suffix on
// the event label and that WithoutGuards drops them. The document machine's
// state/event types render numerically (they declare no String method), so the
// guarded Approve edge is "1 -> 2" labeled "1 [hasReviewer]".
func TestToMermaid_GuardAnnotations(t *testing.T) {
	with := buildDocMachine().ToMermaid()
	if !strings.Contains(with, "1 [hasReviewer]") {
		t.Errorf("expected guarded edge label, got:\n%s", with)
	}
	without := buildDocMachine().ToMermaid(state.WithoutGuards())
	if strings.Contains(without, "[hasReviewer]") {
		t.Errorf("WithoutGuards should drop guard label, got:\n%s", without)
	}
	if !strings.Contains(without, "1 --> 2: 1") {
		t.Errorf("WithoutGuards should keep the event label, got:\n%s", without)
	}
}

// TestToMermaid_Hierarchy asserts a compound state renders as a nested block
// with its own initial child.
func TestToMermaid_Hierarchy(t *testing.T) {
	out := buildJobMachine().ToMermaid()
	if !strings.Contains(out, "state Running {") {
		t.Errorf("expected Running superstate block, got:\n%s", out)
	}
	if !strings.Contains(out, "[*] --> Running__Starting") {
		t.Errorf("expected Running's initial child, got:\n%s", out)
	}
}

// TestToMermaid_ParallelRegions asserts parallel regions render with the --
// divider and qualified member ids.
func TestToMermaid_ParallelRegions(t *testing.T) {
	out := buildWorkerMachine().ToMermaid()
	if !strings.Contains(out, "state Active {") {
		t.Errorf("expected Active parallel block, got:\n%s", out)
	}
	if strings.Count(out, "--\n") < 1 {
		t.Errorf("expected a region divider, got:\n%s", out)
	}
	if !strings.Contains(out, "Active_Execution__Idle") {
		t.Errorf("expected qualified region member id, got:\n%s", out)
	}
}

// TestToMermaid_Owners asserts owner color-coding renders by default and is
// dropped under WithoutOwners.
func TestToMermaid_Owners(t *testing.T) {
	with := buildDocMachine().ToMermaid()
	if !strings.Contains(with, "classDef owner_Author") {
		t.Errorf("expected owner classDef, got:\n%s", with)
	}
	without := buildDocMachine().ToMermaid(state.WithoutOwners())
	if strings.Contains(without, "classDef") {
		t.Errorf("WithoutOwners should drop classDef, got:\n%s", without)
	}
}

// TestToMermaid_Direction asserts the direction option is honored.
func TestToMermaid_Direction(t *testing.T) {
	if out := buildDocMachine().ToMermaid(state.LeftToRight()); !strings.Contains(out, "direction LR") {
		t.Errorf("expected direction LR, got:\n%s", out)
	}
	if out := buildDocMachine().ToMermaid(state.TopToBottom()); !strings.Contains(out, "direction TB") {
		t.Errorf("expected direction TB, got:\n%s", out)
	}
}

// TestToDOT_Structure asserts the DOT output carries the digraph header, start
// marker, clusters for compound/parallel states, and final-node styling.
func TestToDOT_Structure(t *testing.T) {
	out := buildJobMachine().ToDOT()
	for _, want := range []string{
		"digraph \"job\" {",
		"rankdir=LR;",
		"__start [shape=point];",
		"subgraph cluster_",
		"label=\"Running\";",
		"peripheries=2",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("DOT missing %q:\n%s", want, out)
		}
	}
}

// TestToDOT_ParallelClusters asserts each region of a parallel state renders as
// a nested cluster.
func TestToDOT_ParallelClusters(t *testing.T) {
	out := buildWorkerMachine().ToDOT()
	if !strings.Contains(out, "label=\"Active\";") {
		t.Errorf("expected Active cluster label, got:\n%s", out)
	}
	if !strings.Contains(out, "label=\"Execution\";") || !strings.Contains(out, "label=\"Telemetry\";") {
		t.Errorf("expected region cluster labels, got:\n%s", out)
	}
}

// TestToDOT_OwnersAndDirection asserts owner fill and rankdir options.
func TestToDOT_OwnersAndDirection(t *testing.T) {
	with := buildDocMachine().ToDOT()
	if !strings.Contains(with, "fillcolor=") {
		t.Errorf("expected owner fillcolor, got:\n%s", with)
	}
	without := buildDocMachine().ToDOT(state.WithoutOwners())
	if strings.Contains(without, "fillcolor=") {
		t.Errorf("WithoutOwners should drop fillcolor, got:\n%s", without)
	}
	if out := buildDocMachine().ToDOT(state.TopToBottom()); !strings.Contains(out, "rankdir=TB;") {
		t.Errorf("expected rankdir=TB, got:\n%s", out)
	}
}

// TestViz_Deterministic asserts repeated renders are byte-identical.
func TestViz_Deterministic(t *testing.T) {
	for _, g := range vizGoldens() {
		first := g.render()
		second := g.render()
		if first != second {
			t.Errorf("%s %s render is not deterministic", g.machine, g.format)
		}
	}
}

// TestViz_EventlessAndSanitizedIDs renders a string-keyed machine loaded from
// JSON to cover two rendering paths the example machines do not: an eventless,
// guardless transition (which produces a bare, unlabeled edge) and a state name
// carrying characters Mermaid disallows in ids (sanitized to underscores).
func TestViz_EventlessAndSanitizedIDs(t *testing.T) {
	const raw = `{
		"name": "flow",
		"initial": "Start State",
		"hasInitial": true,
		"states": [
			{"name": "Start State", "transitions": [{"from": "Start State", "to": "End-State", "eventLess": true}]},
			{"name": "End-State", "isFinal": true}
		]
	}`
	ir, err := state.LoadFromJSON[string, string, any]([]byte(raw))
	if err != nil {
		t.Fatalf("LoadFromJSON: %v", err)
	}
	m := ir.Provide(state.NewRegistry[any]()).Quench()

	mmd := m.ToMermaid()
	// The space and hyphen collapse to underscores in Mermaid ids.
	if !strings.Contains(mmd, "Start_State --> End_State") {
		t.Errorf("expected sanitized, unlabeled eventless edge, got:\n%s", mmd)
	}
	if strings.Contains(mmd, "Start_State --> End_State:") {
		t.Errorf("eventless guardless edge must carry no label, got:\n%s", mmd)
	}
	if !strings.Contains(mmd, "End_State --> [*]") {
		t.Errorf("expected final marker, got:\n%s", mmd)
	}

	dot := m.ToDOT()
	// DOT quotes the raw names verbatim, so the edge has no label attribute.
	if !strings.Contains(dot, `"Start State" -> "End-State";`) {
		t.Errorf("expected unlabeled DOT edge with raw names, got:\n%s", dot)
	}
}
