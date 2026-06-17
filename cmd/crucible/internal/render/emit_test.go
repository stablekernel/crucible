package render

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
)

var update = flag.Bool("update", false, "update golden files")

// goldenCheck emits D2 for vm and compares it to testdata/<name>.d2.golden,
// writing the golden when -update is set.
func goldenCheck(t *testing.T, name string, vm viewmodel.ViewModel) string {
	t.Helper()
	got, err := EmitD2(vm, DefaultTheme)
	if err != nil {
		t.Fatalf("EmitD2: %v", err)
	}
	path := filepath.Join("testdata", name+".d2.golden")
	if *update {
		if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return got
	}
	want, rErr := os.ReadFile(path)
	if rErr != nil {
		t.Fatalf("read golden (run with -update first): %v", rErr)
	}
	if got != string(want) {
		t.Errorf("EmitD2 output mismatch for %s.\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
	return got
}

func TestEmit_AtomicOnPath(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "a", Name: "a", Kind: viewmodel.NodeAtomic, OnPath: true},
			{ID: "b", Name: "b", Kind: viewmodel.NodeAtomic, OnPath: true},
		},
		Edges: []viewmodel.ViewEdge{
			{From: "a", To: "b", Event: "go", Kind: viewmodel.EdgeEvent, OnPath: true},
		},
	}
	got := goldenCheck(t, "atomic_onpath", vm)
	if !strings.Contains(got, "class: state") {
		t.Error("want class: state for on-path atomic")
	}
	if !strings.Contains(got, `style.stroke: "`+DefaultTheme.Hot+`"`) {
		t.Error("want hot rim stroke on on-path atomic")
	}
	if !strings.Contains(got, "class: hot_edge") {
		t.Error("want hot_edge class for on-path edge")
	}
}

func TestEmit_OffPath(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "a", Name: "a", Kind: viewmodel.NodeAtomic},
			{ID: "b", Name: "b", Kind: viewmodel.NodeAtomic},
		},
		Edges: []viewmodel.ViewEdge{
			{From: "a", To: "b", Event: "go", Kind: viewmodel.EdgeEvent},
		},
	}
	got := goldenCheck(t, "offpath", vm)
	if !strings.Contains(got, "class: dim_node") {
		t.Error("want dim_node for off-path atomic")
	}
	if !strings.Contains(got, "class: dim_edge") {
		t.Error("want dim_edge for off-path event edge")
	}
}

func TestEmit_LifecycleCompartment(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{
				ID:     "session",
				Name:   "Session",
				Kind:   viewmodel.NodeAtomic,
				Entry:  []viewmodel.DetailItem{{Name: "openSocket"}},
				Exit:   []viewmodel.DetailItem{{Name: "clearTimers"}},
				Invoke: []viewmodel.DetailItem{{Name: "paymentService"}},
			},
		},
	}
	got := goldenCheck(t, "lifecycle", vm)
	if !strings.Contains(got, "shape: class") {
		t.Error("want shape: class for lifecycle node")
	}
	for _, want := range []string{"entry: openSocket", "exit: clearTimers", "invoke: paymentService"} {
		if !strings.Contains(got, want) {
			t.Errorf("want lifecycle row %q", want)
		}
	}
}

func TestEmit_FinalNode(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "done", Name: "done", Kind: viewmodel.NodeFinal},
		},
	}
	got := goldenCheck(t, "final", vm)
	if !strings.Contains(got, "class: final") {
		t.Error("want class: final")
	}
	if !strings.Contains(got, `label: ""`) {
		t.Error("want empty label for final node")
	}
}

func TestEmit_ParallelWithRegions(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "par", Name: "par", Kind: viewmodel.NodeParallel},
			{ID: "ra1", Name: "ra1", Kind: viewmodel.NodeAtomic},
			{ID: "rb1", Name: "rb1", Kind: viewmodel.NodeAtomic},
		},
		Containers: []viewmodel.ViewContainer{
			{ID: "par", Name: "par", Kind: "parallel", Children: []string{"regionA", "regionB"}},
			{ID: "regionA", Name: "regionA", Kind: "region", Children: []string{"ra1"}},
			{ID: "regionB", Name: "regionB", Kind: "region", Children: []string{"rb1"}},
		},
	}
	got := goldenCheck(t, "parallel_regions", vm)
	if strings.Count(got, "class: region") != 2 {
		t.Errorf("want two region classes, got %d", strings.Count(got, "class: region"))
	}
	// The parallel container must carry EXPLICIT forge styling so no DarkMauve
	// (mauve fill/border, lavender title) bleeds through. Off-path here -> plain
	// ember stroke at width 2.
	for _, want := range []string{
		"style.fill: " + quote(DefaultTheme.SteelDark),
		"style.stroke: " + quote(DefaultTheme.Ember),
		"style.font-color: " + quote(DefaultTheme.TextWarm),
		"style.stroke-width: 2",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("want parallel container styling %q", want)
		}
	}
}

func TestEmit_CompositeContainerStyling(t *testing.T) {
	// An on-path composite container must use the HOT ember stroke at the heavier
	// width 3, with the steel panel fill and warm title.
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "active", Name: "active", Kind: viewmodel.NodeComposite, OnPath: true},
			{ID: "inner", Name: "inner", Kind: viewmodel.NodeAtomic, OnPath: true},
		},
		Containers: []viewmodel.ViewContainer{
			{ID: "active", Name: "active", Kind: "composite", Children: []string{"inner"}},
		},
	}
	got, err := EmitD2(vm, DefaultTheme)
	if err != nil {
		t.Fatalf("EmitD2: %v", err)
	}
	for _, want := range []string{
		"style.fill: " + quote(DefaultTheme.SteelDark),
		"style.stroke: " + quote(DefaultTheme.Hot),
		"style.font-color: " + quote(DefaultTheme.TextWarm),
		"style.stroke-width: 3",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("want on-path composite styling %q", want)
		}
	}
}

func TestEmit_SpecialCharEscaping(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "a", Name: "a", Kind: viewmodel.NodeAtomic},
			{ID: "b", Name: "b", Kind: viewmodel.NodeAtomic},
		},
		Edges: []viewmodel.ViewEdge{
			{From: "a", To: "b", Event: "done · ok", Kind: viewmodel.EdgeEvent},
		},
	}
	got := goldenCheck(t, "special_chars", vm)
	if !strings.Contains(got, `"done · ok"`) {
		t.Error("want quoted label containing the middle dot")
	}
}

// TestEmit_GuardOnlyEdgeQuoted is the regression for the bracket-array bug: an
// EVENTLESS transition carrying ONLY a guard yields the edge label "[hasStock]".
// Emitted bare ("a -> b: [hasStock]") D2 parses the value as an ARRAY and fails
// to compile; it MUST be emitted double-quoted. The second edge carries an
// embedded quote and backslash to pin the escaping rule (\" and \\). The golden
// captures both quoted forms.
func TestEmit_GuardOnlyEdgeQuoted(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "a", Name: "a", Kind: viewmodel.NodeAtomic},
			{ID: "b", Name: "b", Kind: viewmodel.NodeAtomic},
			{ID: "c", Name: "c", Kind: viewmodel.NodeAtomic},
		},
		Edges: []viewmodel.ViewEdge{
			// Eventless + guard only -> label becomes "[hasStock]".
			{From: "a", To: "b", Kind: viewmodel.EdgeEventless, Guards: []viewmodel.DetailItem{{Name: "hasStock"}}},
			// Event label containing a quote and a backslash -> must escape both.
			{From: "b", To: "c", Event: `say "hi" \ end`, Kind: viewmodel.EdgeEvent},
		},
	}
	got := goldenCheck(t, "guard_only_edge", vm)
	if !strings.Contains(got, `a -> b: "[hasStock]"`) {
		t.Errorf("want quoted guard-only edge label, got:\n%s", got)
	}
	if strings.Contains(got, `a -> b: [hasStock]`) {
		t.Error("guard-only label emitted BARE — D2 would parse it as an array")
	}
	// Backslash escaped first, then quote: \\ and \" both present.
	if !strings.Contains(got, `b -> c: "say \"hi\" \\ end"`) {
		t.Errorf("want escaped quote+backslash in label, got:\n%s", got)
	}
}
