package viewmodel_test

import (
	"os"
	"testing"
	"time"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
	"github.com/stablekernel/crucible/state"
)

// loadComposite loads the shared composite.json fixture relative to this test
// package and fails the test if it cannot be read or parsed.
func loadComposite(t *testing.T) *state.IR[string, string, any] {
	t.Helper()
	b, err := os.ReadFile("../../testdata/composite.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		t.Fatalf("load fixture: %v", err)
	}
	return ir
}

// nodeByID returns the ViewNode with the given ID, failing the test if absent.
func nodeByID(t *testing.T, vm viewmodel.ViewModel, id string) viewmodel.ViewNode {
	t.Helper()
	for i := range vm.Nodes {
		if vm.Nodes[i].ID == id {
			return vm.Nodes[i]
		}
	}
	t.Fatalf("node %q not found; have %v", id, nodeIDs(vm))
	return viewmodel.ViewNode{}
}

func nodeIDs(vm viewmodel.ViewModel) []string {
	out := make([]string, 0, len(vm.Nodes))
	for i := range vm.Nodes {
		out = append(out, vm.Nodes[i].ID)
	}
	return out
}

// edgeByFromTo returns the first edge matching the from/to pair.
func edgeByFromTo(t *testing.T, vm viewmodel.ViewModel, from, to string) viewmodel.ViewEdge {
	t.Helper()
	for i := range vm.Edges {
		if vm.Edges[i].From == from && vm.Edges[i].To == to {
			return vm.Edges[i]
		}
	}
	t.Fatalf("edge %s->%s not found", from, to)
	return viewmodel.ViewEdge{}
}

func containerByID(t *testing.T, vm viewmodel.ViewModel, id string) viewmodel.ViewContainer {
	t.Helper()
	for i := range vm.Containers {
		if vm.Containers[i].ID == id {
			return vm.Containers[i]
		}
	}
	t.Fatalf("container %q not found", id)
	return viewmodel.ViewContainer{}
}

func hasItem(items []viewmodel.DetailItem, name string) bool {
	for i := range items {
		if items[i].Name == name {
			return true
		}
	}
	return false
}

func itemByName(items []viewmodel.DetailItem, name string) (viewmodel.DetailItem, bool) {
	for i := range items {
		if items[i].Name == name {
			return items[i], true
		}
	}
	return viewmodel.DetailItem{}, false
}

func contains(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// fullOpts returns options at the Full detail level (everything implied).
func fullOpts() viewmodel.ProjectionOptions {
	return viewmodel.ProjectionOptions{Level: viewmodel.Full}
}

func TestBuild_NodeKinds_FromFixture(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, fullOpts())

	cases := []struct {
		id   string
		want viewmodel.NodeKind
	}{
		// "active" is both composite (has children) AND the initial state.
		// Documented precedence: composite wins over initial.
		{"active", viewmodel.NodeComposite},
		{"parallel", viewmodel.NodeParallel},
		{"review", viewmodel.NodeFinal},
		{"done", viewmodel.NodeFinal},
		{"ra2", viewmodel.NodeFinal},
		{"working", viewmodel.NodeInvoke}, // has invoke, no children/regions/final
		{"ra1", viewmodel.NodeAtomic},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			got := nodeByID(t, vm, tc.id).Kind
			if got != tc.want {
				t.Fatalf("node %q kind = %q, want %q", tc.id, got, tc.want)
			}
		})
	}
}

func TestBuild_NodeKind_InvokeOnlyConstructed(t *testing.T) {
	ir := &state.IR[string, string, any]{
		Name:       "m",
		Initial:    "other",
		HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "other"},
			{Name: "svc", Invoke: []state.Invocation[string, string, any]{
				{ID: "i1", Src: state.Ref{Name: "doThing"}},
			}},
		},
	}
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Lifecycle})
	if got := nodeByID(t, vm, "svc").Kind; got != viewmodel.NodeInvoke {
		t.Fatalf("svc kind = %q, want invoke", got)
	}
	// "other" is the initial atomic state.
	if got := nodeByID(t, vm, "other").Kind; got != viewmodel.NodeInitial {
		t.Fatalf("other kind = %q, want initial", got)
	}
}

func TestBuild_DetailLadder_Outline(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Outline})

	e := edgeByFromTo(t, vm, "working", "review")
	if len(e.Guards) != 0 || len(e.Effects) != 0 || len(e.Assigns) != 0 {
		t.Fatalf("outline edge should carry no details, got guards=%v effects=%v assigns=%v",
			e.Guards, e.Effects, e.Assigns)
	}
	active := nodeByID(t, vm, "active")
	if len(active.Entry) != 0 || len(active.Exit) != 0 || len(active.Done) != 0 {
		t.Fatalf("outline node should carry no lifecycle items, got %+v", active)
	}
	working := nodeByID(t, vm, "working")
	if len(working.Invoke) != 0 {
		t.Fatalf("outline node should carry no invoke items, got %v", working.Invoke)
	}
}

func TestBuild_DetailLadder_Guards(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Guards})

	e := edgeByFromTo(t, vm, "working", "review")
	if !hasItem(e.Guards, "canSubmit") {
		t.Fatalf("guards level: edge should include guard canSubmit, got %v", e.Guards)
	}
	if len(e.Effects) != 0 || len(e.Assigns) != 0 {
		t.Fatalf("guards level: effects/assigns must be absent, got effects=%v assigns=%v",
			e.Effects, e.Assigns)
	}
}

func TestBuild_DetailLadder_Actions(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Actions})

	e := edgeByFromTo(t, vm, "working", "review")
	if !hasItem(e.Guards, "canSubmit") {
		t.Fatalf("actions level: missing guard canSubmit, got %v", e.Guards)
	}
	if !hasItem(e.Effects, "notify") {
		t.Fatalf("actions level: missing effect notify, got %v", e.Effects)
	}
	if !hasItem(e.Assigns, "stamp") {
		t.Fatalf("actions level: missing assign stamp, got %v", e.Assigns)
	}
	active := nodeByID(t, vm, "active")
	if len(active.Entry) != 0 || len(active.Exit) != 0 {
		t.Fatalf("actions level: lifecycle must still be absent, got entry=%v exit=%v",
			active.Entry, active.Exit)
	}
}

func TestBuild_DetailLadder_Lifecycle(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Lifecycle})

	active := nodeByID(t, vm, "active")
	if !hasItem(active.Entry, "logEntry") {
		t.Fatalf("lifecycle: active entry missing logEntry, got %v", active.Entry)
	}
	if !hasItem(active.Entry, "seedCtx") {
		t.Fatalf("lifecycle: active entry-assign seedCtx should be folded into Entry, got %v", active.Entry)
	}
	if !hasItem(active.Exit, "logExit") {
		t.Fatalf("lifecycle: active exit missing logExit, got %v", active.Exit)
	}
	if !hasItem(active.Exit, "clearCtx") {
		t.Fatalf("lifecycle: active exit-assign clearCtx should be folded into Exit, got %v", active.Exit)
	}
	if !hasItem(active.Done, "logDone") {
		t.Fatalf("lifecycle: active done missing logDone, got %v", active.Done)
	}
	working := nodeByID(t, vm, "working")
	if !hasItem(working.Invoke, "fetchWork") {
		t.Fatalf("lifecycle: working invoke missing fetchWork, got %v", working.Invoke)
	}
	// Edge still resolves at this level.
	e := edgeByFromTo(t, vm, "working", "review")
	if e.Event != "submit" {
		t.Fatalf("lifecycle: edge event = %q, want submit", e.Event)
	}
}

func TestBuild_DetailLadder_Full_DescriptionsAndDelay(t *testing.T) {
	// Build a resolver so descriptions can be populated.
	reg := state.NewRegistry[any]()
	reg.Guard("canSubmit", func(state.GuardCtx[any]) bool { return true },
		state.Describe("checks submit precondition").Category("guards"))
	ir := loadComposite(t)
	resolver := viewmodel.NewRefResolver(state.BuiltinPalette(), reg.Palette())

	vm := viewmodel.Build(ir, resolver, fullOpts())
	e := edgeByFromTo(t, vm, "working", "review")
	g, ok := itemByName(e.Guards, "canSubmit")
	if !ok {
		t.Fatalf("full: guard canSubmit missing, got %v", e.Guards)
	}
	if g.Description != "checks submit precondition" {
		t.Fatalf("full: guard description = %q, want %q", g.Description, "checks submit precondition")
	}

	// Delayed edge built from a struct literal (no fixture has After).
	d := 5 * time.Second
	delayedIR := &state.IR[string, string, any]{
		Name: "d", Initial: "a", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "a", Transitions: []state.Transition[string, string, any]{
				{From: "a", To: "b", After: &d},
			}},
			{Name: "b", IsFinal: true},
		},
	}
	dvm := viewmodel.Build(delayedIR, nil, fullOpts())
	de := edgeByFromTo(t, dvm, "a", "b")
	if de.Kind != viewmodel.EdgeDelayed {
		t.Fatalf("delayed edge kind = %q, want delayed", de.Kind)
	}
	if de.After != d.String() {
		t.Fatalf("delayed edge After = %q, want %q", de.After, d.String())
	}
}

func TestBuild_Toggles_ShowForcesDimension(t *testing.T) {
	ir := loadComposite(t)
	// Outline level normally implies nothing, but Show:[invoke] forces invoke.
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{
		Level: viewmodel.Outline,
		Show:  []viewmodel.Dimension{viewmodel.DimInvoke},
	})
	working := nodeByID(t, vm, "working")
	if !hasItem(working.Invoke, "fetchWork") {
		t.Fatalf("show invoke: working should carry fetchWork, got %v", working.Invoke)
	}
	e := edgeByFromTo(t, vm, "working", "review")
	if len(e.Guards) != 0 || len(e.Effects) != 0 {
		t.Fatalf("show invoke: guards/effects must stay absent, got guards=%v effects=%v",
			e.Guards, e.Effects)
	}
}

func TestBuild_Toggles_HideDropsDimension(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{
		Level: viewmodel.Full,
		Hide:  []viewmodel.Dimension{viewmodel.DimGuards},
	})
	e := edgeByFromTo(t, vm, "working", "review")
	if len(e.Guards) != 0 {
		t.Fatalf("hide guards: guards should be dropped, got %v", e.Guards)
	}
	// Effects/assigns remain present (only guards hidden).
	if !hasItem(e.Effects, "notify") {
		t.Fatalf("hide guards: effects should remain, got %v", e.Effects)
	}
}

func TestBuild_Toggles_ShowBeatsHide(t *testing.T) {
	ir := loadComposite(t)
	// When both Show and Hide name the same dimension, Show wins.
	vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{
		Level: viewmodel.Outline,
		Show:  []viewmodel.Dimension{viewmodel.DimGuards},
		Hide:  []viewmodel.Dimension{viewmodel.DimGuards},
	})
	e := edgeByFromTo(t, vm, "working", "review")
	if !hasItem(e.Guards, "canSubmit") {
		t.Fatalf("show beats hide: guards should be present, got %v", e.Guards)
	}
}

func TestBuild_EdgeKinds_Constructed(t *testing.T) {
	d := 2 * time.Minute
	ir := &state.IR[string, string, any]{
		Name: "edges", Initial: "s", HasInitial: true,
		States: []state.State[string, string, any]{
			{Name: "s", Transitions: []state.Transition[string, string, any]{
				{From: "s", To: "evtless", EventLess: true},
				{From: "s", To: "delayed", After: &d},
				{From: "s", To: "wild", Wildcard: true},
				{From: "s", To: "forbid", Forbidden: true},
				{From: "s", To: "s", On: "ping", Internal: true},
				{From: "s", To: "plain", On: "go"},
			}},
			{Name: "evtless"},
			{Name: "delayed"},
			{Name: "wild"},
			{Name: "forbid"},
			{Name: "plain"},
		},
	}

	t.Run("delays_shown_at_full", func(t *testing.T) {
		vm := viewmodel.Build(ir, nil, fullOpts())
		want := map[string]viewmodel.EdgeKind{
			"evtless": viewmodel.EdgeEventless,
			"delayed": viewmodel.EdgeDelayed,
			"wild":    viewmodel.EdgeWildcard,
			"forbid":  viewmodel.EdgeForbidden,
			"plain":   viewmodel.EdgeEvent,
		}
		for to, kind := range want {
			e := edgeByFromTo(t, vm, "s", to)
			if e.Kind != kind {
				t.Fatalf("edge s->%s kind = %q, want %q", to, e.Kind, kind)
			}
		}
		// Internal self-transition.
		var internalFound bool
		for i := range vm.Edges {
			if vm.Edges[i].To == "s" && vm.Edges[i].Kind == viewmodel.EdgeInternal {
				internalFound = true
			}
		}
		if !internalFound {
			t.Fatalf("internal edge not found among %d edges", len(vm.Edges))
		}
		de := edgeByFromTo(t, vm, "s", "delayed")
		if de.After != d.String() {
			t.Fatalf("delayed After = %q, want %q", de.After, d.String())
		}
	})

	t.Run("delay_string_absent_when_delays_off", func(t *testing.T) {
		vm := viewmodel.Build(ir, nil, viewmodel.ProjectionOptions{Level: viewmodel.Actions})
		de := edgeByFromTo(t, vm, "s", "delayed")
		if de.Kind != viewmodel.EdgeDelayed {
			t.Fatalf("kind should still be delayed, got %q", de.Kind)
		}
		if de.After != "" {
			t.Fatalf("After string should be empty when delays not shown, got %q", de.After)
		}
	})
}

func TestRefResolver_BuiltinResolves(t *testing.T) {
	r := viewmodel.NewRefResolver(state.BuiltinPalette(), nil)
	d, ok := r.Resolve(string(state.GuardStateIn))
	if !ok {
		t.Fatalf("builtin stateIn should resolve")
	}
	if d.Description != "Passes when the instance is currently in the named state." {
		t.Fatalf("stateIn description = %q", d.Description)
	}
	if d.Kind != state.KindGuard {
		t.Fatalf("stateIn kind = %q, want guard", d.Kind)
	}
}

func TestRefResolver_MachineBeatsBuiltin(t *testing.T) {
	reg := state.NewRegistry[any]()
	reg.Guard("canSubmit", func(state.GuardCtx[any]) bool { return true },
		state.Describe("user submit guard").Category("guards"))

	r := viewmodel.NewRefResolver(state.BuiltinPalette(), reg.Palette())
	d, ok := r.Resolve("canSubmit")
	if !ok {
		t.Fatalf("canSubmit should resolve from machine palette")
	}
	if d.Description != "user submit guard" {
		t.Fatalf("canSubmit description = %q, want %q", d.Description, "user submit guard")
	}
	if d.Category != "guards" {
		t.Fatalf("canSubmit category = %q, want guards", d.Category)
	}
}

func TestRefResolver_PrecedenceOverlap(t *testing.T) {
	// A machine descriptor colliding by name with a builtin must win;
	// builtins only fill gaps.
	machine := []state.Descriptor{
		{Kind: state.KindGuard, Name: string(state.GuardStateIn), Description: "overridden"},
	}
	r := viewmodel.NewRefResolver(state.BuiltinPalette(), machine)
	d, ok := r.Resolve(string(state.GuardStateIn))
	if !ok || d.Description != "overridden" {
		t.Fatalf("machine descriptor should win over builtin; got ok=%v desc=%q", ok, d.Description)
	}
}

func TestRefResolver_UnknownFallsBack(t *testing.T) {
	r := viewmodel.NewRefResolver(state.BuiltinPalette(), nil)
	if _, ok := r.Resolve("nope"); ok {
		t.Fatalf("unknown ref should not resolve")
	}

	// Unknown refs in Build fall back to the raw name with no description.
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, r, fullOpts())
	e := edgeByFromTo(t, vm, "working", "review")
	g, ok := itemByName(e.Guards, "canSubmit")
	if !ok {
		t.Fatalf("canSubmit guard item should still exist by raw name, got %v", e.Guards)
	}
	if g.Description != "" {
		t.Fatalf("unknown guard description should be empty, got %q", g.Description)
	}
}

func TestRefResolver_NilPaletteInBuild(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, fullOpts())
	e := edgeByFromTo(t, vm, "working", "review")
	g, ok := itemByName(e.Guards, "canSubmit")
	if !ok {
		t.Fatalf("nil resolver: guard item should exist by raw name")
	}
	if g.Description != "" || g.Category != "" {
		t.Fatalf("nil resolver: category/description should be empty, got %+v", g)
	}
}

func TestBuild_Containers(t *testing.T) {
	ir := loadComposite(t)
	vm := viewmodel.Build(ir, nil, fullOpts())

	active := containerByID(t, vm, "active")
	if active.Kind != "composite" {
		t.Fatalf("active container kind = %q, want composite", active.Kind)
	}
	if !contains(active.Children, "working") || !contains(active.Children, "review") {
		t.Fatalf("active container children = %v, want working+review", active.Children)
	}

	parallel := containerByID(t, vm, "parallel")
	if parallel.Kind != "parallel" {
		t.Fatalf("parallel container kind = %q, want parallel", parallel.Kind)
	}

	region := containerByID(t, vm, "regionA")
	if region.Kind != "region" {
		t.Fatalf("regionA container kind = %q, want region", region.Kind)
	}
	if !contains(region.Children, "ra1") || !contains(region.Children, "ra2") {
		t.Fatalf("regionA container children = %v, want ra1+ra2", region.Children)
	}
}

func TestBuild_DataFlow_FoldsReadsWrites(t *testing.T) {
	reg := state.NewRegistry[any]()
	reg.Guard("canSubmit", func(state.GuardCtx[any]) bool { return true },
		state.Describe("submit guard").Category("guards").Reads("Order"))
	ir := loadComposite(t)
	resolver := viewmodel.NewRefResolver(state.BuiltinPalette(), reg.Palette())

	// dataFlow only at Full.
	vm := viewmodel.Build(ir, resolver, fullOpts())
	e := edgeByFromTo(t, vm, "working", "review")
	g, _ := itemByName(e.Guards, "canSubmit")
	if !containsSub(g.Description, "Order") {
		t.Fatalf("full: data-flow reads should be folded into description, got %q", g.Description)
	}

	// At Lifecycle, dataFlow is off, so reads must not appear.
	vm2 := viewmodel.Build(ir, resolver, viewmodel.ProjectionOptions{Level: viewmodel.Lifecycle})
	e2 := edgeByFromTo(t, vm2, "working", "review")
	g2, _ := itemByName(e2.Guards, "canSubmit")
	if containsSub(g2.Description, "Order") {
		t.Fatalf("lifecycle: data-flow reads should not appear, got %q", g2.Description)
	}
}

func containsSub(s, sub string) bool {
	if sub == "" {
		return true
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestIncluded_LevelLadder(t *testing.T) {
	// The default level (Actions) implies guards+effects+assigns but not lifecycle.
	def := viewmodel.ProjectionOptions{Level: viewmodel.Actions}
	if !viewmodel.Included(def, viewmodel.DimGuards) {
		t.Fatal("Actions should imply guards")
	}
	if !viewmodel.Included(def, viewmodel.DimEffects) {
		t.Fatal("Actions should imply effects")
	}
	if viewmodel.Included(def, viewmodel.DimEntryExit) {
		t.Fatal("Actions should not imply entryExit")
	}
	if viewmodel.Included(def, viewmodel.DimDescriptions) {
		t.Fatal("Actions should not imply descriptions")
	}
}
