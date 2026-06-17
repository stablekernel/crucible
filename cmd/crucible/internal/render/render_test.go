package render

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
	"github.com/stablekernel/crucible/state"
)

// loadComposite loads the shared composite.json fixture, mirroring the idiom in
// the viewmodel package's tests.
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

func renderComposite(t *testing.T, opts viewmodel.ProjectionOptions) string {
	t.Helper()
	ir := loadComposite(t)
	vm, err := viewmodel.BuildScoped(ir, nil, opts)
	if err != nil {
		t.Fatalf("BuildScoped: %v", err)
	}
	out, err := RenderSVG(vm, DefaultTheme)
	if err != nil {
		t.Fatalf("RenderSVG: %v", err)
	}
	return string(out)
}

func TestRender_StructuralAndNoGlow(t *testing.T) {
	// Path scope so an on-path hot_edge exists, guaranteeing the hot color is
	// present alongside ember and steel from the base palette.
	svg := renderComposite(t, viewmodel.ProjectionOptions{
		Level: viewmodel.Full,
		Scope: viewmodel.ScopePath,
		From:  "working",
		To:    "review",
		Mode:  viewmodel.ModeShortest,
	})

	if !regexp.MustCompile(`<svg`).MatchString(svg) {
		t.Error("missing <svg root")
	}
	for _, bad := range []string{"<filter", "feGaussianBlur", `filter="url(#`} {
		if regexp.MustCompile(regexp.QuoteMeta(bad)).MatchString(svg) {
			t.Errorf("glow artifact present: %q", bad)
		}
	}
	for _, want := range []string{DefaultTheme.Ember, DefaultTheme.Hot, DefaultTheme.Steel} {
		if !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(svg) {
			t.Errorf("expected theme color %q in SVG", want)
		}
	}
}

func TestRender_EqualizedRegions(t *testing.T) {
	// A parallel container with two regions of differing content so ELK sizes
	// the region boxes unequally; equalizeRegions must then make the dashed rects
	// share a width and height. The composite.json fixture has only one region,
	// so this case uses an inline view model.
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "par", Name: "par", Kind: viewmodel.NodeParallel},
			{ID: "a1", Name: "a1", Kind: viewmodel.NodeAtomic},
			{ID: "a2", Name: "a2", Kind: viewmodel.NodeAtomic},
			{ID: "a3", Name: "a3", Kind: viewmodel.NodeAtomic},
			{ID: "b1", Name: "b1", Kind: viewmodel.NodeAtomic},
		},
		Edges: []viewmodel.ViewEdge{
			{From: "a1", To: "a2", Event: "x", Kind: viewmodel.EdgeEvent},
			{From: "a2", To: "a3", Event: "y", Kind: viewmodel.EdgeEvent},
		},
		Containers: []viewmodel.ViewContainer{
			{ID: "par", Name: "par", Kind: "parallel", Children: []string{"rA", "rB"}},
			{ID: "rA", Name: "rA", Kind: "region", Children: []string{"a1", "a2", "a3"}},
			{ID: "rB", Name: "rB", Kind: "region", Children: []string{"b1"}},
		},
		Highlight: []string{},
	}
	out, err := RenderSVG(vm, DefaultTheme)
	if err != nil {
		t.Fatalf("RenderSVG: %v", err)
	}
	svg := string(out)

	rectRe := regexp.MustCompile(`<rect\b[^>]*>`)
	wRe := regexp.MustCompile(`width="([0-9.]+)"`)
	hRe := regexp.MustCompile(`height="([0-9.]+)"`)
	var ws, hs []float64
	for _, m := range rectRe.FindAllString(svg, -1) {
		if !regexp.MustCompile(`(?i)dasharray|stroke-dash`).MatchString(m) {
			continue
		}
		if wm := wRe.FindStringSubmatch(m); wm != nil {
			if v, err := strconv.ParseFloat(wm[1], 64); err == nil {
				ws = append(ws, v)
			}
		}
		if hm := hRe.FindStringSubmatch(m); hm != nil {
			if v, err := strconv.ParseFloat(hm[1], 64); err == nil {
				hs = append(hs, v)
			}
		}
	}
	if len(ws) < 2 {
		t.Fatalf("expected at least two dashed region rects, got %d", len(ws))
	}
	for i := 1; i < len(ws); i++ {
		if ws[i] != ws[0] {
			t.Errorf("dashed region widths not equal: %v", ws)
			break
		}
	}
	for i := 1; i < len(hs); i++ {
		if hs[i] != hs[0] {
			t.Errorf("dashed region heights not equal: %v", hs)
			break
		}
	}
}

// TestRender_NoMauveBleed asserts the rendered SVG contains ZERO raw DarkMauve
// (Catppuccin Mocha) palette hexes — neither on live shapes nor in D2's embedded
// utility stylesheet — so no mauve fill / mauve border / lavender title can ever
// leak from the base theme. It checks both an on-path (composite hot border) and
// a whole-scope projection.
func TestRender_NoMauveBleed(t *testing.T) {
	mauve := []string{
		"#CBA6f7", "#6C7086", "#585B70", "#45475A", "#313244",
		"#CDD6F4", "#BAC2DE", "#A6ADC8", "#1E1E2E", "#f38BA8",
	}
	cases := []struct {
		name string
		opts viewmodel.ProjectionOptions
	}{
		{"path", viewmodel.ProjectionOptions{Level: viewmodel.Lifecycle, Scope: viewmodel.ScopePath, From: "working", To: "review", Mode: viewmodel.ModeShortest}},
		{"whole", viewmodel.ProjectionOptions{Level: viewmodel.Full, Scope: viewmodel.ScopeWhole}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svg := renderComposite(t, tc.opts)
			for _, hex := range mauve {
				if regexp.MustCompile(`(?i)` + regexp.QuoteMeta(hex)).MatchString(svg) {
					t.Errorf("mauve/lavender hex %q leaked into %s SVG", hex, tc.name)
				}
			}
		})
	}
}

// TestRender_ContainerForgeStyle verifies the composite/parallel container shape
// carries the explicit forge panel fill (steelDark) and an ember-family border,
// never a DarkMauve default. It inspects the live (non-<style>) SVG body.
func TestRender_ContainerForgeStyle(t *testing.T) {
	svg := renderComposite(t, viewmodel.ProjectionOptions{Level: viewmodel.Full, Scope: viewmodel.ScopeWhole})
	body := regexp.MustCompile(`(?s)<style.*?</style>`).ReplaceAllString(svg, "")
	// At least one container rect: steelDark fill + ember stroke (off-path here).
	rect := regexp.MustCompile(`(?i)<rect\b[^>]*fill="` + regexp.QuoteMeta(DefaultTheme.SteelDark) + `"[^>]*>`)
	found := false
	for _, m := range rect.FindAllString(body, -1) {
		if regexp.MustCompile(`(?i)stroke="` + regexp.QuoteMeta(DefaultTheme.Ember) + `"`).MatchString(m) {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected a container rect with steelDark fill and ember stroke")
	}
}

// TestRender_GuardOnlyEdgeCompiles is the end-to-end regression for the
// bracket-array bug. An EVENTLESS transition carrying only a guard renders the
// edge label "[hasStock]"; before the quoting fix the emitter wrote it BARE, so
// D2 parsed it as an array ("edges cannot be assigned arrays") and RenderSVG
// errored. With the fix the label is double-quoted and RenderSVG must succeed
// and produce an <svg> root. A second edge carries a quote+backslash to exercise
// the escaping path end-to-end.
func TestRender_GuardOnlyEdgeCompiles(t *testing.T) {
	vm := viewmodel.ViewModel{
		Nodes: []viewmodel.ViewNode{
			{ID: "a", Name: "a", Kind: viewmodel.NodeAtomic},
			{ID: "b", Name: "b", Kind: viewmodel.NodeAtomic},
			{ID: "c", Name: "c", Kind: viewmodel.NodeAtomic},
		},
		Edges: []viewmodel.ViewEdge{
			{From: "a", To: "b", Kind: viewmodel.EdgeEventless, Guards: []viewmodel.DetailItem{{Name: "hasStock"}}},
			{From: "b", To: "c", Event: `say "hi" \ end`, Kind: viewmodel.EdgeEvent},
		},
		Highlight: []string{},
	}
	out, err := RenderSVG(vm, DefaultTheme)
	if err != nil {
		t.Fatalf("RenderSVG must succeed for a guard-only eventless edge: %v", err)
	}
	if !regexp.MustCompile(`<svg`).MatchString(string(out)) {
		t.Error("missing <svg root in rendered output")
	}
}

func TestRender_LifecycleRecolored(t *testing.T) {
	// A path scope that includes the composite's lifecycle-bearing "active"
	// state, projected at Lifecycle level so entry/exit/invoke rows are emitted.
	svg := renderComposite(t, viewmodel.ProjectionOptions{
		Level: viewmodel.Lifecycle,
		Scope: viewmodel.ScopePath,
		From:  "working",
		To:    "review",
		Mode:  viewmodel.ModeShortest,
	})
	// The fill-AA2 CSS rule may remain in the <style> stylesheet (harmless once
	// unused); what must be gone is any <text> element still carrying the class.
	for _, tag := range regexp.MustCompile(`(?i)<text\b[^>]*>`).FindAllString(svg, -1) {
		if regexp.MustCompile(`fill-AA2`).MatchString(tag) {
			t.Errorf("lifecycle text still carries fill-AA2 class: %s", tag)
		}
	}
	if !regexp.MustCompile(regexp.QuoteMeta(DefaultTheme.SoftOrange)).MatchString(svg) {
		t.Errorf("expected soft-orange %q after lifecycle recolor", DefaultTheme.SoftOrange)
	}
}
