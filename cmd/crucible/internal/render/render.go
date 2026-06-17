package render

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"oss.terrastruct.com/d2/d2graph"
	"oss.terrastruct.com/d2/d2layouts/d2elklayout"
	"oss.terrastruct.com/d2/d2lib"
	"oss.terrastruct.com/d2/d2renderers/d2svg"
	"oss.terrastruct.com/d2/d2target"
	"oss.terrastruct.com/d2/d2themes/d2themescatalog"
	"oss.terrastruct.com/d2/lib/log"
	"oss.terrastruct.com/d2/lib/textmeasure"
	"oss.terrastruct.com/util-go/go2"

	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
)

// RenderSVG emits D2 from the viewmodel, compiles it with the ELK layout and the
// forge theme overrides, renders to SVG, then runs the forge v5 post-process
// pipeline (stripGlow -> recolorLifecycleRows -> equalizeRegions ->
// centerRegions) and returns the final SVG bytes.
//
//nolint:revive // RenderSVG is the contracted public API name for this package.
func RenderSVG(vm viewmodel.ViewModel, theme Theme) ([]byte, error) {
	d2src, err := EmitD2(vm, theme)
	if err != nil {
		return nil, fmt.Errorf("emit d2: %w", err)
	}

	ruler, err := textmeasure.NewRuler()
	if err != nil {
		return nil, fmt.Errorf("new ruler: %w", err)
	}

	layoutResolver := func(_ string) (d2graph.LayoutGraph, error) {
		return func(ctx context.Context, g *d2graph.Graph) error {
			return d2elklayout.Layout(ctx, g, &d2elklayout.ConfigurableOpts{
				Algorithm:       "layered",
				NodeSpacing:     90,
				EdgeNodeSpacing: 80,
				SelfLoopSpacing: 50,
				Padding:         "[top=50,left=50,bottom=50,right=110]",
			})
		}, nil
	}

	themeID := d2themescatalog.DarkMauve.ID
	renderOpts := &d2svg.RenderOpts{
		Pad:            go2.Pointer(int64(60)),
		ThemeID:        &themeID,
		ThemeOverrides: buildThemeOverrides(theme),
	}
	compileOpts := &d2lib.CompileOptions{
		LayoutResolver: layoutResolver,
		Ruler:          ruler,
	}
	ctx := log.WithDefault(context.Background())
	diagram, _, err := d2lib.Compile(ctx, d2src, compileOpts, renderOpts)
	if err != nil {
		return nil, fmt.Errorf("compile d2: %w", err)
	}
	out, err := d2svg.Render(diagram, renderOpts)
	if err != nil {
		return nil, fmt.Errorf("render svg: %w", err)
	}

	svg := string(out)
	svg, _ = stripGlow(svg, theme)
	svg, _ = recolorLifecycleRows(svg, theme)
	svg, _ = scrubMauve(svg, theme)
	svg, _ = equalizeRegions(svg)
	svg, _ = centerRegions(svg, regionPaths(vm))
	return []byte(svg), nil
}

// darkMauveDefaults maps each raw DarkMauve (Catppuccin Mocha) palette hex that
// D2 v0.7.1 bakes into the SVG's embedded CSS stylesheet — regardless of the
// ThemeOverrides we pass — to its forge equivalent. D2 always emits the full
// palette as inert utility classes (.fill-B1, .stroke-B5, the .appendix/.md
// GitHub-markdown vars, etc.); even though no live shape in our output uses any
// of them (every shape carries an explicit inline forge color), the raw mauve
// and lavender hexes still appear in the file. scrubMauve rewrites them so a
// grep of the output SVG finds ZERO mauve/lavender. Keys are lowercased because
// the comparison is case-insensitive.
//
// The targets mirror buildThemeOverrides: the value each DarkMauve slot is
// overridden to (B1/B2->Ember, B3->Copper, B4->Steel, B5->SteelDark,
// B6/N6->CanvasN6, N1->TextWarm, N2->TextSecondary, N3->ScaleText, N4->Steel,
// N5->SteelDark, N7->Bg, AA2->SoftOrange). DarkMauve reuses some hexes across
// slots (e.g. #45475A is B5/N5/AA4/AB4, #313244 is B6/N6/AA5/AB5); a single
// mapping per hex is therefore unambiguous and lands on the forge color.
func darkMauveDefaults(theme Theme) map[string]string {
	return map[string]string{
		"#cba6f7": theme.Ember,         // B1, B2
		"#6c7086": theme.Copper,        // B3
		"#585b70": theme.Steel,         // B4, N4
		"#45475a": theme.SteelDark,     // B5, N5, AA4, AB4
		"#313244": theme.CanvasN6,      // B6, N6, AA5, AB5
		"#cdd6f4": theme.TextWarm,      // N1
		"#bac2de": theme.TextSecondary, // N2
		"#a6adc8": theme.ScaleText,     // N3
		"#1e1e2e": theme.Bg,            // N7
		"#f38ba8": theme.SoftOrange,    // AA2
	}
}

// scrubMauve rewrites every residual DarkMauve hex (see darkMauveDefaults) to
// its forge equivalent, case-insensitively, so no mauve/lavender survives in the
// output SVG. It returns the number of replacements made. The rewrite is purely
// cosmetic-safe: it only touches color literals, and no live shape depends on
// these defaults (each carries an explicit inline forge color already).
func scrubMauve(svg string, theme Theme) (string, int) {
	count := 0
	for from, to := range darkMauveDefaults(theme) {
		re := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(from))
		svg = re.ReplaceAllStringFunc(svg, func(string) string {
			count++
			return to
		})
	}
	return svg, count
}

// buildThemeOverrides maps theme fields onto the D2 DarkMauve override slots,
// matching the forge v5 reference assignment.
func buildThemeOverrides(t Theme) *d2target.ThemeOverrides {
	return &d2target.ThemeOverrides{
		N1:  go2.Pointer(t.TextWarm),
		N2:  go2.Pointer(t.TextSecondary),
		N3:  go2.Pointer(t.ScaleText),
		N4:  go2.Pointer(t.CanvasN4),
		N5:  go2.Pointer(t.CanvasN5),
		N6:  go2.Pointer(t.CanvasN6),
		N7:  go2.Pointer(t.Bg),
		B1:  go2.Pointer(t.Ember),
		B2:  go2.Pointer(t.Ember),
		B3:  go2.Pointer(t.Copper),
		B4:  go2.Pointer(t.Steel),
		B5:  go2.Pointer(t.SteelDark),
		B6:  go2.Pointer(t.CanvasN6),
		AA2: go2.Pointer(t.SoftOrange),
		AA4: go2.Pointer(t.AccentAA4),
		AA5: go2.Pointer(t.AccentAA5),
		AB4: go2.Pointer(t.AccentAB4),
		AB5: go2.Pointer(t.AccentAB5),
	}
}

// regionPaths derives the fully-qualified dotted D2 paths of every region
// container in the viewmodel, generalising the forge v5 hardcoded
// {"connected.work","connected.heartbeat"} to arbitrary machines.
//
// It reuses the emitter's index (which records parent relationships: container X
// is the parent of Y when Y's ID is in X.Children) so a region's path is built
// by walking its parent chain. Order is deterministic (container document order).
func regionPaths(vm viewmodel.ViewModel) []string {
	idx := buildIndex(vm)
	var paths []string
	for i := range vm.Containers {
		c := vm.Containers[i]
		if c.Kind != "region" {
			continue
		}
		paths = append(paths, idx.dottedPath(c.ID))
	}
	return paths
}

// ---------------------------------------------------------------------------
// SVG post-process — ported verbatim in behavior from forge v5, sourcing all
// colors from the theme rather than package constants.
// ---------------------------------------------------------------------------

// stripGlow removes any <filter> blocks and filter="url(#...)" references, drops
// now-empty <defs>, and returns the number of hot-stroke elements seen. The SVG
// carries zero blur/glow filters afterwards.
func stripGlow(svg string, theme Theme) (string, int) {
	filterBlock := regexp.MustCompile(`(?is)<filter\b[^>]*>.*?</filter>`)
	svg = filterBlock.ReplaceAllString(svg, "")
	filterRef := regexp.MustCompile(`(?i)\s*filter\s*=\s*"url\(#[^"]*\)"`)
	svg = filterRef.ReplaceAllString(svg, "")
	emptyDefs := regexp.MustCompile(`(?is)<defs>\s*</defs>`)
	svg = emptyDefs.ReplaceAllString(svg, "")

	count := 0
	hotLower := strings.ToLower(theme.Hot)
	tagRe := regexp.MustCompile(`(?i)<(path|polyline|line|polygon|rect|ellipse|circle)\b[^>]*?>`)
	for _, tag := range tagRe.FindAllString(svg, -1) {
		if strings.Contains(strings.ToLower(tag), hotLower) {
			count++
		}
	}
	return svg, count
}

// recolorLifecycleRows forces lifecycle compartment row text to the forge
// scheme: values (fill-AA2) -> white textWarm with the class token stripped, "+"
// markers (fill-B2) -> soft orange with the class stripped, and steel-colored
// keys -> soft orange. Returns how many <text> tags were rewritten.
func recolorLifecycleRows(svg string, theme Theme) (string, int) {
	count := 0
	steelLower := strings.ToLower(theme.Steel)
	rowText := regexp.MustCompile(`(?i)<text\b[^>]*>`)
	out := rowText.ReplaceAllStringFunc(svg, func(tag string) string {
		lower := strings.ToLower(tag)
		switch {
		case strings.Contains(lower, "fill-aa2"):
			count++
			tag = regexp.MustCompile(`(?i)fill="[^"]*"`).ReplaceAllString(tag, `fill="`+theme.TextWarm+`"`)
			tag = regexp.MustCompile(`(?i)\s+fill-AA2`).ReplaceAllString(tag, "")
			return tag
		case strings.Contains(lower, "fill-b2"):
			count++
			tag = regexp.MustCompile(`(?i)fill="[^"]*"`).ReplaceAllString(tag, `fill="`+theme.SoftOrange+`"`)
			tag = regexp.MustCompile(`(?i)\s+fill-B2`).ReplaceAllString(tag, "")
			return tag
		case strings.Contains(lower, `fill="`+steelLower+`"`):
			count++
			tag = regexp.MustCompile(`(?i)fill="[^"]*"`).ReplaceAllString(tag, `fill="`+theme.SoftOrange+`"`)
			return tag
		}
		return tag
	})
	return out, count
}

// equalizeRegions resizes the dashed region rects to a shared max width/height
// so the dashed borders read as equal-size plates. Returns how many region rects
// were resized.
func equalizeRegions(svg string) (string, int) {
	rectRe := regexp.MustCompile(`<rect\b[^>]*>`)
	wRe := regexp.MustCompile(`width="([0-9.]+)"`)
	hRe := regexp.MustCompile(`height="([0-9.]+)"`)
	var regions []string
	for _, m := range rectRe.FindAllString(svg, -1) {
		l := strings.ToLower(m)
		if strings.Contains(l, "dasharray") || strings.Contains(l, "stroke-dash") {
			regions = append(regions, m)
		}
	}
	if len(regions) < 2 {
		return svg, 0
	}
	maxW, maxH := "", ""
	parse := func(re *regexp.Regexp, s string) string {
		if mm := re.FindStringSubmatch(s); mm != nil {
			return mm[1]
		}
		return ""
	}
	bigger := func(a, b string) string {
		fa, _ := strconv.ParseFloat(a, 64)
		fb, _ := strconv.ParseFloat(b, 64)
		if fb > fa {
			return b
		}
		return a
	}
	for _, r := range regions {
		w, h := parse(wRe, r), parse(hRe, r)
		if maxW == "" {
			maxW, maxH = w, h
		} else {
			maxW, maxH = bigger(maxW, w), bigger(maxH, h)
		}
	}
	count := 0
	for _, r := range regions {
		nr := wRe.ReplaceAllString(r, `width="`+maxW+`"`)
		nr = hRe.ReplaceAllString(nr, `height="`+maxH+`"`)
		if nr != r {
			svg = strings.Replace(svg, r, nr, 1)
		}
		count++
	}
	return svg, count
}

// centerRegions horizontally centers each region's child contents inside its
// (equalized) region box. regionPaths are the fully-qualified dotted D2 paths of
// the region containers, derived from the viewmodel (generalising forge v5's
// hardcoded list). Returns how many child groups were shifted.
func centerRegions(svg string, regionList []string) (string, int) {
	if len(regionList) == 0 {
		return svg, 0
	}
	b64decode := func(tok string) string {
		for len(tok)%4 != 0 {
			tok += "="
		}
		if b, err := base64.StdEncoding.DecodeString(tok); err == nil {
			return string(b)
		}
		return ""
	}
	blockEnd := func(s string, start int) int {
		depth := 0
		i := start
		for i < len(s) {
			if strings.HasPrefix(s[i:], "<g") && (i+2 >= len(s) || s[i+2] == ' ' || s[i+2] == '>') {
				depth++
				j := strings.IndexByte(s[i:], '>')
				if j < 0 {
					return len(s)
				}
				if s[i+j-1] == '/' {
					depth--
				}
				i += j + 1
				continue
			}
			if strings.HasPrefix(s[i:], "</g>") {
				depth--
				i += 4
				if depth == 0 {
					return i
				}
				continue
			}
			i++
		}
		return len(s)
	}
	classRe := regexp.MustCompile(`class="([^"]*)"`)
	fattr := func(re *regexp.Regexp, s string) (float64, bool) {
		if m := re.FindStringSubmatch(s); m != nil {
			if v, err := strconv.ParseFloat(m[1], 64); err == nil {
				return v, true
			}
		}
		return 0, false
	}
	xRe := regexp.MustCompile(`\bx="([0-9.\-]+)"`)
	wRe := regexp.MustCompile(`\bwidth="([0-9.]+)"`)
	cxRe := regexp.MustCompile(`\bcx="([0-9.\-]+)"`)
	rRe := regexp.MustCompile(`\br="([0-9.]+)"`)
	dashRe := regexp.MustCompile(`(?i)dasharray|stroke-dash`)

	type gblock struct {
		path       string
		start, end int
	}
	var blocks []gblock
	for i := 0; i < len(svg); {
		if strings.HasPrefix(svg[i:], "<g") && (i+2 < len(svg)) && (svg[i+2] == ' ' || svg[i+2] == '>') {
			end := blockEnd(svg, i)
			open := svg[i : strings.IndexByte(svg[i:], '>')+i+1]
			path := ""
			if m := classRe.FindStringSubmatch(open); m != nil {
				if fields := strings.Fields(m[1]); len(fields) > 0 {
					path = b64decode(fields[0])
				}
			}
			blocks = append(blocks, gblock{path, i, end})
			i = end
			continue
		}
		i++
	}

	nodeExtent := func(seg string) (minx, maxx float64, ok bool) {
		minx, maxx = 1e18, -1e18
		tagRe := regexp.MustCompile(`(?i)<(rect|ellipse|circle|path)\b[^>]*>`)
		for _, t := range tagRe.FindAllString(seg, -1) {
			if x, has := fattr(xRe, t); has {
				if w, hasw := fattr(wRe, t); hasw {
					if x < minx {
						minx = x
					}
					if x+w > maxx {
						maxx = x + w
					}
					ok = true
				}
			} else if cx, hascx := fattr(cxRe, t); hascx {
				if r, hasr := fattr(rRe, t); hasr {
					if cx-r < minx {
						minx = cx - r
					}
					if cx+r > maxx {
						maxx = cx + r
					}
					ok = true
				}
			}
		}
		return
	}

	count := 0
	type edit struct {
		start, end int
		dx         float64
	}
	var edits []edit

	for _, region := range regionList {
		prefix := region + "."
		var boxCenter float64
		var haveBox bool
		nMin, nMax := 1e18, -1e18
		var haveNodes bool
		var spans []gblock
		for _, bl := range blocks {
			if bl.path == region {
				seg := svg[bl.start:bl.end]
				rectRe := regexp.MustCompile(`<rect\b[^>]*>`)
				for _, rt := range rectRe.FindAllString(seg, -1) {
					if dashRe.MatchString(rt) {
						x, hx := fattr(xRe, rt)
						w, hw := fattr(wRe, rt)
						if hx && hw {
							boxCenter = x + w/2
							haveBox = true
						}
					}
				}
				continue
			}
			if !strings.HasPrefix(bl.path, prefix) {
				continue
			}
			spans = append(spans, bl)
			if !strings.Contains(bl.path, "(") {
				if lo, hi, ok := nodeExtent(svg[bl.start:bl.end]); ok {
					if lo < nMin {
						nMin = lo
					}
					if hi > nMax {
						nMax = hi
					}
					haveNodes = true
				}
			}
		}
		if !haveBox || !haveNodes || len(spans) == 0 {
			continue
		}
		contentCenter := (nMin + nMax) / 2
		dx := boxCenter - contentCenter
		if dx > -0.5 && dx < 0.5 {
			continue
		}
		for _, s := range spans {
			edits = append(edits, edit{s.start, s.end, dx})
			count++
		}
	}

	sort.Slice(edits, func(a, b int) bool { return edits[a].start > edits[b].start })
	for _, e := range edits {
		seg := svg[e.start:e.end]
		wrapped := fmt.Sprintf(`<g transform="translate(%.3f,0)">%s</g>`, e.dx, seg)
		svg = svg[:e.start] + wrapped + svg[e.end:]
	}
	return svg, count
}
