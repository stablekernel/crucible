package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/goccy/go-graphviz"

	"github.com/stablekernel/crucible/state"
)

// Crucible brand palette applied to rendered images. These are Graphviz DOT
// default attributes (ember borders, charcoal text, copper edges) injected by
// themeDOT; the state package emits the structural, brand-agnostic DOT.
const (
	brandEmber     = "#d9620a" // node border
	brandCharcoal  = "#16191d" // node/edge text
	brandCopper    = "#b06a28" // edge color
	brandBackgound = "transparent"
)

// themeDOT applies the Crucible brand to brand-agnostic DOT produced by
// state.Machine.ToDOT. It inserts graph/node/edge default attributes
// immediately after the opening "digraph ... {" line, so per-element
// attributes that state already emits (node fillcolor for ownership,
// peripheries for final states, per-edge styling) still win — DOT defaults
// apply only where an element does not override them. The brand chrome is
// therefore additive: ownership fills and final-state rings are preserved.
//
// If the expected "digraph ... {" header is not found (an unexpected DOT
// shape), the input is returned unchanged so rendering still succeeds with the
// untouched, structurally valid DOT.
func themeDOT(dot string) string {
	const open = "{\n"
	headerEnd := strings.Index(dot, open)
	if !strings.HasPrefix(dot, "digraph ") || headerEnd < 0 {
		return dot
	}
	insertAt := headerEnd + len(open)

	var b strings.Builder
	b.Grow(len(dot) + 160)
	b.WriteString(dot[:insertAt])
	fmt.Fprintf(&b, "    bgcolor=%q\n", brandBackgound)
	fmt.Fprintf(&b, "    node [color=%q fontcolor=%q]\n", brandEmber, brandCharcoal)
	fmt.Fprintf(&b, "    edge [color=%q fontcolor=%q]\n", brandCopper, brandCharcoal)
	b.WriteString(dot[insertAt:])
	return b.String()
}

// imageFormat selects the raster/vector encoding produced by renderImage.
type imageFormat int

const (
	formatSVG imageFormat = iota
	formatPNG
)

// renderImage renders a quenched machine to themed SVG or PNG bytes via the
// embedded (pure-Go, WASM) Graphviz. The machine emits brand-agnostic DOT,
// which themeDOT decorates with the Crucible palette before parsing so the
// image carries the project brand; the bytes are binary and MUST be written
// verbatim (never through the emit* helpers, which append newlines and would
// corrupt a PNG).
//
// A fresh Graphviz instance spins up a wazero WASM runtime per call. That is
// heavy, but a CLI renders once per invocation, so the cost is paid once and the
// runtime is torn down on return.
func renderImage[S comparable, E comparable, C any](m *state.Machine[S, E, C], format imageFormat) ([]byte, error) {
	dot := themeDOT(m.ToDOT())

	ctx := context.Background()
	g, err := graphviz.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("init graphviz: %w", err)
	}
	defer func() { _ = g.Close() }()

	graph, err := graphviz.ParseBytes([]byte(dot))
	if err != nil {
		return nil, fmt.Errorf("parse dot: %w", err)
	}
	defer func() { _ = graph.Close() }()

	gvFormat, err := toGraphvizFormat(format)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err := g.Render(ctx, graph, gvFormat, &buf); err != nil {
		return nil, fmt.Errorf("render %s: %w", gvFormat, err)
	}
	return buf.Bytes(), nil
}

// toGraphvizFormat maps the internal image format to a go-graphviz format
// constant.
func toGraphvizFormat(format imageFormat) (graphviz.Format, error) {
	switch format {
	case formatSVG:
		return graphviz.SVG, nil
	case formatPNG:
		return graphviz.PNG, nil
	default:
		return "", fmt.Errorf("unsupported image format %d", format)
	}
}
