package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/cmd/crucible/internal/render"
)

// runCmdRaw invokes the CLI dispatcher in-process and returns the exit code
// with captured stdout as raw bytes (so binary output survives) plus stderr
// text.
func runCmdRaw(args ...string) (code int, stdout []byte, stderr string) {
	var out, errBuf bytes.Buffer
	code = run(args, &out, &errBuf)
	return code, out.Bytes(), errBuf.String()
}

// TestRenderSVG_Stdout asserts that rendering svg to stdout exits OK, contains
// an <svg element, carries at least one forge brand color, and is free of the
// raw mauve hex that the render package must scrub.
func TestRenderSVG_Stdout(t *testing.T) {
	code, out, errOut := runCmdRaw("render", "-format", "svg", "testdata/composite.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	outStr := string(out)
	if !strings.Contains(outStr, "<svg") {
		t.Fatalf("svg output missing <svg element:\n%s", outStr[:min(200, len(outStr))])
	}
	hasEmber := strings.Contains(outStr, render.DefaultTheme.Ember)
	hasHot := strings.Contains(outStr, render.DefaultTheme.Hot)
	if !hasEmber && !hasHot {
		t.Errorf("svg output missing forge brand colors (ember=%s hot=%s)", render.DefaultTheme.Ember, render.DefaultTheme.Hot)
	}
	if strings.Contains(outStr, "#cba6f7") {
		t.Errorf("svg output contains raw mauve #cba6f7 which should have been scrubbed")
	}
}

// TestRenderSVG_ToFile asserts that -o writes the SVG to disk: exit OK, empty
// stdout, and file contents containing <svg.
func TestRenderSVG_ToFile(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "machine.svg")
	code, out, errOut := runCmdRaw("render", "-format", "svg", "-o", outPath, "testdata/composite.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty stdout when writing to -o, got %d bytes", len(out))
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	if !strings.Contains(string(data), "<svg") {
		t.Fatalf("output file missing <svg element")
	}
}

// TestRenderSVG_PathScope asserts that -from/-to path scoping exits OK and
// produces a valid SVG.
func TestRenderSVG_PathScope(t *testing.T) {
	code, out, errOut := runCmdRaw("render", "-from", "working", "-to", "review", "-format", "svg", "testdata/composite.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(string(out), "<svg") {
		t.Fatalf("svg output missing <svg element (stderr: %s)", errOut)
	}
}

// TestRenderSVG_DetailLevelsDiffer asserts that full and outline projections
// produce different SVG output, confirming the detail-level flag is wired.
func TestRenderSVG_DetailLevelsDiffer(t *testing.T) {
	_, full, _ := runCmdRaw("render", "-format", "svg", "-detail", "full", "testdata/composite.json")
	_, outline, _ := runCmdRaw("render", "-format", "svg", "-detail", "outline", "testdata/composite.json")
	if string(full) == string(outline) {
		t.Errorf("expected -detail full and -detail outline to produce different SVG output")
	}
}

// TestRenderSVG_PNGRejected asserts that -format png exits with exitUsage and
// explains how to convert.
func TestRenderSVG_PNGRejected(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "-format", "png", "testdata/composite.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
	}
	if !strings.Contains(errOut, "svg") {
		t.Errorf("stderr missing 'svg' in png-rejection message: %s", errOut)
	}
	if !strings.Contains(errOut, "convert") {
		t.Errorf("stderr missing 'convert' in png-rejection message: %s", errOut)
	}
}

// TestRenderSVG_UnknownEndpoint asserts that an unknown -from state exits with
// exitUsage and mentions the unknown state name.
func TestRenderSVG_UnknownEndpoint(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "-from", "nope", "-format", "svg", "testdata/composite.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
	}
	if !strings.Contains(errOut, "nope") {
		t.Errorf("stderr should mention the unknown state name 'nope': %s", errOut)
	}
}

// TestRenderSVG_NoPath asserts that a -from/-to pair with no connecting path
// exits with exitUsage and mentions "no path" without crashing.
func TestRenderSVG_NoPath(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "-from", "review", "-to", "working", "-format", "svg", "testdata/composite.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
	}
	if !strings.Contains(errOut, "no path") {
		t.Errorf("stderr should contain 'no path': %s", errOut)
	}
}

// TestRenderSVG_BadFlagValues asserts that invalid tokens for -detail, -mode,
// and -show each produce exitUsage.
func TestRenderSVG_BadFlagValues(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"bad detail", []string{"render", "-format", "svg", "-detail", "bogus", "testdata/composite.json"}},
		{"bad mode", []string{"render", "-format", "svg", "-mode", "bogus", "-from", "working", "testdata/composite.json"}},
		{"bad show", []string{"render", "-format", "svg", "-show", "bogus", "testdata/composite.json"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmdRaw(tc.args...)
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
			}
		})
	}
}

// TestRenderSVG_ModeRequiresFrom asserts that -mode without -from exits with
// exitUsage and mentions -from.
func TestRenderSVG_ModeRequiresFrom(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "-mode", "all", "-format", "svg", "testdata/composite.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
	}
	if !strings.Contains(errOut, "-from") {
		t.Errorf("stderr should mention -from: %s", errOut)
	}
}

// TestRenderSVG_ToRequiresFrom asserts that -to without -from exits with
// exitUsage and mentions -from.
func TestRenderSVG_ToRequiresFrom(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "-to", "review", "-format", "svg", "testdata/composite.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitUsage, errOut)
	}
	if !strings.Contains(errOut, "-from") {
		t.Errorf("stderr should mention -from: %s", errOut)
	}
}

// TestRenderSVG_ThemeOverride asserts that a custom theme JSON file overrides
// the ember color in the rendered SVG.
func TestRenderSVG_ThemeOverride(t *testing.T) {
	customEmber := "#00ff00"
	theme := map[string]string{"ember": customEmber}
	b, err := json.Marshal(theme)
	if err != nil {
		t.Fatalf("marshal theme: %v", err)
	}
	themePath := filepath.Join(t.TempDir(), "theme.json")
	if err := os.WriteFile(themePath, b, 0o644); err != nil {
		t.Fatalf("write theme: %v", err)
	}

	code, out, errOut := runCmdRaw("render", "-theme", themePath, "-format", "svg", "testdata/composite.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(string(out), customEmber) {
		t.Errorf("svg output missing custom ember color %s", customEmber)
	}
}

// TestRenderSVG_ShowHideAccepted asserts that valid -show and -hide tokens
// are accepted and produce a valid SVG.
func TestRenderSVG_ShowHideAccepted(t *testing.T) {
	code, out, errOut := runCmdRaw("render", "-show", "guards", "-hide", "effects", "-format", "svg", "testdata/composite.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(string(out), "<svg") {
		t.Fatalf("svg output missing <svg element (stderr: %s)", errOut)
	}
}
