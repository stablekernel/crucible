package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// pngMagic is the 8-byte PNG file signature.
var pngMagic = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}

// TestThemeDOT_AddsBrandAndPreservesStructure is the authoritative check that
// the Crucible brand is applied at the DOT level. It asserts the brand hexes
// are injected as graph/node/edge defaults while every structural line of the
// original DOT (nodes, edges, per-element attributes) survives unchanged.
func TestThemeDOT_AddsBrandAndPreservesStructure(t *testing.T) {
	const in = "digraph M {\n" +
		"    rankdir=LR;\n" +
		"    node [shape=box, style=rounded];\n" +
		"    \"a\" [fillcolor=\"#abc\", style=filled];\n" +
		"    \"b\" [peripheries=2];\n" +
		"    \"a\" -> \"b\" [label=\"go\"];\n" +
		"}\n"

	out := themeDOT(in)

	for _, hex := range []string{"#d9620a", "#b06a28", "#16191d"} {
		if !strings.Contains(out, hex) {
			t.Errorf("themed DOT missing brand hex %q:\n%s", hex, out)
		}
	}
	if !strings.Contains(out, `bgcolor="transparent"`) {
		t.Errorf("themed DOT missing transparent bgcolor:\n%s", out)
	}
	// Brand attrs are DOT defaults, so per-element fills/rings must be intact.
	for _, frag := range []string{
		`"a" [fillcolor="#abc", style=filled];`,
		`"b" [peripheries=2];`,
		`"a" -> "b" [label="go"];`,
	} {
		if !strings.Contains(out, frag) {
			t.Errorf("themed DOT dropped structural fragment %q:\n%s", frag, out)
		}
	}
	// The brand defaults must sit inside the digraph body, after the header.
	header := strings.Index(out, "{\n")
	for _, def := range []string{"bgcolor=", "node [color=", "edge [color="} {
		if idx := strings.Index(out, def); idx < header {
			t.Errorf("brand default %q not inserted after digraph header", def)
		}
	}
}

// TestThemeDOT_UnexpectedHeaderReturnedUnchanged verifies the graceful
// fallback: DOT lacking the expected "digraph ... {" header is returned
// verbatim so rendering still succeeds with structurally valid input.
func TestThemeDOT_UnexpectedHeaderReturnedUnchanged(t *testing.T) {
	const in = "graph G {\n    a -- b;\n}\n"
	if out := themeDOT(in); out != in {
		t.Errorf("expected unchanged DOT for non-digraph input, got:\n%s", out)
	}
}

// runCmdRaw invokes the CLI dispatcher in-process and returns the exit code with
// captured stdout as raw bytes (so binary PNG output survives) plus stderr text.
func runCmdRaw(args ...string) (code int, stdout []byte, stderr string) {
	var out, errBuf bytes.Buffer
	code = run(args, &out, &errBuf)
	return code, out.Bytes(), errBuf.String()
}

// TestRender_SVG asserts svg renders to stdout: exit 0 and an <svg element in
// the output. Color is intentionally not asserted here — the authoritative
// brand-color check lives at the DOT level in the state package, since Graphviz
// may normalize hex to rgb()/named forms in the SVG.
func TestRender_SVG(t *testing.T) {
	code, out, errOut := runCmdRaw("render", "testdata/clean.json", "-format", "svg")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(string(out), "<svg") {
		t.Fatalf("svg output missing <svg element:\n%s", out)
	}
}

// TestRender_PNG_ToFile asserts png renders to a -o file beginning with the PNG
// magic bytes, with exit 0 and empty stdout.
func TestRender_PNG_ToFile(t *testing.T) {
	outPath := filepath.Join(t.TempDir(), "machine.png")
	code, out, errOut := runCmdRaw("render", "testdata/clean.json", "-format", "png", "-o", outPath)
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
	if !bytes.HasPrefix(data, pngMagic) {
		t.Fatalf("output file does not start with PNG magic; first bytes: % x", data[:min(8, len(data))])
	}
}

// TestRender_PNG_ToStdout asserts png to stdout (no -o) yields raw bytes that
// begin with the PNG magic signature.
func TestRender_PNG_ToStdout(t *testing.T) {
	code, out, errOut := runCmdRaw("render", "testdata/clean.json", "-format", "png")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !bytes.HasPrefix(out, pngMagic) {
		t.Fatalf("png stdout does not start with PNG magic; first bytes: % x", out[:min(8, len(out))])
	}
}

// TestRender_SVG_BadIR asserts a malformed IR fails the render with exitError
// before any image work.
func TestRender_SVG_BadIR(t *testing.T) {
	code, _, errOut := runCmdRaw("render", "testdata/malformed.json", "-format", "svg")
	if code != exitError {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
	}
	if !strings.Contains(errOut, "crucible render:") {
		t.Fatalf("stderr missing render error prefix: %s", errOut)
	}
}
