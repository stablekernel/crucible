package main

import (
	"bytes"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runCmd invokes the CLI dispatcher in-process and returns the exit code with
// captured stdout and stderr, so every command is tested without a subprocess.
func runCmd(args ...string) (code int, stdout, stderr string) {
	var out, errBuf bytes.Buffer
	code = run(args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

func TestRender(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"mermaid default", []string{"render", "testdata/clean.json"}, "stateDiagram-v2"},
		{"dot flag", []string{"render", "testdata/clean.json", "-format", "dot"}, "digraph"},
		{"flag before path", []string{"render", "-format", "dot", "testdata/clean.json"}, "digraph"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCmd(tc.args...)
			if code != exitOK {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("render output missing %q:\n%s", tc.want, out)
			}
		})
	}
}

func TestRender_UnknownFormat(t *testing.T) {
	code, _, errOut := runCmd("render", "testdata/clean.json", "-format", "svg")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut, "unknown -format") {
		t.Fatalf("stderr missing format error: %s", errOut)
	}
}

func TestDiff(t *testing.T) {
	cases := []struct {
		name     string
		old, new string
		wantBump string
	}{
		{"additive change is minor", "testdata/old.json", "testdata/new_minor.json", "bump: minor"},
		{"breaking change is major", "testdata/old.json", "testdata/new_major.json", "bump: major"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCmd("diff", tc.old, tc.new)
			if code != exitOK {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
			}
			if !strings.Contains(out, tc.wantBump) {
				t.Fatalf("diff output missing %q:\n%s", tc.wantBump, out)
			}
		})
	}
}

func TestDiff_SplitsBreakingAndAdditive(t *testing.T) {
	code, out, _ := runCmd("diff", "testdata/old.json", "testdata/new_major.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d", code, exitOK)
	}
	if !strings.Contains(out, "breaking (2)") {
		t.Errorf("want 2 breaking changes:\n%s", out)
	}
	if !strings.Contains(out, "additive (1)") {
		t.Errorf("want 1 additive change:\n%s", out)
	}
}

func TestValidate(t *testing.T) {
	cases := []struct {
		name string
		path string
		want int
	}{
		{"clean IR validates", "testdata/clean.json", exitOK},
		{"malformed JSON fails", "testdata/malformed.json", exitError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmd("validate", tc.path)
			if code != tc.want {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, tc.want, errOut)
			}
			if tc.want == exitError && strings.TrimSpace(errOut) == "" {
				t.Fatalf("expected a clear stderr message on failure")
			}
		})
	}
}

func TestLint(t *testing.T) {
	cases := []struct {
		name string
		path string
		want int
	}{
		{"clean IR has no findings", "testdata/clean.json", exitOK},
		{"defective IR reports findings", "testdata/defect.json", exitFindings},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCmd("lint", tc.path)
			if code != tc.want {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, tc.want, errOut)
			}
			if tc.want == exitFindings && !strings.Contains(out, "unreachable_state") {
				t.Fatalf("expected an unreachable_state finding:\n%s", out)
			}
		})
	}
}

func TestEject_StdoutContainsKeyIdents(t *testing.T) {
	code, out, errOut := runCmd("eject", "testdata/clean.json", "-package", "machine")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	for _, want := range []string{
		"package machine",
		"func Provide(reg *state.Registry[",
		`reg.Guard("hasItems"`,
		`reg.Action("notify"`,
		`reg.Service("charge"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("ejected source missing %q:\n%s", want, out)
		}
	}
	// The generated source must be parseable Go.
	if _, err := parser.ParseFile(token.NewFileSet(), "gen.go", out, parser.AllErrors); err != nil {
		t.Fatalf("ejected source does not parse: %v\n%s", err, out)
	}
}

func TestEject_WritesOutfile(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "gen.go")
	code, _, errOut := runCmd("eject", "testdata/clean.json", "-package", "machine", "-o", out)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(b), "package machine") {
		t.Fatalf("outfile missing package decl:\n%s", b)
	}
}

func TestVersion(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"-version"}, {"--version"}} {
		code, out, _ := runCmd(args...)
		if code != exitOK {
			t.Fatalf("%v exit = %d, want %d", args, code, exitOK)
		}
		if strings.TrimSpace(out) != version {
			t.Fatalf("%v printed %q, want %q", args, strings.TrimSpace(out), version)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no args", nil},
		{"unknown subcommand", []string{"frobnicate"}},
		{"lint missing path", []string{"lint"}},
		{"diff one path", []string{"diff", "testdata/old.json"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _ := runCmd(tc.args...)
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}

func TestStdinInput(t *testing.T) {
	// render reading from "-" exercises the stdin path; swap os.Stdin for the
	// duration so the dispatcher's hard-wired os.Stdin reads the fixture.
	b, err := os.ReadFile("testdata/clean.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	orig := os.Stdin
	os.Stdin = r
	t.Cleanup(func() { os.Stdin = orig })
	go func() {
		_, _ = w.Write(b)
		_ = w.Close()
	}()

	code, out, errOut := runCmd("render", "-")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(out, "stateDiagram-v2") {
		t.Fatalf("stdin render missing diagram:\n%s", out)
	}
}
