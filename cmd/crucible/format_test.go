package main

import (
	"encoding/json"
	"os"
	"testing"
)

// TestLint_FormatJSON confirms -format json emits a machine-readable report:
// an empty findings array for a clean IR (exit 0) and populated findings for a
// defective IR (exit 1).
func TestLint_FormatJSON(t *testing.T) {
	t.Run("clean IR has empty findings", func(t *testing.T) {
		code, out, errOut := runCmd("lint", "testdata/clean.json", "-format", "json")
		if code != exitOK {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
		}
		var got lintReportJSON
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("unmarshal lint json: %v\n%s", err, out)
		}
		if len(got.Findings) != 0 {
			t.Fatalf("want 0 findings, got %d: %+v", len(got.Findings), got.Findings)
		}
	})

	t.Run("defective IR reports findings", func(t *testing.T) {
		code, out, errOut := runCmd("lint", "testdata/defect.json", "-format", "json")
		if code != exitFindings {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitFindings, errOut)
		}
		var got lintReportJSON
		if err := json.Unmarshal([]byte(out), &got); err != nil {
			t.Fatalf("unmarshal lint json: %v\n%s", err, out)
		}
		if len(got.Findings) == 0 {
			t.Fatal("want findings for a defective IR, got none")
		}
		var sawUnreachable bool
		for _, f := range got.Findings {
			if f.Kind == "unreachable_state" {
				sawUnreachable = true
				if f.Severity != "error" {
					t.Errorf("unreachable_state severity = %q, want error", f.Severity)
				}
				if f.State == "" {
					t.Error("unreachable_state finding has empty state")
				}
			}
		}
		if !sawUnreachable {
			t.Errorf("want an unreachable_state finding, got %+v", got.Findings)
		}
	})
}

// TestLint_FormatSARIF confirms -format sarif emits a valid SARIF 2.1.0 log:
// the version, tool name, and result rule/level are mapped from findings, and a
// stdin source ("-") omits the physical location.
func TestLint_FormatSARIF(t *testing.T) {
	t.Run("defective IR from a file", func(t *testing.T) {
		code, out, errOut := runCmd("lint", "testdata/defect.json", "-format", "sarif")
		if code != exitFindings {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitFindings, errOut)
		}
		var root sarifRoot
		if err := json.Unmarshal([]byte(out), &root); err != nil {
			t.Fatalf("unmarshal sarif: %v\n%s", err, out)
		}
		if root.Version != "2.1.0" {
			t.Errorf("version = %q, want 2.1.0", root.Version)
		}
		if len(root.Runs) != 1 {
			t.Fatalf("want 1 run, got %d", len(root.Runs))
		}
		run := root.Runs[0]
		if run.Tool.Driver.Name != "crucible" {
			t.Errorf("driver name = %q, want crucible", run.Tool.Driver.Name)
		}
		if run.Tool.Driver.Version != version {
			t.Errorf("driver version = %q, want %q", run.Tool.Driver.Version, version)
		}
		if len(run.Results) == 0 {
			t.Fatal("want results for a defective IR, got none")
		}
		first := run.Results[0]
		if first.RuleID != "unreachable_state" {
			t.Errorf("results[0].ruleId = %q, want unreachable_state", first.RuleID)
		}
		if first.Level != "error" {
			t.Errorf("results[0].level = %q, want error", first.Level)
		}
		if len(first.Locations) == 0 || first.Locations[0].PhysicalLocation == nil {
			t.Fatal("file-sourced finding should carry a physical location")
		}
		if got := first.Locations[0].PhysicalLocation.ArtifactLocation.URI; got != "testdata/defect.json" {
			t.Errorf("artifact uri = %q, want testdata/defect.json", got)
		}
	})

	t.Run("stdin source omits physical location", func(t *testing.T) {
		b, err := os.ReadFile("testdata/defect.json")
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

		code, out, errOut := runCmd("lint", "-", "-format", "sarif")
		if code != exitFindings {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitFindings, errOut)
		}
		var root sarifRoot
		if err := json.Unmarshal([]byte(out), &root); err != nil {
			t.Fatalf("unmarshal sarif: %v\n%s", err, out)
		}
		if len(root.Runs) == 0 || len(root.Runs[0].Results) == 0 {
			t.Fatal("want results from stdin, got none")
		}
		for i, res := range root.Runs[0].Results {
			for j, loc := range res.Locations {
				if loc.PhysicalLocation != nil {
					t.Errorf("results[%d].locations[%d] has a physical location for stdin input", i, j)
				}
			}
		}
	})
}

// TestDiff_FormatJSON confirms -format json emits the recommended bump and a
// breaking count alongside the per-change list.
func TestDiff_FormatJSON(t *testing.T) {
	cases := []struct {
		name         string
		old, new     string
		wantBump     string
		wantBreaking int
	}{
		{"additive change is minor", "testdata/old.json", "testdata/new_minor.json", "minor", 0},
		{"breaking change is major", "testdata/old.json", "testdata/new_major.json", "major", 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCmd("diff", tc.old, tc.new, "-format", "json")
			if code != exitOK {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
			}
			var got diffReportJSON
			if err := json.Unmarshal([]byte(out), &got); err != nil {
				t.Fatalf("unmarshal diff json: %v\n%s", err, out)
			}
			if got.Bump != tc.wantBump {
				t.Errorf("bump = %q, want %q", got.Bump, tc.wantBump)
			}
			if got.Breaking != tc.wantBreaking {
				t.Errorf("breaking = %d, want %d", got.Breaking, tc.wantBreaking)
			}
			// The breaking count must equal the breaking changes in the list.
			var listed int
			for _, c := range got.Changes {
				if c.Breaking {
					listed++
				}
			}
			if listed != got.Breaking {
				t.Errorf("breaking count %d disagrees with %d breaking changes listed", got.Breaking, listed)
			}
		})
	}
}

// TestDiff_ExitCode confirms -exit-code returns exitBreaking only for a major
// (breaking) diff, exits zero for a compatible diff, and that without the flag
// even a breaking diff exits zero.
func TestDiff_ExitCode(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCode int
	}{
		{"breaking diff with -exit-code", []string{"diff", "testdata/old.json", "testdata/new_major.json", "-exit-code"}, exitBreaking},
		{"compatible diff with -exit-code", []string{"diff", "testdata/old.json", "testdata/new_minor.json", "-exit-code"}, exitOK},
		{"breaking diff without -exit-code", []string{"diff", "testdata/old.json", "testdata/new_major.json"}, exitOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmd(tc.args...)
			if code != tc.wantCode {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, tc.wantCode, errOut)
			}
		})
	}
}

// TestFormat_UnknownValue confirms unknown or unsupported -format values are
// rejected with a usage exit code for both lint and diff, including when the
// flag trails the positional arguments.
func TestFormat_UnknownValue(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"lint unknown format", []string{"lint", "testdata/clean.json", "-format", "zzz"}},
		{"diff unknown format", []string{"diff", "testdata/old.json", "testdata/new_minor.json", "-format", "zzz"}},
		{"diff sarif unsupported", []string{"diff", "testdata/old.json", "testdata/new_minor.json", "-format", "sarif"}},
		{"diff flag after paths still validated", []string{"diff", "testdata/old.json", "testdata/new_minor.json", "-format", "json"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _ := runCmd(tc.args...)
			// The last case is valid (json after paths) and must succeed; all
			// others are usage errors.
			if tc.name == "diff flag after paths still validated" {
				if code != exitOK {
					t.Fatalf("exit = %d, want %d", code, exitOK)
				}
				return
			}
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}
