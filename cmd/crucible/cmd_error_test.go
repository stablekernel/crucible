package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// missingPath is a path under a fresh temp dir guaranteed not to exist, so
// readInput's os.ReadFile fails and exercises every subcommand's load-error
// branch.
func missingPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "does-not-exist.json")
}

// TestSubcommands_LoadError confirms each IR-loading subcommand returns
// exitError with a command-prefixed message when the IR file is missing.
func TestSubcommands_LoadError(t *testing.T) {
	missing := missingPath(t)
	cases := []struct {
		name   string
		args   []string
		prefix string
	}{
		{"lint", []string{"lint", missing}, "crucible lint:"},
		{"render", []string{"render", missing}, "crucible render:"},
		{"validate", []string{"validate", missing}, "crucible validate:"},
		{"eject", []string{"eject", missing}, "crucible eject:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmd(tc.args...)
			if code != exitError {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
			}
			if !strings.Contains(errOut, tc.prefix) {
				t.Fatalf("stderr missing %q: %s", tc.prefix, errOut)
			}
		})
	}
}

// TestSubcommands_QuenchError confirms the lint, render, and validate commands
// surface a quench failure (an undeclared transition target panics Quench,
// which quench recovers into an error) as exitError on stderr.
func TestSubcommands_QuenchError(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"lint", []string{"lint", "testdata/quench_fail.json"}},
		{"render", []string{"render", "testdata/quench_fail.json"}},
		{"validate", []string{"validate", "testdata/quench_fail.json"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmd(tc.args...)
			if code != exitError {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
			}
			if !strings.Contains(errOut, "quench:") {
				t.Fatalf("stderr missing quench error: %s", errOut)
			}
		})
	}
}

// TestParseSingleArg_FlagParseError confirms the single-arg commands (lint,
// validate) return a usage exit code when flag parsing fails on an unknown flag.
func TestParseSingleArg_FlagParseError(t *testing.T) {
	for _, cmd := range []string{"lint", "validate"} {
		t.Run(cmd, func(t *testing.T) {
			code, _, _ := runCmd(cmd, "testdata/clean.json", "-nope")
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}

// TestRender_FlagParseError confirms an unknown flag fails flag parsing and
// returns a usage exit code.
func TestRender_FlagParseError(t *testing.T) {
	code, _, _ := runCmd("render", "testdata/clean.json", "-nope")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
}

// TestRender_WrongArgCount confirms render rejects a missing IR path with a
// usage message.
func TestRender_WrongArgCount(t *testing.T) {
	code, _, errOut := runCmd("render")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut, "usage: crucible render") {
		t.Fatalf("stderr missing usage: %s", errOut)
	}
}

// TestEject_FlagParseError confirms eject rejects an unknown flag with a usage
// exit code.
func TestEject_FlagParseError(t *testing.T) {
	code, _, _ := runCmd("eject", "testdata/clean.json", "-nope")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
}

// TestEject_WrongArgCount confirms eject rejects a missing IR path with a usage
// message.
func TestEject_WrongArgCount(t *testing.T) {
	code, _, errOut := runCmd("eject")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut, "usage: crucible eject") {
		t.Fatalf("stderr missing usage: %s", errOut)
	}
}

// TestEject_DefaultPackageToStdout confirms eject with no -package flag writes
// generated source to stdout (the gen-default package-name branch).
func TestEject_DefaultPackageToStdout(t *testing.T) {
	code, out, errOut := runCmd("eject", "testdata/clean.json")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(out, "package ") {
		t.Fatalf("ejected source missing a package decl:\n%s", out)
	}
}

// TestEject_WriteOutputError confirms a write to an unwritable output path is
// reported as exitError. The output path names a regular file as a parent
// directory, so os.WriteFile fails.
func TestEject_WriteOutputError(t *testing.T) {
	dir := t.TempDir()
	notDir := filepath.Join(dir, "afile")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	out := filepath.Join(notDir, "gen.go") // parent is a file, not a dir
	code, _, errOut := runCmd("eject", "testdata/clean.json", "-o", out)
	if code != exitError {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
	}
	if !strings.Contains(errOut, "write output") {
		t.Fatalf("stderr missing write error: %s", errOut)
	}
}

// TestDiff_FlagParseError confirms diff rejects an unknown flag with a usage
// exit code.
func TestDiff_FlagParseError(t *testing.T) {
	code, _, _ := runCmd("diff", "-nope", "testdata/old.json", "testdata/new_minor.json")
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d", code, exitUsage)
	}
}

// TestDiff_ReadErrors confirms diff reports a missing old or new IR file as
// exitError, each with its own stderr prefix.
func TestDiff_ReadErrors(t *testing.T) {
	missing := missingPath(t)
	cases := []struct {
		name   string
		args   []string
		prefix string
	}{
		{"old missing", []string{"diff", missing, "testdata/new_minor.json"}, "read old:"},
		{"new missing", []string{"diff", "testdata/old.json", missing}, "read new:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, errOut := runCmd(tc.args...)
			if code != exitError {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
			}
			if !strings.Contains(errOut, tc.prefix) {
				t.Fatalf("stderr missing %q: %s", tc.prefix, errOut)
			}
		})
	}
}

// TestDiff_MalformedInput confirms diff reports a malformed IR document as
// exitError via the DiffJSON decode failure.
func TestDiff_MalformedInput(t *testing.T) {
	code, _, errOut := runCmd("diff", "testdata/malformed.json", "testdata/old.json")
	if code != exitError {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
	}
	if !strings.Contains(errOut, "crucible diff:") {
		t.Fatalf("stderr missing diff error: %s", errOut)
	}
}

// TestHelp confirms the help variants print usage to stdout and exit zero.
func TestHelp(t *testing.T) {
	for _, args := range [][]string{{"-h"}, {"--help"}, {"help"}} {
		code, out, _ := runCmd(args...)
		if code != exitOK {
			t.Fatalf("%v exit = %d, want %d", args, code, exitOK)
		}
		if !strings.Contains(out, "crucible - headless tooling") {
			t.Fatalf("%v help output missing banner:\n%s", args, out)
		}
	}
}

// TestReorderArgs_DoubleDashTerminator confirms a bare "--" terminates flag
// processing so following tokens are treated as positional. render -- <path>
// keeps the default mermaid format and renders the file.
func TestReorderArgs_DoubleDashTerminator(t *testing.T) {
	got := reorderArgs([]string{"--", "-format", "ir.json"})
	want := []string{"-format", "ir.json"}
	if len(got) != len(want) {
		t.Fatalf("reorderArgs returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reorderArgs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestComposite_ExercisesDeepWalk renders and ejects a compound + parallel IR
// whose states carry entry/exit/done actions, entry/exit assigns, an invoke, a
// composite guard expression, children, and regions. This drives the deep
// branches of walkState/walkTransition (the stub registry must enumerate every
// referenced behavior or Quench panics).
func TestComposite_ExercisesDeepWalk(t *testing.T) {
	t.Run("render", func(t *testing.T) {
		code, out, errOut := runCmd("render", "testdata/composite.json")
		if code != exitOK {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
		}
		if !strings.Contains(out, "stateDiagram-v2") {
			t.Fatalf("render output missing diagram:\n%s", out)
		}
	})
	t.Run("eject parses", func(t *testing.T) {
		code, out, errOut := runCmd("eject", "testdata/composite.json")
		if code != exitOK {
			t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
		}
		// Behaviors reachable through the deep walk (region guards, invoke
		// services, exit/done actions, nested-state effects) must appear in the
		// generated stubs.
		for _, want := range []string{"fetchWork", "logExit", "logDone", "regionGuard", "notify"} {
			if !strings.Contains(out, want) {
				t.Errorf("ejected source missing reference %q", want)
			}
		}
	})
}
