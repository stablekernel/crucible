// Command crucible is a headless command-line tool over the crucible
// state-machine IR. It lints, renders, diffs, validates, and ejects a machine's
// serialized IR JSON without running any behavior.
package main

import (
	"fmt"
	"io"
	"os"
)

// version is the crucible CLI's own version, independent of the state module's
// v1 freeze.
const version = "0.1.0"

// exit codes. 0 is success, 1 a runtime or load error, 2 a usage error, and a
// non-zero code (1) also signals lint findings.
const (
	exitOK       = 0
	exitError    = 1
	exitUsage    = 2
	exitFindings = 1
	// exitBreaking is returned by diff -exit-code when the recommended bump is
	// major (at least one breaking change). It shares the value of exitError.
	exitBreaking = 1
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run dispatches a subcommand and returns its exit code. It is the testable
// seam: every subcommand is a func(args, stdout, stderr) int, so the whole CLI
// is exercised without spawning a process.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return exitUsage
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "lint":
		return runLint(rest, stdout, stderr)
	case "render":
		return runRender(rest, stdout, stderr)
	case "diff":
		return runDiff(rest, stdout, stderr)
	case "validate":
		return runValidate(rest, stdout, stderr)
	case "eject":
		return runEject(rest, stdout, stderr)
	case "simulate":
		return runSimulate(rest, stdout, stderr)
	case "version":
		emitln(stdout, version)
		return exitOK
	case "-version", "--version":
		emitln(stdout, version)
		return exitOK
	case "-h", "--help", "help":
		usage(stdout)
		return exitOK
	default:
		emitf(stderr, "crucible: unknown subcommand %q\n\n", cmd)
		usage(stderr)
		return exitUsage
	}
}

// printf, println, and print write diagnostics and command output to an
// io.Writer. A failure writing to stdout or stderr is unrecoverable for a
// command-line tool (the stream the user reads is gone), so the error is
// deliberately ignored here rather than threaded back through every call site.
func emitf(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
func emitln(w io.Writer, a ...any)               { _, _ = fmt.Fprintln(w, a...) }
func emit(w io.Writer, a ...any)                 { _, _ = fmt.Fprint(w, a...) }

// usage prints the top-level command listing.
func usage(w io.Writer) {
	emit(w, `crucible - headless tooling for the crucible state-machine IR

Usage:
  crucible <command> [arguments]

Commands:
  lint      <ir.json> [-format f]     run static analysis; -format text (default), json, or sarif
  render    <ir.json> [-format f] [-o file]   render as mermaid (default), dot, svg, or png (svg/png embed Graphviz; no external install)
  diff      <old.json> <new.json> [-format f] [-exit-code]   classify changes and recommend a semver bump
  validate  <ir.json>                 confirm the IR loads and assembles
  eject     <ir.json> [-package p] [-o f]   generate typed Go behavior stubs
  simulate  <ir.json> -events e1,e2 [flags]   fire events and print the step trace
  version                             print the crucible CLI version

Each command reads an IR JSON file path, or - for stdin.

Flags:
  -version    print the crucible CLI version
  -h, --help  show this help
`)
}
