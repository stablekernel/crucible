package main

import (
	"flag"
	"io"
	"os"

	"github.com/stablekernel/crucible/gen"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
	"github.com/stablekernel/crucible/state/evolution"
)

// runLint loads an IR, assembles it with stub behaviors, runs every static
// analysis check, and prints the findings. It exits non-zero when the analysis
// reports any finding so the command can gate CI.
func runLint(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("lint", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "text", "output format: text, json, or sarif")
	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		emitln(stderr, "usage: crucible lint <ir.json> [-format text|json|sarif]")
		return exitUsage
	}
	switch *format {
	case "text", "json", "sarif":
	default:
		emitf(stderr, "crucible lint: unknown -format %q (want text, json, or sarif)\n", *format)
		return exitUsage
	}

	irPath := fs.Arg(0)
	ir, err := loadIR(irPath, os.Stdin)
	if err != nil {
		emitf(stderr, "crucible lint: %v\n", err)
		return exitError
	}
	m, err := quench(ir)
	if err != nil {
		emitf(stderr, "crucible lint: %v\n", err)
		return exitError
	}

	report := analysis.Analyze(m)
	switch *format {
	case "text":
		emitln(stdout, report.String())
	default:
		if err := formatLint(report, *format, irPath, version, stdout); err != nil {
			emitf(stderr, "crucible lint: %v\n", err)
			return exitError
		}
	}
	if len(report.Findings) > 0 {
		return exitFindings
	}
	return exitOK
}

// runRender loads an IR, assembles it with stub behaviors, and emits the
// machine diagram. -format selects mermaid (the default), dot, svg, or png. The
// svg and png formats are rendered directly via an embedded (pure-Go, WASM)
// Graphviz — no external `dot` install is required — and carry the Crucible
// brand theme. Image bytes go to -o when set, otherwise to stdout; png in
// particular is binary, so -o is the norm.
func runRender(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "mermaid", "diagram format: mermaid, dot, svg, or png")
	out := fs.String("o", "", "output file (default: stdout)")
	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		emitln(stderr, "usage: crucible render <ir.json> [-format mermaid|dot|svg|png] [-o outfile]")
		return exitUsage
	}
	switch *format {
	case "mermaid", "dot", "svg", "png":
	default:
		emitf(stderr, "crucible render: unknown -format %q (want mermaid, dot, svg, or png)\n", *format)
		return exitUsage
	}

	ir, err := loadIR(fs.Arg(0), os.Stdin)
	if err != nil {
		emitf(stderr, "crucible render: %v\n", err)
		return exitError
	}
	m, err := quench(ir)
	if err != nil {
		emitf(stderr, "crucible render: %v\n", err)
		return exitError
	}

	switch *format {
	case "svg":
		return renderImageToOutput(m, formatSVG, *out, stdout, stderr)
	case "png":
		return renderImageToOutput(m, formatPNG, *out, stdout, stderr)
	case "dot":
		emit(stdout, m.ToDOT())
	default:
		emit(stdout, m.ToMermaid())
	}
	return exitOK
}

// renderImageToOutput renders the machine to image bytes and writes them either
// to the named file or to stdout. The bytes are binary, so they bypass the
// emit* helpers (which append newlines and would corrupt a PNG). It returns the
// process exit code.
func renderImageToOutput[S comparable, E comparable, C any](m *state.Machine[S, E, C], format imageFormat, out string, stdout, stderr io.Writer) int {
	img, err := renderImage(m, format)
	if err != nil {
		emitf(stderr, "crucible render: %v\n", err)
		return exitError
	}
	if out == "" {
		_, err = stdout.Write(img)
	} else {
		err = os.WriteFile(out, img, 0o644)
	}
	if err != nil {
		emitf(stderr, "crucible render: write output: %v\n", err)
		return exitError
	}
	return exitOK
}

// runDiff loads two serialized IRs, classifies the changes between them, and
// prints the recommended semver bump along with the breaking and additive
// changes split apart.
func runDiff(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("diff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "text", "output format: text or json")
	exitCode := fs.Bool("exit-code", false, "exit nonzero on breaking (major) changes")
	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 2 {
		emitln(stderr, "usage: crucible diff <old.json> <new.json> [-format text|json] [-exit-code]")
		return exitUsage
	}
	switch *format {
	case "text", "json":
	case "sarif":
		emitln(stderr, "crucible diff: -format sarif is not supported for diff")
		return exitUsage
	default:
		emitf(stderr, "crucible diff: unknown -format %q (want text or json)\n", *format)
		return exitUsage
	}

	oldBytes, err := readInput(fs.Arg(0), os.Stdin)
	if err != nil {
		emitf(stderr, "crucible diff: read old: %v\n", err)
		return exitError
	}
	newBytes, err := readInput(fs.Arg(1), os.Stdin)
	if err != nil {
		emitf(stderr, "crucible diff: read new: %v\n", err)
		return exitError
	}

	report, err := evolution.DiffJSON[string, string, any](oldBytes, newBytes)
	if err != nil {
		emitf(stderr, "crucible diff: %v\n", err)
		return exitError
	}

	if *format == "json" {
		if err := formatDiff(report, *format, stdout); err != nil {
			emitf(stderr, "crucible diff: %v\n", err)
			return exitError
		}
	} else {
		emitf(stdout, "bump: %s\n", report.SemverBump())
		var breaking, additive []evolution.Change
		for _, c := range report.Changes {
			if c.Breaking {
				breaking = append(breaking, c)
			} else {
				additive = append(additive, c)
			}
		}
		emitf(stdout, "\nbreaking (%d):\n", len(breaking))
		for _, c := range breaking {
			emitf(stdout, "  %-24s %s: %s\n", c.Kind, c.Path, c.Description)
		}
		emitf(stdout, "\nadditive (%d):\n", len(additive))
		for _, c := range additive {
			emitf(stdout, "  %-24s %s: %s\n", c.Kind, c.Path, c.Description)
		}
	}

	if *exitCode && report.SemverBump() == evolution.Major {
		return exitBreaking
	}
	return exitOK
}

// runValidate confirms an IR loads and assembles cleanly with stub behaviors. It
// is the well-formedness gate: a malformed JSON document or a structural defect
// the lint rejects exits non-zero with a message on stderr; a clean machine
// exits zero.
func runValidate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if code, ok := parseSingleArg(fs, args, "validate", "<ir.json>", stderr); !ok {
		return code
	}

	ir, err := loadIR(fs.Arg(0), os.Stdin)
	if err != nil {
		emitf(stderr, "crucible validate: %v\n", err)
		return exitError
	}
	if _, err := quench(ir); err != nil {
		emitf(stderr, "crucible validate: %v\n", err)
		return exitError
	}
	emitf(stdout, "ok: %s\n", ir.Name)
	return exitOK
}

// runEject loads an IR and generates typed Go behavior stubs, writing them to
// -o or to stdout. -package overrides the generated package name.
func runEject(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("eject", flag.ContinueOnError)
	fs.SetOutput(stderr)
	pkg := fs.String("package", "", "generated package name (default: gen's default)")
	out := fs.String("o", "", "output file (default: stdout)")
	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		emitln(stderr, "usage: crucible eject <ir.json> [-package name] [-o outfile]")
		return exitUsage
	}

	ir, err := loadIR(fs.Arg(0), os.Stdin)
	if err != nil {
		emitf(stderr, "crucible eject: %v\n", err)
		return exitError
	}

	var opts []gen.Option
	if *pkg != "" {
		opts = append(opts, gen.WithPackageName(*pkg))
	}
	src, err := gen.Eject[string, string, any](*ir, opts...)
	if err != nil {
		emitf(stderr, "crucible eject: %v\n", err)
		return exitError
	}

	if *out == "" {
		_, err = stdout.Write(src)
	} else {
		err = os.WriteFile(*out, src, 0o644)
	}
	if err != nil {
		emitf(stderr, "crucible eject: write output: %v\n", err)
		return exitError
	}
	return exitOK
}

// parseSingleArg parses a flag set expecting exactly one positional argument (an
// IR path). It returns the exit code and false when parsing fails or the
// argument count is wrong; otherwise it returns (0, true).
func parseSingleArg(fs *flag.FlagSet, args []string, name, argHint string, stderr io.Writer) (int, bool) {
	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage, false
	}
	if fs.NArg() != 1 {
		emitf(stderr, "usage: crucible %s %s\n", name, argHint)
		return exitUsage, false
	}
	return exitOK, true
}

// reorderArgs moves flag tokens ahead of positional arguments so a flag may
// appear after the IR path (e.g. "render ir.json -format dot"). Go's flag
// package stops at the first non-flag token, so without this a trailing flag is
// read as a stray positional. Every value-taking flag in this CLI (-format,
// -package, -o) is moved together with its following value token; a -k=v token
// carries its own value. A bare "--" terminates flag processing, and everything
// after it is treated as positional.
func reorderArgs(args []string) []string {
	valueFlags := map[string]bool{
		"-format": true, "-package": true, "-o": true,
		"-events": true, "-events-file": true, "-initial": true, "-guard": true,
	}
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
			// Pull the value token along for a space-separated value flag.
			if valueFlags[a] && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i++
			}
			continue
		}
		positional = append(positional, a)
	}
	return append(flags, positional...)
}
