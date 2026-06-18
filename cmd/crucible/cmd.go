package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/stablekernel/crucible/gen"
	"github.com/stablekernel/crucible/state"
	"github.com/stablekernel/crucible/state/analysis"
	"github.com/stablekernel/crucible/state/evolution"

	"github.com/stablekernel/crucible/cmd/crucible/internal/query"
	"github.com/stablekernel/crucible/cmd/crucible/internal/render"
	"github.com/stablekernel/crucible/cmd/crucible/internal/viewmodel"
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

// stringSliceFlag is a repeatable flag that accumulates string values.
// It implements flag.Value so it can be registered with fs.Var.
type stringSliceFlag []string

// String returns the slice formatted as a comma-joined string.
func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }

// Set appends v to the accumulated slice.
func (s *stringSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// dimensionByToken maps user-visible flag tokens to their Dimension constant.
var dimensionByToken = map[string]viewmodel.Dimension{
	"guards":         viewmodel.DimGuards,
	"effects":        viewmodel.DimEffects,
	"assigns":        viewmodel.DimAssigns,
	"entry-exit":     viewmodel.DimEntryExit,
	"invoke":         viewmodel.DimInvoke,
	"delays":         viewmodel.DimDelays,
	"descriptions":   viewmodel.DimDescriptions,
	"data-flow":      viewmodel.DimDataFlow,
	"context-schema": viewmodel.DimContextSchema,
	"source":         viewmodel.DimSource,
}

// validDimensionTokens is the sorted list of recognized dimension tokens,
// used in error messages.
var validDimensionTokens = []string{
	"assigns", "context-schema", "data-flow", "delays", "descriptions",
	"effects", "entry-exit", "guards", "invoke", "source",
}

// runRender loads an IR, assembles it with stub behaviors, and emits the
// machine diagram. -format selects mermaid (the default), dot, or svg. The svg
// format is rendered via the in-house D2 renderer and carries the Crucible
// brand theme; SVG bytes are written to -o when set, otherwise to stdout.
// -format png is rejected with a conversion hint. Scope and detail projection
// are controlled by -from, -to, -mode, -detail, -show, and -hide.
func runRender(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("render", flag.ContinueOnError)
	fs.SetOutput(stderr)
	format := fs.String("format", "mermaid", "diagram format: mermaid, dot, or svg")
	out := fs.String("o", "", "output file (default: stdout)")
	from := fs.String("from", "", "source state for scope/path filtering")
	to := fs.String("to", "", "target state for path filtering (requires -from)")
	mode := fs.String("mode", "shortest", "path mode: shortest, all, or trace (requires -from)")
	detail := fs.String("detail", "actions", "detail level: outline, guards, actions, lifecycle, or full")
	theme := fs.String("theme", "", "path to a theme JSON file")
	var show, hide stringSliceFlag
	fs.Var(&show, "show", "add a dimension (repeatable): guards, effects, assigns, …")
	fs.Var(&hide, "hide", "suppress a dimension (repeatable)")

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return exitUsage
	}
	if fs.NArg() != 1 {
		emitln(stderr, "usage: crucible render <ir.json> [-format mermaid|dot|svg] [-o outfile] [-from state] [-to state] [-mode shortest|all|trace] [-detail outline|guards|actions|lifecycle|full] [-show dim] [-hide dim] [-theme file]")
		return exitUsage
	}

	// Validate -format first so the existing TestRender_UnknownFormat passes.
	switch *format {
	case "mermaid", "dot", "svg", "png":
	default:
		emitf(stderr, "crucible render: unknown -format %q (want mermaid, dot, svg, or png)\n", *format)
		return exitUsage
	}

	// PNG is rejected with a conversion hint.
	if *format == "png" {
		emitf(stderr, "crucible render: png is not supported directly; render -format svg and convert with resvg or rsvg-convert\n")
		return exitUsage
	}

	// Validate -mode token.
	var svgMode viewmodel.Mode
	switch *mode {
	case "shortest":
		svgMode = viewmodel.ModeShortest
	case "all":
		svgMode = viewmodel.ModeAll
	case "trace":
		svgMode = viewmodel.ModeTrace
	default:
		emitf(stderr, "crucible render: unknown -mode %q (want shortest, all, or trace)\n", *mode)
		return exitUsage
	}

	// Validate -detail token.
	var level viewmodel.DetailLevel
	switch *detail {
	case "outline":
		level = viewmodel.Outline
	case "guards":
		level = viewmodel.Guards
	case "actions":
		level = viewmodel.Actions
	case "lifecycle":
		level = viewmodel.Lifecycle
	case "full":
		level = viewmodel.Full
	default:
		emitf(stderr, "crucible render: unknown -detail %q (want outline, guards, actions, lifecycle, or full)\n", *detail)
		return exitUsage
	}

	// Validate -show tokens.
	showDims, err := parseDimensions(show)
	if err != nil {
		emitf(stderr, "crucible render: -show %v (valid: %s)\n", err, strings.Join(validDimensionTokens, ", "))
		return exitUsage
	}

	// Validate -hide tokens.
	hideDims, err := parseDimensions(hide)
	if err != nil {
		emitf(stderr, "crucible render: -hide %v (valid: %s)\n", err, strings.Join(validDimensionTokens, ", "))
		return exitUsage
	}

	// -to requires -from.
	if *to != "" && *from == "" {
		emitf(stderr, "crucible render: -to requires -from\n")
		return exitUsage
	}

	// Non-shortest mode requires -from.
	if *mode != "shortest" && *from == "" {
		emitf(stderr, "crucible render: mode requires -from/-to\n")
		return exitUsage
	}

	// For mermaid/dot, skip the full pipeline and emit text.
	if *format == "dot" || *format == "mermaid" {
		ir, loadErr := loadIR(fs.Arg(0), os.Stdin)
		if loadErr != nil {
			emitf(stderr, "crucible render: %v\n", loadErr)
			return exitError
		}
		m, quenchErr := quench(ir)
		if quenchErr != nil {
			emitf(stderr, "crucible render: %v\n", quenchErr)
			return exitError
		}
		switch *format {
		case "dot":
			emit(stdout, m.ToDOT())
		default:
			emit(stdout, m.ToMermaid())
		}
		return exitOK
	}

	// SVG: full viewmodel/render pipeline.
	var renderTheme render.Theme
	if *theme != "" {
		renderTheme, err = render.LoadTheme(*theme)
		if err != nil {
			emitf(stderr, "crucible render: %v\n", err)
			return exitUsage
		}
	} else {
		renderTheme = render.DefaultTheme
	}

	ir, loadErr := loadIR(fs.Arg(0), os.Stdin)
	if loadErr != nil {
		emitf(stderr, "crucible render: %v\n", loadErr)
		return exitError
	}
	m, quenchErr := quench(ir)
	if quenchErr != nil {
		emitf(stderr, "crucible render: %v\n", quenchErr)
		return exitError
	}

	resolver := viewmodel.NewRefResolver(state.BuiltinPalette(), m.Palette())

	var scope viewmodel.Scope
	switch {
	case *from != "" && *to != "":
		scope = viewmodel.ScopePath
	case *from != "":
		scope = viewmodel.ScopeReachableFrom
	default:
		scope = viewmodel.ScopeWhole
	}

	opts := viewmodel.ProjectionOptions{
		Level: level,
		Show:  showDims,
		Hide:  hideDims,
		Scope: scope,
		Mode:  svgMode,
		From:  *from,
		To:    *to,
	}

	vm, vmErr := viewmodel.BuildScoped(ir, resolver, opts)
	if vmErr != nil {
		switch {
		case errors.Is(vmErr, query.ErrUnknownState) || errors.Is(vmErr, query.ErrAmbiguousState):
			emitf(stderr, "crucible render: %v\n", vmErr)
		case strings.Contains(vmErr.Error(), "no path"):
			emitf(stderr, "crucible render: %v\n", vmErr)
		default:
			emitf(stderr, "crucible render: %v\n", vmErr)
		}
		return exitUsage
	}

	svg, renderErr := render.RenderSVG(vm, renderTheme)
	if renderErr != nil {
		emitf(stderr, "crucible render: %v\n", renderErr)
		return exitError
	}

	var writeErr error
	if *out == "" {
		_, writeErr = stdout.Write(svg)
	} else {
		writeErr = os.WriteFile(*out, svg, 0o644)
	}
	if writeErr != nil {
		emitf(stderr, "crucible render: write output: %v\n", writeErr)
		return exitError
	}
	return exitOK
}

// parseDimensions converts a slice of user-visible token strings to their
// Dimension constants. It returns an error naming the first unrecognized token.
func parseDimensions(tokens []string) ([]viewmodel.Dimension, error) {
	if len(tokens) == 0 {
		return nil, nil
	}
	dims := make([]viewmodel.Dimension, 0, len(tokens))
	for _, tok := range tokens {
		d, ok := dimensionByToken[tok]
		if !ok {
			return nil, fmt.Errorf("unknown dimension token %q", tok)
		}
		dims = append(dims, d)
	}
	return dims, nil
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
// -package, -o, -from, -to, -mode, -detail, -show, -hide, -theme) is moved
// together with its following value token; a -k=v token carries its own value.
// A bare "--" terminates flag processing, and everything after it is treated as
// positional.
func reorderArgs(args []string) []string {
	valueFlags := map[string]bool{
		"-format": true, "-package": true, "-o": true,
		"-events": true, "-events-file": true, "-initial": true, "-guard": true,
		"-from": true, "-to": true, "-mode": true, "-detail": true,
		"-show": true, "-hide": true, "-theme": true,
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
