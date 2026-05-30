//go:build mage

// Command mage holds the Crucible suite's build automation.
//
// Magefiles live in their own module (github.com/stablekernel/crucible/magefiles)
// so the build tool's dependencies never pollute the library modules. The
// library modules (notably the stdlib-only state kernel) keep clean, minimal
// dependency graphs.
//
// Run `mage -l` to list targets. Each target iterates the suite's Go modules
// (see the modules list below) and runs the equivalent per-module command.
//
// govulncheck and golangci-lint are invoked via `go run` against pinned
// versions so every contributor and CI run uses the same toolchain.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// modules is the list of Go modules in the suite, by directory. As new modules
// land (broker, store, sink), add them here and every target picks them
// up automatically.
var modules = []string{"state", "telemetry", "telemetry/slogadapter"}

// Pinned tool versions — keep in sync with .github/workflows/ci.yml.
const (
	golangciLint = "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2"
	govulncheck  = "golang.org/x/vuln/cmd/govulncheck@v1.3.0"
	// benchstat ships only pseudo-versions from golang.org/x/perf; pinned so
	// local BenchCompare runs match the CI benchmark gate exactly.
	benchstat = "golang.org/x/perf/cmd/benchstat@v0.0.0-20260512194132-3cf34090a3db"
)

// benchCompareCount is the per-benchmark sample count used by BenchCompare. It
// mirrors BENCH_COUNT in the CI bench gate so local results line up with CI.
const benchCompareCount = "8"

// benchCompareThreshold is the maximum allowed head/base ratio before
// BenchCompare reports a regression. It mirrors BENCH_THRESHOLD in CI.
const benchCompareThreshold = "1.20"

// repoRoot returns the suite root (the parent of the magefiles module dir).
func repoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// When invoked from within magefiles/, the root is one level up; when
	// invoked from the root, the cwd is already the root.
	if filepath.Base(wd) == "magefiles" {
		return filepath.Dir(wd), nil
	}
	return wd, nil
}

// inModule runs fn with the working directory set to the given module dir,
// restoring the original directory afterward.
func inModule(mod string, fn func() error) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	prev, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := os.Chdir(filepath.Join(root, mod)); err != nil {
		return err
	}
	defer func() { _ = os.Chdir(prev) }()
	return fn()
}

// forEachModule runs fn for every module, printing a header per module and
// stopping at the first error.
func forEachModule(action string, fn func(mod string) error) error {
	for _, mod := range modules {
		fmt.Printf("==> %s: %s\n", action, mod)
		if err := inModule(mod, func() error { return fn(mod) }); err != nil {
			return fmt.Errorf("%s failed for module %q: %w", action, mod, err)
		}
	}
	return nil
}

// goCmd runs `go args...` in the current working directory, streaming output.
func goCmd(args ...string) error {
	return sh.RunV("go", args...)
}

// Build compiles every module.
func Build() error {
	return forEachModule("build", func(string) error {
		return goCmd("build", "./...")
	})
}

// Test runs the unit tests for every module.
func Test() error {
	return forEachModule("test", func(string) error {
		return goCmd("test", "./...")
	})
}

// TestRace runs the tests for every module with the race detector enabled.
func TestRace() error {
	return forEachModule("test -race", func(string) error {
		return goCmd("test", "-race", "./...")
	})
}

// Lint runs golangci-lint over every module using the shared root config.
func Lint() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	cfg := filepath.Join(root, ".golangci.yml")
	return forEachModule("lint", func(string) error {
		return sh.RunV("go", "run", golangciLint, "run", "--config", cfg, "./...")
	})
}

// Cover runs the tests for every module with coverage profiling.
func Cover() error {
	return forEachModule("cover", func(mod string) error {
		profile := fmt.Sprintf("coverage-%s.out", strings.ReplaceAll(mod, "/", "-"))
		return goCmd("test", "-coverprofile="+profile, "-covermode=atomic", "./...")
	})
}

// coverThreshold is the minimum per-module statement coverage the CoverGate
// target (and the CI coverage job) enforces.
const coverThreshold = 80.0

// CoverGate runs the tests for every module with coverage profiling and fails if
// total coverage drops below the threshold, mirroring the CI coverage gate.
func CoverGate() error {
	return forEachModule("cover-gate", func(mod string) error {
		profile := fmt.Sprintf("coverage-%s.out", strings.ReplaceAll(mod, "/", "-"))
		if err := goCmd("test", "-coverprofile="+profile, "-covermode=atomic", "./..."); err != nil {
			return err
		}
		out, err := sh.Output("go", "tool", "cover", "-func="+profile)
		if err != nil {
			return err
		}
		pct, err := totalCoverage(out)
		if err != nil {
			return err
		}
		fmt.Printf("    total coverage: %.1f%% (threshold %.0f%%)\n", pct, coverThreshold)
		if pct < coverThreshold {
			return fmt.Errorf("coverage %.1f%% is below the %.0f%% threshold", pct, coverThreshold)
		}
		return nil
	})
}

// totalCoverage extracts the total percentage from `go tool cover -func` output.
func totalCoverage(funcOutput string) (float64, error) {
	for _, line := range strings.Split(funcOutput, "\n") {
		if !strings.HasPrefix(line, "total:") {
			continue
		}
		fields := strings.Fields(line)
		pctStr := strings.TrimSuffix(fields[len(fields)-1], "%")
		var pct float64
		if _, err := fmt.Sscanf(pctStr, "%f", &pct); err != nil {
			return 0, err
		}
		return pct, nil
	}
	return 0, fmt.Errorf("no total coverage line in cover output")
}

// Bench runs the benchmarks for every module.
func Bench() error {
	return forEachModule("bench", func(string) error {
		return goCmd("test", "-run", "^$", "-bench", ".", "-benchmem", "./...")
	})
}

// BenchCompare reproduces the CI benchmark regression gate locally. It benches
// the current working tree against a base ref on this one machine, then
// benchstat-diffs them — the same same-runner comparison the CI gate performs.
//
// Usage:
//
//	mage benchCompare        # compares the working tree against origin/main
//	mage benchCompare v0.1.0 # compares against an explicit ref (tag/branch/SHA)
//
// The base ref is checked out into a throwaway git worktree so the current
// working tree is never disturbed. It prints the benchstat table and a verdict,
// and exits non-zero if any gated metric (sec/op, allocs/op) regresses past
// benchCompareThreshold. New or removed benchmarks are skipped, never failed.
func BenchCompare(baseRef string) error {
	if baseRef == "" {
		baseRef = "origin/main"
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}

	tmp, err := os.MkdirTemp("", "crucible-benchcompare-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	headTxt := filepath.Join(tmp, "head.txt")
	baseTxt := filepath.Join(tmp, "base.txt")

	// Bench the working tree (HEAD).
	fmt.Printf("==> benchCompare: benchmarking working tree (state)\n")
	headOut, err := sh.Output("go", "test", "-C", filepath.Join(root, "state"),
		"-run=^$", "-bench=.", "-benchmem", "-count="+benchCompareCount, "./...")
	if err != nil {
		return fmt.Errorf("benchmarking head: %w", err)
	}
	if err := os.WriteFile(headTxt, []byte(headOut), 0o644); err != nil {
		return err
	}

	// Check the base ref out into a throwaway worktree and bench it there.
	baseWT := filepath.Join(tmp, "base")
	fmt.Printf("==> benchCompare: benchmarking base ref %q (state)\n", baseRef)
	if err := sh.RunV("git", "-C", root, "worktree", "add", "--detach", baseWT, baseRef); err != nil {
		return fmt.Errorf("creating base worktree at %q: %w", baseRef, err)
	}
	defer func() { _ = sh.RunV("git", "-C", root, "worktree", "remove", "--force", baseWT) }()

	baseOut, err := sh.Output("go", "test", "-C", filepath.Join(baseWT, "state"),
		"-run=^$", "-bench=.", "-benchmem", "-count="+benchCompareCount, "./...")
	if err != nil {
		return fmt.Errorf("benchmarking base ref %q: %w", baseRef, err)
	}
	if err := os.WriteFile(baseTxt, []byte(baseOut), 0o644); err != nil {
		return err
	}

	// Render the human-readable comparison.
	fmt.Printf("\n==> benchCompare: base %q -> working tree\n", baseRef)
	if err := sh.RunV("go", "run", benchstat, baseTxt, headTxt); err != nil {
		return err
	}

	// Apply the same gate CI uses, off the CSV form.
	csv, err := sh.Output("go", "run", benchstat, "-format", "csv", baseTxt, headTxt)
	if err != nil {
		return err
	}
	return benchGate(csv, benchCompareThreshold)
}

// benchGate parses benchstat CSV output and returns an error if any gated metric
// (sec/op, allocs/op) regresses past threshold (a head/base ratio). It mirrors
// .github/scripts/bench-gate.awk so local and CI verdicts agree. New or removed
// benchmarks (a missing base or head value) are skipped, never failed.
func benchGate(csv, threshold string) error {
	thr, err := strconv.ParseFloat(threshold, 64)
	if err != nil {
		return fmt.Errorf("invalid threshold %q: %w", threshold, err)
	}
	var metric string
	var regressed bool
	for _, line := range strings.Split(csv, "\n") {
		cols := strings.Split(line, ",")
		if len(cols) < 2 {
			continue
		}
		switch cols[1] {
		case "sec/op", "allocs/op":
			metric = cols[1]
			continue
		case "B/op": // tracked by benchstat but not gated
			metric = ""
			continue
		}
		if metric == "" || len(cols) < 4 {
			continue
		}
		name := cols[0]
		if name == "" || name == "geomean" {
			continue
		}
		baseStr, headStr := cols[1], cols[3]
		if baseStr == "" || headStr == "" {
			continue // new or removed benchmark
		}
		base, err1 := strconv.ParseFloat(baseStr, 64)
		head, err2 := strconv.ParseFloat(headStr, 64)
		if err1 != nil || err2 != nil || base <= 0 {
			continue
		}
		ratio := head / base
		if ratio > thr {
			fmt.Printf("REGRESSION  %-28s %-10s %+7.1f%%  (ratio %.3f > %.2f)\n",
				name, metric, (ratio-1)*100, ratio, thr)
			regressed = true
		}
	}
	if regressed {
		return fmt.Errorf("benchmark gate failed: a metric regressed past ratio %.2f", thr)
	}
	fmt.Printf("benchmark gate passed: no metric regressed past ratio %.2f\n", thr)
	return nil
}

// Fuzz runs a short fuzzing pass for every module (CI uses a longer fuzztime).
func Fuzz() error {
	return forEachModule("fuzz", func(string) error {
		return goCmd("test", "-run", "^$", "-fuzz", ".", "-fuzztime", "30s", "./...")
	})
}

// Tidy runs `go mod tidy` for every module (and the magefiles module itself).
func Tidy() error {
	if err := forEachModule("tidy", func(string) error {
		return goCmd("mod", "tidy")
	}); err != nil {
		return err
	}
	fmt.Println("==> tidy: magefiles")
	return goCmd("mod", "tidy")
}

// Vuln runs govulncheck over every module.
func Vuln() error {
	return forEachModule("govulncheck", func(string) error {
		return sh.RunV("go", "run", govulncheck, "./...")
	})
}

// Check is the thorough local pre-flight: lint, race tests, and vulnerability
// scan. Run this before opening a PR.
func Check() {
	mg.SerialDeps(Lint, TestRace, Vuln)
}

// Changelog prints the Unreleased section of a module's CHANGELOG.md — the
// pending entries that the next tag will publish. Pass a module dir (e.g.
// `mage changelog state`); defaults to the primary module (state).
func Changelog(module string) error {
	if module == "" {
		module = "state"
	}
	root, err := repoRoot()
	if err != nil {
		return err
	}
	path := filepath.Join(root, module, "CHANGELOG.md")
	data, err := os.ReadFile(path) //nolint:gosec // module is a fixed in-repo dir name
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	section := unreleasedSection(string(data))
	if section == "" {
		fmt.Printf("==> %s: no Unreleased section in CHANGELOG.md\n", module)
		return nil
	}
	fmt.Printf("==> %s pending release notes:\n\n%s\n", module, section)
	return nil
}

// unreleasedSection extracts the body of the "## [Unreleased]" heading, up to
// the next "## " heading. It returns "" if there is no such section.
func unreleasedSection(changelog string) string {
	const marker = "## [Unreleased]"
	idx := strings.Index(changelog, marker)
	if idx < 0 {
		return ""
	}
	rest := changelog[idx+len(marker):]
	if next := strings.Index(rest, "\n## "); next >= 0 {
		rest = rest[:next]
	}
	return strings.TrimSpace(rest)
}
