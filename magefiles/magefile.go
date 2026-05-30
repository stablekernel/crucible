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
	"strings"

	"github.com/magefile/mage/mg"
	"github.com/magefile/mage/sh"
)

// modules is the list of Go modules in the suite, by directory. As new modules
// land (broker, store, sink), add them here and every target picks them
// up automatically.
var modules = []string{"state"}

// Pinned tool versions — keep in sync with .github/workflows/ci.yml.
const (
	golangciLint = "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2"
	govulncheck  = "golang.org/x/vuln/cmd/govulncheck@v1.3.0"
)

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
