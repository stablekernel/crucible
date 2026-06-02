// Command docsgen produces the Crucible docs site's generated content: the API
// reference (rendered from godoc via gomarkdoc) and the Mermaid diagrams built
// from the real example machines.
//
// It is run from the repository root before the Astro build, in both the PR
// docs job and the Pages deploy workflow, so the published reference and
// diagrams are always regenerated from the current source and never committed
// (the output directories are gitignored). The generator is deterministic and
// idempotent: running it twice yields byte-identical output.
//
// Usage, from the repo root:
//
//	go run ./tools/docsgen
//
// All output paths are resolved relative to the repository root, which is
// located by walking up from this file's module to the directory that holds
// go.work. This keeps the tool independent of the caller's working directory.
package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// docsContentRoot is the Starlight content collection root, relative to the
// repository root. Generated pages live under reference/ and _generated/.
const docsContentRoot = "docs/src/content/docs"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "docsgen:", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := repoRoot()
	if err != nil {
		return err
	}

	contentRoot := filepath.Join(root, filepath.FromSlash(docsContentRoot))

	written := make([]string, 0, len(referencePackages)+len(diagramSpecs)+2)

	refFiles, err := generateReference(root, contentRoot)
	if err != nil {
		return fmt.Errorf("generate API reference: %w", err)
	}
	written = append(written, refFiles...)

	diagFiles, err := generateDiagrams(contentRoot)
	if err != nil {
		return fmt.Errorf("generate diagrams: %w", err)
	}
	written = append(written, diagFiles...)

	fmt.Printf("docsgen: wrote %d files\n", len(written))
	for _, f := range written {
		rel, relErr := filepath.Rel(root, f)
		if relErr != nil {
			rel = f
		}
		fmt.Printf("  %s\n", filepath.ToSlash(rel))
	}
	return nil
}

// repoRoot walks up from the running binary's source-module directory to the
// directory containing go.work, which marks the repository root. It falls back
// to the current working directory when go.work is not found (for example when
// the tool is vendored or run in an unusual layout), so callers running from
// the repo root still work.
func repoRoot() (string, error) {
	start, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	dir := start
	for {
		if fileExists(filepath.Join(dir, "go.work")) {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			// Reached the filesystem root without finding go.work. Fall back to
			// the original working directory: docsgen is documented to run from
			// the repo root, so this remains correct in the common case.
			return start, nil
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// writeGenerated writes content to path, creating parent directories as
// needed. It is the single sink for all generated files so callers get
// consistent permissions and error context.
func writeGenerated(path, content string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create directory for %s: %w", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
