package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

// gomarkdocVersion pins the gomarkdoc CLI. It is invoked via `go run` so the
// tool is fetched on demand and reproducible across machines without adding it
// to docsgen's own build graph. v1.1.0 is the current stable release.
const gomarkdocVersion = "v1.1.0"

// referencePackage describes one godoc package to render into the Reference
// section. mod is the Go module directory (relative to the repo root) the
// gomarkdoc CLI runs in; pkg is the package pattern within that module ("." for
// the module root). slug is the output filename stem and title is the page
// heading; order positions the page within the Reference sidebar group.
type referencePackage struct {
	mod   string
	pkg   string
	slug  string
	title string
	desc  string
	order int
}

// referencePackages enumerates the public API surface documented in the
// Reference section, in sidebar order. state and state/expr are separate Go
// modules; the rest are packages within the state module.
var referencePackages = []referencePackage{
	{
		mod: "state", pkg: ".", slug: "state", title: "state", order: 1,
		desc: "The Crucible state-machine kernel: forging, firing, serialization, and visualization.",
	},
	{
		mod: "state", pkg: "./analysis", slug: "state-analysis", title: "state/analysis", order: 2,
		desc: "Static model-checking over a machine's serializable IR.",
	},
	{
		mod: "state", pkg: "./evolution", slug: "state-evolution", title: "state/evolution", order: 3,
		desc: "Schema-evolution checks between two versions of a machine definition.",
	},
	{
		mod: "state", pkg: "./conformance", slug: "state-conformance", title: "state/conformance", order: 4,
		desc: "Conformance assertions and round-trip oracles for machine behavior.",
	},
	{
		mod: "state", pkg: "./verify", slug: "state-verify", title: "state/verify", order: 5,
		desc: "Bounded model-checking: reachability, invariants, liveness, and test generation.",
	},
	{
		mod: "state", pkg: "./verify/symbolic", slug: "state-verify-symbolic", title: "state/verify/symbolic", order: 6,
		desc: "Symbolic execution support for the verify tier.",
	},
	{
		mod: "state/expr", pkg: ".", slug: "state-expr", title: "state/expr", order: 7,
		desc: "The rich-expression tier: CEL-backed guards, assigns, and time predicates.",
	},
}

// generateReference renders each documented package to a Starlight Markdown
// page under <contentRoot>/reference/ and writes the section index. It returns
// the absolute paths of every file written, in deterministic order.
func generateReference(repoRoot, contentRoot string) ([]string, error) {
	outDir := filepath.Join(contentRoot, "reference")

	// Start from a clean reference/ directory so a renamed or removed package
	// never leaves a stale page behind; this keeps the run idempotent against
	// the previous run's output as well as against a stray committed copy.
	if err := os.RemoveAll(outDir); err != nil {
		return nil, fmt.Errorf("clear reference dir: %w", err)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return nil, fmt.Errorf("create reference dir: %w", err)
	}

	written := make([]string, 0, len(referencePackages)+1)

	indexPath := filepath.Join(outDir, "index.md")
	if err := writeGenerated(indexPath, referenceIndex()); err != nil {
		return nil, err
	}
	written = append(written, indexPath)

	for _, rp := range referencePackages {
		raw, err := renderGomarkdoc(repoRoot, rp)
		if err != nil {
			return nil, err
		}
		page := wrapReference(rp, normalizeGodoc(raw, rp.title))
		outPath := filepath.Join(outDir, rp.slug+".md")
		if err := writeGenerated(outPath, page); err != nil {
			return nil, err
		}
		written = append(written, outPath)
	}
	return written, nil
}

// renderGomarkdoc invokes the pinned gomarkdoc CLI for one package and returns
// its Markdown on stdout. It runs `go run` inside the package's module
// directory so module resolution uses that module's go.mod.
func renderGomarkdoc(repoRoot string, rp referencePackage) (string, error) {
	modDir := filepath.Join(repoRoot, filepath.FromSlash(rp.mod))

	cmd := exec.Command(
		"go", "run",
		"github.com/princjef/gomarkdoc/cmd/gomarkdoc@"+gomarkdocVersion,
		"--format", "github",
		rp.pkg,
	)
	cmd.Dir = modDir
	// GOFLAGS must not leak into gomarkdoc's own flag parser (it rejects flags
	// like -mod), and GOWORK=off keeps `go run` resolving the tool against the
	// package module rather than the workspace, which makes the run reproducible
	// regardless of the caller's workspace state.
	cmd.Env = append(os.Environ(), "GOFLAGS=", "GOWORK=off")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("gomarkdoc %s/%s: %w\n%s", rp.mod, rp.pkg, err, stderr.String())
	}
	return stdout.String(), nil
}

var (
	// godocDoNotEdit matches the leading "Code generated" banner gomarkdoc
	// emits; Starlight does not need it and it adds noise above the frontmatter.
	godocDoNotEdit = regexp.MustCompile(`(?m)^<!-- Code generated by gomarkdoc\. DO NOT EDIT -->\n`)
	// godocLeadingH1 matches the package-name H1 gomarkdoc puts first. Starlight
	// renders the frontmatter title as the page H1, so the duplicate is removed.
	godocLeadingH1 = regexp.MustCompile(`(?m)\A#\s+\S.*\n`)
	// godocSourceLink strips gomarkdoc's optional "view source" links, which it
	// derives from git remote/commit state. Removing them guarantees the output
	// is identical regardless of the checkout's git status (detached worktree,
	// shallow clone, etc.), keeping the generator deterministic.
	godocSourceLink = regexp.MustCompile(`\s*\[\]\(<https://github\.com/[^>]+>\)`)
)

// normalizeGodoc post-processes one package's gomarkdoc output so it renders
// cleanly inside Starlight: it drops the DO NOT EDIT banner, removes the
// duplicate package-name H1 (the frontmatter title becomes the page heading),
// strips git-derived source links for determinism, and trims surrounding blank
// lines. title is accepted for symmetry and future heading rewrites.
func normalizeGodoc(raw, _ string) string {
	out := godocDoNotEdit.ReplaceAllString(raw, "")
	out = strings.TrimLeft(out, "\n")
	out = godocLeadingH1.ReplaceAllString(out, "")
	out = godocSourceLink.ReplaceAllString(out, "")
	return strings.TrimSpace(out) + "\n"
}

// wrapReference prepends Starlight frontmatter to a normalized package page.
func wrapReference(rp referencePackage, body string) string {
	var b strings.Builder
	b.WriteString("---\n")
	fmt.Fprintf(&b, "title: %s\n", yamlString(rp.title))
	fmt.Fprintf(&b, "description: %s\n", yamlString(rp.desc))
	b.WriteString("sidebar:\n")
	fmt.Fprintf(&b, "  order: %d\n", rp.order)
	b.WriteString("---\n\n")
	b.WriteString(generatedNotice)
	b.WriteString("\n")
	b.WriteString(body)
	return b.String()
}

// generatedNotice is the short banner placed at the top of every generated
// reference page, reminding readers the page is produced from source.
const generatedNotice = ":::note\nThis page is generated from the package's Go source documentation. " +
	"Edit the godoc comments in the source, not this file.\n:::\n"

// referenceIndex builds the Reference section overview, linking each generated
// package page in sidebar order.
func referenceIndex() string {
	var b strings.Builder
	b.WriteString("---\n")
	b.WriteString("title: Reference\n")
	fmt.Fprintf(&b, "description: %s\n", yamlString("API reference for the Crucible modules, generated from source."))
	b.WriteString("sidebar:\n")
	b.WriteString("  order: 0\n")
	b.WriteString("---\n\n")
	b.WriteString("The API reference below is generated from the packages' Go source ")
	b.WriteString("documentation (godoc) at build time and is never committed, so it ")
	b.WriteString("always matches the released source.\n\n")
	b.WriteString("## Packages\n\n")
	for _, rp := range referencePackages {
		fmt.Fprintf(&b, "- [`%s`](/crucible/reference/%s/) — %s\n", rp.title, rp.slug, rp.desc)
	}
	return b.String()
}

// yamlString renders s as a YAML double-quoted scalar so frontmatter values
// containing colons, leading symbols, or other indicator characters parse as a
// single string rather than as a mapping. Only the two escapes that matter for
// double-quoted YAML scalars — backslash and double-quote — are handled, which
// covers all docsgen-controlled descriptions.
func yamlString(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	return `"` + escaped + `"`
}
