package main

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/stablekernel/crucible/state/analysis"
	"github.com/stablekernel/crucible/state/evolution"
)

// lintFindingJSON is the JSON DTO for a single lint finding.
type lintFindingJSON struct {
	Kind       string `json:"kind"`
	Severity   string `json:"severity"`
	State      string `json:"state"`
	Transition string `json:"transition"`
	Message    string `json:"message"`
}

// lintReportJSON is the JSON DTO for a lint report.
type lintReportJSON struct {
	Findings []lintFindingJSON `json:"findings"`
}

// diffChangeJSON is the JSON DTO for a single diff change.
type diffChangeJSON struct {
	Kind        string `json:"kind"`
	Path        string `json:"path"`
	Description string `json:"description"`
	Breaking    bool   `json:"breaking"`
}

// diffReportJSON is the JSON DTO for a diff report.
type diffReportJSON struct {
	Bump     string           `json:"bump"`
	Breaking int              `json:"breaking"`
	Changes  []diffChangeJSON `json:"changes"`
}

// toLintReportJSON maps an analysis.Report to a lintReportJSON DTO.
func toLintReportJSON(r analysis.Report) lintReportJSON {
	findings := make([]lintFindingJSON, 0, len(r.Findings))
	for _, f := range r.Findings {
		findings = append(findings, lintFindingJSON{
			Kind:       string(f.Kind),
			Severity:   string(f.Severity),
			State:      f.State,
			Transition: f.Transition,
			Message:    f.Message,
		})
	}
	return lintReportJSON{Findings: findings}
}

// toDiffReportJSON maps an evolution.Report to a diffReportJSON DTO.
func toDiffReportJSON(r evolution.Report) diffReportJSON {
	changes := make([]diffChangeJSON, 0, len(r.Changes))
	breaking := 0
	for _, c := range r.Changes {
		if c.Breaking {
			breaking++
		}
		changes = append(changes, diffChangeJSON{
			Kind:        string(c.Kind),
			Path:        c.Path,
			Description: c.Description,
			Breaking:    c.Breaking,
		})
	}
	return diffReportJSON{
		Bump:     string(r.SemverBump()),
		Breaking: breaking,
		Changes:  changes,
	}
}

// formatLint writes the lint report in the requested format to w. irPath is the
// IR's source path, recorded as a SARIF physical location ("-" for stdin is
// omitted). version stamps the SARIF tool driver.
func formatLint(r analysis.Report, format, irPath, version string, w io.Writer) error {
	switch format {
	case "json":
		b, err := json.MarshalIndent(toLintReportJSON(r), "", "  ")
		if err != nil {
			return fmt.Errorf("marshal lint report: %w", err)
		}
		emitln(w, string(b))
		return nil
	case "sarif":
		b, err := lintToSARIF(r, irPath, version)
		if err != nil {
			return fmt.Errorf("build sarif output: %w", err)
		}
		emitln(w, string(b))
		return nil
	default:
		return fmt.Errorf("unknown format %q", format)
	}
}

// formatDiff writes the diff report in the requested format to w.
func formatDiff(r evolution.Report, format string, w io.Writer) error {
	switch format {
	case "json":
		b, err := json.MarshalIndent(toDiffReportJSON(r), "", "  ")
		if err != nil {
			return fmt.Errorf("marshal diff report: %w", err)
		}
		emitln(w, string(b))
		return nil
	default:
		return fmt.Errorf("unknown format %q", format)
	}
}
