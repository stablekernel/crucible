package main

import (
	"encoding/json"
	"fmt"

	"github.com/stablekernel/crucible/state/analysis"
)

// sarifRoot is the top-level SARIF 2.1.0 log object.
type sarifRoot struct {
	Version string     `json:"version"`
	Schema  string     `json:"$schema"`
	Runs    []sarifRun `json:"runs"`
}

// sarifRun is a single analysis run.
type sarifRun struct {
	Tool    sarifTool     `json:"tool"`
	Results []sarifResult `json:"results"`
}

// sarifTool names the analysis tool that produced the run.
type sarifTool struct {
	Driver sarifDriver `json:"driver"`
}

// sarifDriver identifies the tool's driver component.
type sarifDriver struct {
	Name           string `json:"name"`
	InformationURI string `json:"informationUri"`
	Version        string `json:"version"`
}

// sarifResult is a single finding.
type sarifResult struct {
	RuleID    string          `json:"ruleId"`
	Level     string          `json:"level"`
	Message   sarifMessage    `json:"message"`
	Locations []sarifLocation `json:"locations"`
}

// sarifMessage carries a finding's human-readable text.
type sarifMessage struct {
	Text string `json:"text"`
}

// sarifLocation locates a finding logically (state/transition names) and,
// when the IR came from a file, physically.
type sarifLocation struct {
	LogicalLocations []sarifLogicalLocation `json:"logicalLocations"`
	PhysicalLocation *sarifPhysicalLocation `json:"physicalLocation,omitempty"`
}

// sarifLogicalLocation names a logical program element (a state or transition).
type sarifLogicalLocation struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// sarifPhysicalLocation points at the IR artifact on disk.
type sarifPhysicalLocation struct {
	ArtifactLocation sarifArtifactLocation `json:"artifactLocation"`
}

// sarifArtifactLocation is the artifact URI.
type sarifArtifactLocation struct {
	URI string `json:"uri"`
}

// lintToSARIF converts an analysis.Report to a SARIF 2.1.0 JSON byte slice.
// irPath records the source artifact (omitted when "-" for stdin); version
// stamps the tool driver.
func lintToSARIF(r analysis.Report, irPath, version string) ([]byte, error) {
	results := make([]sarifResult, 0, len(r.Findings))
	for _, f := range r.Findings {
		level := "warning"
		if f.Severity == analysis.SeverityError {
			level = "error"
		}

		locs := []sarifLogicalLocation{{Name: f.State, Kind: "state"}}
		if f.Transition != "" {
			locs = append(locs, sarifLogicalLocation{Name: f.Transition, Kind: "transition"})
		}

		loc := sarifLocation{LogicalLocations: locs}
		if irPath != "-" {
			loc.PhysicalLocation = &sarifPhysicalLocation{
				ArtifactLocation: sarifArtifactLocation{URI: irPath},
			}
		}

		results = append(results, sarifResult{
			RuleID:    string(f.Kind),
			Level:     level,
			Message:   sarifMessage{Text: f.Message},
			Locations: []sarifLocation{loc},
		})
	}

	root := sarifRoot{
		Version: "2.1.0",
		Schema:  "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
		Runs: []sarifRun{{
			Tool: sarifTool{
				Driver: sarifDriver{
					Name:           "crucible",
					InformationURI: "https://github.com/stablekernel/crucible",
					Version:        version,
				},
			},
			Results: results,
		}},
	}

	b, err := json.MarshalIndent(root, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal sarif: %w", err)
	}
	return b, nil
}
