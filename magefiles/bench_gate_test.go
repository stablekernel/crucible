//go:build mage

// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

// TestBenchGate covers the benchstat-CSV parser that decides whether a benchmark
// regressed past the allowed head/base ratio. The CSV groups data rows under a
// per-metric units row (sec/op, allocs/op, or the non-gated B/op); each data row
// is "<name>,<base>,<baseCI>,<head>,<headCI>,<vs>,<P>", so column 2 is the base
// and column 4 the head value. The cases exercise the gated-regression,
// within-threshold, non-gated-metric, new/removed-benchmark, and malformed-row
// branches against the fixed default threshold.
func TestBenchGate(t *testing.T) {
	t.Parallel()

	const threshold = "1.20"

	tests := []struct {
		name    string
		csv     string
		wantErr bool
	}{
		{
			name:    "sec/op within threshold passes",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\nBenchFire,100,±1%,110,±1%,+10%,0.01\n",
			wantErr: false,
		},
		{
			name:    "sec/op past threshold regresses",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\nBenchFire,100,±1%,130,±1%,+30%,0.01\n",
			wantErr: true,
		},
		{
			name:    "allocs/op within threshold passes",
			csv:     ",allocs/op,CI,allocs/op,CI,vs base,P\nBenchFire,10,±0%,11,±0%,+10%,0.01\n",
			wantErr: false,
		},
		{
			name:    "B/op is reported but never gated",
			csv:     ",B/op,CI,B/op,CI,vs base,P\nBenchFire,100,±1%,1000,±1%,+900%,0.01\n",
			wantErr: false,
		},
		{
			name:    "new benchmark (empty base) is skipped",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\nBenchNew,,,130,±1%,~,0.01\n",
			wantErr: false,
		},
		{
			name:    "removed benchmark (empty head) is skipped",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\nBenchGone,130,±1%,,,~,0.01\n",
			wantErr: false,
		},
		{
			name:    "geomean summary row is skipped",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\ngeomean,100,±1%,200,±1%,+100%,0.01\n",
			wantErr: false,
		},
		{
			name:    "zero base is skipped (no ratio)",
			csv:     ",sec/op,CI,sec/op,CI,vs base,P\nBenchZero,0,±1%,130,±1%,~,0.01\n",
			wantErr: false,
		},
		{
			name:    "data row before any units row is ignored",
			csv:     "BenchOrphan,100,±1%,130,±1%,+30%,0.01\n",
			wantErr: false,
		},
		{
			name:    "blank and preamble lines are ignored",
			csv:     "goos: darwin\n\n,sec/op,CI,sec/op,CI,vs base,P\nBenchFire,100,±1%,110,±1%,+10%,0.01\n",
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := benchGate(tc.csv, threshold)
			if (err != nil) != tc.wantErr {
				t.Fatalf("benchGate() error = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

// TestBenchGate_AllocsRegression isolates the allocs/op regression assertion so
// the gated path for the second metric is covered explicitly: 6/4 = 1.5 exceeds
// the 1.20 threshold, so the gate must fail.
func TestBenchGate_AllocsRegression(t *testing.T) {
	t.Parallel()
	csv := ",allocs/op,CI,allocs/op,CI,vs base,P\nBenchAlloc,4,±0%,6,±0%,+50%,0.01\n"
	if err := benchGate(csv, "1.20"); err == nil {
		t.Fatal("benchGate(allocs/op 4->6) = nil, want a regression error")
	}
}

// TestBenchGate_InvalidThreshold covers the threshold-parse error branch: a
// non-numeric threshold cannot be a ratio, so benchGate reports it rather than
// silently passing.
func TestBenchGate_InvalidThreshold(t *testing.T) {
	t.Parallel()
	err := benchGate("", "not-a-number")
	if err == nil {
		t.Fatal("benchGate with a non-numeric threshold = nil, want an error")
	}
	if !strings.Contains(err.Error(), "invalid threshold") {
		t.Fatalf("error = %v, want an invalid-threshold message", err)
	}
}
