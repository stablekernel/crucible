package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSimulate_TextSeededGuards(t *testing.T) {
	code, out, errOut := runCmd(
		"simulate", "testdata/clean.json",
		"-events", "checkout,paid",
		"-guard", "hasItems=true",
		"-guard", "isPaid=true",
	)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	for _, want := range []string{"cart -> paying", "paying -> done", "final: done"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestSimulate_GuardBlocks(t *testing.T) {
	code, out, errOut := runCmd("simulate", "testdata/clean.json", "-events", "checkout")
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(out, "GuardFailed") {
		t.Errorf("output missing GuardFailed outcome:\n%s", out)
	}
	if !strings.Contains(out, "cart -> cart") {
		t.Errorf("guard block should keep state at cart:\n%s", out)
	}
	if !strings.Contains(out, "final: cart") {
		t.Errorf("final state should be cart:\n%s", out)
	}
}

func TestSimulate_FormatJSON(t *testing.T) {
	code, out, errOut := runCmd(
		"simulate", "testdata/clean.json",
		"-events", "checkout,paid",
		"-guard", "hasItems=true",
		"-guard", "isPaid=true",
		"-format", "json",
	)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	var dto simulateResultDTO
	if err := json.Unmarshal([]byte(out), &dto); err != nil {
		t.Fatalf("unmarshal JSON output: %v\n%s", err, out)
	}
	if dto.FinalState != "done" {
		t.Errorf("finalState = %q, want done", dto.FinalState)
	}
	if len(dto.Steps) != 2 {
		t.Fatalf("len(steps) = %d, want 2:\n%s", len(dto.Steps), out)
	}
	for i, step := range dto.Steps {
		if step.Outcome != "Success" {
			t.Errorf("step %d outcome = %q, want Success", i, step.Outcome)
		}
	}
}

func TestSimulate_EventsFile(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		data string
	}{
		{"scenario object", `{"events":[{"event":"checkout"},{"event":"paid"}]}`},
		{"bare array", `["checkout","paid"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(tc.name, " ", "_")+".json")
			if err := os.WriteFile(path, []byte(tc.data), 0o644); err != nil {
				t.Fatalf("write events file: %v", err)
			}
			code, out, errOut := runCmd(
				"simulate", "testdata/clean.json",
				"-events-file", path,
				"-guard", "hasItems=true",
				"-guard", "isPaid=true",
			)
			if code != exitOK {
				t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
			}
			if !strings.Contains(out, "final: done") {
				t.Errorf("final state should be done:\n%s", out)
			}
		})
	}
}

func TestSimulate_Validation(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
	}{
		{"neither events nor events-file", []string{"simulate", "testdata/clean.json"}},
		{"both events and events-file", []string{
			"simulate", "testdata/clean.json",
			"-events", "checkout", "-events-file", filepath.Join(dir, "x.json"),
		}},
		{"bad guard token", []string{
			"simulate", "testdata/clean.json",
			"-events", "checkout", "-guard", "hasItems",
		}},
		{"bad format value", []string{
			"simulate", "testdata/clean.json",
			"-events", "checkout", "-format", "yaml",
		}},
		{"empty events string", []string{
			"simulate", "testdata/clean.json", "-events", "",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _ := runCmd(tc.args...)
			if code != exitUsage {
				t.Fatalf("exit = %d, want %d", code, exitUsage)
			}
		})
	}
}

func TestSimulate_UnknownEvent(t *testing.T) {
	code, _, errOut := runCmd("simulate", "testdata/clean.json", "-events", "nope")
	if code != exitError {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitError, errOut)
	}
}

func TestSimulate_InitialOverride(t *testing.T) {
	code, out, errOut := runCmd(
		"simulate", "testdata/clean.json",
		"-initial", "paying",
		"-events", "paid",
		"-guard", "isPaid=true",
	)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(out, "initial: paying") {
		t.Errorf("output should start from paying:\n%s", out)
	}
	if !strings.Contains(out, "final: done") {
		t.Errorf("final state should be done:\n%s", out)
	}
}

func TestSimulate_FlagsAfterPath(t *testing.T) {
	code, out, errOut := runCmd(
		"simulate", "testdata/clean.json",
		"-events", "checkout",
		"-guard", "hasItems=true",
	)
	if code != exitOK {
		t.Fatalf("exit = %d, want %d (stderr: %s)", code, exitOK, errOut)
	}
	if !strings.Contains(out, "cart -> paying") {
		t.Errorf("output missing transition:\n%s", out)
	}
}
