// SPDX-License-Identifier: Apache-2.0

//go:build integration

package file_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	filesink "github.com/stablekernel/crucible/sink/file"
)

// eventIT is the payload the integration test sinks through the outlet.
type eventIT struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
}

// TestIntegrationSinkAppendsJSONLToRealFile drives the real Outlet path against
// a file in a temp directory opened by the destination, then reads the file
// back to prove each Sink produced one JSONL line.
func TestIntegrationSinkAppendsJSONLToRealFile(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "events.jsonl")
	outlet, err := filesink.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	ctx := context.Background()
	want := []eventIT{
		{ID: "A-1", Kind: "placed"},
		{ID: "A-2", Kind: "shipped"},
	}
	for _, e := range want {
		if err = outlet.Sink(ctx, e); err != nil {
			t.Fatalf("Sink(%v) error = %v", e, err)
		}
	}

	if f, ok := outlet.(csink.Flusher); ok {
		if err = f.Flush(ctx); err != nil {
			t.Fatalf("Flush() error = %v", err)
		}
	}
	if s, ok := outlet.(csink.Shutdowner); ok {
		if err = s.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown() error = %v", err)
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != len(want) {
		t.Fatalf("file has %d lines, want %d (%q)", len(lines), len(want), data)
	}
	if lines[0] != `{"id":"A-1","kind":"placed"}` {
		t.Errorf("line 0 = %q, want placed record", lines[0])
	}
	if lines[1] != `{"id":"A-2","kind":"shipped"}` {
		t.Errorf("line 1 = %q, want shipped record", lines[1])
	}
}
