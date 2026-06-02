// SPDX-License-Identifier: Apache-2.0

package file_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	filesink "github.com/stablekernel/crucible/sink/file"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// marshalable is a simple payload type that encodes cleanly to JSON.
type marshalable struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

// unmarshalable cannot be encoded by encoding/json (contains a channel).
type unmarshalable struct {
	Ch chan int
}

func TestNew_WritesJSONLLine(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	if err := outlet.Sink(context.Background(), marshalable{ID: "abc", Value: 42}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	got := buf.String()
	if !strings.HasSuffix(got, "\n") {
		t.Fatalf("output does not end with newline: %q", got)
	}
	want := `{"id":"abc","value":42}` + "\n"
	if got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestNew_WritesMultipleLinesInOrder(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	ctx := context.Background()
	payloads := []marshalable{
		{ID: "first", Value: 1},
		{ID: "second", Value: 2},
		{ID: "third", Value: 3},
	}
	for _, p := range payloads {
		if err := outlet.Sink(ctx, p); err != nil {
			t.Fatalf("Sink(%+v) error = %v", p, err)
		}
	}

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("got %d lines, want 3: %q", len(lines), buf.String())
	}
	wantLines := []string{
		`{"id":"first","value":1}`,
		`{"id":"second","value":2}`,
		`{"id":"third","value":3}`,
	}
	for i, want := range wantLines {
		if lines[i] != want {
			t.Errorf("line[%d] = %q, want %q", i, lines[i], want)
		}
	}
}

func TestNew_ConcurrentSink_RaceClean(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	const goroutines = 64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(n int) {
			defer wg.Done()
			_ = outlet.Sink(context.Background(), marshalable{ID: "x", Value: n})
		}(i)
	}
	wg.Wait()

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != goroutines {
		t.Errorf("got %d lines, want %d", len(lines), goroutines)
	}
}

func TestNew_MarshalError_ReturnsWrappedSinkError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	err := outlet.Sink(context.Background(), unmarshalable{Ch: make(chan int)})
	if err == nil {
		t.Fatal("Sink(unmarshalable) returned nil, want error")
	}

	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("error is %T, want *csink.Error", err)
	}
	if se.Phase != csink.PhaseApply {
		t.Errorf("Phase = %q, want %q", se.Phase, csink.PhaseApply)
	}
	if se.Outlet != "file" {
		t.Errorf("Outlet = %q, want %q", se.Outlet, "file")
	}
}

func TestNew_Flush_NoopOnBuffer(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	f, ok := outlet.(csink.Flusher)
	if !ok {
		t.Fatal("outlet does not implement csink.Flusher")
	}

	// bytes.Buffer has no Sync; Flush must be a no-op returning nil.
	if err := f.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v, want nil", err)
	}
	// Idempotent.
	if err := f.Flush(context.Background()); err != nil {
		t.Fatalf("second Flush() error = %v, want nil", err)
	}
}

func TestNew_Shutdown_NoopForNonOwned(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	s, ok := outlet.(csink.Shutdowner)
	if !ok {
		t.Fatal("outlet does not implement csink.Shutdowner")
	}

	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v, want nil", err)
	}
	// Idempotent.
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v, want nil", err)
	}
}

func TestOpen_WritesToFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "out.jsonl")

	outlet, err := filesink.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	if err = outlet.Sink(context.Background(), marshalable{ID: "file-test", Value: 7}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	s, ok := outlet.(csink.Shutdowner)
	if !ok {
		t.Fatal("outlet does not implement csink.Shutdowner")
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	want := `{"id":"file-test","value":7}` + "\n"
	if string(data) != want {
		t.Errorf("file contents = %q, want %q", string(data), want)
	}
}

func TestOpen_ShutdownIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "idem.jsonl")

	outlet, err := filesink.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	s := outlet.(csink.Shutdowner)
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown() error = %v", err)
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown() error = %v, want nil (idempotent)", err)
	}
}

func TestOpen_FlushSyncsFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "flush.jsonl")

	outlet, err := filesink.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer func() {
		_ = outlet.(csink.Shutdowner).Shutdown(context.Background())
	}()

	if err = outlet.Sink(context.Background(), marshalable{ID: "flush-me", Value: 0}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	f, ok := outlet.(csink.Flusher)
	if !ok {
		t.Fatal("outlet does not implement csink.Flusher")
	}
	if err = f.Flush(context.Background()); err != nil {
		t.Fatalf("Flush() error = %v, want nil", err)
	}
}

func TestOpen_BadPath_ReturnsError(t *testing.T) {
	t.Parallel()

	// A path that cannot be created (directory component does not exist).
	_, err := filesink.Open("/nonexistent-dir-crucible-file/out.jsonl")
	if err == nil {
		t.Fatal("Open() returned nil, want error for bad path")
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet {
		return filesink.New(&bytes.Buffer{})
	})
}

// TestConformanceFile runs conformance against an Open-based outlet that owns a
// real file, exercising the Flusher (Sync) and Shutdowner (Close) paths.
func TestConformanceFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// The harness calls the factory several times, so collect every opened outlet
	// and close them all when the test ends. Registered after t.TempDir, this
	// cleanup runs first (cleanups are LIFO) and releases the file handles before
	// TempDir's RemoveAll — Windows cannot delete a file that is still open.
	var opened []csink.Outlet
	t.Cleanup(func() {
		for _, o := range opened {
			if s, ok := o.(csink.Shutdowner); ok {
				_ = s.Shutdown(context.Background())
			}
		}
	})

	sinktest.OutletConformance(t, func() csink.Outlet {
		path := filepath.Join(dir, t.Name()+".jsonl")
		o, err := filesink.Open(path)
		if err != nil {
			t.Fatalf("Open() error = %v", err)
		}
		opened = append(opened, o)
		return o
	})
}

// TestNew_MarshalError_DoesNotWriteToBuffer verifies that a marshal failure
// leaves the buffer unchanged.
func TestNew_MarshalError_DoesNotWriteToBuffer(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	outlet := filesink.New(&buf)

	_ = outlet.Sink(context.Background(), unmarshalable{Ch: make(chan int)})

	if buf.Len() != 0 {
		t.Errorf("buffer has %d bytes after marshal error, want 0", buf.Len())
	}
}

// TestNew_AcceptsArbitraryTypes verifies several arbitrary payload types encode
// without error.
func TestNew_AcceptsArbitraryTypes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		payload any
	}{
		{"string", "hello"},
		{"int", 42},
		{"float", math.Pi},
		{"bool", true},
		{"nil", nil},
		{"map", map[string]int{"a": 1}},
		{"slice", []int{1, 2, 3}},
		{"struct", marshalable{ID: "z", Value: 99}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var buf bytes.Buffer
			outlet := filesink.New(&buf)
			if err := outlet.Sink(context.Background(), tc.payload); err != nil {
				t.Fatalf("Sink(%v) error = %v", tc.payload, err)
			}
			if !strings.HasSuffix(buf.String(), "\n") {
				t.Errorf("output missing trailing newline: %q", buf.String())
			}
		})
	}
}
