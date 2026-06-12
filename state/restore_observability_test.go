package state_test

// This file pins that Restore can re-attach the observability seams an instance
// runs with — the Inspector and the structured *slog.Logger — which a plain
// Restore otherwise drops, leaving the restored instance silent. WithRestoreInspector
// and WithRestoreLogger mirror WithInspector / WithLogger at Cast, so a host that
// resumes an instance keeps observing it.

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestRestore_WithRestoreInspector_EmitsOnFire restores an instance with a fresh
// inspector and asserts a subsequent Fire feeds that inspector the live
// event/transition stream — the restored instance is observable again.
func TestRestore_WithRestoreInspector_EmitsOnFire(t *testing.T) {
	m := buildDocMachine()
	ctx := context.Background()

	// Cast and snapshot at Draft (no fire needed; snapshot is a pure read).
	src := m.Cast(&Document{Status: Draft, ReviewerID: strptr("rev-1")}, state.WithInitialState(Draft))
	snap := src.Snapshot()

	insp := &recordingInspector{}
	restored, err := m.Restore(snap, state.WithRestoreInspector[DocState](insp))
	if err != nil {
		t.Fatalf("Restore err = %v", err)
	}

	if res := restored.Fire(ctx, Submit); res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}

	if len(insp.ofKind(state.InspectEvent)) == 0 {
		t.Fatal("restored inspector saw no event observation; inspector not re-attached")
	}
	trans := insp.ofKind(state.InspectTransition)
	if len(trans) != 1 {
		t.Fatalf("restored inspector transition observations = %d, want 1", len(trans))
	}
	if trans[0].From != "Draft" || trans[0].To != "Submitted" {
		t.Fatalf("restored transition from/to = %q/%q, want Draft/Submitted", trans[0].From, trans[0].To)
	}
}

// TestRestore_WithRestoreLogger_WritesOnFire restores an instance with a fresh
// *slog.Logger backed by a capturing handler and asserts a subsequent Fire writes
// the terse structured record — the restored instance logs again.
func TestRestore_WithRestoreLogger_WritesOnFire(t *testing.T) {
	m := buildDocMachine()
	ctx := context.Background()

	src := m.Cast(&Document{Status: Draft, ReviewerID: strptr("rev-1")}, state.WithInitialState(Draft))
	snap := src.Snapshot()

	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	restored, err := m.Restore(snap, state.WithRestoreLogger[DocState](logger))
	if err != nil {
		t.Fatalf("Restore err = %v", err)
	}

	if res := restored.Fire(ctx, Submit); res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}

	out := buf.String()
	if out == "" {
		t.Fatal("restored logger captured nothing; logger not re-attached")
	}
	if !strings.Contains(out, "Submit") {
		t.Fatalf("restored log record missing event %q; got:\n%s", "Submit", out)
	}
}

// TestRestore_NoObservabilityOptions_StaysSilent asserts the default posture is
// preserved: a Restore without the observability options re-attaches neither seam,
// so a restored instance performs no inspection IO (mirroring a plain Cast).
func TestRestore_NoObservabilityOptions_StaysSilent(t *testing.T) {
	m := buildDocMachine()
	ctx := context.Background()

	src := m.Cast(&Document{Status: Draft, ReviewerID: strptr("rev-1")}, state.WithInitialState(Draft))
	snap := src.Snapshot()

	restored, err := m.Restore(snap)
	if err != nil {
		t.Fatalf("Restore err = %v", err)
	}
	// No inspector/logger attached: the Fire must still succeed and produce no panic
	// or observation side effect (there is nothing to assert beyond a clean Fire).
	if res := restored.Fire(ctx, Submit); res.Err != nil {
		t.Fatalf("Fire err = %v", res.Err)
	}
}
