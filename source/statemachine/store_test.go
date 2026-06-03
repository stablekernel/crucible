// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source/statemachine"
)

func TestMemStore_LoadMissing(t *testing.T) {
	t.Parallel()
	s := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	_, ok, err := s.Load(context.Background(), locked)
	if err != nil {
		t.Fatalf("Load err = %v, want nil", err)
	}
	if ok {
		t.Fatal("Load ok = true, want false for missing key")
	}
}

func TestMemStore_SaveLoadRoundTrip(t *testing.T) {
	t.Parallel()
	s := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()
	rec := statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Version: 1, LastEventID: "e1"}
	if err := s.Save(context.Background(), locked, rec, 0); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	got, ok, _ := s.Load(context.Background(), locked)
	if !ok || got.Version != 1 || got.LastEventID != "e1" {
		t.Fatalf("round trip = %+v ok=%v", got, ok)
	}
}

func TestMemStore_OptimisticConcurrency(t *testing.T) {
	t.Parallel()
	s := statemachine.NewMemStore[turnstileState, turnstileState, turnstileEvent, *turnstile]()

	// First write must expect version 0.
	if err := s.Save(context.Background(), locked, rec(1, "e1"), 1); !errors.Is(err, statemachine.ErrConflict) {
		t.Fatalf("first save with wrong expected version: %v, want ErrConflict", err)
	}
	if err := s.Save(context.Background(), locked, rec(1, "e1"), 0); err != nil {
		t.Fatalf("first save: %v", err)
	}
	// A stale expected version is rejected.
	if err := s.Save(context.Background(), locked, rec(2, "e2"), 0); !errors.Is(err, statemachine.ErrConflict) {
		t.Fatalf("stale save: %v, want ErrConflict", err)
	}
	// The matching expected version succeeds.
	if err := s.Save(context.Background(), locked, rec(2, "e2"), 1); err != nil {
		t.Fatalf("matching save: %v", err)
	}
}

func rec(v int64, id string) statemachine.Record[turnstileState, turnstileEvent, *turnstile] {
	return statemachine.Record[turnstileState, turnstileEvent, *turnstile]{Version: v, LastEventID: id}
}
