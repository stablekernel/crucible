package durable_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// These tests answer a review finding on a result that settles when its service runs:
// its Record kept no Go type, only a parked result's did, so replay let the decoder
// return any type, though the live settle held it to the type the ServiceFn returned.
// Every successful settle in a Record of this format now records the type, and replay
// holds the decoder to it as for a settled parked result.

// settledUser runs the scn service live, settling a scnUser under live, and returns
// what Recover needs.
func settledUser(t *testing.T, fn state.ServiceFn[svcCtx], live ...durable.Option[string, string, svcCtx]) (*state.Machine[string, string, svcCtx], *scnAssigns, durable.Option[string, string, svcCtx], *durable.MemStore) {
	t.Helper()
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	h := scnStart(t, m, store, append([]durable.Option[string, string, svcCtx]{reg}, live...)...)
	mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
	return m, a, reg, store
}

// TestServiceResult_SettledRecordKeepsTheType: a scnUser settled live under a decoder
// for it records its type, and a Recover whose decoder returns another type, a pointer
// to it or another struct, fails with ErrEventDataEncoding and runs no Assign, as for a
// settled parked result (see TestParkedResult_SettledRecordKeepsTheType). The same
// decoder, and JSON's generic form, still replay it.
func TestServiceResult_SettledRecordKeepsTheType(t *testing.T) {
	ctx := context.Background()
	userType := "github.com/stablekernel/crucible/durable_test.scnUser"
	for _, tc := range []struct {
		name string
		live []durable.Option[string, string, svcCtx]
	}{
		{"decoded live as its type", []durable.Option[string, string, svcCtx]{decoderFor("done", durable.DecodeAs[scnUser])}},
		{"decoded live as JSON's generic form", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, a, reg, store := settledUser(t, returning(scnUser{ID: 7, Name: "ann"}, nil), tc.live...)
			if got := serviceTypes(t, store); len(got) != 1 || got[0] != userType {
				t.Fatalf("the settle records the type %q, want %s", got, userType)
			}
			for _, wrong := range []struct {
				name string
				opt  durable.Option[string, string, svcCtx]
			}{
				{"a pointer to it", decoderFor("done", durable.DecodeAs[*scnUser])},
				{"another struct", decoderFor("done", durable.DecodeAs[scnAdmin])},
			} {
				done := a.done
				_, err := durable.Recover(ctx, m, store, "scn-1", reg, wrong.opt)
				if !errors.Is(err, durable.ErrEventDataEncoding) {
					t.Fatalf("Recover with a decoder for %s: %v, want ErrEventDataEncoding", wrong.name, err)
				}
				if a.done != done {
					t.Fatalf("replay ran onDone's Assign with %s, a type the live settle refuses", describe(a.seen[len(a.seen)-1]))
				}
			}
			for _, right := range []struct {
				name string
				opt  []durable.Option[string, string, svcCtx]
			}{
				{"its own type", []durable.Option[string, string, svcCtx]{decoderFor("done", durable.DecodeAs[scnUser])}},
				{"JSON's generic form", nil},
			} {
				if _, err := durable.Recover(ctx, m, store, "scn-1", append([]durable.Option[string, string, svcCtx]{reg}, right.opt...)...); err != nil {
					t.Fatalf("Recover with a decoder for %s: %v", right.name, err)
				}
			}
		})
	}
}

// TestServiceResult_SettledNilResultReplays: a nil result settles and replays under any
// decoder, as live: it arrives as nil whatever the decoder (see
// TestServiceResult_NilResult_ArrivesNil).
func TestServiceResult_SettledNilResultReplays(t *testing.T) {
	ctx := context.Background()
	m, a, reg, store := settledUser(t, returning(nil, nil), decoderFor("done", durable.DecodeAs[scnUser]))
	for _, opt := range []durable.Option[string, string, svcCtx]{
		decoderFor("done", durable.DecodeAs[scnUser]),
		decoderFor("done", durable.DecodeAs[scnAdmin]),
		decoderFor("done", durable.DecodeAs[any]),
	} {
		if _, err := durable.Recover(ctx, m, store, "scn-1", reg, opt); err != nil {
			t.Fatalf("Recover: %v", err)
		}
	}
	for i, v := range a.seen {
		if v != nil {
			t.Fatalf("run %d saw %s, want nil", i, describe(v))
		}
	}
}
