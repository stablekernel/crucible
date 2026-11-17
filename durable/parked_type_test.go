package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stablekernel/crucible/durable"
	alpha "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model"
	beta "github.com/stablekernel/crucible/durable/internal/typefixture/beta/model"
	"github.com/stablekernel/crucible/state"
)

// These tests answer two review findings on the Go type a parked result records: what
// a type moved or renamed after the park does to the result, and whether the Record
// that finally settles a parked result keeps the type, so replay holds the decoder to
// it as the live settle did.

// alphaResultType is the type name a parked alpha.Result records.
const alphaResultType = "github.com/stablekernel/crucible/durable/internal/typefixture/alpha/model.Result"

// movedFromType is where alpha.Result lived before the move these tests stand in for.
const movedFromType = "github.com/stablekernel/crucible/durable/internal/typefixture/old/model.Result"

// parkedAlpha parks an alpha.Result for scn-1 under a decoder that refuses it, and
// returns the machine, its Assigns, the registry option and the store.
func parkedAlpha(t *testing.T) (*state.Machine[string, string, svcCtx], *scnAssigns, durable.Option[string, string, svcCtx], *durable.MemStore, *int64) {
	t.Helper()
	runs := new(int64)
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(runs, 1)
		return alpha.Result{ID: 7, Name: "ann"}, nil
	}
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	h := scnStart(t, m, store, reg, failingDecoder())
	_, ran, err := h.RunService(context.Background(), state.InvokeID(scnInvoke, "loading", 0))
	mustPark(t, "RunService", ran, err)
	return m, a, reg, store, runs
}

// rewriteServiceTypes copies scn-1's journal into a fresh MemStore with every recorded
// service entry's Type from replaced by to, as if the type had been moved after the
// version that recorded it.
func rewriteServiceTypes(t *testing.T, src *durable.MemStore, from, to string) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, recs, err := src.Load(ctx, "scn-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dst := durable.NewMemStore()
	if err = dst.Checkpoint(ctx, "scn-1", snap, -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	rewrote := 0
	for _, rec := range recs {
		for i, e := range rec.Entries {
			if e.Kind != state.JournalServiceResult {
				continue
			}
			var p map[string]any
			if err = json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decoding a service entry: %v", err)
			}
			if p["type"] == from {
				p["type"] = to
				rewrote++
			}
			if rec.Entries[i].Payload, err = json.Marshal(p); err != nil {
				t.Fatalf("encoding a service entry: %v", err)
			}
		}
		if _, err = dst.Append(ctx, "scn-1", rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if rewrote == 0 {
		t.Fatalf("no service entry recorded the type %s", from)
	}
	return dst
}

// serviceTypes returns the Type of each recorded service entry for scn-1, in order.
func serviceTypes(t *testing.T, st durable.Store) []string {
	t.Helper()
	_, recs, err := st.Load(context.Background(), "scn-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var out []string
	for _, rec := range recs {
		for _, e := range rec.Entries {
			if e.Kind != state.JournalServiceResult {
				continue
			}
			var p struct {
				Type   string `json:"type"`
				Parked bool   `json:"parked"`
			}
			if err = json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatalf("decoding a service entry: %v", err)
			}
			out = append(out, p.Type)
		}
	}
	return out
}

// TestParkedResult_MovedType pins what a type moved, or renamed, after a park does to
// its result. The park recorded the old full name, and settling holds the decoder to
// it, so a decoder for the type in its new home is refused on every RunService: the
// result stays parked, and no Assign runs. The way out is a decoder that gives JSON's
// generic form for the event, which a parked settle always accepts, as live does; the
// ServiceFn never runs again either way. After a park the result's original value is
// gone, so nothing tells a moved type from a wrong decoder's same-shaped one (see
// TestParkedResult_TypeCheckKeepsTheImportPath).
func TestParkedResult_MovedType(t *testing.T) {
	ctx := context.Background()
	m, a, reg, src, runs := parkedAlpha(t)
	store := rewriteServiceTypes(t, src, alphaResultType, movedFromType)
	id := state.InvokeID(scnInvoke, "loading", 0)
	asAlpha := decoderFor("done", durable.DecodeAs[alpha.Result])
	for i := 0; i < 2; i++ {
		h, err := durable.Recover(ctx, m, store, "scn-1", reg, asAlpha)
		if err != nil {
			t.Fatalf("Recover: %v", err)
		}
		_, ran, err := h.RunService(ctx, id)
		mustPark(t, "RunService with the moved type's decoder", ran, err)
		if !strings.Contains(err.Error(), movedFromType) {
			t.Fatalf("RunService: %v, want it to name the recorded type %s", err, movedFromType)
		}
	}
	if a.done != 0 || a.fails != 0 {
		t.Fatalf("an Assign ran: onDone %d, onError %d", a.done, a.fails)
	}
	h, err := durable.Recover(ctx, m, store, "scn-1", reg) // the default decoder
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, _, err = h.RunService(ctx, id); err != nil {
		t.Fatalf("RunService with the default decoder: %v, want the generic form to settle it", err)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if _, ok := a.seen[0].(map[string]any); a.done != 1 || !ok {
		t.Fatalf("onDone's Assign ran %d times and saw %v, want once with JSON's generic form", a.done, a.seen)
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
}

// TestParkedResult_SettledRecordKeepsTheType: the Record that settles a parked result
// records the type the park did, and replay holds the decoder to it, as the live settle
// did. A Recover whose decoder gives another type fails with ErrEventDataEncoding and
// runs no Assign, rather than hand onDone's Assign a value of a type the live run never
// let through.
func TestParkedResult_SettledRecordKeepsTheType(t *testing.T) {
	ctx := context.Background()
	r := newParkRun(t)
	fixed := decoderFor("done", durable.DecodeAs[scnUser])
	if _, runErr, err := r.recoverAndRun(fixed); err != nil || runErr != nil {
		t.Fatalf("Recover: %v, RunService: %v, with the fixed decoder", err, runErr)
	}
	userType := "github.com/stablekernel/crucible/durable_test.scnUser"
	if got := serviceTypes(t, r.store); len(got) != 2 || got[0] != userType || got[1] != userType {
		t.Fatalf("the service entries record the types %q, want the park's and the settle's both %s", got, userType)
	}
	done := r.a.done
	_, err := durable.Recover(ctx, r.m, r.store, "scn-1", r.reg, decoderFor("done", durable.DecodeAs[scnAdmin]))
	if !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover with a decoder for another type: %v, want ErrEventDataEncoding", err)
	}
	if r.a.done != done {
		t.Fatalf("replay ran onDone's Assign with %s, a type the live settle refuses", describe(r.a.seen[len(r.a.seen)-1]))
	}
	// The fixed decoder, and JSON's generic form, still replay it.
	if _, err = durable.Recover(ctx, r.m, r.store, "scn-1", r.reg, fixed); err != nil {
		t.Fatalf("Recover with the fixed decoder: %v", err)
	}
	if _, err = durable.Recover(ctx, r.m, r.store, "scn-1", r.reg); err != nil {
		t.Fatalf("Recover with the default decoder: %v", err)
	}
}

// TestParkedResult_StructLiteralKeepsTheImportPath: a result whose type is a struct
// literal with a field of alpha's model.Result is parked under that full name, so a
// decoder for the literal with beta's model.Result in its place can't settle it, as for
// the named types (see TestParkedResult_TypeCheckKeepsTheImportPath).
func TestParkedResult_StructLiteralKeepsTheImportPath(t *testing.T) {
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		return struct{ R alpha.Result }{alpha.Result{ID: 7, Name: "ann"}}, nil
	}
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	id := state.InvokeID(scnInvoke, "loading", 0)
	h := scnStart(t, m, store, reg, failingDecoder())
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	rh, err := durable.Recover(ctx, m, store, "scn-1", reg, decoderFor("done", durable.DecodeAs[struct{ R beta.Result }]))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	_, ran, err = rh.RunService(ctx, id)
	mustPark(t, "RunService with a decoder for beta's literal", ran, err)
	if a.done != 0 {
		t.Fatalf("onDone's Assign ran with %v", a.seen)
	}
	rh, err = durable.Recover(ctx, m, store, "scn-1", reg, decoderFor("done", durable.DecodeAs[struct{ R alpha.Result }]))
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, _, err = rh.RunService(ctx, id); err != nil {
		t.Fatalf("RunService with a decoder for alpha's literal: %v", err)
	}
}
