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

// These tests answer a review finding on the Go type every settle records: a settled
// result is pinned to its full type name, so a pure refactor that moves or renames the
// type makes Recover and StateAt fail with ErrEventDataEncoding for every journal
// already settled under the old name. WithResultTypeAlias tells the Runner what a
// recorded name now means, on every path that holds a result to its recorded name.

// alphaAlias is the option mapping the type alpha.Result recorded before a rename, as
// rewriteServiceTypes stands in for one, to alpha.Result.
func alphaAlias() durable.Option[string, string, svcCtx] {
	return durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, alpha.Result{})
}

// settledAlpha settles an alpha.Result for scn-1 live, under a decoder for it, and
// returns what Recover and StateAt need.
func settledAlpha(t *testing.T) (*state.Machine[string, string, svcCtx], *scnAssigns, durable.Option[string, string, svcCtx], *durable.MemStore) {
	t.Helper()
	fn := returning(alpha.Result{ID: 7, Name: "ann"}, nil)
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	h := scnStart(t, m, store, reg, decoderFor("done", durable.DecodeAs[alpha.Result]))
	mustRunService(t, h, state.InvokeID(scnInvoke, "loading", 0))
	return m, a, reg, store
}

// renamedSettled settles an alpha.Result live and returns the machine, its Assigns, the
// registry option and a journal whose settle records the type under the old name
// movedFromType, as if the settle ran before the rename.
func renamedSettled(t *testing.T) (*state.Machine[string, string, svcCtx], *scnAssigns, durable.Option[string, string, svcCtx], *durable.MemStore) {
	t.Helper()
	m, a, reg, src := settledAlpha(t)
	return m, a, reg, rewriteServiceTypes(t, src, alphaResultType, movedFromType)
}

// TestResultTypeAlias_RenamedSettledResult: a settle recorded under the type's old
// name fails Recover and StateAt with ErrEventDataEncoding under a decoder for the
// type's new name, naming the recorded type; WithResultTypeAlias admits the new type,
// on both paths, and the value onDone's Assign sees is the one the service returned.
func TestResultTypeAlias_RenamedSettledResult(t *testing.T) {
	ctx := context.Background()
	m, a, reg, store := renamedSettled(t)
	steps, stepsErr := durable.Steps(ctx, store, "scn-1")
	if stepsErr != nil || len(steps) != 1 {
		t.Fatalf("Steps = %v, %v; want the settle", steps, stepsErr)
	}
	asAlpha := decoderFor("done", durable.DecodeAs[alpha.Result])
	for _, what := range []struct {
		name string
		call func() error
	}{
		{"Recover", func() error {
			_, err := durable.Recover(ctx, m, store, "scn-1", reg, asAlpha)
			return err
		}},
		{"StateAt", func() error {
			_, err := durable.StateAt(ctx, m, store, "scn-1", steps[0], reg, asAlpha)
			return err
		}},
	} {
		err := what.call()
		if !errors.Is(err, durable.ErrEventDataEncoding) {
			t.Fatalf("%s without the alias: %v, want ErrEventDataEncoding", what.name, err)
		}
		if !strings.Contains(err.Error(), movedFromType) {
			t.Fatalf("%s without the alias: %v, want it to name the recorded type %s", what.name, err, movedFromType)
		}
	}
	if a.done != 1 || a.fails != 0 {
		t.Fatalf("a failed replay ran an Assign: onDone %d, onError %d", a.done, a.fails)
	}

	h, err := durable.Recover(ctx, m, store, "scn-1", reg, asAlpha, alphaAlias())
	if err != nil {
		t.Fatalf("Recover with the alias: %v", err)
	}
	if got := h.Instance().Current(); got != "ready" {
		t.Fatalf("Recover with the alias: in %q, want ready", got)
	}
	if a.done != 2 || describe(a.seen[1]) != describe(alpha.Result{ID: 7, Name: "ann"}) {
		t.Fatalf("replay with the alias ran onDone %d times and saw %v, want the alpha.Result", a.done, a.seen)
	}
	v, err := durable.StateAt(ctx, m, store, "scn-1", steps[0], reg, asAlpha, alphaAlias())
	if err != nil {
		t.Fatalf("StateAt with the alias: %v", err)
	}
	if got := v.Instance().Current(); got != "ready" {
		t.Fatalf("StateAt with the alias: in %q, want ready", got)
	}
}

// threeSettlesMachine settles fn three times: "loading" invokes fetch, and each onDone
// enters the next state, which invokes fetch again, so a checkpoint after the second
// settle leaves the third one in the tail a Recover from that checkpoint replays.
func threeSettlesMachine(fn state.ServiceFn[svcCtx], a *scnAssigns) *state.Machine[string, string, svcCtx] {
	return state.ForgeFor[svcCtx]("scn").
		Service("fetch", fn).
		Reducer("noteDone", func(in state.AssignCtx[svcCtx]) svcCtx {
			a.done++
			a.seen = append(a.seen, in.Event)
			return in.Entity
		}).
		State("loading").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("second").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("third").Invoke("fetch", state.WithInvokeOnDone("done")).
		State("ready").Final().
		Initial("loading").
		Transition("loading").On("done").GoTo("second").Assign("noteDone").
		Transition("second").On("done").GoTo("third").Assign("noteDone").
		Transition("third").On("done").GoTo("ready").Assign("noteDone").
		Quench()
}

// TestResultTypeAlias_RenamedSettledResultThroughCheckpoint: a checkpoint that
// compacts the earlier settles leaves the last one in the tail, and a Recover from it
// replays that settle's recorded type: the old name fails it, and WithResultTypeAlias
// settles it, with the value the service returned.
func TestResultTypeAlias_RenamedSettledResultThroughCheckpoint(t *testing.T) {
	ctx := context.Background()
	fn := returning(alpha.Result{ID: 7, Name: "ann"}, nil)
	a := &scnAssigns{}
	m := threeSettlesMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	asAlpha := decoderFor("done", durable.DecodeAs[alpha.Result])
	// A checkpoint lands after the second settle (steps 0 and 2 of three cross no
	// boundary; step 1 crosses 2), compacting the first two and leaving the third.
	every := durable.WithCheckpointEvery[string, string, svcCtx](2)
	h := scnStart(t, m, store, reg, asAlpha, every)
	for _, st := range []string{"loading", "second", "third"} {
		mustRunService(t, h, state.InvokeID(scnInvoke, st, 0))
	}
	// The first two settles checkpointed; the tail holds the third one.
	_, recs, lerr := store.Load(ctx, "scn-1")
	if lerr != nil || len(recs) != 1 || recs[0].Step != 2 {
		t.Fatalf("the journal holds %v (err %v), want only the third settle at step 2", recs, lerr)
	}
	renamed := rewriteServiceTypes(t, store, alphaResultType, movedFromType)
	if _, err := durable.Recover(ctx, m, renamed, "scn-1", reg, asAlpha, every); !errors.Is(err, durable.ErrEventDataEncoding) {
		t.Fatalf("Recover from the checkpoint without the alias: %v, want ErrEventDataEncoding", err)
	}
	if a.done != 3 || a.fails != 0 {
		t.Fatalf("a failed replay ran an Assign: onDone %d, onError %d", a.done, a.fails)
	}
	rh, err := durable.Recover(ctx, m, renamed, "scn-1", reg, asAlpha, alphaAlias(), every)
	if err != nil {
		t.Fatalf("Recover from the checkpoint with the alias: %v", err)
	}
	if got := rh.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if a.done != 4 || describe(a.seen[3]) != describe(alpha.Result{ID: 7, Name: "ann"}) {
		t.Fatalf("replay with the alias ran onDone %d times and saw %v, want the alpha.Result", a.done, a.seen)
	}
}

// rewriteParkedCheckpoint copies scn-1's journal into a fresh MemStore with every
// parked result its checkpoint carries recorded under from replaced by to, as if the
// type had been renamed after the checkpoint that kept the result.
func rewriteParkedCheckpoint(t *testing.T, src *durable.MemStore, from, to string) *durable.MemStore {
	t.Helper()
	ctx := context.Background()
	snap, recs, err := src.Load(ctx, "scn-1")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var env map[string]json.RawMessage
	if err = json.Unmarshal(snap, &env); err != nil || len(env["snapshot"]) == 0 {
		t.Fatalf("the checkpoint for scn-1 is not an envelope (err %v): %s", err, snap)
	}
	var parked []struct {
		ID     string          `json:"id"`
		Result json.RawMessage `json:"result"`
		Type   string          `json:"type"`
	}
	if err = json.Unmarshal(env["parkedServices"], &parked); err != nil || len(parked) == 0 {
		t.Fatalf("the checkpoint carries no parked result (err %v): %s", err, env["parkedServices"])
	}
	rewrote := 0
	for i := range parked {
		if parked[i].Type == from {
			parked[i].Type = to
			rewrote++
		}
	}
	if rewrote == 0 {
		t.Fatalf("no parked result recorded the type %s", from)
	}
	if env["parkedServices"], err = json.Marshal(parked); err != nil {
		t.Fatalf("encoding the parked results: %v", err)
	}
	if snap, err = json.Marshal(env); err != nil {
		t.Fatalf("encoding the checkpoint envelope: %v", err)
	}
	dst := durable.NewMemStore()
	if err = dst.Checkpoint(ctx, "scn-1", snap, -1); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	for _, rec := range recs {
		if _, err = dst.Append(ctx, "scn-1", rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return dst
}

// TestResultTypeAlias_RenamedParkedResultThroughCheckpoint: a checkpoint keeps a
// parked result with its recorded type, and a Recover from it settles that result
// through RunService. The old name keeps it parked, naming the recorded type; the
// alias settles it, with the value the service returned and the ServiceFn never run
// again.
func TestResultTypeAlias_RenamedParkedResultThroughCheckpoint(t *testing.T) {
	ctx := context.Background()
	runs := new(int64)
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(runs, 1)
		return alpha.Result{ID: 7, Name: "ann"}, nil
	}
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	every := durable.WithCheckpointEvery[string, string, svcCtx](1)
	h := scnStart(t, m, store, reg, failingDecoder(), every)
	id := state.InvokeID(scnInvoke, "loading", 0)
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	if _, recs, lerr := store.Load(ctx, "scn-1"); lerr != nil || len(recs) != 0 {
		t.Fatalf("the tail holds %d Records (err %v), want none: the park step should checkpoint", len(recs), lerr)
	}
	renamed := rewriteParkedCheckpoint(t, store, alphaResultType, movedFromType)
	asAlpha := decoderFor("done", durable.DecodeAs[alpha.Result])

	rh, err := durable.Recover(ctx, m, renamed, "scn-1", reg, asAlpha, every)
	if err != nil {
		t.Fatalf("Recover without the alias: %v", err)
	}
	_, ran, err = rh.RunService(ctx, id)
	mustPark(t, "RunService without the alias", ran, err)
	if !strings.Contains(err.Error(), movedFromType) {
		t.Fatalf("RunService without the alias: %v, want it to name the recorded type %s", err, movedFromType)
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}

	rh, err = durable.Recover(ctx, m, renamed, "scn-1", reg, asAlpha, alphaAlias(), every)
	if err != nil {
		t.Fatalf("Recover with the alias: %v", err)
	}
	if _, _, err = rh.RunService(ctx, id); err != nil {
		t.Fatalf("RunService with the alias: %v", err)
	}
	if got := rh.Instance().Current(); got != "ready" {
		t.Fatalf("in %q, want ready", got)
	}
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
	if a.done != 1 || describe(a.seen[0]) != describe(alpha.Result{ID: 7, Name: "ann"}) {
		t.Fatalf("onDone's Assign ran %d times and saw %v, want once with the alpha.Result", a.done, a.seen)
	}
}

// TestResultTypeAlias_OnlyTheAliasedName: an alias for one old name admits only the
// type it names. A decoder for another type, whose JSON is the result's, is refused;
// so is a decoder for the aliased type when the alias covers a different old name.
func TestResultTypeAlias_OnlyTheAliasedName(t *testing.T) {
	ctx := context.Background()
	m, a, reg, store := renamedSettled(t)
	for _, wrong := range []struct {
		name string
		opts []durable.Option[string, string, svcCtx]
	}{
		{"another type under the aliased name", []durable.Option[string, string, svcCtx]{
			decoderFor("done", durable.DecodeAs[beta.Result]), alphaAlias(),
		}},
		{"the aliased type under an alias for another name", []durable.Option[string, string, svcCtx]{
			decoderFor("done", durable.DecodeAs[alpha.Result]),
			durable.WithResultTypeAlias[string, string, svcCtx]("example.com/gone/model.Old", alpha.Result{}),
		}},
	} {
		done := a.done
		_, err := durable.Recover(ctx, m, store, "scn-1", append([]durable.Option[string, string, svcCtx]{reg}, wrong.opts...)...)
		if !errors.Is(err, durable.ErrEventDataEncoding) {
			t.Fatalf("Recover with %s: %v, want ErrEventDataEncoding", wrong.name, err)
		}
		if a.done != done || a.fails != 0 {
			t.Fatalf("Recover with %s ran an Assign: onDone %d (was %d), onError %d", wrong.name, a.done, done, a.fails)
		}
	}
}

// TestResultTypeAlias_BadInputIgnored: bad input changes nothing, as the other Runner
// options: an empty recorded name, a nil current value, a name aliased to the type's
// own current name, and one recorded name given for two different types, where the
// first stands and the repeat is ignored. The strict check stands for each, exactly as
// with no option at all, and the same alias twice is the one alias.
func TestResultTypeAlias_BadInputIgnored(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		opts    []durable.Option[string, string, svcCtx]
		settles bool
	}{
		{"an empty recorded name", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx]("", alpha.Result{}),
		}, false},
		{"a nil current value", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, nil),
		}, false},
		{"a name aliased to the type's own current name", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx](alphaResultType, alpha.Result{}),
		}, false},
		{"another type after the alias for a name", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, alpha.Result{}),
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, beta.Result{}),
		}, true},
		{"the alias after another type for a name", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, beta.Result{}),
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, alpha.Result{}),
		}, false},
		{"the same alias twice", []durable.Option[string, string, svcCtx]{
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, alpha.Result{}),
			durable.WithResultTypeAlias[string, string, svcCtx](movedFromType, alpha.Result{}),
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, a, reg, store := renamedSettled(t)
			opts := append([]durable.Option[string, string, svcCtx]{reg, decoderFor("done", durable.DecodeAs[alpha.Result])}, tc.opts...)
			_, err := durable.Recover(ctx, m, store, "scn-1", opts...)
			if tc.settles {
				if err != nil {
					t.Fatalf("Recover: %v, want the first alias to stand", err)
				}
				if got := len(a.seen); got != 2 {
					t.Fatalf("onDone's Assign ran %d times, want live and replay", got)
				}
				if describe(a.seen[1]) != describe(alpha.Result{ID: 7, Name: "ann"}) {
					t.Fatalf("replay saw %v, want the alpha.Result", a.seen)
				}
				return
			}
			if !errors.Is(err, durable.ErrEventDataEncoding) {
				t.Fatalf("Recover: %v, want the ignored input to leave the strict check standing", err)
			}
			if a.done != 1 || a.fails != 0 {
				t.Fatalf("replay ran an Assign: onDone %d, onError %d", a.done, a.fails)
			}
		})
	}
}

// TestResultTypeAlias_KeepsTheHandleParkedResultItsType: the alias applies to a
// recorded result's name, not to the type a result parked by the Handle itself is
// held to: a Handle whose alias claims the parked type's name for another type still
// refuses a decoder that returns it, as live, and the result stays parked.
func TestResultTypeAlias_KeepsTheHandleParkedResultItsType(t *testing.T) {
	ctx := context.Background()
	runs := new(int64)
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) {
		atomic.AddInt64(runs, 1)
		return alpha.Result{ID: 7, Name: "ann"}, nil
	}
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	reg := durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn))
	store := durable.NewMemStore()
	// The decoder refuses the result once, so it parks, then decodes it as
	// beta.Result, whose JSON is alpha's: only the type the Handle holds with the
	// park keeps that from settling through the alias.
	var calls int64
	flip := decoderFor("done", func(data []byte) (any, error) {
		if atomic.AddInt64(&calls, 1) == 1 {
			return nil, errors.New("decoder refused")
		}
		return durable.DecodeAs[beta.Result](data)
	})
	claim := durable.WithResultTypeAlias[string, string, svcCtx](alphaResultType, beta.Result{})
	h := scnStart(t, m, store, reg, flip, claim)
	id := state.InvokeID(scnInvoke, "loading", 0)
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	_, ran, err = h.RunService(ctx, id)
	mustPark(t, "RunService settling the parked result", ran, err)
	if got := atomic.LoadInt64(runs); got != 1 {
		t.Fatalf("the ServiceFn ran %d times, want 1", got)
	}
	if a.done != 0 || a.fails != 0 {
		t.Fatalf("an Assign ran: onDone %d, onError %d", a.done, a.fails)
	}
}
