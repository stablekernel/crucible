package durable_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// otherLocalResult decodes data as a type declared in this function, named result as
// the one TestParkedResult_SameNamedLocalType's ServiceFn returns is: reflect spells the
// two alike, by package path and name, but they are distinct types.
func otherLocalResult(_ string, data []byte) (any, error) {
	type result struct {
		ID int `json:"id"`
	}
	var r result
	err := json.Unmarshal(data, &r)
	return r, err
}

// TestParkedResult_SameNamedLocalType pins that a parked settle holds the decoder to the
// result's own type, as the live settle that parked it did, even when another type has
// the same name and package, such as two function-local types named result: the live
// check compares the types, and a parked settle on the same Handle compares them too, so
// the result stays parked and no Assign runs. After a Recover only the recorded name is
// left, which can't tell the two apart, so the parked settle accepts the other type: a
// result of a function-local type is identified by its name alone once it is recorded.
func TestParkedResult_SameNamedLocalType(t *testing.T) {
	type result struct {
		ID int `json:"id"`
	}
	ctx := context.Background()
	fn := func(context.Context, state.ServiceCtx[svcCtx]) (any, error) { return result{ID: 7}, nil }
	a := &scnAssigns{}
	m := scnMachine(fn, a)
	opts := []durable.Option[string, string, svcCtx]{
		durable.WithServiceRegistry[string, string, svcCtx](fetchRegistry(fn)),
		durable.WithEventDataDecoder[string, string, svcCtx](otherLocalResult),
	}
	store := durable.NewMemStore()
	h := scnStart(t, m, store, opts...)
	id := state.InvokeID(scnInvoke, "loading", 0)
	_, ran, err := h.RunService(ctx, id)
	mustPark(t, "RunService", ran, err)
	_, ran, err = h.RunService(ctx, id)
	mustPark(t, "RunService of the parked result", ran, err)
	if a.done != 0 || a.fails != 0 {
		t.Fatalf("Assigns ran: done %d, fail %d; want none: the decoder's result is another type than the ServiceFn's", a.done, a.fails)
	}
	recovered, err := durable.Recover(ctx, m, store, "scn-1", opts...)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if _, ran, err = recovered.RunService(ctx, id); !ran || err != nil || a.done != 1 {
		t.Fatalf("RunService after Recover = ran %v, %v, done %d; want it settled: only the recorded name is left", ran, err, a.done)
	}
}
