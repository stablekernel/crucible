// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestOutletFuncSatisfiesOutlet(t *testing.T) {
	t.Parallel()

	want := errors.New("boom")
	var o sink.Outlet = sink.OutletFunc(func(_ context.Context, _ any) error { return want })

	if got := o.Sink(context.Background(), "x"); !errors.Is(got, want) {
		t.Fatalf("OutletFunc.Sink() = %v, want %v", got, want)
	}
}

func TestOutletFuncPassesPayloadAndContext(t *testing.T) {
	t.Parallel()

	type ctxKey string
	const key ctxKey = "k"
	ctx := context.WithValue(context.Background(), key, "v")

	var gotPayload any
	var gotCtxVal any
	o := sink.OutletFunc(func(c context.Context, p any) error {
		gotCtxVal = c.Value(key)
		gotPayload = p
		return nil
	})

	if err := o.Sink(ctx, 42); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if gotPayload != 42 {
		t.Errorf("payload = %v, want 42", gotPayload)
	}
	if gotCtxVal != "v" {
		t.Errorf("ctx value = %v, want %q", gotCtxVal, "v")
	}
}
