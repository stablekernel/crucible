// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/source"
)

func TestChain_Order(t *testing.T) {
	t.Parallel()
	var order []string
	mw := func(label string) source.Middleware {
		return func(next source.Handler) source.Handler {
			return func(ctx context.Context, m source.Message) source.Result {
				order = append(order, "in:"+label)
				r := next(ctx, m)
				order = append(order, "out:"+label)
				return r
			}
		}
	}
	base := func(context.Context, source.Message) source.Result {
		order = append(order, "handler")
		return source.Ack()
	}

	h := source.Chain(base, mw("A"), mw("B"), mw("C"))
	h(context.Background(), testMsg{})

	want := []string{"in:A", "in:B", "in:C", "handler", "out:C", "out:B", "out:A"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestChain_Empty(t *testing.T) {
	t.Parallel()
	called := false
	base := func(context.Context, source.Message) source.Result {
		called = true
		return source.Ack()
	}
	h := source.Chain(base)
	h(context.Background(), testMsg{})
	if !called {
		t.Fatal("Chain with no middleware should call the base handler")
	}
}

func TestChain_NilMiddlewareSkipped(t *testing.T) {
	t.Parallel()
	count := 0
	mw := func(next source.Handler) source.Handler {
		return func(ctx context.Context, m source.Message) source.Result {
			count++
			return next(ctx, m)
		}
	}
	base := func(context.Context, source.Message) source.Result { return source.Ack() }
	h := source.Chain(base, nil, mw, nil)
	h(context.Background(), testMsg{})
	if count != 1 {
		t.Fatalf("middleware ran %d times, want 1 (nils skipped)", count)
	}
}

func TestChain_ShortCircuit(t *testing.T) {
	t.Parallel()
	innerCalled := false
	guard := func(source.Handler) source.Handler {
		return func(context.Context, source.Message) source.Result {
			return source.Term(nil) // never calls next
		}
	}
	base := func(context.Context, source.Message) source.Result {
		innerCalled = true
		return source.Ack()
	}
	r := source.Chain(base, guard)(context.Background(), testMsg{})
	if innerCalled {
		t.Fatal("short-circuiting middleware should not call inner handler")
	}
	if r.Action != source.ActionTerm {
		t.Fatalf("result action = %v, want Term", r.Action)
	}
}
