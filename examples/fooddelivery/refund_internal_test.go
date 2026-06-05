// SPDX-License-Identifier: Apache-2.0

package fooddelivery

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/state"
)

// TestRefundFn covers the refund service body directly, including its
// nothing-to-refund error branch: a refund attempted against an order with no
// authorization hold has nothing to reverse and must fail loudly rather than
// returning a bogus amount. The happy path (an order carrying a hold reverses
// subtotal+tip) is also asserted so both arms are exercised in one place.
func TestRefundFn(t *testing.T) {
	t.Parallel()

	t.Run("no hold is an error", func(t *testing.T) {
		t.Parallel()
		out, err := refundFn(context.Background(), state.ServiceCtx[Order]{
			Entity: Order{Subtotal: 5000, Tip: 1500},
		})
		if err == nil {
			t.Fatalf("refundFn with no AuthHold = (%v, nil), want an error", out)
		}
		if out != nil {
			t.Fatalf("refundFn error path returned %v, want nil amount", out)
		}
	})

	t.Run("held authorization reverses subtotal plus tip", func(t *testing.T) {
		t.Parallel()
		out, err := refundFn(context.Background(), state.ServiceCtx[Order]{
			Entity: Order{Subtotal: 5000, Tip: 1500, AuthHold: "tok-1"},
		})
		if err != nil {
			t.Fatalf("refundFn with a hold returned error: %v", err)
		}
		amount, ok := out.(int64)
		if !ok {
			t.Fatalf("refundFn returned %T, want int64", out)
		}
		if amount != 6500 {
			t.Fatalf("refund amount = %d, want 6500 (subtotal+tip)", amount)
		}
	})
}
