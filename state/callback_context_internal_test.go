package state

import (
	"context"
	"testing"
)

// ctxTagKey tags the context a test passes to Fire.
type ctxTagKey struct{}

// tagGuard is a GuardBinding that records the tag on the context EvalGuard gets.
type tagGuard struct{ seen *string }

func (g tagGuard) EvalGuard(ctx context.Context, _ GuardRequest[bindOrder]) (GuardResult, error) {
	*g.seen, _ = ctx.Value(ctxTagKey{}).(string)
	return GuardResult{OK: true}, nil
}

// TestBindGuard_GetsTheFiresContext proves a guard registered with BindGuard gets the
// Fire's context through EvalGuard, not a background one.
func TestBindGuard_GetsTheFiresContext(t *testing.T) {
	var seen string
	b := ForgeFor[bindOrder]("bound")
	b.reg.BindGuard("bound", tagGuard{seen: &seen})
	m := b.
		State("from").
		Transition("from").On("go").GoTo("to").When("bound").
		State("to").
		Initial("from").
		Quench()
	ctx := context.WithValue(context.Background(), ctxTagKey{}, "fire")
	if res := m.Cast(bindOrder{}, WithInitialState("from")).Fire(ctx, "go"); res.Err != nil {
		t.Fatalf("Fire: %v", res.Err)
	}
	if seen != "fire" {
		t.Fatalf("the bound guard's EvalGuard saw %q, want fire", seen)
	}
}
