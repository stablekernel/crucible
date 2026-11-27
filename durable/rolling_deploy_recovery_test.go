package durable_test

import (
	"context"
	"testing"

	"github.com/stablekernel/crucible/durable"
	"github.com/stablekernel/crucible/state"
)

// The #253 defect: in a rolling deploy, an instance one version starts and another
// then fires on holds a mixed-format journal, and Recover refuses it only at the
// Record the other version wrote — after replaying, and dispatching the effects of,
// every Record before it. The issue's comment is explicit: whatever fixes #253
// should refuse before any effect is sent.

// TestRecover_AMixedVersionJournalRefusesBeforeAnyEffectIsSent pins that refusal: a
// journal whose tail holds an older version's Record is refused, and no domain
// effect of the Records before the refusal lands on the way out — the handler sees
// nothing, because none of what that journal drove can be trusted.
func TestRecover_AMixedVersionJournalRefusesBeforeAnyEffectIsSent(t *testing.T) {
	t.Skip("pending #253: Recover dispatches the effects of the Records before the mixed-format refusal; and no documented path recovers such an instance")
	ctx := context.Background()
	m := state.ForgeFor[actorCtx]("mixedVersion").
		Action("notify", notifyAction()).
		State("idle").
		State("gone").Final().
		Initial("idle").
		Transition("idle").On("go").GoTo("gone").Do("notify").
		Quench()
	store, id := durable.NewMemStore(), durable.InstanceID("mixedVersion")
	// The live run, on this version: the baseline and a step whose transition emits
	// a notify, with no handler wired, so nothing is marked dispatched.
	h, err := durable.NewRunner(m, store).Start(ctx, id, actorCtx{}, state.WithInitialState("idle"))
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err = h.Fire(ctx, "go"); err != nil {
		t.Fatalf("Fire(go): %v", err)
	}
	// The older binary in the rolling deploy fires on the instance: its Record
	// carries the older journal format onto this version's checkpoint.
	older := durable.Record{Step: 1, Format: 2, Event: []byte(`"poke"`)}
	if _, err = store.Append(ctx, id, older); err != nil {
		t.Fatalf("Append the older version's Record: %v", err)
	}
	// Recover must refuse the mixed journal — neither version can recover it — and
	// the refusal must come before any effect is sent.
	rec := newEffectRecorder()
	if _, err = durable.Recover(ctx, m, store, id, durable.WithEffectHandler[string, string, actorCtx](rec.handler())); err == nil {
		t.Fatal("Recover of the mixed-version journal succeeded; want the refusal")
	}
	if got := len(rec.values); got != 0 {
		t.Fatalf("Recover dispatched %d effects (%v) before refusing, want none: no effect of a journal Recover cannot recover may land", got, rec.values)
	}
}
