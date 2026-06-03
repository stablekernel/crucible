// SPDX-License-Identifier: Apache-2.0

package statemachine_test

import (
	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/state"
)

// This file defines the neutral example statechart and message fakes the package
// tests share: a turnstile lifecycle with a guarded transition and an emitted
// effect, plus an in-memory source.Message. The domain carries no real-world
// coupling.

// turnstileState is the example state type.
type turnstileState int

const (
	locked turnstileState = iota
	unlocked
)

// String renders a turnstileState by name so traces read symbolically.
func (s turnstileState) String() string {
	switch s {
	case locked:
		return "locked"
	case unlocked:
		return "unlocked"
	default:
		return "turnstile?"
	}
}

// turnstileEvent is the example event type.
type turnstileEvent int

const (
	coin turnstileEvent = iota
	push
	maintenance // in the alphabet of no state: used to probe unreachable routing
)

// String renders a turnstileEvent by name.
func (e turnstileEvent) String() string {
	switch e {
	case coin:
		return "coin"
	case push:
		return "push"
	case maintenance:
		return "maintenance"
	default:
		return "turnstile-event?"
	}
}

// turnstile is the example entity (context). funded gates the coin transition so
// a guard rejection is exercisable.
type turnstile struct {
	Funded bool `json:"funded"`
}

// openedEffect is the concrete domain effect the unlock transition emits.
type openedEffect struct {
	By string
}

// buildTurnstile forges the example machine: locked --coin[funded]--> unlocked
// --push--> locked, with the unlock transition emitting an openedEffect. The coin
// transition is guarded so an unfunded coin is rejected as invalid-for-state.
func buildTurnstile() *state.Machine[turnstileState, turnstileEvent, *turnstile] {
	return state.Forge[turnstileState, turnstileEvent, *turnstile]("turnstile").
		Guard("funded", func(ctx state.GuardCtx[*turnstile]) bool {
			return ctx.Entity != nil && ctx.Entity.Funded
		}).
		Action("opened", func(state.ActionCtx[*turnstile]) (state.Effect, error) {
			return openedEffect{By: "coin"}, nil
		}).
		State(locked).
		State(unlocked).
		Initial(locked).
		CurrentStateFn(func(*turnstile) turnstileState { return locked }).
		Transition(locked).On(coin).GoTo(unlocked).
		When("funded").
		Do("opened").
		Transition(unlocked).On(push).GoTo(locked).
		Quench(state.Strict())
}

// fakeCursor is a string-backed source.Cursor.
type fakeCursor string

func (c fakeCursor) String() string { return string(c) }

// fakeMessage is an in-memory source.Message for tests. headers carry the
// content type and message id; value carries the routing payload (unused by the
// router fakes, which route on the id).
type fakeMessage struct {
	key     []byte
	value   []byte
	headers source.Headers
	subject string
	cursor  fakeCursor
}

func (m fakeMessage) Key() []byte             { return m.key }
func (m fakeMessage) Value() []byte           { return m.value }
func (m fakeMessage) Headers() source.Headers { return m.headers }
func (m fakeMessage) Subject() string         { return m.subject }
func (m fakeMessage) PartitionKey() string    { return "" }
func (m fakeMessage) Cursor() source.Cursor   { return m.cursor }
func (m fakeMessage) As(any) bool             { return false }

// msg builds a fakeMessage carrying the given message-id header and cursor.
func msg(id, cursor string) fakeMessage {
	return fakeMessage{
		headers: source.Headers{{Key: "message-id", Value: id}},
		subject: "turnstile",
		cursor:  fakeCursor(cursor),
	}
}
