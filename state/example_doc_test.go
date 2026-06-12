package state_test

import (
	"time"

	"github.com/stablekernel/crucible/state"
)

// This file defines a neutral example machine — a document-approval lifecycle —
// shared by the package tests. It exercises states, owned-by tags,
// guarded transitions, named action refs with params, and wait modes. The
// domain is deliberately generic and carries no real-world coupling.

// DocState is the example state type.
type DocState int

const (
	Draft DocState = iota
	Submitted
	Approved
	Published
	Archived
)

// String renders a DocState by name so diagrams and traces read symbolically.
func (s DocState) String() string {
	switch s {
	case Draft:
		return "Draft"
	case Submitted:
		return "Submitted"
	case Approved:
		return "Approved"
	case Published:
		return "Published"
	case Archived:
		return "Archived"
	default:
		return "DocState?"
	}
}

// DocEvent is the example event type.
type DocEvent int

const (
	Submit DocEvent = iota
	Approve
	RequestChanges
	Publish
	Archive
)

// String renders a DocEvent by name so diagrams and traces read symbolically.
func (e DocEvent) String() string {
	switch e {
	case Submit:
		return "Submit"
	case Approve:
		return "Approve"
	case RequestChanges:
		return "RequestChanges"
	case Publish:
		return "Publish"
	case Archive:
		return "Archive"
	default:
		return "DocEvent?"
	}
}

// Document is the example entity (context) type.
type Document struct {
	Status      DocState
	ReviewerID  *string
	PublishedAt *time.Time
}

// emitEvent is the example action: it returns a concrete domain effect. The
// kernel treats the return as an opaque Effect.
func emitEvent(ctx state.ActionCtx[*Document]) (state.Effect, error) {
	name, _ := ctx.Params["event"].(string)
	return emittedEvent{Name: name}, nil
}

// emittedEvent is a concrete domain effect type.
type emittedEvent struct {
	Name string
}

// buildDocMachine forges the example machine. It is the single source of the
// example used across the package tests.
func buildDocMachine() *state.Machine[DocState, DocEvent, *Document] {
	return state.Forge[DocState, DocEvent, *Document]("document").
		Guard("hasReviewer", func(ctx state.GuardCtx[*Document]) bool {
			return ctx.Entity.ReviewerID != nil
		}).
		Action("emit", emitEvent).
		State(Draft).OwnedBy("Author").
		State(Submitted).OwnedBy("Author").
		State(Approved).OwnedBy("Reviewer").
		Requires(state.Requirement[*Document]{
			Name:      "reviewerAssigned",
			Predicate: func(d *Document) bool { return d != nil && d.ReviewerID != nil },
		}).
		State(Published).OwnedBy("Reviewer").
		State(Archived).OwnedBy("Author").
		Initial(Draft).
		CurrentStateFn(func(d *Document) DocState { return d.Status }).
		Transition(Draft).On(Submit).GoTo(Submitted).
		Do("emit", state.P{"event": "submitted"}).
		WaitMode(state.SyncReply).
		Transition(Submitted).On(Approve).GoTo(Approved).
		When("hasReviewer").
		Do("emit", state.P{"event": "approved"}).
		Transition(Submitted).On(RequestChanges).GoTo(Draft).
		Transition(Approved).On(Publish).GoTo(Published).
		Do("emit", state.P{"event": "published"}).
		Transition(Draft).On(Archive).GoTo(Archived).
		Transition(Submitted).On(Archive).GoTo(Archived).
		// Quench without Strict: *Document is a pointer context, which now raises
		// the soft clean-replay determinism warning. Strict would turn that warning
		// into a panic; this shared fixture only needs a well-formed, well-built
		// machine, so it builds plainly.
		Quench()
}

func strptr(s string) *string { return &s }
