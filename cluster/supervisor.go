package cluster

import (
	"context"
	"sync"

	"github.com/stablekernel/crucible/state"
)

// Decision is how a Supervisor reacts to a child actor's escalated failure.
type Decision int

const (
	// Escalate forwards the failure to the supervisor's escalation sink, propagating
	// it up the supervision hierarchy. With no sink configured it leaves the failure
	// as the kernel's recorded default. It is the zero value.
	Escalate Decision = iota
	// Stop contains the failure at this level: the failed actor stays down and the
	// failure is not forwarded.
	Stop
)

// String renders a Decision for diagnostics.
func (d Decision) String() string {
	switch d {
	case Stop:
		return "stop"
	case Escalate:
		return "escalate"
	default:
		return "unknown"
	}
}

// HandledEscalation records one failure a Supervisor processed, for observability.
type HandledEscalation struct {
	// ActorID is the registry id of the actor that failed.
	ActorID string
	// Src is the actor ref name the failed actor was spawned from.
	Src string
	// Decision is the strategy the supervisor applied for that src.
	Decision Decision
	// Err is the underlying failure that escalated.
	Err error
}

// Supervisor turns the kernel's raw escalation seam into a per-src supervision
// policy: each child actor failure is routed to a Decision by the src it was
// spawned from, with a default for any src not listed. It is the host-side
// supervision-strategy layer over state.ActorEscalation / EscalationHandler —
// restart and backoff build on the same routing. A Supervisor is safe for
// concurrent use.
type Supervisor struct {
	def    Decision
	perSrc map[string]Decision
	sink   state.EscalationHandler

	mu      sync.Mutex
	handled []HandledEscalation
}

// SupervisorOption configures a Supervisor. New strategies arrive as additional
// options, so the constructor signature never breaks.
type SupervisorOption func(*Supervisor)

// WithDefaultDecision sets the decision applied to a failure whose src has no
// explicit decision. Without it the default is Escalate.
func WithDefaultDecision(d Decision) SupervisorOption {
	return func(s *Supervisor) { s.def = d }
}

// WithDecision sets the decision applied to failures of actors spawned from src.
func WithDecision(src string, d Decision) SupervisorOption {
	return func(s *Supervisor) { s.perSrc[src] = d }
}

// WithEscalationSink sets where an Escalate decision forwards the failure — the
// next handler up the supervision hierarchy. Without it, an Escalate decision
// leaves the failure as the kernel's recorded default.
func WithEscalationSink(h state.EscalationHandler) SupervisorOption {
	return func(s *Supervisor) { s.sink = h }
}

// NewSupervisor builds a Supervisor. Wire it into a system with
// ActorSystem.WithEscalationHandler(sup.Handle).
func NewSupervisor(opts ...SupervisorOption) *Supervisor {
	s := &Supervisor{perSrc: make(map[string]Decision)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// DecisionFor returns the decision configured for src, or the default.
func (s *Supervisor) DecisionFor(src string) Decision {
	if d, ok := s.perSrc[src]; ok {
		return d
	}
	return s.def
}

// Handle is the state.EscalationHandler the Supervisor exposes: it records the
// failure, then applies the src's decision — forwarding to the escalation sink for
// Escalate, or containing it for Stop. Wire it with
// ActorSystem.WithEscalationHandler(sup.Handle).
func (s *Supervisor) Handle(ctx context.Context, esc *state.ActorEscalation) {
	d := s.DecisionFor(esc.Src)
	s.mu.Lock()
	s.handled = append(s.handled, HandledEscalation{
		ActorID:  esc.ActorID,
		Src:      esc.Src,
		Decision: d,
		Err:      esc.Err,
	})
	s.mu.Unlock()

	if d == Escalate && s.sink != nil {
		s.sink(ctx, esc)
	}
}

// Handled returns a snapshot of the failures this supervisor has processed, in
// order. The returned slice is a copy and safe to retain.
func (s *Supervisor) Handled() []HandledEscalation {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]HandledEscalation, len(s.handled))
	copy(out, s.handled)
	return out
}
