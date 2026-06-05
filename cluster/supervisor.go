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
	// Restart re-spawns the failed actor through the configured Respawner, bounded
	// by the per-src restart budget set with WithRestart. When the budget is spent
	// (or no Respawner is wired) the failure escalates instead. Configure it with
	// WithRestart, not WithDecision.
	Restart
	// Backoff defers the restart: it schedules the re-spawn after an exponentially
	// growing delay and the host applies due restarts via Tick, so a failing actor
	// is not hammered with immediate restarts. Bounded by the per-src budget set
	// with WithBackoff; on exhaustion the failure escalates. Configure it with
	// WithBackoff.
	Backoff
)

// String renders a Decision for diagnostics.
func (d Decision) String() string {
	switch d {
	case Stop:
		return "stop"
	case Escalate:
		return "escalate"
	case Restart:
		return "restart"
	case Backoff:
		return "backoff"
	default:
		return "unknown"
	}
}

// Respawner re-creates a failed actor in its local system, replacing the dead
// instance registered under id. *System satisfies it (via Respawn), so wiring
// restart is just handing the supervisor the System.
type Respawner interface {
	Respawn(ctx context.Context, src, id string, input map[string]any) (state.ActorRef, error)
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
	def     Decision
	perSrc  map[string]Decision
	limits  map[string]int           // per-src restart budget (Restart and Backoff)
	backoff map[string]backoffPolicy // per-src backoff schedule (Backoff)
	clock   state.Clock
	sink    state.EscalationHandler

	mu        sync.Mutex
	respawner Respawner
	restarts  map[string]int // per-actor-id restarts already spent
	pending   []pendingRestart
	handled   []HandledEscalation
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

// WithRestart sets the Restart decision for failures of actors spawned from src,
// re-spawning the actor up to maxRestarts times (counted per actor id). When the
// budget is spent the failure escalates instead, so a crash-looping actor cannot
// restart-storm. Restart needs a Respawner wired with WithRespawner or SetRespawner.
func WithRestart(src string, maxRestarts int) SupervisorOption {
	return func(s *Supervisor) {
		s.perSrc[src] = Restart
		s.limits[src] = maxRestarts
	}
}

// WithRespawner wires the Respawner a Restart decision re-spawns through. The
// node's *System is the usual respawner. SetRespawner does the same after
// construction, for the common case where the System is built after the Supervisor.
func WithRespawner(r Respawner) SupervisorOption {
	return func(s *Supervisor) { s.respawner = r }
}

// NewSupervisor builds a Supervisor. Wire it into a system with
// ActorSystem.WithEscalationHandler(sup.Handle).
func NewSupervisor(opts ...SupervisorOption) *Supervisor {
	s := &Supervisor{
		perSrc:   make(map[string]Decision),
		limits:   make(map[string]int),
		backoff:  make(map[string]backoffPolicy),
		restarts: make(map[string]int),
	}
	for _, opt := range opts {
		opt(s)
	}
	if s.clock == nil {
		s.clock = state.SystemClock()
	}
	return s
}

// SetRespawner binds the Respawner a Restart decision re-spawns through, after
// construction. It is the ergonomic path when the System (the respawner) is built
// after the Supervisor, since the System's ActorSystem is wired with sup.Handle.
func (s *Supervisor) SetRespawner(r Respawner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respawner = r
}

// DecisionFor returns the decision configured for src, or the default.
func (s *Supervisor) DecisionFor(src string) Decision {
	if d, ok := s.perSrc[src]; ok {
		return d
	}
	return s.def
}

// Handle is the state.EscalationHandler the Supervisor exposes: it applies the
// src's decision — re-spawning for Restart (within budget), forwarding to the sink
// for Escalate, or containing it for Stop — and records the decision it actually
// applied. A Restart whose budget is spent or whose Respawner is missing escalates
// instead, and that fallthrough is what gets recorded. Wire it with
// ActorSystem.WithEscalationHandler(sup.Handle).
func (s *Supervisor) Handle(ctx context.Context, esc *state.ActorEscalation) {
	applied := s.DecisionFor(esc.Src)
	switch applied {
	case Restart:
		if !s.tryRestart(ctx, esc) {
			applied = Escalate // budget spent or no respawner: give up and propagate
		}
	case Backoff:
		if !s.scheduleBackoff(esc) {
			applied = Escalate // budget spent or no respawner
		}
	}
	if applied == Escalate && s.sink != nil {
		s.sink(ctx, esc)
	}

	s.mu.Lock()
	s.handled = append(s.handled, HandledEscalation{
		ActorID:  esc.ActorID,
		Src:      esc.Src,
		Decision: applied,
		Err:      esc.Err,
	})
	s.mu.Unlock()
}

// tryRestart re-spawns the failed actor if its src's restart budget is not yet
// spent and a Respawner is wired, reporting whether a restart was performed. The
// respawn runs outside the supervisor mutex so it may re-enter the system safely.
func (s *Supervisor) tryRestart(ctx context.Context, esc *state.ActorEscalation) bool {
	s.mu.Lock()
	if s.respawner == nil || s.restarts[esc.ActorID] >= s.limits[esc.Src] {
		s.mu.Unlock()
		return false
	}
	s.restarts[esc.ActorID]++
	respawner := s.respawner
	s.mu.Unlock()

	// A respawn that fails to start still counts against the budget; the next
	// failure (or its absence) is what the caller observes.
	_, _ = respawner.Respawn(ctx, esc.Src, esc.ActorID, nil)
	return true
}

// Forget discards the supervisor's per-actor restart bookkeeping for actorID: its
// spent-restart counter and any not-yet-applied backoff restart scheduled for it.
// A host calls it when an actor is permanently stopped (not restarted), so the
// supervisor's restart map does not accumulate one entry per distinct actor id for
// the process lifetime under churn. Forgetting an unknown id is a no-op. After
// Forget a re-spawn of the same id starts a fresh restart budget, so call it only
// for a genuine teardown, never between a failure and its restart.
func (s *Supervisor) Forget(actorID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.restarts, actorID)
	if len(s.pending) == 0 {
		return
	}
	kept := s.pending[:0]
	for _, p := range s.pending {
		if p.actorID != actorID {
			kept = append(kept, p)
		}
	}
	s.pending = kept
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
