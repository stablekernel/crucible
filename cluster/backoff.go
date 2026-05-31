package cluster

import (
	"context"
	"math"
	"time"

	"github.com/stablekernel/crucible/state"
)

// backoffPolicy is the per-src exponential restart schedule for the Backoff
// decision: up to maxRestarts re-spawns, the nth delayed by initial*factor^n
// capped at max.
type backoffPolicy struct {
	maxRestarts int
	initial     time.Duration
	max         time.Duration
	factor      float64
}

// pendingRestart is a scheduled re-spawn awaiting its due time.
type pendingRestart struct {
	actorID string
	src     string
	dueAt   time.Time
}

// WithBackoff sets the Backoff decision for failures of actors spawned from src:
// the actor is re-spawned up to maxRestarts times, the nth restart deferred by
// initial*factor^n (n counted from zero), capped at max. When the budget is spent
// the failure escalates. Backoff needs a Respawner (WithRespawner / SetRespawner)
// and reads time through the supervisor's clock (WithClock, default the system
// clock); the host applies due restarts with Tick.
func WithBackoff(src string, maxRestarts int, initial, maxDelay time.Duration, factor float64) SupervisorOption {
	return func(s *Supervisor) {
		s.perSrc[src] = Backoff
		s.limits[src] = maxRestarts
		s.backoff[src] = backoffPolicy{maxRestarts: maxRestarts, initial: initial, max: maxDelay, factor: factor}
	}
}

// WithClock sets the time source the Backoff decision schedules against. Without
// it a Supervisor uses the system clock; a test wires a fake clock to drive
// backoff deterministically.
func WithClock(c state.Clock) SupervisorOption {
	return func(s *Supervisor) { s.clock = c }
}

// scheduleBackoff records a deferred re-spawn for the failed actor if its src's
// budget is not yet spent and a Respawner is wired, reporting whether one was
// scheduled. The delay grows with the restart count already spent for the actor.
func (s *Supervisor) scheduleBackoff(esc *state.ActorEscalation) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.respawner == nil {
		return false
	}
	n := s.restarts[esc.ActorID]
	pol := s.backoff[esc.Src]
	if n >= pol.maxRestarts {
		return false
	}
	s.restarts[esc.ActorID]++
	s.pending = append(s.pending, pendingRestart{
		actorID: esc.ActorID,
		src:     esc.Src,
		dueAt:   s.clock.Now().Add(backoffDelay(pol, n)),
	})
	return true
}

// backoffDelay is initial*factor^n capped at max (and floored at initial when the
// factor would shrink it).
func backoffDelay(pol backoffPolicy, n int) time.Duration {
	d := float64(pol.initial) * math.Pow(pol.factor, float64(n))
	if d < float64(pol.initial) {
		d = float64(pol.initial)
	}
	if pol.max > 0 && d > float64(pol.max) {
		d = float64(pol.max)
	}
	return time.Duration(d)
}

// Tick performs every scheduled backoff restart that is now due, re-spawning each
// through the Respawner, and returns how many it restarted. A host calls it from
// its own timer loop (or a test after advancing a fake clock); it is a no-op when
// nothing is due.
func (s *Supervisor) Tick(ctx context.Context) int {
	now := s.clock.Now()
	s.mu.Lock()
	respawner := s.respawner
	var due, keep []pendingRestart
	for _, p := range s.pending {
		if p.dueAt.After(now) {
			keep = append(keep, p)
		} else {
			due = append(due, p)
		}
	}
	s.pending = keep
	s.mu.Unlock()

	for _, p := range due {
		_, _ = respawner.Respawn(ctx, p.src, p.actorID, nil)
	}
	return len(due)
}
