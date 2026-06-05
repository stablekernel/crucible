// SPDX-License-Identifier: Apache-2.0

// Package retry provides a classification-aware retry [source.Middleware] for the
// crucible source seam. It inspects the [source.Result] a handler returns and,
// for a transient ([source.Retryable]) failure, converts it into a delayed
// redelivery whose backoff grows with the attempt count — while a permanent
// failure ([source.Poison]/[source.InvalidForState]) passes straight through to
// terminate without wasting redeliveries, and a success or drop is left
// untouched.
//
// The attempt count is carried on a typed context value ([WithAttempt]/[Attempt])
// rather than a magic-string header, and the next attempt is propagated to inner
// middleware (notably source/dlq) by the same channel. The backoff schedule is
// fully config-driven through functional options; nothing is hardcoded.
package retry

import (
	"context"
	"math"
	"math/rand/v2"
	"time"

	"github.com/stablekernel/crucible/source"
)

// attemptKey is the unexported context key type the attempt count travels on, so
// it never collides with another package's context values.
type attemptKey struct{}

// WithAttempt returns a child context carrying attempt n. The [Middleware]
// reads this on entry to decide the backoff and writes the incremented value
// before redelivery; tests and the dead-letter middleware read it via [Attempt].
func WithAttempt(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, attemptKey{}, n)
}

// Attempt returns the attempt number carried on ctx and whether one was present.
// The first delivery is attempt 1; an absent value reports (1, false) so a caller
// can treat a fresh message as its first attempt without special-casing.
func Attempt(ctx context.Context) (int, bool) {
	if v, ok := ctx.Value(attemptKey{}).(int); ok {
		return v, true
	}
	return 1, false
}

// Backoff computes the redelivery delay for a given attempt. It is the seam a
// caller overrides through [WithBackoff] (or [WithBackoffFunc]) to shape the
// schedule; the default is exponential with full jitter.
type Backoff interface {
	// Delay returns the wait before attempt n+1 given that attempt n just failed.
	// attempt is 1-based; the returned delay is non-negative.
	Delay(attempt int) time.Duration
}

// BackoffFunc adapts a function to the [Backoff] interface.
type BackoffFunc func(attempt int) time.Duration

// Delay implements [Backoff].
func (f BackoffFunc) Delay(attempt int) time.Duration { return f(attempt) }

// expBackoff is the default exponential-with-jitter schedule: base*factor^(n-1),
// capped at max, with optional full jitter applied through the injected rand.
type expBackoff struct {
	base   time.Duration
	max    time.Duration
	factor float64
	jitter bool
	randF  func() float64
}

// Delay implements [Backoff].
func (b expBackoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(b.base) * math.Pow(b.factor, float64(attempt-1))
	if b.max > 0 && d > float64(b.max) {
		d = float64(b.max)
	}
	if b.jitter {
		d *= b.randF()
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

type config struct {
	maxAttempts int
	backoff     Backoff
}

func defaultConfig() config {
	return config{
		maxAttempts: 5,
		backoff: expBackoff{
			base:   100 * time.Millisecond,
			max:    30 * time.Second,
			factor: 2.0,
			jitter: true,
			randF:  rand.Float64,
		},
	}
}

// Option configures the retry [Middleware]. Options are additive with sensible
// defaults; a nil or out-of-range value is ignored, leaving the default in place.
type Option func(*config)

// WithMaxAttempts caps the number of delivery attempts. When a Retryable failure
// occurs on the final attempt, the message is terminated ([source.Term]) instead
// of redelivered, so it can fall through to dead-letter middleware. The default
// is 5. A value < 1 is ignored.
func WithMaxAttempts(n int) Option {
	return func(c *config) {
		if n >= 1 {
			c.maxAttempts = n
		}
	}
}

// WithBackoff sets an exponential-with-optional-jitter schedule: the delay before
// attempt n+1 is base*factor^(n-1), capped at maxDelay. With jitter true the delay
// is scaled by a uniform random factor in [0,1) (full jitter) to de-correlate
// retries across consumers. A base <= 0 or factor < 1 is ignored.
func WithBackoff(base, maxDelay time.Duration, factor float64, jitter bool) Option {
	return func(c *config) {
		if base <= 0 || factor < 1 {
			return
		}
		c.backoff = expBackoff{
			base:   base,
			max:    maxDelay,
			factor: factor,
			jitter: jitter,
			randF:  rand.Float64,
		}
	}
}

// WithBackoffFunc installs a custom [Backoff], overriding the exponential
// default for callers who need a bespoke schedule (a fixed delay, a Fibonacci
// curve, a table). A nil backoff is ignored.
func WithBackoffFunc(b Backoff) Option {
	return func(c *config) {
		if b != nil {
			c.backoff = b
		}
	}
}

// WithJitterSource injects the [0,1) random source used for full jitter, so a
// test can make a jittered schedule deterministic. It applies to the exponential
// schedules this package builds (the default and [WithBackoff]); it has no effect
// on a custom [Backoff] installed with [WithBackoffFunc], which owns its own
// randomness. Order matters: apply WithJitterSource after the WithBackoff it
// should patch. A nil source is ignored.
func WithJitterSource(randF func() float64) Option {
	return func(c *config) {
		if randF == nil {
			return
		}
		if b, ok := c.backoff.(expBackoff); ok {
			b.randF = randF
			c.backoff = b
		}
	}
}

// Middleware returns a [source.Middleware] that applies classification-aware
// retry to the handler it wraps:
//
//   - [source.Retryable] (an [source.ActionNak]) below the attempt cap becomes a
//     delayed redelivery ([source.NakAfter]) using the configured backoff, and the
//     incremented attempt is threaded onto the context for the next delivery.
//   - [source.Retryable] at the attempt cap is escalated to [source.ActionTerm]
//     (classification preserved as Retryable) so it falls through to dead-letter.
//   - [source.Poison] and [source.InvalidForState] ([source.ActionTerm]) pass
//     through unchanged: a permanently-bad or wrong-state message is never retried.
//   - A success ([source.ActionAck]) or [source.Drop] is returned untouched.
//
// The attempt count is read from the context ([Attempt]); a fresh message is
// attempt 1. The middleware never sleeps — it returns a [source.Result] the
// engine acts on — so it adds no latency of its own and is safe under the
// Hopper's per-lane concurrency.
func Middleware(opts ...Option) source.Middleware {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return func(next source.Handler) source.Handler {
		return func(ctx context.Context, m source.Message) source.Result {
			res := next(ctx, m)

			// Only nak/retryable failures are the retry middleware's business.
			if res.Action != source.ActionNak || res.Class != source.Retryable {
				return res
			}

			attempt, _ := Attempt(ctx)
			if attempt >= cfg.maxAttempts {
				// Exhausted: terminate so dead-letter middleware can park it,
				// keeping the Retryable class so the reason reads as "gave up
				// retrying" rather than "poison".
				return source.Result{
					Action: source.ActionTerm,
					Class:  source.Retryable,
					Err:    res.Err,
				}
			}

			delay := cfg.backoff.Delay(attempt)
			return source.NakAfter(delay, res.Err)
		}
	}
}
