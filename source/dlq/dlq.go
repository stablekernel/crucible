// SPDX-License-Identifier: Apache-2.0

// Package dlq provides dead-letter routing for the crucible source seam: a
// [source.Middleware] that captures a terminal failure (a [source.ActionTerm]) and
// publishes it, with typed reason metadata, to a caller-supplied [DeadLetter]
// destination. The core stays backend-neutral — the package never names Kafka, a
// DLQ topic, or a database. The caller injects whatever publishes the parked
// message; this package only decides what to park and stamps a typed
// [DeadLetterRecord] onto it.
//
// A parking store is itself a replayable [source.Inlet]: park a message here, and
// later drain the parking destination back through the same [source.Handler] with
// its attempt count reset. [MemDeadLetter] is an in-memory implementation that is
// both a [DeadLetter] sink and a [source.Inlet], so the round-trip is unit-testable
// with no infrastructure.
package dlq

import (
	"context"
	"errors"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/retry"
)

// DeadLetter is the caller-provided destination a terminal message is parked to.
// It is the single dependency the middleware reaches through, so the core stays
// neutral: a Kafka producer to a DLQ topic, a database table writer, or the
// in-memory [MemDeadLetter] all satisfy it. Implementations must be safe for
// concurrent use; the Hopper may publish from several lanes at once.
type DeadLetter interface {
	// Park persists one dead-lettered record. Returning an error tells the
	// middleware the park failed, which it surfaces so the engine can decide
	// whether to retry settlement rather than silently dropping the message.
	Park(ctx context.Context, rec DeadLetterRecord) error
}

// DeadLetterRecord is the typed envelope stamped onto a parked message. It
// captures everything needed to triage the failure and to replay the message
// later: the original payload and routing, the failure [source.Classification]
// and reason, the attempt count reached, and the last error's text. It is a
// value type with named fields — never a magic-string metadata map.
type DeadLetterRecord struct {
	// Key is the original message key (partition/routing key), copied so the
	// record does not alias inlet state.
	Key []byte
	// Value is the original raw payload bytes.
	Value []byte
	// Headers are the original message headers.
	Headers source.Headers
	// Subject is the topic or subject the message originally arrived on.
	Subject string
	// PartitionKey is the original ordering domain, preserved so a replay can
	// re-shard onto the same lane.
	PartitionKey string
	// Class is the failure classification that sent the message to dead-letter
	// ([source.Poison], [source.InvalidForState], or [source.Retryable] when retries
	// were exhausted).
	Class source.Classification
	// Reason is a stable, human-readable summary of why the message was parked
	// (the classification name), for dashboards and triage without parsing Err.
	Reason string
	// Attempts is the number of delivery attempts the message reached before it
	// was parked, read from the retry attempt counter.
	Attempts int
	// LastError is the text of the final error, captured for triage. The package
	// stores the rendered string rather than the error value so a record can be
	// serialized to any backend without a custom codec.
	LastError string
}

type config struct {
	parkRetryable bool
}

func defaultConfig() config {
	// By default, retryable-but-exhausted terminations are parked too: the retry
	// middleware escalates an exhausted Retryable failure to Term, and the whole
	// point of a DLQ is to catch those. Poison/InvalidForState are always parked.
	return config{parkRetryable: true}
}

// Option configures the dead-letter [Middleware]. Options are additive with
// sensible defaults.
type Option func(*config)

// WithParkRetryable controls whether a terminal failure still classified
// [source.Retryable] (a retry-exhausted message escalated to Term) is parked.
// The default is true. Set false to park only genuinely permanent failures
// ([source.Poison]/[source.InvalidForState]) and let exhausted retries terminate
// without a parked record.
func WithParkRetryable(park bool) Option {
	return func(c *config) { c.parkRetryable = park }
}

// Middleware returns a [source.Middleware] that routes terminal failures to dl.
// When the wrapped handler returns a [source.ActionTerm], the middleware builds a
// [DeadLetterRecord] — stamping the classification, reason, attempt count (read
// from the retry context counter), original headers, last error, and original
// subject — and calls [DeadLetter.Park]. Non-terminal results (ack, nak, drop,
// in-progress, manual) pass through untouched.
//
// If Park fails, the middleware returns a [source.ActionNak] so the engine can
// re-attempt settlement rather than losing the message; the original error is
// joined with the park error. A nil dl makes the middleware a pass-through.
func Middleware(dl DeadLetter, opts ...Option) source.Middleware {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return func(next source.Handler) source.Handler {
		if dl == nil {
			return next
		}
		return func(ctx context.Context, m source.Message) source.Result {
			res := next(ctx, m)
			if res.Action != source.ActionTerm {
				return res
			}
			if res.Class == source.Retryable && !cfg.parkRetryable {
				return res
			}

			attempts, _ := retry.Attempt(ctx)
			rec := newRecord(m, res, attempts)
			if err := dl.Park(ctx, rec); err != nil {
				// Do not lose the message: ask the engine to re-settle. Surface
				// both the original failure and the park failure.
				return source.Result{
					Action: source.ActionNak,
					Class:  source.Retryable,
					Err:    errors.Join(res.Err, err),
				}
			}
			return res
		}
	}
}

// newRecord builds the typed dead-letter envelope from a message and its terminal
// result, copying byte slices so the record never aliases inlet-owned buffers.
func newRecord(m source.Message, res source.Result, attempts int) DeadLetterRecord {
	rec := DeadLetterRecord{
		Key:          cloneBytes(m.Key()),
		Value:        cloneBytes(m.Value()),
		Headers:      cloneHeaders(m.Headers()),
		Subject:      m.Subject(),
		PartitionKey: m.PartitionKey(),
		Class:        res.Class,
		Reason:       res.Class.String(),
		Attempts:     attempts,
	}
	if res.Err != nil {
		rec.LastError = res.Err.Error()
	}
	return rec
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

func cloneHeaders(h source.Headers) source.Headers {
	if len(h) == 0 {
		return nil
	}
	out := make(source.Headers, len(h))
	copy(out, h)
	return out
}
