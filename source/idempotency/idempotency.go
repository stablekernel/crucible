// SPDX-License-Identifier: Apache-2.0

// Package idempotency provides a deduplicating [source.Middleware] for the
// crucible source seam: it suppresses re-processing of a message whose
// idempotency key has already been handled, acking the duplicate ([source.Skip],
// classified [source.Drop]) without re-running the handler. It is backed by the
// core [source.Deduper] capability, or any caller-supplied [Store]; the no-op
// default deduplicates nothing, so adding the middleware without a store is safe
// and inert.
//
// The dedup key is derived by a configurable [KeyFunc] read from the
// [source.Message] (default: the message key, falling back to a header), never a
// magic-string lookup baked into the engine. For the state-machine binding the
// key is the machine's state version, making redelivery provably idempotent with
// no external store; this package supplies the generic, store-backed form.
package idempotency

import (
	"context"

	"github.com/stablekernel/crucible/source"
)

// DefaultKeyHeader is the typed header key the default [KeyFunc] reads an
// idempotency key from when the message carries no [source.Message.Key]. It is a
// named constant so producers and consumers agree on one key without an inline
// string.
const DefaultKeyHeader = "crucible-idempotency-key"

// Store records and reports which idempotency keys have been processed. It is the
// caller-provided seam the middleware deduplicates against — an in-memory set, a
// Redis SETNX, a database unique index. It mirrors [source.Deduper] so any
// existing Deduper is also a Store via [FromDeduper]. Implementations must be
// safe for concurrent use.
type Store interface {
	// Seen reports whether key has already been processed and, if not, records it
	// — atomically, so two concurrent deliveries of the same key cannot both
	// observe false. A non-nil error is treated as "unknown": the middleware fails
	// open and lets the handler run rather than dropping a possibly-new message.
	Seen(ctx context.Context, key string) (seen bool, err error)
}

// KeyFunc derives the idempotency key for a message and reports whether a key
// could be derived. A false ok means "no key" — the middleware then lets the
// message through undeduplicated rather than collapsing all keyless messages onto
// one empty key.
type KeyFunc func(m source.Message) (key string, ok bool)

// DefaultKeyFunc derives the key from the message key bytes, falling back to the
// [DefaultKeyHeader] header, and reports ok=false when neither is present.
func DefaultKeyFunc(m source.Message) (string, bool) {
	if k := m.Key(); len(k) > 0 {
		return string(k), true
	}
	if v, present := m.Headers().Get(DefaultKeyHeader); present && v != "" {
		return v, true
	}
	return "", false
}

type config struct {
	store   Store
	keyFunc KeyFunc
}

func defaultConfig() config {
	return config{
		store:   nil, // no-op: deduplicates nothing
		keyFunc: DefaultKeyFunc,
	}
}

// Option configures the idempotency [Middleware]. Options are additive with
// no-op defaults.
type Option func(*config)

// WithStore sets the [Store] the middleware deduplicates against. The default is
// none (a pass-through). A nil store is ignored.
func WithStore(s Store) Option {
	return func(c *config) {
		if s != nil {
			c.store = s
		}
	}
}

// WithDeduper sets the dedup backend from a core [source.Deduper], adapting it to
// a [Store]. The default is none. A nil deduper is ignored.
func WithDeduper(d source.Deduper) Option {
	return func(c *config) {
		if d != nil {
			c.store = FromDeduper(d)
		}
	}
}

// WithKeyFunc sets the [KeyFunc] that extracts the idempotency key from a
// message. The default is [DefaultKeyFunc]. A nil function is ignored.
func WithKeyFunc(f KeyFunc) Option {
	return func(c *config) {
		if f != nil {
			c.keyFunc = f
		}
	}
}

// FromDeduper adapts a [source.Deduper] to a [Store], so the engine's dedup
// capability and this middleware share one backend.
func FromDeduper(d source.Deduper) Store { return deduperStore{d} }

type deduperStore struct{ d source.Deduper }

func (s deduperStore) Seen(ctx context.Context, key string) (bool, error) {
	return s.d.Seen(ctx, key)
}

// Middleware returns a [source.Middleware] that deduplicates by idempotency key.
// Before invoking the wrapped handler it derives the key ([KeyFunc]) and asks the
// [Store] whether it has been seen:
//
//   - already seen: the handler is skipped and the message is acked-and-dropped
//     ([source.Skip]); the duplicate is settled without side effects.
//   - not seen (now recorded): the handler runs normally and its [source.Result]
//     is returned unchanged.
//   - no derivable key, or a store error: the middleware fails open and runs the
//     handler, never silently dropping a message it cannot confidently call a
//     duplicate.
//
// With no store configured the middleware is a pass-through, so it is safe to add
// unconditionally and supply a store later.
func Middleware(opts ...Option) source.Middleware {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return func(next source.Handler) source.Handler {
		if cfg.store == nil {
			return next
		}
		return func(ctx context.Context, m source.Message) source.Result {
			key, ok := cfg.keyFunc(m)
			if !ok {
				return next(ctx, m)
			}
			seen, err := cfg.store.Seen(ctx, key)
			if err != nil {
				// Fail open: better to risk re-processing than to drop.
				return next(ctx, m)
			}
			if seen {
				return source.Skip()
			}
			return next(ctx, m)
		}
	}
}
