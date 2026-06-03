// SPDX-License-Identifier: Apache-2.0

package source

// Middleware decorates a [Handler], returning a new Handler that wraps the
// original. It is the composition seam for cross-cutting concerns — retry
// classification, dead-letter routing, idempotency, schema validation,
// tracing — each shipped as its own opt-in decorator rather than baked into the
// engine. A middleware may run logic before the inner handler, after it (by
// inspecting the returned [Result]), or short-circuit it entirely.
type Middleware func(Handler) Handler

// Chain composes middleware around h and returns the resulting [Handler]. The
// first middleware in the list is the outermost: it runs first on the way in and
// last on the way out. Composing
//
//	Chain(h, A, B, C)
//
// yields A(B(C(h))), so a message flows A → B → C → h and the [Result] returns
// h → C → B → A. With no middleware, Chain returns h unchanged.
func Chain(h Handler, mw ...Middleware) Handler {
	// Apply in reverse so the first listed middleware ends up outermost.
	for i := len(mw) - 1; i >= 0; i-- {
		if m := mw[i]; m != nil {
			h = m(h)
		}
	}
	return h
}
