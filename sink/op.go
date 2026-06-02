// SPDX-License-Identifier: Apache-2.0

package sink

import "context"

// Op is a unit of work against a typed destination client C. A destination
// package ships Op constructors covering its API surface (puts, updates,
// deletes, transactional and batch writes); the registry maps a payload type to
// the Op that persists it.
type Op[C any] interface {
	Apply(ctx context.Context, client C) error
}

// OpFunc adapts a plain function to an Op. It is the bring-your-own-logic escape
// hatch: any func with the right shape becomes an Op without a named type.
type OpFunc[C any] func(ctx context.Context, client C) error

// Apply calls the underlying function.
func (f OpFunc[C]) Apply(ctx context.Context, client C) error { return f(ctx, client) }
