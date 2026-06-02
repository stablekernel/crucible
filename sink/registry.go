// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"reflect"
	"sync"
)

// Registry maps a payload's concrete type to a transformer producing a value of
// type D (typically Op[C] for some destination client C). There is no
// package-level registry and no init-time state: every Registry is constructed
// with NewRegistry and injected, so two registries never share entries. It is
// safe for concurrent Register and Lookup.
type Registry[D any] struct {
	mu    sync.RWMutex
	table map[reflect.Type]func(context.Context, any) D
}

// NewRegistry returns an empty Registry.
func NewRegistry[D any]() *Registry[D] {
	return &Registry[D]{table: make(map[reflect.Type]func(context.Context, any) D)}
}

// Register binds payload type P to fn in r. It is a free function rather than a
// method because Go does not permit type parameters on methods; P is inferred
// from fn. Registering the same P again overwrites the prior transformer.
func Register[P any, D any](r *Registry[D], fn func(ctx context.Context, p P) D) {
	key := reflect.TypeOf((*P)(nil)).Elem()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.table[key] = func(ctx context.Context, payload any) D {
		return fn(ctx, payload.(P))
	}
}

// Lookup returns the transformer registered for payload's concrete type and
// whether one was found. The returned func accepts payload as any and asserts it
// back to the registered concrete type internally.
func (r *Registry[D]) Lookup(payload any) (func(context.Context, any) D, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	fn, ok := r.table[reflect.TypeOf(payload)]
	return fn, ok
}
