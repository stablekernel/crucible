// SPDX-License-Identifier: Apache-2.0

package sink

import "context"

// Emitter binds a typed destination client C and a registry of Op[C] into an
// Outlet. Named destination packages (sql, dynamo, …) wrap it with a narrow
// client interface and a default registry; any consumer can also use it directly
// with their own client type, no new package required.
//
// On Sink, the Emitter looks up the payload's transformer, builds the Op, and
// applies it to the client. A lookup miss returns ErrUnregistered (which the
// Manifold treats as a silent skip); an Apply failure is wrapped as *Error
// with PhaseApply.
type Emitter[C any] struct {
	name     string
	client   C
	registry *Registry[Op[C]]
}

// EmitterOption configures an Emitter.
type EmitterOption func(*emitterConfig)

type emitterConfig struct{ name string }

// WithName sets the outlet name used in errors and logs. The default is the
// type name of the client C.
func WithName(name string) EmitterOption {
	return func(c *emitterConfig) {
		if name != "" {
			c.name = name
		}
	}
}

// NewEmitter binds client and registry into an Emitter. The registry maps each
// payload type to the Op that persists it against C.
func NewEmitter[C any](client C, registry *Registry[Op[C]], opts ...EmitterOption) *Emitter[C] {
	cfg := emitterConfig{name: typeName(client)}
	for _, o := range opts {
		o(&cfg)
	}
	return &Emitter[C]{name: cfg.name, client: client, registry: registry}
}

// Sink looks up payload's Op and applies it to the bound client.
func (e *Emitter[C]) Sink(ctx context.Context, payload any) error {
	transform, ok := e.registry.Lookup(payload)
	if !ok {
		return ErrUnregistered
	}
	op := transform(ctx, payload)
	if err := op.Apply(ctx, e.client); err != nil {
		return &Error{Outlet: e.name, Phase: PhaseApply, PayloadType: typeName(payload), Err: err}
	}
	return nil
}

var _ Outlet = (*Emitter[any])(nil)
