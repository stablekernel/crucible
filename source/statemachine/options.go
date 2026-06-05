// SPDX-License-Identifier: Apache-2.0

package statemachine

import (
	"context"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/telemetry"
)

// Sink receives the effects a transition emitted, in emission order, in the same
// step that fired the transition and before the message is acked. It is the
// consume→transition→emit hand-off, declared as a tiny interface so this module
// never hard-depends on crucible/sink: a crucible/sink Manifold, a publisher, or
// any effect handler can be adapted to it. The default is a no-op
// ([discardSink]); wire one with [WithSink].
//
// Emit is called once per emitted effect. A non-nil error fails the step before
// the ack, so the message is redelivered ([source.Nak]); an emit that must not
// block the transition should swallow its own errors.
type Sink interface {
	// Emit hands one transition effect to the sink. The effect's concrete type is a
	// crucible/state effect value (for example state.SendTo); a handler type-switches
	// on it. Returning an error nak's the message.
	Emit(ctx context.Context, effect any) error
}

// SinkFunc adapts a plain function to a [Sink].
type SinkFunc func(ctx context.Context, effect any) error

// Emit calls the underlying function.
func (f SinkFunc) Emit(ctx context.Context, effect any) error { return f(ctx, effect) }

// discardSink is the no-op default Sink: it accepts and drops every effect.
type discardSink struct{}

func (discardSink) Emit(context.Context, any) error { return nil }

// EventID extracts the idempotency id of an inbound message: the stable
// per-message identifier the exactly-once dedup keys on. A redelivery of the
// same logical message must yield the same id. The default reads the
// "message-id" header ([DefaultEventIDHeader]) and falls back to the message
// [source.Cursor] string; override it with [WithEventID] to read a different
// header or derive an id from the decoded value.
//
// An empty id disables dedup for that message: with no id to compare against the
// persisted LastEventID, a redelivery re-fires the transition. The bindings
// surface this on their span (the "statemachine.exactly_once" attribute is false)
// so a message stream that yields no id is visible in traces rather than silently
// losing the guarantee. Return a non-empty stable id to keep exactly-once.
type EventID func(m source.Message) string

// DefaultEventIDHeader is the header [DefaultEventID] reads a message's
// idempotency id from when present.
const DefaultEventIDHeader = "message-id"

// DefaultEventID is the EventID used when none is configured: the
// [DefaultEventIDHeader] header value if present, else the message's cursor
// string. The cursor is a stream-local coordinate, so the fallback is unique
// within a stream but not across re-published messages — supply [WithEventID]
// with a domain id (an order id, a CloudEvents id) for cross-stream dedup.
func DefaultEventID(m source.Message) string {
	if id, ok := m.Headers().Get(DefaultEventIDHeader); ok && id != "" {
		return id
	}
	if c := m.Cursor(); c != nil {
		return c.String()
	}
	return ""
}

// Option configures a [Drive] or [DriveFunc] binding. Options are additive: a
// new capability arrives as a new option, never a changed signature.
type Option func(*config)

type config struct {
	sink     Sink
	eventID  EventID
	tracer   telemetry.Tracer
	spanName string
}

func newConfig(opts ...Option) config {
	cfg := config{
		sink:     discardSink{},
		eventID:  DefaultEventID,
		tracer:   telemetry.NopTracer(),
		spanName: "statemachine.drive",
	}
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}

// WithSink sets the [Sink] a transition's emitted effects are handed to, in the
// same step, before the ack. The default discards effects. A nil sink is ignored.
func WithSink(s Sink) Option {
	return func(c *config) {
		if s != nil {
			c.sink = s
		}
	}
}

// WithEventID sets the [EventID] extractor used for exactly-once dedup. The
// default reads the [DefaultEventIDHeader] header and falls back to the cursor;
// supply a domain id for dedup across re-published streams. A nil extractor is
// ignored.
func WithEventID(fn EventID) Option {
	return func(c *config) {
		if fn != nil {
			c.eventID = fn
		}
	}
}

// WithTracer sets the tracer the binding starts a per-message span on (decode,
// route, fire, emit, persist). The default is [telemetry.NopTracer]. Wire the
// same tracer the [Sink] uses so emit spans nest under the drive span. A nil
// tracer is ignored.
func WithTracer(t telemetry.Tracer) Option {
	return func(c *config) {
		if t != nil {
			c.tracer = t
		}
	}
}

// WithSpanName overrides the per-message span name (default "statemachine.drive").
func WithSpanName(name string) Option {
	return func(c *config) {
		if name != "" {
			c.spanName = name
		}
	}
}
