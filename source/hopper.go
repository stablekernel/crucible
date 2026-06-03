// SPDX-License-Identifier: Apache-2.0

package source

import (
	"context"
	"errors"
	"hash/fnv"
	"io"
	"log/slog"
	"sync"

	"github.com/stablekernel/crucible/telemetry"
)

// Hopper is the consume engine: it drives a [Subscription] with bounded,
// per-key-ordered concurrency, decodes each payload, runs the middleware chain,
// invokes the [Handler], and settles the message according to the [Result] the
// handler returns. It mirrors sink.Manifold — the one orchestrator that owns the
// hard parts (ordering, backpressure, settle) so every [Inlet] adapter stays
// thin.
//
// Ordering. Each message is routed to an ordered lane keyed by its
// [Message.PartitionKey] (or, when that is empty, by a hash of [Message.Key]).
// A lane is a single goroutine that processes its queue strictly in arrival
// order, so two messages with the same key are never reordered, while distinct
// keys run in parallel up to [WithConcurrency]. This is the guarantee a
// statechart instance needs: its events arrive in order.
//
// Backpressure. [WithMaxInFlight] bounds the messages delivered but not yet
// settled; when the window is full the fetch loop blocks before pulling the next
// message, so a slow handler throttles the subscription rather than buffering
// unboundedly.
//
// Lifecycle. Run consumes until the context is canceled, the subscription
// drains ([ErrDrained]), or Close is called. On a clean drain it returns nil; in
// every shutdown path it stops fetching, lets in-flight messages finish and
// settle, and then returns. Construct with [New]; the zero value is unusable.
type Hopper struct {
	name        string
	logger      *slog.Logger
	tracer      telemetry.Tracer
	registry    *Registry
	middleware  []Middleware
	concurrency int
	maxInFlight int

	received telemetry.Counter
	acked    telemetry.Counter
	nak      telemetry.Counter
	term     telemetry.Counter
	dropped  telemetry.Counter
	failed   telemetry.Counter
	lag      telemetry.Gauge

	closeOnce sync.Once
	closed    chan struct{}
}

// New constructs a Hopper with the given options. With no options it runs a
// single ordered lane, an unbounded in-flight window, no codec (the raw message
// reaches the handler), and is silent and untraced.
func New(opts ...Option) *Hopper {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	m := cfg.meter
	return &Hopper{
		name:        cfg.name,
		logger:      cfg.logger,
		tracer:      cfg.tracer,
		registry:    cfg.registry,
		middleware:  append([]Middleware(nil), cfg.middleware...),
		concurrency: cfg.concurrency,
		maxInFlight: cfg.maxInFlight,
		received:    m.Counter("source.received", telemetry.WithDescription("messages received from the subscription")),
		acked:       m.Counter("source.acked", telemetry.WithDescription("messages acknowledged after successful handling")),
		nak:         m.Counter("source.nak", telemetry.WithDescription("messages nak'd for redelivery")),
		term:        m.Counter("source.term", telemetry.WithDescription("messages terminated (dead-lettered)")),
		dropped:     m.Counter("source.dropped", telemetry.WithDescription("messages acked and discarded as out of scope")),
		failed:      m.Counter("source.failed", telemetry.WithDescription("settle failures observed after handling")),
		lag:         m.Gauge("source.lag", telemetry.WithUnit("{message}"), telemetry.WithDescription("unconsumed messages behind the stream tail")),
		closed:      make(chan struct{}),
	}
}

// Receive subscribes to in with cfg and drives the resulting subscription with
// h, a convenience for the common Subscribe-then-Run path. It closes the
// subscription on return.
func (hp *Hopper) Receive(ctx context.Context, in Inlet, cfg SubscribeConfig, h Handler) error {
	sub, err := in.Subscribe(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Close() }()
	return hp.run(ctx, sub, h)
}

// Run drives sub with h until the context is canceled, the subscription drains,
// or the Hopper is closed. It returns nil on a clean drain ([ErrDrained]) or a
// context cancellation, and the underlying error otherwise. Run does not close
// sub; the caller (or [Receive]) owns that.
func (hp *Hopper) Run(ctx context.Context, sub Subscription, h Handler) error {
	return hp.run(ctx, sub, h)
}

// lane is a single ordered worker: a queue channel feeding one goroutine that
// processes messages strictly in arrival order.
type lane struct {
	queue chan *delivery
}

// delivery couples a message with the subscription it must be settled against.
type delivery struct {
	msg Message
	sub Subscription
}

func (hp *Hopper) run(ctx context.Context, sub Subscription, h Handler) error {
	// Bind the configured codec and middleware around the handler once.
	handler := Chain(h, hp.middleware...)

	// runCtx cancels both the fetch loop and the in-flight work when either the
	// caller cancels, the Hopper closes, or the fetch loop exits.
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Tie Close into runCtx: closing the Hopper cancels the run.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-hp.closed:
			cancel()
		case <-stop:
		}
	}()

	// inFlight bounds delivered-but-unsettled messages (backpressure). nil means
	// unbounded.
	var inFlight chan struct{}
	if hp.maxInFlight > 0 {
		inFlight = make(chan struct{}, hp.maxInFlight)
	}

	// laneSlots bounds the number of lanes actively processing in parallel.
	laneSlots := make(chan struct{}, hp.concurrency)

	var (
		mu      sync.Mutex
		lanes   = make(map[uint64]*lane)
		laneWG  sync.WaitGroup
		workWG  sync.WaitGroup
		runErr  error
		errOnce sync.Once
	)

	// laneFor returns the ordered lane for key, starting its goroutine on first
	// use. Callers hold no lock; laneFor takes mu.
	laneFor := func(key uint64) *lane {
		mu.Lock()
		defer mu.Unlock()
		if l, ok := lanes[key]; ok {
			return l
		}
		l := &lane{queue: make(chan *delivery, 1)}
		lanes[key] = l
		laneWG.Add(1)
		go func() {
			defer laneWG.Done()
			for d := range l.queue {
				hp.process(runCtx, handler, d)
				if inFlight != nil {
					<-inFlight
				}
				<-laneSlots
				workWG.Done()
			}
		}()
		return l
	}

	defer func() {
		// Drain: stop accepting new work, close every lane queue so the lane
		// goroutines exit once their queues empty, then wait.
		mu.Lock()
		for _, l := range lanes {
			close(l.queue)
		}
		mu.Unlock()
		workWG.Wait()
		laneWG.Wait()
	}()

	for {
		// Backpressure: reserve an in-flight slot before fetching, so a full
		// window blocks the fetch loop instead of buffering unboundedly.
		if inFlight != nil {
			select {
			case inFlight <- struct{}{}:
			case <-runCtx.Done():
				return hp.exitErr(ctx, runErr)
			}
		}

		msg, err := sub.Next(runCtx)
		if err != nil {
			if inFlight != nil {
				<-inFlight // release the slot we reserved but did not use
			}
			if errors.Is(err, ErrDrained) || errors.Is(err, context.Canceled) || runCtx.Err() != nil {
				return hp.exitErr(ctx, runErr)
			}
			errOnce.Do(func() { runErr = err })
			return hp.exitErr(ctx, runErr)
		}

		hp.received.Add(runCtx, 1)
		hp.reportLag(runCtx, sub)

		// Reserve a lane slot (cross-key parallelism bound) before enqueueing, so
		// the number of lanes draining concurrently never exceeds concurrency.
		select {
		case laneSlots <- struct{}{}:
		case <-runCtx.Done():
			if inFlight != nil {
				<-inFlight
			}
			return hp.exitErr(ctx, runErr)
		}

		workWG.Add(1)
		l := laneFor(hp.laneKey(msg))
		select {
		case l.queue <- &delivery{msg: msg, sub: sub}:
		case <-runCtx.Done():
			// Unwind the reservations for the message we never enqueued.
			workWG.Done()
			<-laneSlots
			if inFlight != nil {
				<-inFlight
			}
			return hp.exitErr(ctx, runErr)
		}
	}
}

// exitErr maps a run's terminal error onto the public contract: a clean drain
// ([ErrDrained]), a context cancellation, and a Close are all graceful and
// return nil; only a genuine fetch error propagates. The parent context is
// accepted to document that its cancellation is the expected, non-error
// shutdown path.
func (hp *Hopper) exitErr(_ context.Context, runErr error) error {
	return runErr
}

// process decodes, runs the handler chain, and settles one message. It is the
// single point where a delivery decision reaches the backend.
func (hp *Hopper) process(ctx context.Context, handler Handler, d *delivery) {
	m := d.msg
	ctx, span := hp.tracer.Start(
		ctx, "source.process",
		telemetry.String("source.name", hp.name),
		telemetry.String("source.subject", m.Subject()),
		telemetry.String("source.partition_key", m.PartitionKey()),
	)
	defer span.End()

	// Decode (when a registry is configured) and attach the value to the message
	// the handler sees. A decode failure is poison: terminate, do not retry.
	dec := m
	if hp.registry != nil {
		v, err := hp.registry.Decode(m)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(telemetry.StatusError, "decode failed")
			hp.settle(ctx, span, d, Term(err))
			return
		}
		dec = &decoded{Message: m, value: v}
	}

	r := handler(ctx, dec)
	if r.Err != nil {
		span.RecordError(r.Err)
	}
	span.SetAttributes(
		telemetry.String("source.action", r.Action.String()),
		telemetry.String("source.class", r.Class.String()),
	)
	hp.settle(ctx, span, d, r)
}

// settle applies r to the message via the subscription, counts the outcome, and
// logs/marks a settle failure.
func (hp *Hopper) settle(ctx context.Context, span telemetry.Span, d *delivery, r Result) {
	switch {
	case r.Action == ActionAck && r.Class == Drop:
		hp.dropped.Add(ctx, 1)
	case r.Action == ActionAck:
		hp.acked.Add(ctx, 1)
	case r.Action == ActionNak:
		hp.nak.Add(ctx, 1)
	case r.Action == ActionTerm:
		hp.term.Add(ctx, 1)
	}

	if err := d.sub.Settle(ctx, d.msg, r); err != nil {
		hp.failed.Add(ctx, 1)
		span.RecordError(err)
		span.SetStatus(telemetry.StatusError, "settle failed")
		hp.logger.ErrorContext(
			ctx, "source: settle failed",
			slog.String("source", hp.name),
			slog.String("subject", d.msg.Subject()),
			slog.String("action", r.Action.String()),
			slog.Any("error", err),
		)
	}
}

// reportLag feeds the lag gauge from a LagReporter subscription, if the backend
// exposes one. A reporting error is swallowed: lag is a best-effort health
// signal, not part of the consume contract.
func (hp *Hopper) reportLag(ctx context.Context, sub Subscription) {
	lr, ok := sub.(LagReporter)
	if !ok {
		return
	}
	if n, err := lr.Lag(ctx); err == nil {
		hp.lag.Record(ctx, float64(n))
	}
}

// laneKey selects the ordered lane for m: its PartitionKey when non-empty, else
// a hash of its Key, else lane 0 (every keyless message shares one ordered
// lane, preserving global order for them).
func (hp *Hopper) laneKey(m Message) uint64 {
	if pk := m.PartitionKey(); pk != "" {
		return hashBytes([]byte(pk))
	}
	if k := m.Key(); len(k) > 0 {
		return hashBytes(k)
	}
	return 0
}

func hashBytes(b []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(b)
	return h.Sum64()
}

// Close begins a graceful drain: the running Hopper stops fetching, finishes and
// settles in-flight messages, and Run returns. Close is idempotent and never
// blocks on the drain; it signals, and Run completes the drain.
func (hp *Hopper) Close() error {
	hp.closeOnce.Do(func() { close(hp.closed) })
	return nil
}

var _ io.Closer = (*Hopper)(nil)

// decoded wraps a Message with its decoded value, exposing it through Decoded so
// a handler reads the typed value without re-decoding. It embeds Message so the
// neutral surface (Key, Value, Headers, …) passes through unchanged.
type decoded struct {
	Message
	value any
}

// decodedCarrier is implemented by the wrapper a codec-configured Hopper hands
// the handler, so [Decoded] can recover the decoded value without re-decoding.
type decodedCarrier interface {
	decodedValue() (any, bool)
}

func (d *decoded) decodedValue() (any, bool) { return d.value, true }

// Decoded returns the value a codec-configured Hopper decoded for m, and whether
// one is present. When the Hopper has no registry, or m was not produced by such
// a Hopper, it returns (nil, false) and the handler reads m.Value itself.
func Decoded(m Message) (any, bool) {
	if dc, ok := m.(decodedCarrier); ok {
		return dc.decodedValue()
	}
	return nil, false
}
