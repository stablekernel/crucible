// SPDX-License-Identifier: Apache-2.0

// Package jetstream is a source ingress adapter that consumes a NATS JetStream
// stream through a durable pull consumer. It wraps the jetstream subpackage of
// [github.com/nats-io/nats.go] (NOT the legacy JetStreamContext) behind narrow
// seam interfaces so the real client satisfies them structurally while unit
// tests drive hand-rolled fakes with no live server.
//
// Construct an [Inlet] with [New] and functional options, then drive its
// [github.com/stablekernel/crucible/source.Subscription] with a
// source.Hopper (or read it directly via Next/Settle).
//
//	in, err := jetstream.New(
//	    jetstream.WithURL(nats.DefaultURL),
//	    jetstream.WithStream("ORDERS"),
//	    jetstream.WithDurable("orders-worker"),
//	    jetstream.WithAckWait(30*time.Second),
//	    jetstream.WithMaxAckPending(256),
//	)
//	sub, err := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders.>"}})
//	m, err := sub.Next(ctx)         // or drive with a source.Hopper
//	err = sub.Settle(ctx, m, source.Ack())
//
// # JetStream divergences from the source contract
//
// JetStream has no partitions and no assignment lifecycle, so this adapter does
// NOT implement [github.com/stablekernel/crucible/source.ConsumerGroups]. A
// durable consumer is the competing-consumer analog instead, surfaced through
// [github.com/stablekernel/crucible/source.SharedDurable]: multiple processes
// sharing a durable name load-balance the stream without partition assignment.
//
// Replay is consumer-recreate, not an in-place cursor move: a
// [github.com/stablekernel/crucible/source.Seekable] seek tears down the current
// consumer and rebuilds it with a DeliverByStartTime/DeliverByStartSequence
// policy. The seek takes effect on the next Next; in-flight messages from the
// prior consumer are abandoned (not settled).
//
// JetStream offers no consume-side transaction, so this adapter does NOT
// implement [github.com/stablekernel/crucible/source.Transactional]; the
// capability is absent rather than faked.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/stablekernel/crucible/source"
)

// KeyHeader is the message header an inbound message may set to carry an
// explicit partitioning/routing key. When present, the adapter uses its value
// as [source.Message.Key]; otherwise the key falls back to the message subject
// so the Hopper still shards deterministically.
const KeyHeader = "Crucible-Key"

// jsAPI is the narrow slice of the jetstream.JetStream surface this adapter
// uses. The real client returned by jetstream.New satisfies it structurally, so
// unit tests can substitute a fake without a live server.
type jsAPI interface {
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error)
	OrderedConsumer(ctx context.Context, stream string, cfg jetstream.OrderedConsumerConfig) (jetstream.Consumer, error)
}

// compile-time assertion that the real JetStream client satisfies the seam.
var _ jsAPI = jetstream.JetStream(nil)

// Inlet is a NATS JetStream ingress adapter. It opens durable pull-consumer
// subscriptions onto a configured stream. Build one with [New]; it is safe for
// concurrent use.
type Inlet struct {
	conn    *gonats.Conn
	ownConn bool // true when New dialed the connection and must close it
	js      jsAPI
	cfg     config
}

// config holds the resolved construction options for an [Inlet].
type config struct {
	url            string
	conn           *gonats.Conn
	js             jsAPI
	stream         string
	durable        string
	ackWait        time.Duration
	maxDeliver     int
	maxAckPending  int
	pullMaxMsgs    int
	filterSubjects []string
}

// Option configures an [Inlet] at construction. Options compose; later options
// override earlier ones for the same field.
type Option func(*config)

// WithURL dials a NATS connection at url. Mutually informative with [WithConn]:
// supply exactly one. When WithURL is used the [Inlet] owns the connection and
// closes it on [Inlet.Close].
func WithURL(url string) Option { return func(c *config) { c.url = url } }

// WithConn uses an existing NATS connection. The caller retains ownership; the
// [Inlet] does not close a connection it did not dial.
func WithConn(conn *gonats.Conn) Option { return func(c *config) { c.conn = conn } }

// WithStream sets the JetStream stream to consume from. Required.
func WithStream(stream string) Option { return func(c *config) { c.stream = stream } }

// WithDurable sets the durable consumer name. A non-empty durable makes the
// consumer shared (competing-consumer load balancing across processes) and is
// what satisfies [source.SharedDurable]. Empty leaves the consumer ephemeral.
func WithDurable(name string) Option { return func(c *config) { c.durable = name } }

// WithAckWait sets how long the server waits for an ack before redelivering a
// message. Zero leaves the server default.
func WithAckWait(d time.Duration) Option { return func(c *config) { c.ackWait = d } }

// WithMaxDeliver caps redelivery attempts per message. Zero leaves the server
// default (unlimited). When exceeded the message stops being redelivered.
func WithMaxDeliver(n int) Option { return func(c *config) { c.maxDeliver = n } }

// WithMaxAckPending bounds outstanding unacknowledged messages: the primary
// backpressure knob. The server stops delivering once this many messages are
// in flight. Zero leaves the server default.
func WithMaxAckPending(n int) Option { return func(c *config) { c.maxAckPending = n } }

// WithPullMaxMessages sets the pull batch size the message iterator buffers.
// Zero leaves the client default. Combined with [WithMaxAckPending] it shapes
// fetch-side backpressure.
func WithPullMaxMessages(n int) Option { return func(c *config) { c.pullMaxMsgs = n } }

// WithFilterSubjects restricts delivery to messages matching the given subject
// filters. Empty consumes the whole stream. When [source.SubscribeConfig.Topics]
// is non-empty it takes precedence over this option.
func WithFilterSubjects(subjects ...string) Option {
	return func(c *config) { c.filterSubjects = subjects }
}

// WithJetStream uses an already-constructed JetStream client (anything
// satisfying [github.com/nats-io/nats.go/jetstream.JetStream]) instead of
// dialing one. Use it when the caller manages the connection and JetStream
// context itself, or to supply a fake in tests. Mutually exclusive with
// [WithURL]; the [Inlet] never closes a client it did not build.
func WithJetStream(js jetstream.JetStream) Option { return func(c *config) { c.js = js } }

// withJetStream injects the narrow jsAPI seam, for in-package tests that need a
// fake without satisfying the full JetStream interface.
func withJetStream(js jsAPI) Option { return func(c *config) { c.js = js } }

// New builds an [Inlet] from opts. Exactly one of [WithURL] or [WithConn] must
// be supplied, and [WithStream] is required. New dials the connection (when
// [WithURL] is used) and constructs the JetStream client eagerly so
// misconfiguration surfaces here rather than at first Subscribe.
func New(opts ...Option) (*Inlet, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.stream == "" {
		return nil, fmt.Errorf("jetstream: WithStream is required")
	}

	// Test seam: a pre-built jsAPI bypasses dialing entirely.
	if cfg.js != nil {
		return &Inlet{js: cfg.js, conn: cfg.conn, cfg: cfg}, nil
	}

	conn := cfg.conn
	ownConn := false
	switch {
	case conn != nil && cfg.url != "":
		return nil, fmt.Errorf("jetstream: WithURL and WithConn are mutually exclusive")
	case conn != nil:
		// caller-owned connection
	case cfg.url != "":
		c, err := gonats.Connect(cfg.url)
		if err != nil {
			return nil, fmt.Errorf("jetstream: connect %q: %w", cfg.url, err)
		}
		conn, ownConn = c, true
	default:
		return nil, fmt.Errorf("jetstream: one of WithURL or WithConn is required")
	}

	js, err := jetstream.New(conn)
	if err != nil {
		if ownConn {
			conn.Close()
		}
		return nil, fmt.Errorf("jetstream: new client: %w", err)
	}
	return &Inlet{conn: conn, ownConn: ownConn, js: js, cfg: cfg}, nil
}

// Subscribe opens a durable pull-consumer Subscription. cfg.Topics, when set,
// becomes the consumer's filter subjects (overriding [WithFilterSubjects]);
// cfg.Group, when set, overrides [WithDurable] as the durable consumer name. The
// consumer is created lazily on the first [subscription.Next] so Subscribe stays
// cheap and a seek before the first read picks the start policy.
func (in *Inlet) Subscribe(ctx context.Context, cfg source.SubscribeConfig) (source.Subscription, error) {
	filters := in.cfg.filterSubjects
	if len(cfg.Topics) > 0 {
		filters = cfg.Topics
	}
	durable := in.cfg.durable
	if cfg.Group != "" {
		durable = cfg.Group
	}
	return &subscription{
		js:             in.js,
		stream:         in.cfg.stream,
		durable:        durable,
		ackWait:        in.cfg.ackWait,
		maxDeliver:     in.cfg.maxDeliver,
		maxAckPending:  in.cfg.maxAckPending,
		pullMaxMsgs:    in.cfg.pullMaxMsgs,
		filterSubjects: filters,
		startPolicy:    jetstream.DeliverAllPolicy,
	}, nil
}

// Close releases the inlet's resources. It closes the NATS connection only when
// the inlet dialed it (via [WithURL]); a caller-supplied connection is left
// open. Close live Subscriptions first.
func (in *Inlet) Close() error {
	if in.ownConn && in.conn != nil {
		in.conn.Close()
	}
	return nil
}

// As assigns the inlet's underlying NATS connection to target if target is a
// **gonats.Conn, returning whether it did. It is the escape hatch to reach the
// driver for operations outside this adapter's surface.
func (in *Inlet) As(target any) bool {
	if p, ok := target.(**gonats.Conn); ok {
		*p = in.conn
		return true
	}
	return false
}

// ordered is the marker an OrderedDelivery seek toggles; not exported.
var errSeekClosed = errors.New("jetstream: cannot seek a closed subscription")

// subscription is a live JetStream pull-consumer read surface. It implements
// source.Subscription plus the SharedDurable, Seekable, OrderedDelivery, and
// LagReporter capabilities. It is single-consumer for Next; Settle is safe to
// call concurrently because the underlying jetstream.Msg ack methods are.
type subscription struct {
	js             jsAPI
	stream         string
	durable        string
	ackWait        time.Duration
	maxDeliver     int
	maxAckPending  int
	pullMaxMsgs    int
	filterSubjects []string

	mu          sync.Mutex
	cons        jetstream.Consumer
	iter        jetstream.MessagesContext
	closed      bool
	ordered     bool
	startPolicy jetstream.DeliverPolicy
	startSeq    uint64
	startTime   *time.Time
}

// ensureIter lazily creates the consumer and message iterator, recreating them
// after a seek invalidated the prior pair. The caller holds s.mu.
func (s *subscription) ensureIter(ctx context.Context) error {
	if s.closed {
		return source.ErrDrained
	}
	if s.iter != nil {
		return nil
	}
	cons, err := s.makeConsumer(ctx)
	if err != nil {
		return err
	}
	var opts []jetstream.PullMessagesOpt
	if s.pullMaxMsgs > 0 {
		opts = append(opts, jetstream.PullMaxMessages(s.pullMaxMsgs))
	}
	iter, err := cons.Messages(opts...)
	if err != nil {
		return fmt.Errorf("jetstream: open iterator: %w", err)
	}
	s.cons, s.iter = cons, iter
	return nil
}

// makeConsumer creates the durable (or ordered) consumer with the current start
// policy. The caller holds s.mu.
func (s *subscription) makeConsumer(ctx context.Context) (jetstream.Consumer, error) {
	if s.ordered {
		cfg := jetstream.OrderedConsumerConfig{
			FilterSubjects: s.filterSubjects,
			DeliverPolicy:  s.startPolicy,
			OptStartSeq:    s.startSeq,
			OptStartTime:   s.startTime,
		}
		cons, err := s.js.OrderedConsumer(ctx, s.stream, cfg)
		if err != nil {
			return nil, fmt.Errorf("jetstream: create ordered consumer: %w", err)
		}
		return cons, nil
	}
	cfg := jetstream.ConsumerConfig{
		Durable:        s.durable,
		AckPolicy:      jetstream.AckExplicitPolicy,
		AckWait:        s.ackWait,
		MaxDeliver:     s.maxDeliver,
		MaxAckPending:  s.maxAckPending,
		FilterSubjects: s.filterSubjects,
		DeliverPolicy:  s.startPolicy,
		OptStartSeq:    s.startSeq,
		OptStartTime:   s.startTime,
	}
	cons, err := s.js.CreateOrUpdateConsumer(ctx, s.stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("jetstream: create consumer: %w", err)
	}
	return cons, nil
}

// Next returns the next message, blocking until one is available, ctx is
// canceled, or the subscription is drained. It maps the iterator's
// closed-after-stop signal onto [source.ErrDrained].
func (s *subscription) Next(ctx context.Context) (source.Message, error) {
	s.mu.Lock()
	if err := s.ensureIter(ctx); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	iter := s.iter
	s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	msg, err := iter.Next()
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			return nil, source.ErrDrained
		}
		return nil, fmt.Errorf("jetstream: next: %w", err)
	}
	return newMessage(msg), nil
}

// Settle applies the handler [source.Result] to m by translating its Action onto
// the JetStream ack vocabulary. ActionAck uses DoubleAck for a server-confirmed
// acknowledgement; ActionManual is a no-op (the handler settled via As).
func (s *subscription) Settle(ctx context.Context, m source.Message, r source.Result) error {
	var jm jetstream.Msg
	if !m.As(&jm) {
		return fmt.Errorf("jetstream: settle: message is not a jetstream message")
	}
	switch r.Action {
	case source.ActionAck:
		if err := jm.DoubleAck(ctx); err != nil {
			return fmt.Errorf("jetstream: ack: %w", err)
		}
		return nil
	case source.ActionNak:
		if r.Requeue > 0 {
			if err := jm.NakWithDelay(r.Requeue); err != nil {
				return fmt.Errorf("jetstream: nak with delay: %w", err)
			}
			return nil
		}
		if err := jm.Nak(); err != nil {
			return fmt.Errorf("jetstream: nak: %w", err)
		}
		return nil
	case source.ActionTerm:
		if r.Err != nil {
			if err := jm.TermWithReason(r.Err.Error()); err != nil {
				return fmt.Errorf("jetstream: term: %w", err)
			}
			return nil
		}
		if err := jm.Term(); err != nil {
			return fmt.Errorf("jetstream: term: %w", err)
		}
		return nil
	case source.ActionInProgress:
		if err := jm.InProgress(); err != nil {
			return fmt.Errorf("jetstream: in progress: %w", err)
		}
		return nil
	case source.ActionManual:
		return nil
	default:
		return fmt.Errorf("jetstream: settle: unknown action %v", r.Action)
	}
}

// Close stops the message iterator and marks the subscription drained. It is
// idempotent. In-flight messages already returned by Next are left for the
// caller to settle; once stopped, Next returns [source.ErrDrained].
func (s *subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.iter != nil {
		s.iter.Stop()
		s.iter = nil
	}
	return nil
}

// Durable reports the durable consumer name, satisfying [source.SharedDurable].
func (s *subscription) Durable() string { return s.durable }

// OrderedDelivery is the marker promoting this subscription to a single-stream,
// totally-ordered OrderedConsumer. It must be enabled before the first Next; it
// is the capability surface, not the default.
func (s *subscription) OrderedDelivery() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ordered = true
}

// SeekToTime repositions delivery to the first message at or after t by
// recreating the consumer with a DeliverByStartTime policy. JetStream replay is
// consumer-recreate: any in-flight messages from the prior consumer are
// abandoned, and the change takes effect on the next Next.
func (s *subscription) SeekToTime(_ context.Context, t time.Time) error {
	return s.reseek(func() {
		s.startPolicy = jetstream.DeliverByStartTimePolicy
		s.startTime = &t
		s.startSeq = 0
	})
}

// SeekToCursor repositions delivery to resume from just after c, a previously
// observed [source.Cursor] carrying a stream sequence. It recreates the consumer
// with a DeliverByStartSequence policy at c+1.
func (s *subscription) SeekToCursor(_ context.Context, c source.Cursor) error {
	cur, ok := c.(seqCursor)
	if !ok {
		return fmt.Errorf("jetstream: seek: cursor is not a jetstream cursor")
	}
	return s.reseek(func() {
		s.startPolicy = jetstream.DeliverByStartSequencePolicy
		s.startSeq = uint64(cur) + 1
		s.startTime = nil
	})
}

// SeekToStart repositions delivery to the earliest retained message by
// recreating the consumer with a DeliverAll policy.
func (s *subscription) SeekToStart(_ context.Context) error {
	return s.reseek(func() {
		s.startPolicy = jetstream.DeliverAllPolicy
		s.startSeq = 0
		s.startTime = nil
	})
}

// SeekToEnd repositions delivery to the tail by recreating the consumer with a
// DeliverNew policy, so only messages produced after the seek are delivered.
func (s *subscription) SeekToEnd(_ context.Context) error {
	return s.reseek(func() {
		s.startPolicy = jetstream.DeliverNewPolicy
		s.startSeq = 0
		s.startTime = nil
	})
}

// reseek applies a start-policy mutation and tears down the current consumer so
// the next Next rebuilds it at the new position. The new consumer is created
// lazily on that Next, keeping the seek synchronous and cheap.
func (s *subscription) reseek(mut func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSeekClosed
	}
	mut()
	if s.iter != nil {
		s.iter.Stop()
		s.iter = nil
		s.cons = nil
	}
	return nil
}

// Lag reports outstanding messages between the consumer and the stream tail,
// satisfying [source.LagReporter]. It returns the consumer's NumPending plus
// in-flight (AckPending) count. Before the consumer is created Lag is zero.
func (s *subscription) Lag(ctx context.Context) (int64, error) {
	s.mu.Lock()
	cons := s.cons
	s.mu.Unlock()
	if cons == nil {
		return 0, nil
	}
	info, err := cons.Info(ctx)
	if err != nil {
		return 0, fmt.Errorf("jetstream: lag: %w", err)
	}
	return int64(info.NumPending) + int64(info.NumAckPending), nil
}

// compile-time capability assertions: this adapter honestly advertises exactly
// the JetStream-shaped capabilities and no others.
var (
	_ source.Subscription    = (*subscription)(nil)
	_ source.SharedDurable   = (*subscription)(nil)
	_ source.Seekable        = (*subscription)(nil)
	_ source.OrderedDelivery = (*subscription)(nil)
	_ source.LagReporter     = (*subscription)(nil)
)
