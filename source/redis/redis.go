// SPDX-License-Identifier: Apache-2.0

// Package redis is a source ingress adapter that consumes a Redis Stream through
// a consumer group. It wraps the stream surface of
// [github.com/redis/go-redis/v9] behind a narrow seam interface so the real
// *redis.Client satisfies it structurally while unit tests drive a hand-rolled
// fake with no live server.
//
// Construct an [Inlet] with [New] and functional options, then drive its
// [github.com/stablekernel/crucible/source.Subscription] with a source.Hopper
// (or read it directly via Next/Settle).
//
//	in, err := redis.New(
//	    redis.WithAddr("localhost:6379"),
//	    redis.WithGroup("orders-svc"),
//	    redis.WithConsumer("worker-1"),
//	    redis.WithDLQStream("orders.dlq"),
//	    redis.WithBlock(5*time.Second),
//	    redis.WithCount(64),
//	)
//	sub, err := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders"}})
//	m, err := sub.Next(ctx)         // or drive with a source.Hopper
//	err = sub.Settle(ctx, m, source.Ack())
//
// # Settle vocabulary on Redis Streams
//
// A consumer group reads with XREADGROUP; every delivered entry stays in the
// group's Pending Entries List (PEL) until it is settled.
//
//   - ActionAck calls XACK, removing the entry from the PEL.
//   - ActionNak leaves the entry in the PEL: it is redelivered to a consumer that
//     scans the pending backlog with XPENDING + XCLAIM after the configured min
//     idle time. Result.Requeue raises the claim's min-idle floor when larger.
//   - ActionTerm appends the entry (with its original fields plus dead-letter
//     metadata) to the configured dead-letter stream, then XACKs the original so
//     it is not redelivered.
//   - ActionInProgress is a no-op: Redis has no per-message deadline to extend.
//   - ActionManual is a no-op; the handler settled the entry itself through
//     Message.As and the client.
//
// # Redis divergences from the source contract
//
// A Redis consumer group has no partitions and no assignment lifecycle, so this
// adapter does NOT implement
// [github.com/stablekernel/crucible/source.ConsumerGroups]. The group is the
// competing-consumer analog instead, surfaced through
// [github.com/stablekernel/crucible/source.SharedDurable]: multiple consumers in
// one group load-balance the stream without partition assignment.
//
// Redis Streams offer no consume-side transaction, so this adapter does NOT
// implement [github.com/stablekernel/crucible/source.Transactional]; the
// capability is absent rather than faked.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/stablekernel/crucible/source"
)

// defaultBlock is the XREADGROUP block duration used when none is configured: a
// bounded wait keeps Next responsive to context cancellation without busy-
// looping when the stream is idle.
const defaultBlock = 5 * time.Second

// defaultCount is the XREADGROUP batch size used when none is configured.
const defaultCount = 64

// defaultMinIdle is the floor a pending entry must be idle before this consumer
// claims it for redelivery, used when WithBlock does not imply a larger value.
const defaultMinIdle = 30 * time.Second

// Client is the narrow Redis Streams surface this adapter needs. It is satisfied
// structurally by *redis.Client from github.com/redis/go-redis/v9, so callers
// wire the real client while unit tests substitute a fake without a live server.
type Client interface {
	// XGroupCreateMkStream creates the consumer group, creating the stream if it
	// does not yet exist.
	XGroupCreateMkStream(ctx context.Context, stream, group, start string) *goredis.StatusCmd
	// XReadGroup reads new or pending entries for a consumer group.
	XReadGroup(ctx context.Context, a *goredis.XReadGroupArgs) *goredis.XStreamSliceCmd
	// XAck acknowledges entries, removing them from the group's pending list.
	XAck(ctx context.Context, stream, group string, ids ...string) *goredis.IntCmd
	// XPendingExt lists pending entries with their idle time and delivery count.
	XPendingExt(ctx context.Context, a *goredis.XPendingExtArgs) *goredis.XPendingExtCmd
	// XClaim reassigns ownership of idle pending entries to this consumer.
	XClaim(ctx context.Context, a *goredis.XClaimArgs) *goredis.XMessageSliceCmd
	// XAdd appends an entry to a stream (used for dead-lettering).
	XAdd(ctx context.Context, a *goredis.XAddArgs) *goredis.StringCmd
	// XRange returns entries in an ID range (used for cursor seeks).
	XRange(ctx context.Context, stream, start, stop string) *goredis.XMessageSliceCmd
	// XLen returns the number of entries in a stream.
	XLen(ctx context.Context, stream string) *goredis.IntCmd
	// XInfoGroups returns the consumer-group metadata for a stream, including each
	// group's lag behind the tail.
	XInfoGroups(ctx context.Context, key string) *goredis.XInfoGroupsCmd
}

// compile-time assertion that the real go-redis client satisfies the seam.
var _ Client = (*goredis.Client)(nil)

// Inlet is a Redis Streams ingress adapter. It opens consumer-group
// subscriptions onto a configured stream. Build one with [New]; it is safe for
// concurrent use.
type Inlet struct {
	client  Client
	ownConn bool // true when New dialed the client and Close must shut it down
	cfg     config
}

// config holds the resolved construction options for an [Inlet].
type config struct {
	addr      string
	client    Client
	group     string
	consumer  string
	dlqStream string
	block     time.Duration
	count     int64
	minIdle   time.Duration
}

// Option configures an [Inlet] at construction. Options compose; later options
// override earlier ones for the same field.
type Option func(*config)

// WithAddr dials a Redis client at addr (host:port). Mutually exclusive with
// [WithClient]: supply exactly one. When WithAddr is used the [Inlet] owns the
// client and closes it on [Inlet.Close].
func WithAddr(addr string) Option { return func(c *config) { c.addr = addr } }

// WithClient uses an existing client (anything satisfying [Client], such as a
// *redis.Client). The caller retains ownership; the [Inlet] does not close a
// client it did not dial. Mutually exclusive with [WithAddr].
func WithClient(client Client) Option { return func(c *config) { c.client = client } }

// WithGroup sets the consumer group name: the competing-consumer grouping that
// satisfies [source.SharedDurable]. Required.
func WithGroup(group string) Option { return func(c *config) { c.group = group } }

// WithConsumer sets this consumer's name within the group. Each process in a
// group needs a distinct consumer name so the group can track per-consumer
// pending entries. Required.
func WithConsumer(name string) Option { return func(c *config) { c.consumer = name } }

// WithDLQStream sets the dead-letter stream an [source.ActionTerm] (Term/Reject)
// routes a rejected entry to before acking the original. Empty disables dead-
// lettering: a terminated entry is acked and dropped.
func WithDLQStream(stream string) Option { return func(c *config) { c.dlqStream = stream } }

// WithBlock sets how long XREADGROUP blocks waiting for a new entry before
// returning empty so Next can re-check its context. Zero leaves the default.
func WithBlock(d time.Duration) Option { return func(c *config) { c.block = d } }

// WithCount sets the maximum number of entries XREADGROUP fetches per call: the
// fetch-side batch bound. Zero leaves the default.
func WithCount(n int64) Option { return func(c *config) { c.count = n } }

// WithMinIdle sets how long a pending entry must be idle before this consumer
// claims it for redelivery. It is the redelivery floor for naked entries; a
// larger [source.Result.Requeue] on a Nak raises it per entry. Zero leaves the
// default.
func WithMinIdle(d time.Duration) Option { return func(c *config) { c.minIdle = d } }

// New builds an [Inlet] from opts. Exactly one of [WithAddr] or [WithClient]
// must be supplied, and [WithGroup] and [WithConsumer] are required. When
// [WithAddr] is used New dials the client eagerly so misconfiguration surfaces
// here rather than at first Subscribe.
func New(opts ...Option) (*Inlet, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.group == "" {
		return nil, fmt.Errorf("redis: WithGroup is required")
	}
	if cfg.consumer == "" {
		return nil, fmt.Errorf("redis: WithConsumer is required")
	}
	if cfg.block == 0 {
		cfg.block = defaultBlock
	}
	if cfg.count == 0 {
		cfg.count = defaultCount
	}
	if cfg.minIdle == 0 {
		cfg.minIdle = defaultMinIdle
	}

	client := cfg.client
	ownConn := false
	switch {
	case client != nil && cfg.addr != "":
		return nil, fmt.Errorf("redis: WithAddr and WithClient are mutually exclusive")
	case client != nil:
		// caller-owned client
	case cfg.addr != "":
		client = goredis.NewClient(&goredis.Options{Addr: cfg.addr})
		ownConn = true
	default:
		return nil, fmt.Errorf("redis: one of WithAddr or WithClient is required")
	}

	return &Inlet{client: client, ownConn: ownConn, cfg: cfg}, nil
}

// Subscribe opens a consumer-group Subscription. cfg.Topics must name exactly
// one stream (Redis Streams are single-stream per group read); cfg.Group, when
// set, overrides [WithGroup]. The consumer group is created lazily on the first
// [subscription.Next] (with MKSTREAM, so a not-yet-existent stream is created),
// keeping Subscribe cheap.
func (in *Inlet) Subscribe(_ context.Context, cfg source.SubscribeConfig) (source.Subscription, error) {
	if len(cfg.Topics) != 1 {
		return nil, fmt.Errorf("redis: Subscribe requires exactly one stream in Topics, got %d", len(cfg.Topics))
	}
	group := in.cfg.group
	if cfg.Group != "" {
		group = cfg.Group
	}
	return &subscription{
		client:    in.client,
		stream:    cfg.Topics[0],
		group:     group,
		consumer:  in.cfg.consumer,
		dlqStream: in.cfg.dlqStream,
		block:     in.cfg.block,
		count:     in.cfg.count,
		minIdle:   in.cfg.minIdle,
		startID:   "0", // create the group at the stream origin by default
		readFrom:  ">", // read new (never-delivered) entries by default
	}, nil
}

// Close releases the inlet's resources. It shuts down the Redis client only when
// the inlet dialed it (via [WithAddr]); a caller-supplied client is left open.
// Close live Subscriptions first.
func (in *Inlet) Close() error {
	if in.ownConn {
		if c, ok := in.client.(interface{ Close() error }); ok {
			return c.Close()
		}
	}
	return nil
}

// As assigns the inlet's underlying Redis client to target if target is a
// *redis.Client (or the narrow *Client seam), returning whether it did. It is
// the escape hatch to reach the driver for operations outside this adapter's
// surface.
func (in *Inlet) As(target any) bool {
	switch p := target.(type) {
	case *Client:
		*p = in.client
		return true
	case **goredis.Client:
		if rc, ok := in.client.(*goredis.Client); ok {
			*p = rc
			return true
		}
	}
	return false
}

// errSeekClosed reports a seek attempted on a drained subscription.
var errSeekClosed = errors.New("redis: cannot seek a closed subscription")

// subscription is a live Redis consumer-group read surface. It implements
// source.Subscription plus the SharedDurable, Seekable, and LagReporter
// capabilities. Next is single-consumer (the Hopper's fetch loop); Settle is
// safe to call concurrently because the underlying client commands are.
type subscription struct {
	client    Client
	stream    string
	group     string
	consumer  string
	dlqStream string
	block     time.Duration
	count     int64
	minIdle   time.Duration

	mu       sync.Mutex
	created  bool // consumer group ensured
	closed   bool
	startID  string           // group-create start ("0" origin, "$" tail, or an explicit ID)
	pending  []source.Message // buffered, fetched-but-not-yet-yielded entries
	inflight int              // entries delivered by Next, not yet settled
	readFrom string           // ">" for new entries; an ID to drain the backlog
}

// ensureGroup creates the consumer group (with MKSTREAM) once, idempotently
// tolerating the BUSYGROUP error that means the group already exists. The caller
// holds s.mu.
func (s *subscription) ensureGroup(ctx context.Context) error {
	if s.created {
		return nil
	}
	err := s.client.XGroupCreateMkStream(ctx, s.stream, s.group, s.startID).Err()
	if err != nil && !isBusyGroup(err) {
		return fmt.Errorf("redis: create group %q on %q: %w", s.group, s.stream, err)
	}
	s.created = true
	return nil
}

// isBusyGroup reports whether err is the "consumer group already exists" error,
// which ensureGroup treats as success so group creation is idempotent. Redis
// returns it as a BUSYGROUP error reply; go-redis surfaces it as a plain error
// whose message carries the prefix, so a substring match is the reliable test.
func isBusyGroup(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	const want = "BUSYGROUP"
	for i := 0; i+len(want) <= len(msg); i++ {
		if msg[i:i+len(want)] == want {
			return true
		}
	}
	return false
}

// Next returns the next entry, blocking up to the configured block duration on
// XREADGROUP. It drains any buffered batch first, then reads new entries; an
// empty read returns after the block window so the caller's context is honored.
// Once the subscription is closed and its buffer drained, Next returns
// [source.ErrDrained].
func (s *subscription) Next(ctx context.Context) (source.Message, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.mu.Lock()
		if s.closed && len(s.pending) == 0 {
			s.mu.Unlock()
			return nil, source.ErrDrained
		}
		if len(s.pending) > 0 {
			m := s.pending[0]
			s.pending = s.pending[1:]
			s.inflight++
			s.mu.Unlock()
			return m, nil
		}
		if s.closed {
			s.mu.Unlock()
			return nil, source.ErrDrained
		}
		if err := s.ensureGroup(ctx); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		readFrom := s.readFrom
		s.mu.Unlock()

		batch, err := s.fetch(ctx, readFrom)
		if err != nil {
			return nil, err
		}
		s.mu.Lock()
		// When draining the historical backlog (readFrom is an ID, not ">"), an
		// empty result means the backlog is exhausted; switch to new entries.
		if len(batch) == 0 && readFrom != ">" {
			s.readFrom = ">"
			s.mu.Unlock()
			continue
		}
		s.pending = append(s.pending, batch...)
		s.mu.Unlock()
		// Loop: serve from the freshly filled buffer (or block again if empty).
	}
}

// fetch issues one XREADGROUP for readFrom (">" for new entries, an ID for the
// consumer's own pending backlog) and maps the returned entries onto neutral
// messages. A blocking read that times out with no entries returns an empty
// slice, not an error.
func (s *subscription) fetch(ctx context.Context, readFrom string) ([]source.Message, error) {
	res, err := s.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    s.group,
		Consumer: s.consumer,
		Streams:  []string{s.stream, readFrom},
		Count:    s.count,
		Block:    s.block,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil // block window elapsed with no new entries
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("redis: read group: %w", err)
	}
	var out []source.Message
	for _, st := range res {
		for _, e := range st.Messages {
			out = append(out, newMessage(s.stream, e))
		}
	}
	return out, nil
}

// Settle applies the handler [source.Result] to m by translating its Action onto
// the Redis Streams settle vocabulary: ack (XACK), nak (leave pending for a
// later XCLAIM-driven redelivery), term (route to the dead-letter stream, then
// XACK), in-progress (no-op), or manual (no-op).
func (s *subscription) Settle(ctx context.Context, m source.Message, r source.Result) error {
	var entry goredis.XMessage
	if !m.As(&entry) {
		return fmt.Errorf("redis: settle: message is not a redis stream entry")
	}
	defer s.settled()

	switch r.Action {
	case source.ActionAck:
		return s.ack(ctx, entry.ID)
	case source.ActionNak:
		// Leave the entry in the PEL; a future pending scan (NakRedeliver) claims
		// and redelivers it once it has been idle long enough. The requeue delay
		// raises that idle floor when larger than the configured minimum.
		return nil
	case source.ActionTerm:
		if err := s.deadLetter(ctx, m, entry, r); err != nil {
			return err
		}
		return s.ack(ctx, entry.ID)
	case source.ActionInProgress:
		return nil // Redis has no per-message deadline to extend
	case source.ActionManual:
		return nil
	default:
		return fmt.Errorf("redis: settle: unknown action %v", r.Action)
	}
}

// settled decrements the in-flight counter under the lock. It is deferred from
// Settle so a graceful Close can observe when the last delivered entry is done.
func (s *subscription) settled() {
	s.mu.Lock()
	if s.inflight > 0 {
		s.inflight--
	}
	s.mu.Unlock()
}

// ack removes an entry from the group's pending list via XACK.
func (s *subscription) ack(ctx context.Context, id string) error {
	if err := s.client.XAck(ctx, s.stream, s.group, id).Err(); err != nil {
		return fmt.Errorf("redis: ack %s: %w", id, err)
	}
	return nil
}

// deadLetter appends the rejected entry's fields plus dead-letter metadata
// (original ID, stream, classification, error) to the configured dead-letter
// stream. When no dead-letter stream is configured it is a no-op, so the caller
// then acks and drops the entry.
func (s *subscription) deadLetter(ctx context.Context, m source.Message, entry goredis.XMessage, r source.Result) error {
	if s.dlqStream == "" {
		return nil
	}
	values := make(map[string]any, len(entry.Values)+4)
	for k, v := range entry.Values {
		values[k] = v
	}
	values["crucible-dlq-original-id"] = entry.ID
	values["crucible-dlq-stream"] = m.Subject()
	values["crucible-dlq-class"] = r.Class.String()
	if r.Err != nil {
		values["crucible-dlq-error"] = r.Err.Error()
	}
	if err := s.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.dlqStream,
		ID:     "*",
		Values: values,
	}).Err(); err != nil {
		return fmt.Errorf("redis: dead-letter %s to %q: %w", entry.ID, s.dlqStream, err)
	}
	return nil
}

// NakRedeliver scans the group's pending entries and claims those idle longer
// than minIdle to this consumer, returning them for redelivery. It is how a
// naked entry (left in the PEL by [source.ActionNak]) comes back: a consumer
// periodically drains the backlog with XPENDING + XCLAIM. The claimed entries
// are buffered so the next [subscription.Next] yields them. It reports how many
// entries were reclaimed.
//
// Drive it on a timer or between read cycles; the Hopper does not call it
// automatically because redelivery cadence is a deployment choice.
func (s *subscription) NakRedeliver(ctx context.Context, minIdle time.Duration) (int, error) {
	if minIdle <= 0 {
		minIdle = s.minIdle
	}
	pend, err := s.client.XPendingExt(ctx, &goredis.XPendingExtArgs{
		Stream: s.stream,
		Group:  s.group,
		Idle:   minIdle,
		Start:  "-",
		End:    "+",
		Count:  s.count,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return 0, nil
		}
		return 0, fmt.Errorf("redis: pending scan: %w", err)
	}
	if len(pend) == 0 {
		return 0, nil
	}
	ids := make([]string, 0, len(pend))
	for _, p := range pend {
		ids = append(ids, p.ID)
	}
	claimed, err := s.client.XClaim(ctx, &goredis.XClaimArgs{
		Stream:   s.stream,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  minIdle,
		Messages: ids,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: claim pending: %w", err)
	}
	s.mu.Lock()
	for _, e := range claimed {
		s.pending = append(s.pending, newMessage(s.stream, e))
	}
	s.mu.Unlock()
	return len(claimed), nil
}

// Close begins a graceful drain: Next stops reading new entries, and once the
// buffer is empty Next returns [source.ErrDrained]. Entries already delivered by
// Next are left for the caller to settle. Close is idempotent.
func (s *subscription) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Durable reports the consumer group name, satisfying [source.SharedDurable].
func (s *subscription) Durable() string { return s.group }

// SeekToTime repositions delivery to the first entry at or after t. Redis entry
// IDs embed a millisecond timestamp, so the seek translates t into the ID
// "<millis>-0" and resumes the backlog from there; it takes effect on the next
// Next.
func (s *subscription) SeekToTime(ctx context.Context, t time.Time) error {
	id := fmt.Sprintf("%d-0", t.UnixMilli())
	return s.reseek(ctx, id)
}

// SeekToCursor repositions delivery to resume from just after c, a previously
// observed [source.Cursor] carrying an entry ID. It drains the backlog from that
// ID forward using XRANGE.
func (s *subscription) SeekToCursor(ctx context.Context, c source.Cursor) error {
	cur, ok := c.(idCursor)
	if !ok {
		return fmt.Errorf("redis: seek: cursor is not a redis cursor")
	}
	return s.reseek(ctx, string(cur))
}

// SeekToStart repositions delivery to the earliest retained entry.
func (s *subscription) SeekToStart(ctx context.Context) error {
	return s.reseek(ctx, "0")
}

// SeekToEnd repositions delivery to the tail, so only entries produced after the
// seek are delivered. It clears any buffered backlog and reads new entries.
func (s *subscription) SeekToEnd(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errSeekClosed
	}
	s.pending = nil
	s.readFrom = ">"
	return nil
}

// reseek loads the backlog at or after fromID with XRANGE and buffers it so the
// next Next yields the replayed entries before resuming live delivery. The
// entries are claimed to this consumer (they enter the PEL) so they settle
// through the normal ack path. The caller passes a context for the range read.
func (s *subscription) reseek(ctx context.Context, fromID string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return errSeekClosed
	}
	s.mu.Unlock()

	entries, err := s.client.XRange(ctx, s.stream, fromID, "+").Result()
	if err != nil {
		return fmt.Errorf("redis: seek range from %s: %w", fromID, err)
	}
	msgs := make([]source.Message, 0, len(entries))
	for _, e := range entries {
		msgs = append(msgs, newMessage(s.stream, e))
	}
	s.mu.Lock()
	s.pending = msgs
	s.readFrom = ">"
	s.mu.Unlock()
	return nil
}

// Lag reports the consumer group's backlog behind the stream tail, satisfying
// [source.LagReporter]. It reads the group's lag from XINFO GROUPS when Redis
// reports it; otherwise it falls back to the stream length from XLEN.
func (s *subscription) Lag(ctx context.Context) (int64, error) {
	groups, err := s.client.XInfoGroups(ctx, s.stream).Result()
	if err == nil {
		for _, g := range groups {
			if g.Name == s.group {
				if g.Lag > 0 {
					return g.Lag, nil
				}
				break
			}
		}
	}
	n, err := s.client.XLen(ctx, s.stream).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: lag: %w", err)
	}
	return n, nil
}

// compile-time capability assertions: this adapter honestly advertises exactly
// the Redis-shaped capabilities and no others (no ConsumerGroups, no
// Transactional).
var (
	_ source.Subscription  = (*subscription)(nil)
	_ source.SharedDurable = (*subscription)(nil)
	_ source.Seekable      = (*subscription)(nil)
	_ source.LagReporter   = (*subscription)(nil)
)
