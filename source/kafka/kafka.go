// SPDX-License-Identifier: Apache-2.0

// Package kafka is a source ingress adapter that consumes records from Apache
// Kafka (and API-compatible brokers such as RedPanda) through the pure-Go
// franz-go client. It depends only on the standard library, crucible/source,
// and franz-go. Construct an [Inlet] with [New], hand it to a source.Hopper,
// and the engine drives the consume loop, decoding, ordering, and settlement.
//
// # Ack model
//
// Delivery is at-least-once within a live subscription: the adapter never
// commits an offset before its handler reports success. The franz-go client is
// configured with AutoCommitMarks, so only records the engine settles
// successfully are marked, and the marked offsets are committed on graceful
// drain and on rebalance. Each handler [source.Result] maps onto Kafka as
// follows:
//
//   - Ack marks the record for commit (commit-after-process).
//   - Nak never marks the record and redelivers it in-session: the partition is
//     paused, re-seeked to the record's offset so it is fetched again, then
//     resumed; a requeue delay waits out the pause. Redelivery across restarts
//     or rebalances rides committed offsets, so a concurrently committed higher
//     offset can pass the nacked record — a documented best-effort divergence,
//     not an in-session one.
//   - Term produces the record to the configured dead-letter topic, then marks
//     it for commit so it is not re-read — except on a transactional
//     subscription, where Term reports [ErrTermInsideTransaction]: route poison
//     through Begin's transaction instead, producing to the dead-letter topic
//     via the handed [source.Tx] so DLQ write and offset commit are atomic.
//   - InProgress is a no-op: Kafka has no per-message ack deadline to extend.
//   - Manual is a no-op: the handler settled the record itself through
//     [source.Message.As] and the underlying *kgo.Client.
//
// # Cold start
//
// A brand-new consumer group (no committed offsets) starts at the earliest
// retained record in each assigned partition — franz-go's default. Build the
// inlet with [WithStartOffset] to start at the stream tail instead. Once a
// group has committed offsets, restarts always resume from them.
//
// # Capabilities
//
// The [Subscription] this adapter opens satisfies several optional capability
// interfaces the engine discovers by type assertion, without leaking vendor
// types: [source.Seekable] (live offset reposition via SetOffsets),
// [source.ConsumerGroups] (rebalance assign/revoke/lost hooks with
// drain-and-commit on revoke), [source.PartitionOrdered] (per-partition order),
// [source.LagReporter] (end-offset lag), and [source.Transactional] (Kafka
// exactly-once, via a group transact session) when constructed with
// [WithTransactional].
//
// # Vendor boundary and escape hatch
//
// The neutral seam stays vendor-free: no franz-go type crosses the
// [source.Inlet], [source.Subscription], or [source.Message] surface, and the
// capability interfaces ([source.Seekable], [source.ConsumerGroups], …) carry
// only crucible types. Typed power seams do expose franz-go deliberately:
// [WithSASL] takes sasl.Mechanism values, [WithBalancer] takes
// kgo.GroupBalancer values, [WithClientOptions] appends raw kgo.Opt entries,
// and [WithClient] injects a pre-built *kgo.Client. Power users reach the
// underlying *kgo.Client through the inlet's As method ([Inlet.As]) and a
// delivered record through [source.Message.As] with a **kgo.Record target.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"

	"github.com/stablekernel/crucible/source"
)

// ErrNoSeedBrokers reports that [New] was called without any seed brokers and
// without an injected client. Match it with errors.Is.
var ErrNoSeedBrokers = errors.New("source/kafka: no seed brokers configured")

// ErrNoDLQTopic reports that a record was termed (dead-lettered) but the inlet
// was constructed without a dead-letter topic. Configure one with
// [WithDLQTopic]. Match it with errors.Is.
var ErrNoDLQTopic = errors.New("source/kafka: term requested but no dead-letter topic configured")

// ErrNoCommittedOffsets reports that [source.LagReporter] was called before
// the group committed its first offset. Without a commit there is no baseline
// to measure lag from; poll and settle at least one record first. Match it
// with errors.Is.
var ErrNoCommittedOffsets = errors.New("source/kafka: lag requires at least one committed offset")

// ErrTermInsideTransaction reports that a handler returned [source.Term] (or
// [source.Reject]) against a transactional subscription outside
// [source.Transactional.Begin]. A DLQ produce there would write outside the
// open EOS session and break atomicity with the consumed offset. On a
// transactional subscription, route poison through Begin instead: inside fn,
// produce the rejected record to the dead-letter topic via the handed
// [source.Tx] (a ProducedRecord with the DLQ topic), then return nil — the
// DLQ record and the input offset then commit as one atomic unit. Match it
// with errors.Is.
var ErrTermInsideTransaction = errors.New("source/kafka: dead-letter via term is not supported outside Begin on a transactional subscription")

// errTransactionalSingleSubscribe reports a second [Inlet.Subscribe] on a
// transactional inlet. The exactly-once session backing a transactional inlet
// fences a single consumer, so only one subscription per transactional inlet is
// valid; open a fresh inlet for a second consumer. Match it with errors.Is.
var errTransactionalSingleSubscribe = errors.New("source/kafka: a transactional inlet allows only one Subscribe")

// config holds an [Inlet]'s resolved settings before a client is built. Every
// field has a zero-value default; the only requirement is at least one seed
// broker (or an injected client).
type config struct {
	seedBrokers []string
	clientID    string
	dlqTopic    string
	sasl        []sasl.Mechanism
	tlsConfig   *tls.Config
	balancers   []kgo.GroupBalancer
	maxPoll     int
	transact    bool
	transactID  string
	extraOpts   []kgo.Opt
	client      *kgo.Client

	startOffset    StartOffset
	startOffsetSet bool
}

// StartOffset selects where a brand-new consumer group — one with no committed
// offsets — begins consuming each assigned partition. Once a group has
// committed offsets, restarts always resume from them; this policy governs
// only the cold start.
type StartOffset uint8

const (
	// StartEarliest begins a cold group at the earliest retained record of each
	// partition (franz-go's default, and this package's zero value): a new
	// consumer sees the full history of its topics.
	StartEarliest StartOffset = iota
	// StartLatest begins a cold group at each partition's tail: only records
	// produced after the subscription joins are delivered.
	StartLatest
)

// WithStartOffset sets where a brand-new consumer group (no committed offsets)
// starts consuming: [StartEarliest] (the default) reads from the earliest
// retained record, [StartLatest] reads only records produced after joining.
// It maps onto franz-go's kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()|AtEnd());
// partitions that already have a committed offset always resume from the
// commit regardless of this setting. Values outside the [StartOffset]
// constants fall back to the default.
func WithStartOffset(o StartOffset) Option {
	return func(c *config) {
		switch o {
		case StartEarliest, StartLatest:
			c.startOffset = o
			c.startOffsetSet = true
		}
	}
}

// Option configures an [Inlet]. Options are additive with zero-value defaults;
// a nil or empty value passed to a With* option leaves the default in place.
type Option func(*config)

// WithSeedBrokers sets the broker addresses (host:port) the inlet dials to
// discover the cluster. At least one is required unless a client is injected
// with [WithClient]. Empty input is ignored.
func WithSeedBrokers(brokers ...string) Option {
	return func(c *config) {
		for _, b := range brokers {
			if b != "" {
				c.seedBrokers = append(c.seedBrokers, b)
			}
		}
	}
}

// WithClientID sets the Kafka client ID the broker logs and quotas requests
// under. The default is franz-go's. An empty ID is ignored.
func WithClientID(id string) Option {
	return func(c *config) {
		if id != "" {
			c.clientID = id
		}
	}
}

// WithSASL configures one or more SASL authentication mechanisms (PLAIN, SCRAM,
// OAUTHBEARER, AWS_MSK_IAM) built from franz-go's pkg/sasl helpers. The
// franz-go sasl.Mechanism is an interface, not a struct, so this keeps no
// franz-go data type in the exported surface beyond the auth abstraction itself.
// Nil entries are ignored.
func WithSASL(mechanisms ...sasl.Mechanism) Option {
	return func(c *config) {
		for _, m := range mechanisms {
			if m != nil {
				c.sasl = append(c.sasl, m)
			}
		}
	}
}

// WithTLS enables TLS on broker connections using cfg. A nil cfg is ignored
// (connections stay plaintext); pass a non-nil &tls.Config{} for system-default
// TLS.
func WithTLS(cfg *tls.Config) Option {
	return func(c *config) {
		if cfg != nil {
			c.tlsConfig = cfg
		}
	}
}

// WithBalancer sets the consumer-group partition-assignment balancer(s), in
// preference order, used when a subscription joins a group. The default is
// franz-go's cooperative-sticky balancer. Construct balancers with
// kgo.CooperativeStickyBalancer, kgo.RangeBalancer, and friends. Nil entries
// are ignored.
func WithBalancer(balancers ...kgo.GroupBalancer) Option {
	return func(c *config) {
		for _, b := range balancers {
			if b != nil {
				c.balancers = append(c.balancers, b)
			}
		}
	}
}

// WithDLQTopic sets the topic an ActionTerm result produces a rejected record
// to before committing it. Without a dead-letter topic a Term fails with
// [ErrNoDLQTopic] rather than silently dropping the record. An empty topic is
// ignored.
func WithDLQTopic(topic string) Option {
	return func(c *config) {
		if topic != "" {
			c.dlqTopic = topic
		}
	}
}

// WithMaxPollRecords bounds how many records a single fetch yields, the lever
// the engine's bounded in-flight window rides on. The default (0) lets
// franz-go decide. A value < 1 is ignored.
func WithMaxPollRecords(n int) Option {
	return func(c *config) {
		if n >= 1 {
			c.maxPoll = n
		}
	}
}

// WithTransactional enables Kafka exactly-once semantics: the subscription
// satisfies [source.Transactional], fencing the records produced while
// processing and the consumed offset inside one producer transaction so
// consume-process-produce is atomic. The inlet builds a kgo.GroupTransactSession
// instead of a plain client, configured with the given transactional ID,
// read-committed fetch isolation (so it never reads another producer's
// uncommitted records), and no auto-commit (the transaction commits offsets).
//
// transactionalID must be stable and unique to this logical consumer across
// restarts: it is the producer fencing token Kafka uses to detect a zombie
// instance. Reusing one transactional ID across two live consumers of the same
// partitions fences one of them. An empty transactionalID is ignored and the
// inlet stays non-transactional.
func WithTransactional(transactionalID string) Option {
	return func(c *config) {
		if transactionalID != "" {
			c.transact = true
			c.transactID = transactionalID
		}
	}
}

// WithClientOptions appends raw franz-go options for tuning this package does
// not surface (fetch sizes, timeouts, instrumentation). They are applied after
// the options this package derives, so they win on conflict. It is the
// power-user seam; prefer the typed With* options above.
func WithClientOptions(opts ...kgo.Opt) Option {
	return func(c *config) {
		for _, o := range opts {
			if o != nil {
				c.extraOpts = append(c.extraOpts, o)
			}
		}
	}
}

// WithClient injects a pre-built *kgo.Client, bypassing this package's client
// construction entirely. The inlet then neither dials nor closes the client:
// its lifecycle is the caller's. Use it to share a client or apply
// configuration this package does not expose. A nil client is ignored.
//
// A client injected this way must already be configured to consume as a group
// member with AutoCommitMarks (and BlockRebalanceOnPoll) for the ack model to
// hold; the typed options are the supported path.
func WithClient(client *kgo.Client) Option {
	return func(c *config) {
		if client != nil {
			c.client = client
		}
	}
}

// Inlet is a franz-go-backed [source.Inlet]. It builds (or wraps) a *kgo.Client
// and opens a [source.Subscription] that the engine drives. One Inlet maps to
// one client; open a single Subscription per Inlet (the Hopper drives one).
//
// Inlet is safe for concurrent use across its own methods, though a single
// Subscribe is the expected pattern.
type Inlet struct {
	cfg          config
	client       *kgo.Client
	ownsClient   bool
	transactSess *kgo.GroupTransactSession
}

// New constructs an [Inlet] from opts. It does not dial: the franz-go client is
// built lazily on the first Subscribe (so the consumer group and topics are
// known), unless a client was injected with [WithClient]. New fails only if no
// seed brokers and no client were provided.
func New(opts ...Option) (*Inlet, error) {
	cfg := config{}
	for _, o := range opts {
		if o != nil {
			o(&cfg)
		}
	}
	if cfg.client == nil && len(cfg.seedBrokers) == 0 {
		return nil, ErrNoSeedBrokers
	}
	in := &Inlet{cfg: cfg}
	if cfg.client != nil {
		in.client = cfg.client
		in.ownsClient = false
	}
	return in, nil
}

// baseOpts assembles the franz-go options shared by every consume client this
// package builds: seed brokers, client ID, auth, TLS, and any raw extras.
func (in *Inlet) baseOpts() []kgo.Opt {
	opts := []kgo.Opt{kgo.SeedBrokers(in.cfg.seedBrokers...)}
	if in.cfg.clientID != "" {
		opts = append(opts, kgo.ClientID(in.cfg.clientID))
	}
	if len(in.cfg.sasl) > 0 {
		opts = append(opts, kgo.SASL(in.cfg.sasl...))
	}
	if in.cfg.tlsConfig != nil {
		opts = append(opts, kgo.DialTLSConfig(in.cfg.tlsConfig))
	}
	opts = append(opts, in.cfg.extraOpts...)
	return opts
}

// consumeOpts adds the consumer-group, mark-commit, and safe-rebalance options
// that implement the ack model onto the base options for cfg.
func (in *Inlet) consumeOpts(sc source.SubscribeConfig) []kgo.Opt {
	opts := in.baseOpts()
	opts = append(opts,
		kgo.ConsumeTopics(sc.Topics...),
		// AutoCommitMarks: only records the engine marks (on a successful
		// settle) are eligible to commit — never commit-before-process.
		kgo.AutoCommitMarks(),
		// BlockRebalanceOnPoll gives the engine a safe processing window: a
		// rebalance cannot move partitions mid-batch; the subscription drains
		// and commits in OnRevoked, then releases the rebalance.
		kgo.BlockRebalanceOnPoll(),
	)
	if sc.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(sc.Group))
	}
	if len(in.cfg.balancers) > 0 {
		opts = append(opts, kgo.Balancers(in.cfg.balancers...))
	}
	opts = in.appendStartOffset(opts)
	return opts
}

// appendStartOffset appends the typed cold-start policy onto an option set.
func (in *Inlet) appendStartOffset(opts []kgo.Opt) []kgo.Opt {
	if !in.cfg.startOffsetSet {
		return opts
	}
	off := kgo.NewOffset().AtStart()
	if in.cfg.startOffset == StartLatest {
		off = kgo.NewOffset().AtEnd()
	}
	return append(opts, kgo.ConsumeStartOffset(off))
}

// transactOpts assembles the franz-go options for an EOS group transact session:
// the base group options without auto-commit (the transaction commits offsets),
// the transactional ID that fences a zombie producer, and read-committed fetch
// isolation so the consumer never reads another producer's uncommitted records.
// Auto-committing is disabled during transactional consuming, so this path does
// NOT add AutoCommitMarks; the engine still marks records, and End commits the
// marked offsets transactionally.
func (in *Inlet) transactOpts(sc source.SubscribeConfig) []kgo.Opt {
	opts := in.baseOpts()
	opts = append(opts,
		kgo.ConsumeTopics(sc.Topics...),
		kgo.TransactionalID(in.cfg.transactID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// A safe processing window: a rebalance cannot move partitions mid-batch,
		// and franz-go aborts the in-flight transaction on a rebalance.
		kgo.BlockRebalanceOnPoll(),
	)
	if sc.Group != "" {
		opts = append(opts, kgo.ConsumerGroup(sc.Group))
	}
	if len(in.cfg.balancers) > 0 {
		opts = append(opts, kgo.Balancers(in.cfg.balancers...))
	}
	opts = in.appendStartOffset(opts)
	return opts
}

// Subscribe builds (or reuses) the franz-go client for cfg and returns a live
// [source.Subscription]. At least one topic is required. The returned
// subscription owns the rebalance hooks, so it must be installed before the
// client begins polling; this method wires them when it builds the client.
func (in *Inlet) Subscribe(_ context.Context, cfg source.SubscribeConfig) (source.Subscription, error) {
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("source/kafka: subscribe: %w", errors.New("at least one topic required"))
	}

	// A transactional inlet is backed by a single GroupTransactSession that can
	// fence exactly one consumer; a second Subscribe cannot share it, and a
	// second subscription would silently come up without a transact session
	// (Begin would report not-transactional). Reject it loudly instead.
	if in.cfg.transact && in.transactSess != nil {
		return nil, fmt.Errorf("source/kafka: %w", errTransactionalSingleSubscribe)
	}

	sub := &subscription{
		dlqTopic: in.cfg.dlqTopic,
		maxPoll:  in.cfg.maxPoll,
	}

	if in.client == nil {
		opts := in.consumeOpts(cfg)
		if in.cfg.transact {
			// The transactional path uses read-committed isolation, a
			// transactional ID, and no auto-commit; build its own option set.
			opts = in.transactOpts(cfg)
		}
		// The subscription's rebalance trampolines are installed at client
		// build time so assign/revoke callbacks registered later still fire.
		opts = append(opts,
			kgo.OnPartitionsAssigned(sub.onAssigned),
			kgo.OnPartitionsRevoked(sub.onRevoked),
			kgo.OnPartitionsLost(sub.onLost),
		)
		if in.cfg.transact {
			sess, err := kgo.NewGroupTransactSession(opts...)
			if err != nil {
				return nil, fmt.Errorf("source/kafka: new transact session: %w", err)
			}
			in.transactSess = sess
			in.client = sess.Client()
			sub.transactSess = sess
		} else {
			client, err := kgo.NewClient(opts...)
			if err != nil {
				return nil, fmt.Errorf("source/kafka: new client: %w", err)
			}
			in.client = client
		}
		in.ownsClient = true
	}

	sub.client = in.client
	sub.group = cfg.Group
	return sub, nil
}

// Close releases the inlet's client when the inlet built it. When a client was
// injected with [WithClient] the caller owns its lifecycle and Close is a no-op
// for it. Close live subscriptions first; closing the inlet does not settle
// in-flight records.
func (in *Inlet) Close() error {
	if !in.ownsClient || in.client == nil {
		return nil
	}
	// Release any rebalance the final poll left blocked. With
	// BlockRebalanceOnPoll set, LeaveGroup during Close waits for an
	// AllowRebalance that the now-stopped poll loop will never make, so Close
	// would deadlock; calling it here lets the client leave the group cleanly.
	in.client.AllowRebalance()
	if in.transactSess != nil {
		in.transactSess.Close()
	} else {
		in.client.Close()
	}
	in.client = nil
	return nil
}

// As assigns the underlying *kgo.Client to target if target is a **kgo.Client,
// returning whether it did. It is the documented escape hatch for power users
// who must reach the franz-go client (custom admin calls, manual commits);
// prefer the neutral [source.Inlet] surface. It reports false before the first
// Subscribe builds the client.
func (in *Inlet) As(target any) bool {
	if p, ok := target.(**kgo.Client); ok && in.client != nil {
		*p = in.client
		return true
	}
	return false
}

// Compile-time assertion that *Inlet satisfies the core ingress adapter.
var _ source.Inlet = (*Inlet)(nil)
