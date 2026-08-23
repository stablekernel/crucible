// SPDX-License-Identifier: Apache-2.0

//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/source"
	kafkasource "github.com/stablekernel/crucible/source/kafka"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v23.3.3"

// TestIntegrationConsumeAckTermRoundTrip starts a real RedPanda broker, produces
// records to a topic, consumes them through the Inlet, settles one Ack and one
// Term, and proves the committed offset advanced and the termed record landed on
// the dead-letter topic. It skips cleanly when Docker is unreachable.
func TestIntegrationConsumeAckTermRoundTrip(t *testing.T) {
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		t.Skipf("redpanda.Run unavailable (image pull or startup failed); skipping: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("KafkaSeedBroker() error = %v", err)
	}

	const (
		topic = "orders"
		dlq   = "orders.DLQ"
		group = "orders-consumer"
	)

	// Create the source and dead-letter topics up front so neither the producer
	// below nor the Inlet's internal DLQ producer races topic auto-creation. The
	// Inlet's DLQ client does not enable AllowAutoTopicCreation, so the DLQ topic
	// must exist before the first Term settles.
	createTopics(ctx, t, broker, topic, dlq)

	// Produce two records with a separate client.
	prod, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("producer client error = %v", err)
	}
	t.Cleanup(prod.Close)

	produce(ctx, t, prod, topic, "A-1", "good")
	produce(ctx, t, prod, topic, "A-2", "poison")

	// Consume through the Inlet.
	inlet, err := kafkasource.New(
		kafkasource.WithSeedBrokers(broker),
		kafkasource.WithClientID("it-consumer"),
		kafkasource.WithDLQTopic(dlq),
		kafkasource.WithClientOptions(kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = inlet.Close() })

	sub, err := inlet.Subscribe(ctx, source.SubscribeConfig{Topics: []string{topic}, Group: group})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}

	// Pull both records and settle: A-1 ack, A-2 term (dead-letter).
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	seen := map[string]bool{}
	for len(seen) < 2 {
		m, nerr := sub.Next(pollCtx)
		if nerr != nil {
			t.Fatalf("Next() error = %v (saw %v)", nerr, seen)
		}
		key := string(m.Key())
		seen[key] = true
		switch key {
		case "A-1":
			if serr := sub.Settle(pollCtx, m, source.Ack()); serr != nil {
				t.Fatalf("Settle(ack) error = %v", serr)
			}
		case "A-2":
			if serr := sub.Settle(pollCtx, m, source.Term(errors.New("poison payload"))); serr != nil {
				t.Fatalf("Settle(term) error = %v", serr)
			}
		default:
			t.Fatalf("unexpected key %q", key)
		}
	}

	// Close commits marked offsets.
	if cerr := sub.Close(); cerr != nil {
		t.Fatalf("Close() error = %v", cerr)
	}

	// Prove the termed record landed on the dead-letter topic.
	dlqClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(dlq),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("dlq client error = %v", err)
	}
	t.Cleanup(dlqClient.Close)

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 30*time.Second)
	defer dlqCancel()
	fetches := dlqClient.PollFetches(dlqCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("dlq PollFetches errors = %v", errs)
	}
	recs := fetches.Records()
	if len(recs) != 1 || string(recs[0].Key) != "A-2" {
		t.Fatalf("dlq records = %#v, want one A-2", recs)
	}
	if !hasHeader(recs[0].Headers, "crucible-source-topic", "orders") {
		t.Errorf("dlq record missing crucible-source-topic=orders header: %+v", recs[0].Headers)
	}
	if !hasHeader(recs[0].Headers, "crucible-class", "poison") {
		t.Errorf("dlq record missing crucible-class=poison header: %+v", recs[0].Headers)
	}
}

// TestIntegrationTransactionalEOSRoundTrip starts a real RedPanda broker and
// proves exactly-once consume-process-produce: it produces an input record,
// consumes it through a transactional Inlet, and runs two transactions for it.
// The first transaction produces an output record but is aborted (its work
// function fails), so neither the output record nor the input offset is
// committed; a read-committed reader sees no output, and the input is
// redelivered. The second transaction produces the same output and commits, so
// exactly one output record exists and the offset advances. The aborted attempt
// leaves no duplicate.
func TestIntegrationTransactionalEOSRoundTrip(t *testing.T) {
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		t.Skipf("redpanda.Run unavailable (image pull or startup failed); skipping: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("KafkaSeedBroker() error = %v", err)
	}

	const (
		inTopic  = "eos.in"
		outTopic = "eos.out"
		group    = "eos-consumer"
		txID     = "eos-it-v1"
	)
	createTopics(ctx, t, broker, inTopic, outTopic)

	prod, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("producer client error = %v", err)
	}
	t.Cleanup(prod.Close)
	produce(ctx, t, prod, inTopic, "K-1", "input")

	inlet, err := kafkasource.New(
		kafkasource.WithSeedBrokers(broker),
		kafkasource.WithClientID("eos-consumer"),
		kafkasource.WithTransactional(txID),
		kafkasource.WithClientOptions(kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = inlet.Close() })

	sub, err := inlet.Subscribe(ctx, source.SubscribeConfig{Topics: []string{inTopic}, Group: group})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	tx, ok := sub.(source.Transactional)
	if !ok {
		t.Fatal("subscription does not satisfy source.Transactional with WithTransactional")
	}

	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// First receive: open a transaction that produces the output, then fail the
	// work function so the transaction aborts. Nothing should be committed.
	m1, err := sub.Next(pollCtx)
	if err != nil {
		t.Fatalf("Next() #1 error = %v", err)
	}
	if string(m1.Key()) != "K-1" {
		t.Fatalf("first record key = %q, want K-1", m1.Key())
	}
	abortErr := errors.New("deliberate abort")
	beginErr := tx.Begin(pollCtx, m1, func(c context.Context, txn source.Tx) error {
		if perr := txn.Produce(c, source.ProducedRecord{Topic: outTopic, Key: []byte("K-1"), Value: []byte("output")}); perr != nil {
			return perr
		}
		return abortErr // abort: discard the produced record, do not commit the offset
	})
	if !errors.Is(beginErr, abortErr) {
		t.Fatalf("Begin() #1 = %v, want the abort error", beginErr)
	}

	// The input must be redelivered because its offset was not committed. Re-seek
	// the subscription to start so the redelivery is deterministic even though the
	// aborted transaction left the consumer position past the record.
	if sk, ok := sub.(source.Seekable); ok {
		if serr := sk.SeekToStart(pollCtx); serr != nil {
			t.Fatalf("SeekToStart() error = %v", serr)
		}
	}

	m2, err := sub.Next(pollCtx)
	if err != nil {
		t.Fatalf("Next() #2 (redelivery) error = %v", err)
	}
	if string(m2.Key()) != "K-1" {
		t.Fatalf("redelivered key = %q, want K-1 (aborted offset not committed)", m2.Key())
	}

	// Second transaction: produce the same output and commit. This is the only
	// transaction that lands an output record and advances the offset.
	commitErr := tx.Begin(pollCtx, m2, func(c context.Context, txn source.Tx) error {
		return txn.Produce(c, source.ProducedRecord{Topic: outTopic, Key: []byte("K-1"), Value: []byte("output")})
	})
	if commitErr != nil {
		t.Fatalf("Begin() #2 (commit) error = %v", commitErr)
	}

	if cerr := sub.Close(); cerr != nil {
		t.Fatalf("Close() error = %v", cerr)
	}

	// A read-committed reader must see exactly one output record: the aborted
	// transaction's produce is invisible, so there is no duplicate.
	outClient, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(outTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatalf("out client error = %v", err)
	}
	t.Cleanup(outClient.Close)

	var got []*kgo.Record
	readCtx, readCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readCancel()
	for len(got) < 1 {
		f := outClient.PollFetches(readCtx)
		if errs := f.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if fe.Err != nil && !errors.Is(fe.Err, context.DeadlineExceeded) {
					t.Fatalf("out PollFetches error = %v", fe.Err)
				}
			}
			break
		}
		got = append(got, f.Records()...)
	}
	if len(got) != 1 {
		t.Fatalf("committed output records = %d, want exactly 1 (no duplicate across the aborted transaction)", len(got))
	}
	if string(got[0].Key) != "K-1" || string(got[0].Value) != "output" {
		t.Errorf("output record = %s/%s, want K-1/output", got[0].Key, got[0].Value)
	}
}

// createTopics creates the given topics (one partition, replication factor one)
// against the broker and waits for the admin call to succeed, so produces never
// race auto-creation. An already-exists result is treated as success.
func createTopics(ctx context.Context, t *testing.T, broker string, topics ...string) {
	t.Helper()
	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client error = %v", err)
	}
	defer admClient.Close()

	adm := kadm.NewClient(admClient)
	createCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(createCtx, 1, 1, nil, topics...)
	if err != nil {
		t.Fatalf("CreateTopics(%v) error = %v", topics, err)
	}
	for _, ct := range resp {
		if ct.Err != nil && !errors.Is(ct.Err, kerr.TopicAlreadyExists) {
			t.Fatalf("CreateTopics(%q) error = %v", ct.Topic, ct.Err)
		}
	}
}

func produce(ctx context.Context, t *testing.T, c *kgo.Client, topic, key, value string) {
	t.Helper()
	r := &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value)}
	if err := c.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce %s error = %v", key, err)
	}
}

func hasHeader(hs []kgo.RecordHeader, key, value string) bool {
	for _, h := range hs {
		if h.Key == key && string(h.Value) == value {
			return true
		}
	}
	return false
}

func skipWithoutDocker(t *testing.T) {
	t.Helper()
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	defer func() { _ = provider.Close() }()
	if err := provider.Health(context.Background()); err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
}

// deliveryLog records every (topic, partition, offset) a group member
// delivered, so the rebalance test can assert the union across members covers
// every produced record exactly once.
type deliveryLog struct {
	mu     sync.Mutex
	counts map[string]int
}

func newDeliveryLog() *deliveryLog { return &deliveryLog{counts: map[string]int{}} }

func (d *deliveryLog) record(cursor string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.counts[cursor]++
}

func (d *deliveryLog) snapshot() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]int, len(d.counts))
	for k, v := range d.counts {
		out[k] = v
	}
	return out
}

// TestIntegrationRebalanceRevokeNoDuplicates drives two members of one consumer
// group through a membership change: B joins mid-stream, then leaves gracefully,
// forcing two rebalances while A keeps consuming. The adapter's revoke hook
// commits marked offsets before releasing partitions, so every record must be
// delivered exactly once across the whole group despite the churn.
func TestIntegrationRebalanceRevokeNoDuplicates(t *testing.T) {
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := redpanda.Run(ctx, redpandaImage)
	if err != nil {
		t.Skipf("redpanda.Run unavailable (image pull or startup failed); skipping: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	broker, err := container.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("KafkaSeedBroker() error = %v", err)
	}

	const (
		topic    = "rebalance"
		group    = "rebalance-it"
		records  = 40
		warmup   = 8 // records A consumes before B joins
		runFor   = 8 * time.Second
		tailFor  = 3 * time.Second
		pollStep = 500 * time.Millisecond
	)

	// Two partitions so the group actually splits work between members.
	admClient, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("admin client error = %v", err)
	}
	adm := kadm.NewClient(admClient)
	if _, err = adm.CreateTopics(ctx, 2, 1, nil, topic); err != nil {
		admClient.Close()
		t.Fatalf("CreateTopics(%s) error = %v", topic, err)
	}
	admClient.Close()

	prod, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("producer client error = %v", err)
	}
	t.Cleanup(prod.Close)
	for i := range records {
		produce(ctx, t, prod, topic, fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i))
	}

	log := newDeliveryLog()

	// drive consumes from sub until stop closes, acking and logging every
	// delivery keyed by its cursor ("topic/partition@offset"). Transient
	// rebalance-window errors are retried, not fatal.
	drive := func(sub source.Subscription, stop <-chan struct{}, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			pollCtx, cancel := context.WithTimeout(context.Background(), pollStep)
			m, err := sub.Next(pollCtx)
			cancel()
			if err != nil {
				continue // quiet broker, rebalance in progress, or step deadline
			}
			settleCtx, scancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = sub.Settle(settleCtx, m, source.Ack())
			scancel()
			if err == nil {
				log.record(m.Cursor().String())
			}
		}
	}
	_ = drive

	subA, inletA := mustSubscribe(t, broker, group, topic)
	defer func() { _ = inletA.Close() }()

	stopA := make(chan struct{})
	var wgA sync.WaitGroup
	wgA.Add(1)
	go drive(subA, stopA, &wgA)

	// Warm up: let A deliver at least warmup records so its marks are pending
	// when the first rebalance hits.
	deadline := time.Now().Add(20 * time.Second)
	for len(log.snapshot()) < warmup && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if len(log.snapshot()) < warmup {
		t.Fatalf("consumer A delivered %d records in 20s, want >= %d", len(log.snapshot()), warmup)
	}

	// B joins: forces a rebalance that moves partitions while A has settled-
	// but-maybe-uncommitted work, exercising the revoke-commit path.
	subB, inletB := mustSubscribe(t, broker, group, topic)
	stopB := make(chan struct{})
	var wgB sync.WaitGroup
	wgB.Add(1)
	go drive(subB, stopB, &wgB)

	time.Sleep(runFor)
	close(stopB)
	wgB.Wait()
	// Graceful leave: Close commits B's marks before releasing its partitions.
	if err := subB.Close(); err != nil {
		t.Fatalf("member B Close() error = %v", err)
	}
	_ = inletB.Close()
	time.Sleep(tailFor) // second rebalance folds B's partitions back into A
	close(stopA)
	wgA.Wait()
	if err := subA.Close(); err != nil {
		t.Fatalf("member A Close() error = %v", err)
	}

	got := log.snapshot()
	if len(got) != records {
		t.Fatalf("delivered %d distinct records, want %d", len(got), records)
	}
	var dupes []string
	for k, n := range got {
		if n != 1 {
			dupes = append(dupes, fmt.Sprintf("%s×%d", k, n))
		}
	}
	if len(dupes) > 0 {
		t.Fatalf("rebalance produced duplicate deliveries: %v", dupes)
	}
}

// mustSubscribe opens an Inlet and a group subscription against broker,
// failing the test on error.
func mustSubscribe(t *testing.T, broker, group, topic string) (source.Subscription, *kafkasource.Inlet) {
	t.Helper()
	inlet, err := kafkasource.New(
		kafkasource.WithSeedBrokers(broker),
		kafkasource.WithClientID(group+"-"+fmt.Sprint(time.Now().UnixNano())),
		kafkasource.WithClientOptions(
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	sub, err := inlet.Subscribe(context.Background(), source.SubscribeConfig{
		Topics: []string{topic},
		Group:  group,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	return sub, inlet
}
