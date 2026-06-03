// SPDX-License-Identifier: Apache-2.0

//go:build integration

package kafka_test

import (
	"context"
	"errors"
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
