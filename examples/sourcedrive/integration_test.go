// SPDX-License-Identifier: Apache-2.0

//go:build integration

package sourcedrive_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stablekernel/crucible/examples/sourcedrive"
	"github.com/stablekernel/crucible/source"
	kafkasource "github.com/stablekernel/crucible/source/kafka"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v23.3.3"

// TestIntegration_DriveStatechartFromRedPanda starts a real RedPanda broker,
// produces a funded pay command and a redelivery of the same command id, then
// consumes them through a Kafka inlet bound to a fulfillment statechart. It
// proves the first delivery advances the instance to shipped and the redelivery
// is a no-op ack (exactly-once into the machine) against a live broker. It skips
// cleanly when Docker is unreachable.
func TestIntegration_DriveStatechartFromRedPanda(t *testing.T) {
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
		topic = "fulfillment"
		group = "sourcedrive-it"
		key   = "ship-1"
	)

	// Create the topic up front so neither the producer below nor the inlet races
	// topic auto-creation on the broker.
	createTopics(ctx, t, broker, topic)

	// Produce a funded pay command and an exact redelivery (same message-id).
	prod, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("producer client error = %v", err)
	}
	t.Cleanup(prod.Close)

	body, _ := json.Marshal(sourcedrive.Command{Op: "pay"})
	for range 2 {
		r := &kgo.Record{
			Topic: topic,
			Key:   []byte(key),
			Value: body,
			Headers: []kgo.RecordHeader{
				{Key: "content-type", Value: []byte("application/json")},
				{Key: "message-id", Value: []byte("evt-1")},
			},
		}
		if perr := prod.ProduceSync(ctx, r).FirstErr(); perr != nil {
			t.Fatalf("produce error = %v", perr)
		}
	}

	// Consume the two records through the fulfillment handler.
	inlet, err := kafkasource.New(
		kafkasource.WithSeedBrokers(broker),
		kafkasource.WithClientID("sourcedrive-it"),
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

	f := sourcedrive.NewFulfillment()
	if serr := f.Seed(ctx, key, true); serr != nil {
		t.Fatalf("seed: %v", serr)
	}

	// Pull and settle both records directly, mirroring the consume loop, so the
	// test controls the stop condition without a background Hopper.
	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for range 2 {
		m, nerr := sub.Next(pollCtx)
		if nerr != nil {
			t.Fatalf("Next() error = %v", nerr)
		}
		res := f.Handler(pollCtx, m)
		if serr := sub.Settle(pollCtx, m, res); serr != nil {
			t.Fatalf("Settle() error = %v", serr)
		}
	}
	if cerr := sub.Close(); cerr != nil {
		t.Fatalf("Close() error = %v", cerr)
	}

	rec, ok, lerr := f.Store.Load(ctx, key)
	if lerr != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, lerr)
	}
	if rec.Snapshot.Current != "shipped" {
		t.Fatalf("state = %q, want shipped", rec.Snapshot.Current)
	}
	if rec.Version != 2 {
		t.Fatalf("version = %d, want 2 (one transition; redelivery deduped)", rec.Version)
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
