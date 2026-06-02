// SPDX-License-Identifier: Apache-2.0

//go:build integration

package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"

	csink "github.com/stablekernel/crucible/sink"
	kafkasink "github.com/stablekernel/crucible/sink/kafka"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkProducesToRealBroker starts a real Kafka container, drives
// the real franz-go produce path through the Outlet, then consumes the topic to
// prove the record landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkProducesToRealBroker(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1")
	if err != nil {
		t.Fatalf("kafka.Run() error = %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("Brokers() error = %v", err)
	}

	const topic = "orders"
	producerClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient(producer) error = %v", err)
	}
	t.Cleanup(producerClient.Close)

	reg := kafkasink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[kafkasink.Producer] {
		return kafkasink.Produce(topic, []byte(o.ID), []byte("placed"))
	})

	outlet := kafkasink.New(kafkasink.NewProducer(producerClient), reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("kgo.NewClient(consumer) error = %v", err)
	}
	t.Cleanup(consumer.Close)

	pollCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(pollCtx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("PollFetches() errors = %v", errs)
	}
	records := fetches.Records()
	if len(records) != 1 || string(records[0].Key) != "A-1" || string(records[0].Value) != "placed" {
		t.Fatalf("consumed records = %#v, want one A-1=placed", records)
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
