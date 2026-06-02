// SPDX-License-Identifier: Apache-2.0

//go:build integration

package gcppubsub_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/testcontainers/testcontainers-go"
	tcpubsub "github.com/testcontainers/testcontainers-go/modules/gcloud/pubsub"

	csink "github.com/stablekernel/crucible/sink"
	gcppubsubsink "github.com/stablekernel/crucible/sink/gcppubsub"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkPublishesToEmulator starts a real Pub/Sub emulator
// container, creates a topic and subscription, drives the real Pub/Sub publish
// path through the Outlet, then receives the message to prove it landed. It
// skips cleanly when Docker is not reachable.
func TestIntegrationSinkPublishesToEmulator(t *testing.T) {
	// No t.Parallel: this test sets PUBSUB_EMULATOR_HOST through t.Setenv, which
	// is process-global and incompatible with parallel tests.
	skipWithoutDocker(t)

	ctx := context.Background()
	const project = "crucible-test"
	container, err := tcpubsub.Run(
		ctx,
		"gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators",
		tcpubsub.WithProjectID(project),
	)
	if err != nil {
		t.Skipf("pubsub.Run() unavailable (image pull or startup failed); skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	// The Pub/Sub client auto-detects the emulator from this environment
	// variable and dials it with insecure transport.
	t.Setenv("PUBSUB_EMULATOR_HOST", container.URI())

	const (
		topicID = "projects/crucible-test/topics/orders"
		subID   = "projects/crucible-test/subscriptions/orders-sub"
	)
	client, err := pubsub.NewClient(ctx, project)
	if err != nil {
		t.Fatalf("pubsub.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if _, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicID}); err != nil {
		t.Fatalf("CreateTopic() error = %v", err)
	}
	if _, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subID,
		Topic: topicID,
	}); err != nil {
		t.Fatalf("CreateSubscription() error = %v", err)
	}

	publisher := client.Publisher(topicID)
	t.Cleanup(publisher.Stop)

	reg := gcppubsubsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[gcppubsubsink.Publisher] {
		return gcppubsubsink.Publish([]byte(o.ID), map[string]string{"kind": "placed"})
	})

	outlet := gcppubsubsink.New(gcppubsubsink.Adapt(publisher), reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	got := receiveOne(t, ctx, client.Subscriber(subID))
	if string(got.Data) != "A-1" || got.Attributes["kind"] != "placed" {
		t.Fatalf("received message data=%q attrs=%v, want A-1 / kind=placed", got.Data, got.Attributes)
	}
}

// receiveOne blocks until one message arrives on the subscriber or the deadline
// elapses, acknowledging and returning the first message it sees.
func receiveOne(t *testing.T, ctx context.Context, sub *pubsub.Subscriber) *pubsub.Message {
	t.Helper()
	recvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	got := make(chan *pubsub.Message, 1)
	err := sub.Receive(recvCtx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()
		select {
		case got <- m:
		default:
		}
		cancel()
	})
	if err != nil && recvCtx.Err() == nil {
		t.Fatalf("Receive() error = %v", err)
	}

	select {
	case m := <-got:
		return m
	default:
		t.Fatalf("no message received before deadline")
		return nil
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
