// SPDX-License-Identifier: Apache-2.0

//go:build integration

package nats_test

import (
	"context"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	csink "github.com/stablekernel/crucible/sink"
	natssink "github.com/stablekernel/crucible/sink/nats"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkPublishesToRealServer starts a real NATS container,
// subscribes to a subject, drives the real nats.go Publish path through the
// Outlet, then receives the message to prove it landed. It skips cleanly when
// Docker is not reachable.
func TestIntegrationSinkPublishesToRealServer(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := tcnats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Skipf("nats.Run() unavailable (image pull or startup failed); skipping integration test: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("ConnectionString() error = %v", err)
	}
	conn, err := gonats.Connect(url)
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(conn.Close)

	const subject = "orders.placed"
	sub, err := conn.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("SubscribeSync() error = %v", err)
	}
	if err = conn.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	reg := natssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[natssink.Client] {
		return natssink.Publish(subject, []byte(o.ID))
	})

	outlet := natssink.New(conn, reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg() error = %v", err)
	}
	if string(msg.Data) != "A-1" {
		t.Fatalf("received message = %q, want A-1", msg.Data)
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
