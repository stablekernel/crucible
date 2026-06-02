// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redis_test

import (
	"context"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	csink "github.com/stablekernel/crucible/sink"
	redissink "github.com/stablekernel/crucible/sink/redis"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string
}

// TestIntegrationSinkAddsToRealStream starts a real Redis container, drives the
// real go-redis XAdd path through the Outlet, then reads the stream back to
// prove the message landed. It skips cleanly when Docker is not reachable.
func TestIntegrationSinkAddsToRealStream(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		t.Fatalf("redis.Run() error = %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	endpoint, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("ConnectionString() error = %v", err)
	}
	opts, err := goredis.ParseURL(endpoint)
	if err != nil {
		t.Fatalf("ParseURL() error = %v", err)
	}
	client := goredis.NewClient(opts)
	t.Cleanup(func() { _ = client.Close() })

	const stream = "orders"
	reg := redissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[redissink.Client] {
		return redissink.XAdd(stream, map[string]any{"id": o.ID})
	})

	outlet := redissink.New(client, reg)
	if err = outlet.Sink(ctx, orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	entries, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Values["id"] != "A-1" {
		t.Fatalf("stream entries = %#v, want one id A-1", entries)
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
