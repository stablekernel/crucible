// SPDX-License-Identifier: Apache-2.0

//go:build integration

package redis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/stablekernel/crucible/source"
	credis "github.com/stablekernel/crucible/source/redis"
)

// TestIntegrationConsumeAckNakTerm starts a real Redis container, XADDs three
// entries to a stream, consumes them through the Inlet's consumer group, and
// exercises the settle vocabulary against a live server:
//
//   - the ack'd entry is XACK'd off the pending list,
//   - the nak'd entry stays pending and is redelivered via NakRedeliver, then
//     ack'd,
//   - the term'd entry lands on the dead-letter stream and is ack'd off the
//     original.
//
// It skips cleanly when Docker is not reachable.
func TestIntegrationConsumeAckNakTerm(t *testing.T) {
	t.Parallel()
	skipWithoutDocker(t)

	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		t.Skipf("redis.Run() unavailable (image pull or startup failed); skipping: %v", err)
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

	const (
		stream = "orders"
		dlq    = "orders.dlq"
	)
	// Produce three entries: one to ack, one to nak-then-ack, one to term.
	for _, body := range []string{"ack-me", "nak-me", "term-me"} {
		if err = client.XAdd(ctx, &goredis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: map[string]any{"value": body},
		}).Err(); err != nil {
			t.Fatalf("XAdd(%q) error = %v", body, err)
		}
	}

	in, err := credis.New(
		credis.WithClient(client),
		credis.WithGroup("it-workers"),
		credis.WithConsumer("worker-1"),
		credis.WithDLQStream(dlq),
		credis.WithBlock(time.Second),
		credis.WithCount(16),
		credis.WithMinIdle(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })

	sub, err := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{stream}})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// First pass: ack one, term one, nak one (leaving it pending).
	var nakedID string
	for seen := 0; seen < 3; {
		nctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		m, nerr := sub.Next(nctx)
		cancel()
		if nerr != nil {
			if errors.Is(nerr, context.DeadlineExceeded) {
				continue
			}
			t.Fatalf("Next() error = %v", nerr)
		}
		seen++
		switch string(m.Value()) {
		case "ack-me":
			if err := sub.Settle(ctx, m, source.Ack()); err != nil {
				t.Fatalf("Settle(ack) error = %v", err)
			}
		case "term-me":
			if err := sub.Settle(ctx, m, source.Term(errors.New("poison"))); err != nil {
				t.Fatalf("Settle(term) error = %v", err)
			}
		case "nak-me":
			nakedID = m.Cursor().String()
			if err := sub.Settle(ctx, m, source.Nak(errors.New("transient"))); err != nil {
				t.Fatalf("Settle(nak) error = %v", err)
			}
		}
	}

	// The term'd entry must have landed on the dead-letter stream.
	dlqEntries, err := client.XRange(ctx, dlq, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange(dlq) error = %v", err)
	}
	if len(dlqEntries) != 1 || dlqEntries[0].Values["value"] != "term-me" {
		t.Fatalf("dlq entries = %#v, want one term-me", dlqEntries)
	}

	// The nak'd entry stays pending: redeliver it after it idles past MinIdle.
	// NakRedeliver is a Redis-specific surface beyond the source.Subscription
	// interface, reached by asserting the concrete capability.
	redeliverer, ok := sub.(interface {
		NakRedeliver(ctx context.Context, minIdle time.Duration) (int, error)
	})
	if !ok {
		t.Fatal("subscription does not expose NakRedeliver")
	}
	time.Sleep(100 * time.Millisecond)
	n, err := redeliverer.NakRedeliver(ctx, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NakRedeliver() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("NakRedeliver() reclaimed %d, want 1", n)
	}
	m, err := sub.Next(ctx)
	if err != nil {
		t.Fatalf("Next() after redeliver error = %v", err)
	}
	if string(m.Value()) != "nak-me" || m.Cursor().String() != nakedID {
		t.Fatalf("redelivered = %q (%s), want nak-me (%s)", m.Value(), m.Cursor(), nakedID)
	}
	if err := sub.Settle(ctx, m, source.Ack()); err != nil {
		t.Fatalf("Settle(ack after nak) error = %v", err)
	}

	// Nothing remains pending once every entry is settled.
	pending, err := client.XPending(ctx, stream, "it-workers").Result()
	if err != nil {
		t.Fatalf("XPending() error = %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d, want 0 after all settled", pending.Count)
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
