// SPDX-License-Identifier: Apache-2.0

//go:build integration

package jetstream_test

import (
	"context"
	"errors"
	"testing"
	"time"

	gonats "github.com/nats-io/nats.go"
	njs "github.com/nats-io/nats.go/jetstream"
	"github.com/testcontainers/testcontainers-go"
	tcnats "github.com/testcontainers/testcontainers-go/modules/nats"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/jetstream"
)

// TestIntegrationConsumeAckNakTerm starts a real NATS container with JetStream
// enabled, publishes to a stream, consumes through the Inlet, and exercises the
// settle vocabulary: a Nak'd message is redelivered, then ack'd; a Term'd
// message is not redelivered. It skips cleanly when Docker is not reachable.
func TestIntegrationConsumeAckNakTerm(t *testing.T) {
	skipWithoutDocker(t)

	ctx := context.Background()
	// The tcnats module starts the server with -js, so JetStream is enabled.
	container, err := tcnats.Run(ctx, "nats:2.10-alpine")
	if err != nil {
		t.Skipf("nats.Run() unavailable (image pull or startup failed); skipping: %v", err)
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

	js, err := njs.New(conn)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}

	const stream = "ORDERS"
	const subject = "orders.placed"
	if _, err = js.CreateStream(ctx, njs.StreamConfig{
		Name:     stream,
		Subjects: []string{"orders.>"},
	}); err != nil {
		t.Fatalf("CreateStream() error = %v", err)
	}

	// Publish three messages: one to ack, one to nak-then-ack, one to term.
	for _, body := range []string{"ack-me", "nak-me", "term-me"} {
		if _, err = js.Publish(ctx, subject, []byte(body)); err != nil {
			t.Fatalf("Publish(%q) error = %v", body, err)
		}
	}

	in, err := jetstream.New(
		jetstream.WithConn(conn),
		jetstream.WithStream(stream),
		jetstream.WithDurable("it-workers"),
		jetstream.WithAckWait(time.Second),
		jetstream.WithMaxAckPending(64),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = in.Close() })

	sub, err := in.Subscribe(ctx, source.SubscribeConfig{Topics: []string{subject}})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })

	// Drain the three published messages, settling each by body.
	deadline := time.Now().Add(20 * time.Second)
	var ackedNak bool
	var nakSeen int
	for !ackedNak {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for nak redelivery + ack")
		}
		nctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := sub.Next(nctx)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			t.Fatalf("Next() error = %v", err)
		}
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
			nakSeen++
			if nakSeen == 1 {
				// first delivery: nak for redelivery
				if err := sub.Settle(ctx, m, source.Nak(errors.New("transient"))); err != nil {
					t.Fatalf("Settle(nak) error = %v", err)
				}
				continue
			}
			// redelivered: ack it
			if err := sub.Settle(ctx, m, source.Ack()); err != nil {
				t.Fatalf("Settle(ack after nak) error = %v", err)
			}
			ackedNak = true
		}
	}

	if nakSeen < 2 {
		t.Fatalf("nak-me delivered %d times, want >= 2 (redelivery)", nakSeen)
	}

	// The term'd and ack'd messages must not redeliver: a further read should
	// time out rather than yield them again.
	nctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if m, err := sub.Next(nctx); err == nil {
		t.Fatalf("unexpected redelivery of %q after ack/term", m.Value())
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
