// SPDX-License-Identifier: Apache-2.0

// Command sourcedrive consumes a Kafka topic of fulfillment commands and drives
// a crucible statechart instance per shipment key, acking each message only
// after its transition is durably persisted.
//
// Usage:
//
//	go run ./cmd/sourcedrive -brokers localhost:9092 -topic fulfillment -group sourcedrive
//
// Produce JSON commands keyed by shipment id, for example a record keyed
// "ship-1" with body {"op":"pay"}; the program logs each applied transition.
package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/stablekernel/crucible/examples/sourcedrive"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(ctx, os.Args[1:], os.Stderr, logger); err != nil {
		logger.Error("sourcedrive exited", slog.Any("err", err))
		os.Exit(1)
	}
}

// run parses args into a [sourcedrive.KafkaConfig] and drives the consume loop
// with [sourcedrive.RunKafka]. It is split out of main so the flag wiring and
// config assembly are unit-testable; main itself is only signal wiring and the
// process exit code.
func run(ctx context.Context, args []string, errOut io.Writer, logger *slog.Logger) error {
	fs := flag.NewFlagSet("sourcedrive", flag.ContinueOnError)
	fs.SetOutput(errOut)
	brokers := fs.String("brokers", "localhost:9092", "comma-separated Kafka seed brokers")
	topic := fs.String("topic", "fulfillment", "topic to consume fulfillment commands from")
	group := fs.String("group", "sourcedrive", "consumer group")
	clientID := fs.String("client-id", "sourcedrive", "client id reported to the broker")
	dlq := fs.String("dlq-topic", "", "optional dead-letter topic for poison messages")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := sourcedrive.KafkaConfig{
		Brokers:  sourcedrive.SplitBrokers(*brokers),
		Topic:    *topic,
		Group:    *group,
		ClientID: *clientID,
		DLQTopic: *dlq,
	}

	return sourcedrive.RunKafka(ctx, logger, cfg)
}
