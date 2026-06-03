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
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/stablekernel/crucible/examples/sourcedrive"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka seed brokers")
	topic := flag.String("topic", "fulfillment", "topic to consume fulfillment commands from")
	group := flag.String("group", "sourcedrive", "consumer group")
	clientID := flag.String("client-id", "sourcedrive", "client id reported to the broker")
	dlq := flag.String("dlq-topic", "", "optional dead-letter topic for poison messages")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := sourcedrive.KafkaConfig{
		Brokers:  splitAndTrim(*brokers),
		Topic:    *topic,
		Group:    *group,
		ClientID: *clientID,
		DLQTopic: *dlq,
	}

	if err := sourcedrive.RunKafka(ctx, logger, cfg); err != nil {
		logger.Error("sourcedrive exited", slog.Any("err", err))
		os.Exit(1)
	}
}

// splitAndTrim splits a comma-separated broker list into trimmed, non-empty
// addresses.
func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
