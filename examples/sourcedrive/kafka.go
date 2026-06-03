// SPDX-License-Identifier: Apache-2.0

package sourcedrive

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	kafkasource "github.com/stablekernel/crucible/source/kafka"
)

// KafkaConfig is the broker wiring for RunKafka.
type KafkaConfig struct {
	// Brokers are the Kafka/RedPanda seed broker addresses.
	Brokers []string
	// Topic is the topic the fulfillment commands are consumed from.
	Topic string
	// Group is the consumer group the consume loop joins.
	Group string
	// ClientID identifies this consumer to the broker.
	ClientID string
	// DLQTopic, when set, is where poison messages (undecodable bodies,
	// state-invalid events) are produced before their offsets commit.
	DLQTopic string
}

// RunKafka is the broker-touching entrypoint: it constructs a source/kafka.Inlet
// over cfg's seed brokers and drives a fresh fulfillment through it with Run.
// Consuming a message fires a transition; the offset commits only after the
// transition is durably persisted, so redelivery is idempotent into the machine
// and a state-invalid event is dead-lettered rather than retried forever.
//
// It blocks until ctx is canceled or the consume loop fails. The cmd/sourcedrive
// program calls it; it is kept out of Run so the consume loop stays unit-testable
// with an in-memory inlet.
func RunKafka(ctx context.Context, logger *slog.Logger, cfg KafkaConfig) error {
	opts := []kafkasource.Option{
		kafkasource.WithSeedBrokers(cfg.Brokers...),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kafkasource.WithClientID(cfg.ClientID))
	}
	if cfg.DLQTopic != "" {
		opts = append(opts, kafkasource.WithDLQTopic(cfg.DLQTopic))
	}

	inlet, err := kafkasource.New(opts...)
	if err != nil {
		return fmt.Errorf("new kafka inlet: %w", err)
	}
	defer func() { _ = inlet.Close() }()

	f := NewFulfillment()
	return Run(ctx, logger, inlet, f, []string{cfg.Topic}, cfg.Group)
}

// SplitBrokers splits a comma-separated broker list into trimmed, non-empty
// seed-broker addresses. It lives in the package (not the cmd shell) so the
// flag-parsing entrypoint stays a thin, untested wrapper around tested logic.
func SplitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
