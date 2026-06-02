// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"context"
	"fmt"

	csink "github.com/stablekernel/crucible/sink"
	kafkasink "github.com/stablekernel/crucible/sink/kafka"
)

// recordingProducer is a stand-in Producer that records the topics it publishes
// to. In production, NewProducer adapts a *kgo.Client onto the Producer
// interface instead.
type recordingProducer struct{ topics []string }

func (r *recordingProducer) Produce(_ context.Context, topic string, _, _ []byte) error {
	r.topics = append(r.topics, topic)
	return nil
}

type userRegistered struct{ Email string }

func ExampleNew() {
	p := &recordingProducer{}
	reg := kafkasink.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegistered) csink.Op[kafkasink.Producer] {
		return kafkasink.Produce("users", []byte(u.Email), []byte("registered"))
	})

	outlet := kafkasink.New(p, reg)
	_ = outlet.Sink(context.Background(), userRegistered{Email: "a@example.com"})

	fmt.Println(p.topics[0])
	// Output: users
}
