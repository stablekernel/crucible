// SPDX-License-Identifier: Apache-2.0

package kafka_test

import (
	"context"
	"fmt"

	kafkasource "github.com/stablekernel/crucible/source/kafka"
)

// ExampleNew builds an Inlet over a Kafka cluster and reports its name. In a
// real program the Inlet is handed to a source.Hopper, which drives the consume
// loop, decoding, ordering, and settlement; here we only construct it to keep
// the example broker-free.
func ExampleNew() {
	inlet, err := kafkasource.New(
		kafkasource.WithSeedBrokers("localhost:9092"),
		kafkasource.WithClientID("orders-consumer"),
		kafkasource.WithDLQTopic("orders.DLQ"),
	)
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer func() { _ = inlet.Close() }()

	// The inlet is a source.Inlet: hand it to a Hopper to consume.
	//   sub, _ := inlet.Subscribe(ctx, source.SubscribeConfig{Topics: []string{"orders"}, Group: "orders"})
	//   hopper.Run(ctx, sub, handler)
	_ = context.Background()

	fmt.Println("inlet ready")
	// Output: inlet ready
}
