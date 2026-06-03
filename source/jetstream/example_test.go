// SPDX-License-Identifier: Apache-2.0

package jetstream_test

import (
	"context"
	"fmt"

	gonats "github.com/nats-io/nats.go"
	njs "github.com/nats-io/nats.go/jetstream"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/jetstream"
)

// exampleJS is a stand-in jetstream.JetStream seam that yields one message, so
// the example runs without a live NATS server. It embeds njs.JetStream (nil) and
// overrides only the two methods the adapter uses. A real program passes
// jetstream.WithURL or jetstream.WithConn instead.
type exampleJS struct {
	njs.JetStream
	msg njs.Msg
}

func (e exampleJS) CreateOrUpdateConsumer(context.Context, string, njs.ConsumerConfig) (njs.Consumer, error) {
	return exampleConsumer{msg: e.msg}, nil
}

func (e exampleJS) OrderedConsumer(context.Context, string, njs.OrderedConsumerConfig) (njs.Consumer, error) {
	return exampleConsumer{msg: e.msg}, nil
}

type exampleConsumer struct {
	njs.Consumer
	msg njs.Msg
}

func (c exampleConsumer) Messages(...njs.PullMessagesOpt) (njs.MessagesContext, error) {
	return &exampleIter{msg: c.msg}, nil
}

type exampleIter struct {
	msg  njs.Msg
	done bool
}

func (it *exampleIter) Next(...njs.NextOpt) (njs.Msg, error) {
	if it.done {
		return nil, njs.ErrMsgIteratorClosed
	}
	it.done = true
	return it.msg, nil
}
func (it *exampleIter) Stop()  {}
func (it *exampleIter) Drain() {}

// exampleMsg is a minimal njs.Msg whose Ack records that it was acknowledged.
type exampleMsg struct {
	njs.Msg
	acked *bool
}

func (m exampleMsg) Data() []byte    { return []byte("A-1") }
func (m exampleMsg) Subject() string { return "orders.placed" }
func (m exampleMsg) Headers() gonats.Header {
	return gonats.Header{jetstream.KeyHeader: []string{"A-1"}}
}

func (m exampleMsg) Metadata() (*njs.MsgMetadata, error) {
	return &njs.MsgMetadata{Sequence: njs.SequencePair{Stream: 1}}, nil
}
func (m exampleMsg) DoubleAck(context.Context) error { *m.acked = true; return nil }

// Example shows the consume-and-settle loop: open a durable pull subscription,
// take the next message off the stream, and ack it after handling. In a real
// program the seam is a live NATS connection supplied via WithURL or WithConn.
func Example() {
	var acked bool
	js := exampleJS{msg: exampleMsg{acked: &acked}}

	in, err := jetstream.New(
		jetstream.WithStream("ORDERS"),
		jetstream.WithDurable("orders-worker"),
		jetstream.WithJetStream(js),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = in.Close() }()

	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: []string{"orders.>"}})
	if err != nil {
		panic(err)
	}
	defer func() { _ = sub.Close() }()

	m, err := sub.Next(context.Background())
	if err != nil {
		panic(err)
	}
	if err := sub.Settle(context.Background(), m, source.Ack()); err != nil {
		panic(err)
	}

	fmt.Println(m.Subject())
	fmt.Println(string(m.Key()))
	fmt.Println(acked)
	// Output:
	// orders.placed
	// A-1
	// true
}
