// SPDX-License-Identifier: Apache-2.0

package nats_test

import (
	"context"
	"fmt"

	gonats "github.com/nats-io/nats.go"
	csink "github.com/stablekernel/crucible/sink"
	natssink "github.com/stablekernel/crucible/sink/nats"
)

// recordingClient is a stand-in Client that records subjects and data.
type recordingClient struct {
	subjects []string
	data     [][]byte
}

func (r *recordingClient) Publish(subject string, data []byte) error {
	r.subjects = append(r.subjects, subject)
	r.data = append(r.data, data)
	return nil
}

func (r *recordingClient) PublishMsg(m *gonats.Msg) error {
	r.subjects = append(r.subjects, m.Subject)
	r.data = append(r.data, m.Data)
	return nil
}

type productListed struct{ SKU string }

func ExampleNew() {
	c := &recordingClient{}
	reg := natssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p productListed) csink.Op[natssink.Client] {
		return natssink.Publish("catalog.listed", []byte(p.SKU))
	})

	outlet := natssink.New(c, reg)
	_ = outlet.Sink(context.Background(), productListed{SKU: "SKU-42"})

	fmt.Println(c.subjects[0])
	fmt.Println(string(c.data[0]))
	// Output:
	// catalog.listed
	// SKU-42
}

func ExamplePublishMsg() {
	c := &recordingClient{}
	reg := natssink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p productListed) csink.Op[natssink.Client] {
		return natssink.PublishMsg(&gonats.Msg{
			Subject: "catalog.listed.v2",
			Data:    []byte(p.SKU),
		})
	})

	outlet := natssink.New(c, reg)
	_ = outlet.Sink(context.Background(), productListed{SKU: "SKU-99"})

	fmt.Println(c.subjects[0])
	fmt.Println(string(c.data[0]))
	// Output:
	// catalog.listed.v2
	// SKU-99
}
