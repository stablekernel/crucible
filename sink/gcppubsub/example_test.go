// SPDX-License-Identifier: Apache-2.0

package gcppubsub_test

import (
	"context"
	"fmt"

	csink "github.com/stablekernel/crucible/sink"
	"github.com/stablekernel/crucible/sink/gcppubsub"
)

// recordingPublisher is a stand-in Publisher that records the data it publishes.
type recordingPublisher struct{ data []string }

func (r *recordingPublisher) Publish(_ context.Context, data []byte, _ map[string]string) (string, error) {
	r.data = append(r.data, string(data))
	return "srv-id", nil
}

type userRegistered struct{ Email string }

func ExampleNew() {
	pub := &recordingPublisher{}
	reg := gcppubsub.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegistered) csink.Op[gcppubsub.Publisher] {
		return gcppubsub.Publish([]byte(u.Email), map[string]string{"event": "userRegistered"})
	})

	outlet := gcppubsub.New(pub, reg)
	_ = outlet.Sink(context.Background(), userRegistered{Email: "a@example.com"})

	fmt.Println(pub.data[0])
	// Output: a@example.com
}
