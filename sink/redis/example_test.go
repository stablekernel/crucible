// SPDX-License-Identifier: Apache-2.0

package redis_test

import (
	"context"
	"fmt"

	goredisp "github.com/redis/go-redis/v9"
	csink "github.com/stablekernel/crucible/sink"
	redissink "github.com/stablekernel/crucible/sink/redis"
)

// streamRecorder is a recording Client that captures stream names written via
// XAdd. It is used by the example to produce deterministic output.
type streamRecorder struct {
	streams  []string
	channels []string
}

func (r *streamRecorder) XAdd(_ context.Context, a *goredisp.XAddArgs) *goredisp.StringCmd {
	r.streams = append(r.streams, a.Stream)
	return goredisp.NewStringResult("0-1", nil)
}

func (r *streamRecorder) Publish(_ context.Context, channel string, _ any) *goredisp.IntCmd {
	r.channels = append(r.channels, channel)
	return goredisp.NewIntResult(1, nil)
}

type orderCreated struct{ OrderID string }

// ExampleNew demonstrates wiring a registry and sinking a payload to a Redis
// Stream. In production, replace streamRecorder with the real *redis.Client.
func ExampleNew() {
	rc := &streamRecorder{}
	reg := redissink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderCreated) csink.Op[redissink.Client] {
		return redissink.XAdd("orders", map[string]any{"order_id": o.OrderID})
	})

	outlet := redissink.New(rc, reg)
	_ = outlet.Sink(context.Background(), orderCreated{OrderID: "A-1"})

	fmt.Println(rc.streams[0])
	// Output: orders
}
