// SPDX-License-Identifier: Apache-2.0

// Package redis is a sink destination that publishes payloads to Redis. It
// supports two write surfaces: Redis Streams via [XAdd] and Pub/Sub via
// [Publish]. Register a transformer per payload type that maps it to one of
// those operations, then attach the result of [New] to a sink.Manifold.
//
// The [Client] interface is the narrow surface this destination needs; the
// real *redis.Client from github.com/redis/go-redis/v9 satisfies it
// structurally without any extra wiring.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
	csink "github.com/stablekernel/crucible/sink"
)

// Client is the narrow Redis surface this destination needs. It is satisfied
// by *redis.Client from github.com/redis/go-redis/v9, so callers wire the
// real client without this package owning the connection lifecycle.
type Client interface {
	// XAdd appends a message to a Redis Stream.
	XAdd(ctx context.Context, a *redis.XAddArgs) *redis.StringCmd
	// Publish posts a message to a Redis Pub/Sub channel.
	Publish(ctx context.Context, channel string, message any) *redis.IntCmd
}

// XAdd returns an Op that appends values to a Redis Stream. The stream is
// created automatically if it does not exist. The message ID is assigned by
// the server ("*").
func XAdd(stream string, values map[string]any) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		return c.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			ID:     "*",
			Values: values,
		}).Err()
	})
}

// Publish returns an Op that posts message to a Redis Pub/Sub channel.
func Publish(channel string, message any) csink.Op[Client] {
	return csink.OpFunc[Client](func(ctx context.Context, c Client) error {
		return c.Publish(ctx, channel, message).Err()
	})
}

// NewRegistry returns an empty registry of Op[Client] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Client]] {
	return csink.NewRegistry[csink.Op[Client]]()
}

// New builds an Outlet that applies each payload's registered Op[Client] to
// client. The outlet is named "redis" unless overridden with sink.WithName.
func New(client Client, reg *csink.Registry[csink.Op[Client]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Client](client, reg, append([]csink.EmitterOption{csink.WithName("redis")}, opts...)...)
}
