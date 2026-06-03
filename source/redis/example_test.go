// SPDX-License-Identifier: Apache-2.0

package redis_test

import (
	"context"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/stablekernel/crucible/source"
	credis "github.com/stablekernel/crucible/source/redis"
)

// exampleClient is a stand-in Client seam that yields one stream entry, so the
// example runs without a live Redis server. It implements only the methods the
// consume-and-ack path touches; a real program passes redis.WithAddr or
// redis.WithClient(realClient) instead.
type exampleClient struct {
	credis.Client
	delivered bool
}

func (c *exampleClient) XGroupCreateMkStream(ctx context.Context, _, _, _ string) *goredis.StatusCmd {
	return goredis.NewStatusCmd(ctx)
}

func (c *exampleClient) XReadGroup(ctx context.Context, a *goredis.XReadGroupArgs) *goredis.XStreamSliceCmd {
	cmd := goredis.NewXStreamSliceCmd(ctx)
	if c.delivered {
		cmd.SetErr(goredis.Nil) // no further entries
		return cmd
	}
	c.delivered = true
	cmd.SetVal([]goredis.XStream{{
		Stream: a.Streams[0],
		Messages: []goredis.XMessage{{
			ID:     "1526919030474-0",
			Values: map[string]any{"value": "placed", "crucible-key": "A-1"},
		}},
	}})
	return cmd
}

func (c *exampleClient) XAck(ctx context.Context, _, _ string, _ ...string) *goredis.IntCmd {
	cmd := goredis.NewIntCmd(ctx)
	cmd.SetVal(1)
	return cmd
}

// Example shows the consume-and-settle loop: open a consumer-group
// subscription, take the next entry off the stream, and ack it after handling.
// In a real program the seam is a live client supplied via WithAddr or
// WithClient.
func Example() {
	in, err := credis.New(
		credis.WithClient(&exampleClient{}),
		credis.WithGroup("orders-svc"),
		credis.WithConsumer("worker-1"),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = in.Close() }()

	sub, err := in.Subscribe(context.Background(), source.SubscribeConfig{Topics: []string{"orders"}})
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
	fmt.Println(string(m.Value()))
	// Output:
	// orders
	// A-1
	// placed
}
