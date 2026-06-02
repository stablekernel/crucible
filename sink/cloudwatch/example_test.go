// SPDX-License-Identifier: Apache-2.0

package cloudwatch_test

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	csink "github.com/stablekernel/crucible/sink"
	cw "github.com/stablekernel/crucible/sink/cloudwatch"
)

// loggingClient records the log group of each PutLogEvents call.
type loggingClient struct{ groups []string }

func (l *loggingClient) PutLogEvents(_ context.Context, params *cloudwatchlogs.PutLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.PutLogEventsOutput, error) {
	if params.LogGroupName != nil {
		l.groups = append(l.groups, *params.LogGroupName)
	}
	return &cloudwatchlogs.PutLogEventsOutput{}, nil
}

type orderPlaced struct{ OrderID string }

func ExampleNew() {
	const (
		logGroup  = "/app/orders"
		logStream = "placed"
	)
	c := &loggingClient{}
	reg := cw.NewRegistry()
	csink.Register(reg, func(_ context.Context, e orderPlaced) csink.Op[cw.Client] {
		return cw.PutLogEvent(logGroup, logStream, "placed:"+e.OrderID)
	})

	outlet := cw.New(c, reg)
	_ = outlet.Sink(context.Background(), orderPlaced{OrderID: "ORD-99"})

	fmt.Println(c.groups[0])
	// Output: /app/orders
}
