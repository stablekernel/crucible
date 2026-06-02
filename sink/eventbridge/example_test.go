// SPDX-License-Identifier: Apache-2.0

package eventbridge_test

import (
	"context"
	"fmt"

	awseb "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	csink "github.com/stablekernel/crucible/sink"
	ebsink "github.com/stablekernel/crucible/sink/eventbridge"
)

// recordingClient records the detail-type of each PutEvents entry.
type recordingClient struct{ detailTypes []string }

func (r *recordingClient) PutEvents(_ context.Context, params *awseb.PutEventsInput, _ ...func(*awseb.Options)) (*awseb.PutEventsOutput, error) {
	for _, e := range params.Entries {
		if e.DetailType != nil {
			r.detailTypes = append(r.detailTypes, *e.DetailType)
		}
	}
	return &awseb.PutEventsOutput{}, nil
}

type productViewed struct{ ProductID string }

func ExampleNew() {
	const (
		bus        = "commerce"
		source     = "com.example.catalog"
		detailType = "ProductViewed"
	)

	c := &recordingClient{}
	reg := ebsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, e productViewed) csink.Op[ebsink.Client] {
		return ebsink.PutEvent(bus, source, detailType, `{"id":"`+e.ProductID+`"}`)
	})

	outlet := ebsink.New(c, reg)
	_ = outlet.Sink(context.Background(), productViewed{ProductID: "PROD-99"})

	fmt.Println(c.detailTypes[0])
	// Output: ProductViewed
}
