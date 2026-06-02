// SPDX-License-Identifier: Apache-2.0

package otel_test

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	csink "github.com/stablekernel/crucible/sink"
	otelsink "github.com/stablekernel/crucible/sink/otel"
)

type userRegistered struct{ Plan string }

func ExampleNew() {
	// A manual reader collects on demand, so the example is deterministic with no
	// collector or network.
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("example")

	reg := otelsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, u userRegistered) csink.Op[otelsink.Meter] {
		return otelsink.Counter("users.registered", 1, attribute.String("plan", u.Plan))
	})

	outlet := otelsink.New(meter, reg)
	_ = outlet.Sink(context.Background(), userRegistered{Plan: "pro"})

	var rm metricdata.ResourceMetrics
	_ = reader.Collect(context.Background(), &rm)
	sum := rm.ScopeMetrics[0].Metrics[0].Data.(metricdata.Sum[int64])
	fmt.Printf("%s=%d\n", rm.ScopeMetrics[0].Metrics[0].Name, sum.DataPoints[0].Value)
	// Output: users.registered=1
}
