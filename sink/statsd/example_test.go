// SPDX-License-Identifier: Apache-2.0

package statsd_test

import (
	"context"
	"fmt"
	"sort"
	"time"

	csink "github.com/stablekernel/crucible/sink"
	statsdsink "github.com/stablekernel/crucible/sink/statsd"
)

// printingClient writes each StatsD call to stdout for the example output.
type printingClient struct{ lines []string }

func (p *printingClient) Count(n string, v int64, _ []string, _ float64) error {
	p.lines = append(p.lines, fmt.Sprintf("count %s=%d", n, v))
	return nil
}

func (p *printingClient) Gauge(n string, v float64, _ []string, _ float64) error {
	p.lines = append(p.lines, fmt.Sprintf("gauge %s=%g", n, v))
	return nil
}
func (p *printingClient) Histogram(string, float64, []string, float64) error    { return nil }
func (p *printingClient) Distribution(string, float64, []string, float64) error { return nil }
func (p *printingClient) Timing(string, time.Duration, []string, float64) error { return nil }
func (p *printingClient) Set(string, string, []string, float64) error           { return nil }

// ExampleNewAggregator folds two counts of the same metric into one summed
// emission and keeps the last gauge write, emitting on Flush.
func ExampleNewAggregator() {
	pc := &printingClient{}
	agg := statsdsink.NewAggregator(pc, statsdsink.WithInterval(0))
	ctx := context.Background()

	_ = agg.Sink(ctx, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "orders.placed", Int: 2, Rate: 1})
	_ = agg.Sink(ctx, statsdsink.Metric{Type: statsdsink.TypeCount, Name: "orders.placed", Int: 3, Rate: 1})
	_ = agg.Sink(ctx, statsdsink.Metric{Type: statsdsink.TypeGauge, Name: "queue.depth", Value: 1, Rate: 1})
	_ = agg.Sink(ctx, statsdsink.Metric{Type: statsdsink.TypeGauge, Name: "queue.depth", Value: 9, Rate: 1})

	_ = agg.(csink.Flusher).Flush(ctx)

	sort.Strings(pc.lines)
	for _, l := range pc.lines {
		fmt.Println(l)
	}
	// Output:
	// count orders.placed=5
	// gauge queue.depth=9
}
