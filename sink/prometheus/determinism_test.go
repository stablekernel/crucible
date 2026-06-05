// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"context"
	"net/http"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	prom "github.com/stablekernel/crucible/sink/prometheus"
)

// TestPushMetrics_MultiLabelDeterministicOrder verifies that a metric with
// several labels serializes its labels in a stable, sorted order regardless of
// map iteration order. Serializing the same metric many times must produce
// byte-identical output.
func TestPushMetrics_MultiLabelDeterministicOrder(t *testing.T) {
	t.Parallel()

	metrics := []prom.Metric{
		{
			Name:  "http_requests_total",
			Type:  prom.TypeCounter,
			Value: "7",
			Labels: map[string]string{
				"method": "GET",
				"status": "200",
				"region": "us-east-1",
				"app":    "api",
			},
		},
	}

	const want = `http_requests_total{app="api",method="GET",region="us-east-1",status="200"} 7`

	var first string
	for i := 0; i < 50; i++ {
		fd := &fakeDoer{status: http.StatusOK}
		reg := prom.NewRegistry()
		csink.Register(reg, func(_ context.Context, _ deployFinished) csink.Op[prom.Doer] {
			return prom.PushMetrics("http://gw", "job", metrics)
		})
		outlet := prom.New(fd, reg)
		if err := outlet.Sink(context.Background(), deployFinished{Env: "prod"}); err != nil {
			t.Fatalf("Sink() error = %v", err)
		}
		if i == 0 {
			first = fd.body
			if !contains(first, want) {
				t.Fatalf("body = %q, want it to contain %q", first, want)
			}
			continue
		}
		if fd.body != first {
			t.Fatalf("label order is not deterministic:\n run 0: %q\n run %d: %q", first, i, fd.body)
		}
	}
}

func contains(haystack, needle string) bool {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}
