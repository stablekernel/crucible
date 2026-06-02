// SPDX-License-Identifier: Apache-2.0

//go:build integration

package prometheus_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	promsink "github.com/stablekernel/crucible/sink/prometheus"
)

// orderCountIT is the payload the integration test sinks through the outlet.
type orderCountIT struct {
	Value string
}

// TestIntegrationSinkPushesToRealGateway drives the real Outlet path against a
// live httptest.Server standing in for a Prometheus Pushgateway, capturing the
// request so the test can assert the exposition body and job path landed.
func TestIntegrationSinkPushesToRealGateway(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		gotPath string
		gotBody string
		gotType string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotPath = r.URL.Path
		gotBody = string(body)
		gotType = r.Header.Get("Content-Type")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	reg := promsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, c orderCountIT) csink.Op[promsink.Doer] {
		return promsink.PushMetrics(srv.URL, "orders", []promsink.Metric{
			{Name: "orders_total", Type: promsink.TypeCounter, Value: c.Value},
		})
	})

	outlet := promsink.New(srv.Client(), reg)
	if err := outlet.Sink(context.Background(), orderCountIT{Value: "7"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotPath != "/metrics/job/orders" {
		t.Errorf("path = %q, want /metrics/job/orders", gotPath)
	}
	if !strings.Contains(gotType, "text/plain") {
		t.Errorf("content-type = %q, want text/plain exposition", gotType)
	}
	if !strings.Contains(gotBody, "# TYPE orders_total counter") {
		t.Errorf("body missing TYPE line, got %q", gotBody)
	}
	if !strings.Contains(gotBody, "orders_total 7") {
		t.Errorf("body missing sample, got %q", gotBody)
	}
}
