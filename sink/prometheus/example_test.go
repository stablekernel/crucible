// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"

	csink "github.com/stablekernel/crucible/sink"
	prom "github.com/stablekernel/crucible/sink/prometheus"
)

type pageViewed struct{ Path string }

func ExampleNew() {
	// Stand up a local Pushgateway stub so the example is self-contained.
	var received string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Build a registry and register a transformer for pageViewed events.
	reg := prom.NewRegistry()
	csink.Register(reg, func(_ context.Context, e pageViewed) csink.Op[prom.Doer] {
		return prom.PushMetrics(srv.URL, "webapp", []prom.Metric{
			{
				Name:   "page_views_total",
				Type:   prom.TypeCounter,
				Value:  "1",
				Labels: map[string]string{"path": e.Path},
			},
		})
	})

	outlet := prom.New(srv.Client(), reg)
	_ = outlet.Sink(context.Background(), pageViewed{Path: "/home"})

	lines := strings.Split(strings.TrimSpace(received), "\n")
	for _, l := range lines {
		fmt.Println(l)
	}

	// Output:
	// # TYPE page_views_total counter
	// page_views_total{path="/home"} 1
}
