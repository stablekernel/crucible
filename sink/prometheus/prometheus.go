// SPDX-License-Identifier: Apache-2.0

// Package prometheus is a sink destination that pushes metrics to a Prometheus
// Pushgateway. It depends only on the standard library and crucible/sink --
// there is no Prometheus client library dependency. Register a transformer that
// turns each payload type into a [Push] operation (or a custom [OpFunc]), then
// attach the result of [New] to a sink.Manifold.
//
// The destination speaks the Prometheus text exposition format over stdlib
// net/http. Each [Push] call performs a single HTTP POST to
// <gatewayURL>/metrics/job/<job> and returns an error on any non-2xx response.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package prometheus

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	csink "github.com/stablekernel/crucible/sink"
)

// Doer is the narrow HTTP surface this destination needs. It is satisfied by
// *http.Client and any hand-rolled test double.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Metric is a single time-series sample in the Prometheus text exposition
// format. Name, Type, and Value are required; Labels may be nil or empty.
//
// The MetricType constants Type* enumerate valid values for the Type field, but
// the field is a plain string so callers may supply any valid exposition type.
type Metric struct {
	// Name is the metric name (e.g. "http_requests_total").
	Name string
	// Type is the Prometheus metric type hint (e.g. "counter", "gauge").
	// Use the Type* constants for well-known values.
	Type string
	// Value is the sample value serialized as a string (e.g. "1", "3.14").
	Value string
	// Labels are the key=value label pairs attached to this sample.
	Labels map[string]string
}

// Metric type constants for use in Metric.Type.
const (
	TypeCounter   = "counter"
	TypeGauge     = "gauge"
	TypeHistogram = "histogram"
	TypeSummary   = "summary"
	TypeUntyped   = "untyped"
)

// Push returns an Op that POSTs the Prometheus text exposition format body
// to <gatewayURL>/metrics/job/<job>. A non-2xx HTTP response is an error.
// The body must be valid Prometheus text exposition format; callers are
// responsible for correct formatting.
func Push(gatewayURL, job, body string) csink.Op[Doer] {
	return csink.OpFunc[Doer](func(ctx context.Context, client Doer) error {
		url := strings.TrimRight(gatewayURL, "/") + "/metrics/job/" + job
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
		if err != nil {
			return fmt.Errorf("prometheus: build request: %w", err)
		}
		req.Header.Set("Content-Type", "text/plain; version=0.0.4")
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("prometheus: do request: %w", err)
		}
		defer resp.Body.Close() //nolint:errcheck
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("prometheus: pushgateway returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		// Drain the body so the connection is reusable.
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil
	})
}

// PushMetrics returns an Op that serializes metrics into the Prometheus text
// exposition format and POSTs the result to <gatewayURL>/metrics/job/<job>.
// It is a higher-level alternative to [Push] when callers want the package to
// handle serialization.
func PushMetrics(gatewayURL, job string, metrics []Metric) csink.Op[Doer] {
	return csink.OpFunc[Doer](func(ctx context.Context, client Doer) error {
		body := marshalMetrics(metrics)
		return Push(gatewayURL, job, body).Apply(ctx, client)
	})
}

// marshalMetrics serializes a slice of Metric values into the Prometheus text
// exposition format.
func marshalMetrics(metrics []Metric) string {
	var b strings.Builder
	for _, m := range metrics {
		if m.Type != "" {
			fmt.Fprintf(&b, "# TYPE %s %s\n", m.Name, m.Type)
		}
		b.WriteString(m.Name)
		if len(m.Labels) > 0 {
			b.WriteByte('{')
			// Sort the label keys so the serialized output is deterministic
			// regardless of map iteration order.
			keys := make([]string, 0, len(m.Labels))
			for k := range m.Labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for i, k := range keys {
				if i > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, `%s="%s"`, k, m.Labels[k])
			}
			b.WriteByte('}')
		}
		fmt.Fprintf(&b, " %s\n", m.Value)
	}
	return b.String()
}

// NewRegistry returns an empty registry of Op[Doer] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Doer]] {
	return csink.NewRegistry[csink.Op[Doer]]()
}

// New builds an Outlet that applies each payload's registered Op[Doer] to
// client. The outlet is named "prometheus" unless overridden with
// sink.WithName.
func New(client Doer, reg *csink.Registry[csink.Op[Doer]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Doer](client, reg, append([]csink.EmitterOption{csink.WithName("prometheus")}, opts...)...)
}
