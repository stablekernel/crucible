// SPDX-License-Identifier: Apache-2.0

// Package http is a sink destination that delivers payloads over HTTP using the
// standard library's net/http. It depends only on the standard library and
// crucible/sink — there is no third-party SDK dependency. Register a transformer
// that turns each payload type into a [Post] or [PostJSON] operation, then
// attach the result of [New] to a sink.Manifold.
//
// # Stability
//
// Experimental (pre-v1); the API may change until the suite locks v1.0.0.
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	gohttp "net/http"
	"strings"

	csink "github.com/stablekernel/crucible/sink"
)

// Doer is the narrow net/http surface this destination needs. It is satisfied
// by *http.Client (the standard library's default client), so consumers can
// wire any *http.Client — custom timeouts, transports, or test servers — without
// this package importing anything beyond the standard library.
type Doer interface {
	Do(req *gohttp.Request) (*gohttp.Response, error)
}

// Post returns an Op that sends an HTTP POST request to url with the given
// content-type header and body. The response body is always drained and closed.
// A non-2xx status code is returned as an error that includes the status text.
func Post(url, contentType string, body []byte) csink.Op[Doer] {
	return csink.OpFunc[Doer](func(ctx context.Context, doer Doer) error {
		req, err := gohttp.NewRequestWithContext(ctx, gohttp.MethodPost, url, strings.NewReader(string(body)))
		if err != nil {
			return fmt.Errorf("http sink: build request: %w", err)
		}
		if contentType != "" {
			req.Header.Set("Content-Type", contentType)
		}

		resp, err := doer.Do(req)
		if err != nil {
			return fmt.Errorf("http sink: do request: %w", err)
		}
		defer func() { _, _ = io.Copy(io.Discard, resp.Body); _ = resp.Body.Close() }()

		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("http sink: unexpected status %s", resp.Status)
		}
		return nil
	})
}

// PostJSON returns an Op that marshals v to JSON and sends an HTTP POST request
// to url with Content-Type "application/json". The response body is always
// drained and closed. A non-2xx status code is returned as an error.
func PostJSON(url string, v any) csink.Op[Doer] {
	return csink.OpFunc[Doer](func(ctx context.Context, doer Doer) error {
		data, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("http sink: marshal json: %w", err)
		}
		return Post(url, "application/json", data).Apply(ctx, doer)
	})
}

// NewRegistry returns an empty registry of Op[Doer] for callers to populate
// with sink.Register.
func NewRegistry() *csink.Registry[csink.Op[Doer]] {
	return csink.NewRegistry[csink.Op[Doer]]()
}

// New builds an Outlet that applies each payload's registered Op[Doer] to
// doer. The outlet is named "http" unless overridden with sink.WithName.
func New(doer Doer, reg *csink.Registry[csink.Op[Doer]], opts ...csink.EmitterOption) csink.Outlet {
	return csink.NewEmitter[Doer](doer, reg, append([]csink.EmitterOption{csink.WithName("http")}, opts...)...)
}
