// SPDX-License-Identifier: Apache-2.0

//go:build integration

package http_test

import (
	"context"
	"io"
	gohttp "net/http"
	"net/http/httptest"
	"sync"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	httpsink "github.com/stablekernel/crucible/sink/http"
)

// orderPlacedIT is the payload the integration test sinks through the outlet.
type orderPlacedIT struct {
	ID string `json:"id"`
}

// TestIntegrationSinkPostsToRealServer drives the real Outlet path against a
// live httptest.Server, capturing the request so the test can assert the body,
// method, and content type landed.
func TestIntegrationSinkPostsToRealServer(t *testing.T) {
	t.Parallel()

	var (
		mu          sync.Mutex
		gotMethod   string
		gotPath     string
		gotType     string
		gotBody     []byte
		gotRequests int
	)

	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotType = r.Header.Get("Content-Type")
		gotBody = body
		gotRequests++
		mu.Unlock()
		w.WriteHeader(gohttp.StatusAccepted)
	}))
	t.Cleanup(srv.Close)

	reg := httpsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, o orderPlacedIT) csink.Op[httpsink.Doer] {
		return httpsink.PostJSON(srv.URL+"/events", o)
	})

	outlet := httpsink.New(srv.Client(), reg)
	if err := outlet.Sink(context.Background(), orderPlacedIT{ID: "A-1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotRequests != 1 {
		t.Fatalf("server received %d requests, want 1", gotRequests)
	}
	if gotMethod != gohttp.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotPath != "/events" {
		t.Errorf("path = %q, want /events", gotPath)
	}
	if gotType != "application/json" {
		t.Errorf("content-type = %q, want application/json", gotType)
	}
	if string(gotBody) != `{"id":"A-1"}` {
		t.Errorf("body = %q, want %q", gotBody, `{"id":"A-1"}`)
	}
}
