// SPDX-License-Identifier: Apache-2.0

package prometheus_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	csink "github.com/stablekernel/crucible/sink"
	prom "github.com/stablekernel/crucible/sink/prometheus"
	"github.com/stablekernel/crucible/sink/sinktest"
)

// fakeDoer records the last request and optionally returns a canned response.
type fakeDoer struct {
	req      *http.Request
	body     string
	status   int
	respBody string
	err      error
}

func (f *fakeDoer) Do(req *http.Request) (*http.Response, error) {
	f.req = req
	b, _ := io.ReadAll(req.Body)
	f.body = string(b)
	if f.err != nil {
		return nil, f.err
	}
	status := f.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(f.respBody)),
	}, nil
}

// --- payload types used across tests ---

type (
	appStarted     struct{ Version string }
	deployFinished struct{ Env string }
)

// newOutlet builds a fresh Outlet with appStarted registered.
func newOutlet(d prom.Doer) csink.Outlet {
	reg := prom.NewRegistry()
	csink.Register(reg, func(_ context.Context, e appStarted) csink.Op[prom.Doer] {
		return prom.Push("http://gw", "myapp",
			"# TYPE app_starts counter\napp_starts 1\n")
	})
	return prom.New(d, reg)
}

// --- unit tests ---

func TestPush_Success(t *testing.T) {
	t.Parallel()

	fd := &fakeDoer{status: http.StatusOK}
	outlet := newOutlet(fd)

	if err := outlet.Sink(context.Background(), appStarted{Version: "v1"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if fd.req == nil {
		t.Fatal("expected an HTTP request to be made")
	}
	if fd.req.Method != http.MethodPost {
		t.Fatalf("method = %q, want POST", fd.req.Method)
	}
	wantURL := "http://gw/metrics/job/myapp"
	if fd.req.URL.String() != wantURL {
		t.Fatalf("URL = %q, want %q", fd.req.URL, wantURL)
	}
	if ct := fd.req.Header.Get("Content-Type"); ct != "text/plain; version=0.0.4" {
		t.Fatalf("Content-Type = %q, want text/plain exposition header", ct)
	}
	if !strings.Contains(fd.body, "app_starts 1") {
		t.Fatalf("body = %q, want metric line", fd.body)
	}
}

func TestPush_Non2xxError(t *testing.T) {
	t.Parallel()

	fd := &fakeDoer{status: http.StatusBadRequest, respBody: "bad job label"}
	outlet := newOutlet(fd)

	err := outlet.Sink(context.Background(), appStarted{Version: "v1"})
	if err == nil {
		t.Fatal("Sink() returned nil, want error on 400")
	}

	var se *csink.Error
	if !errors.As(err, &se) {
		t.Fatalf("errors.As(*csink.Error) = false, got %T: %v", err, err)
	}
	if se.Phase != csink.PhaseApply {
		t.Fatalf("Phase = %q, want %q", se.Phase, csink.PhaseApply)
	}
	if se.Outlet != "prometheus" {
		t.Fatalf("Outlet = %q, want prometheus", se.Outlet)
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("error = %v, want mention of status 400", err)
	}
}

func TestPush_NetworkError(t *testing.T) {
	t.Parallel()

	netErr := errors.New("connection refused")
	fd := &fakeDoer{err: netErr}
	outlet := newOutlet(fd)

	err := outlet.Sink(context.Background(), appStarted{Version: "v1"})
	if !errors.Is(err, netErr) {
		t.Fatalf("Sink() = %v, want to wrap %v", err, netErr)
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "prometheus" {
		t.Fatalf("error = %+v, want *csink.Error{Outlet:prometheus, Phase:apply}", se)
	}
}

func TestUnregisteredPayloadSkips(t *testing.T) {
	t.Parallel()

	fd := &fakeDoer{}
	outlet := newOutlet(fd)

	type unknown struct{}
	err := outlet.Sink(context.Background(), unknown{})
	if !errors.Is(err, csink.ErrUnregistered) {
		t.Fatalf("Sink(unregistered) = %v, want ErrUnregistered", err)
	}
	if fd.req != nil {
		t.Fatal("expected no HTTP request for unregistered payload")
	}
}

func TestPushMetrics_SerializesCorrectly(t *testing.T) {
	t.Parallel()

	metrics := []prom.Metric{
		{Name: "deploy_count", Type: prom.TypeCounter, Value: "42", Labels: map[string]string{"env": "prod"}},
	}
	fd := &fakeDoer{status: http.StatusOK}
	reg := prom.NewRegistry()
	csink.Register(reg, func(_ context.Context, e deployFinished) csink.Op[prom.Doer] {
		return prom.PushMetrics("http://gw", "deploy", metrics)
	})
	outlet := prom.New(fd, reg)

	if err := outlet.Sink(context.Background(), deployFinished{Env: "prod"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if !strings.Contains(fd.body, "# TYPE deploy_count counter") {
		t.Fatalf("body = %q, missing TYPE line", fd.body)
	}
	if !strings.Contains(fd.body, `deploy_count{env="prod"} 42`) {
		t.Fatalf("body = %q, missing metric sample", fd.body)
	}
}

func TestPush_HTTPTestServer(t *testing.T) {
	t.Parallel()

	var received string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received = string(b)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := prom.NewRegistry()
	csink.Register(reg, func(_ context.Context, e appStarted) csink.Op[prom.Doer] {
		return prom.Push(srv.URL, "testjob", "# TYPE up gauge\nup 1\n")
	})
	outlet := prom.New(srv.Client(), reg)

	if err := outlet.Sink(context.Background(), appStarted{Version: "v2"}); err != nil {
		t.Fatalf("Sink() error = %v", err)
	}
	if !strings.Contains(received, "up 1") {
		t.Fatalf("server received %q, want metric body", received)
	}
}

func TestPush_HTTPTestServer_Non2xx(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	reg := prom.NewRegistry()
	csink.Register(reg, func(_ context.Context, e appStarted) csink.Op[prom.Doer] {
		return prom.Push(srv.URL, "testjob", "up 1\n")
	})
	outlet := prom.New(srv.Client(), reg)

	err := outlet.Sink(context.Background(), appStarted{Version: "v1"})
	if err == nil {
		t.Fatal("Sink() returned nil, want error on 403")
	}
	var se *csink.Error
	if !errors.As(err, &se) || se.Phase != csink.PhaseApply || se.Outlet != "prometheus" {
		t.Fatalf("error = %+v, want *csink.Error{Outlet:prometheus, Phase:apply}", err)
	}
}

func TestConformance(t *testing.T) {
	t.Parallel()
	sinktest.OutletConformance(t, func() csink.Outlet { return newOutlet(&fakeDoer{status: http.StatusOK}) })
}
