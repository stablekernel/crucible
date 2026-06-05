// SPDX-License-Identifier: Apache-2.0

package http_test

import (
	"context"
	"strings"
	"testing"

	httpsink "github.com/stablekernel/crucible/sink/http"
)

// TestPost_BuildRequestError exercises the NewRequestWithContext failure branch:
// a URL containing a control character cannot be parsed into a request, so Post
// returns a build-request error before any Do call.
func TestPost_BuildRequestError(t *testing.T) {
	t.Parallel()

	doer := &fakeDoer{}
	// A control character in the URL makes http.NewRequestWithContext fail.
	err := httpsink.Post("http://example.test/\x7f", "application/json", []byte("{}")).
		Apply(context.Background(), doer)
	if err == nil {
		t.Fatal("Apply() = nil, want a build-request error")
	}
	if !strings.Contains(err.Error(), "build request") {
		t.Errorf("error = %v, want it to mention building the request", err)
	}
	if len(doer.requests) != 0 {
		t.Errorf("Do() called %d times, want 0 (request never built)", len(doer.requests))
	}
}

// unmarshalable is a type json.Marshal rejects: a channel field cannot be
// encoded, driving the PostJSON marshal-error branch.
type unmarshalable struct {
	Ch chan int
}

// TestPostJSON_MarshalError exercises the json.Marshal failure branch: an
// unmarshalable payload returns a marshal error before any request is built.
func TestPostJSON_MarshalError(t *testing.T) {
	t.Parallel()

	doer := &fakeDoer{}
	err := httpsink.PostJSON("http://example.test/items", unmarshalable{Ch: make(chan int)}).
		Apply(context.Background(), doer)
	if err == nil {
		t.Fatal("Apply() = nil, want a marshal error")
	}
	if !strings.Contains(err.Error(), "marshal json") {
		t.Errorf("error = %v, want it to mention marshaling json", err)
	}
	if len(doer.requests) != 0 {
		t.Errorf("Do() called %d times, want 0 (marshal failed first)", len(doer.requests))
	}
}
