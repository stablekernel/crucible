// SPDX-License-Identifier: Apache-2.0

package http_test

import (
	"context"
	"fmt"
	"io"
	gohttp "net/http"
	"net/http/httptest"

	csink "github.com/stablekernel/crucible/sink"

	httpsink "github.com/stablekernel/crucible/sink/http"
)

type invoicePaid struct{ InvoiceID string }

func ExampleNew() {
	// Start a test server that records the request and responds 200 OK.
	var received string
	srv := httptest.NewServer(gohttp.HandlerFunc(func(w gohttp.ResponseWriter, r *gohttp.Request) {
		body, _ := io.ReadAll(r.Body)
		received = string(body)
		w.WriteHeader(gohttp.StatusOK)
	}))
	defer srv.Close()

	reg := httpsink.NewRegistry()
	csink.Register(reg, func(_ context.Context, p invoicePaid) csink.Op[httpsink.Doer] {
		return httpsink.Post(srv.URL+"/webhooks", "application/json", []byte(`{"invoice":"`+p.InvoiceID+`"}`))
	})

	outlet := httpsink.New(srv.Client(), reg)
	_ = outlet.Sink(context.Background(), invoicePaid{InvoiceID: "INV-42"})

	fmt.Println(received)
	// Output: {"invoice":"INV-42"}
}
