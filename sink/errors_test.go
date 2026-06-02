// SPDX-License-Identifier: Apache-2.0

package sink_test

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/sink"
)

func TestErrUnregisteredMatchesThroughWrap(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("emitter: %w", sink.ErrUnregistered)
	if !errors.Is(wrapped, sink.ErrUnregistered) {
		t.Fatalf("errors.Is(wrapped, ErrUnregistered) = false, want true")
	}
}

func TestErrorUnwrapAndAs(t *testing.T) {
	t.Parallel()

	se := &sink.Error{
		Outlet:      "dynamo",
		Phase:       sink.PhaseApply,
		PayloadType: "main.Order",
		Err:         io.EOF,
	}
	var err error = se

	if !errors.Is(err, io.EOF) {
		t.Errorf("errors.Is(err, io.EOF) = false, want true")
	}
	var got *sink.Error
	if !errors.As(err, &got) {
		t.Fatalf("errors.As(err, *Error) = false, want true")
	}
	if got.Outlet != "dynamo" || got.Phase != sink.PhaseApply || got.PayloadType != "main.Order" {
		t.Errorf("recovered Error = %+v, want outlet=dynamo phase=apply payload=main.Order", got)
	}
}

func TestErrorMessageIncludesContext(t *testing.T) {
	t.Parallel()

	se := &sink.Error{Outlet: "sql", Phase: sink.PhaseFlush, PayloadType: "int", Err: errors.New("boom")}
	msg := se.Error()
	for _, want := range []string{"sql", "flush", "int", "boom"} {
		if !strings.Contains(msg, want) {
			t.Errorf("Error() = %q, missing %q", msg, want)
		}
	}
}
