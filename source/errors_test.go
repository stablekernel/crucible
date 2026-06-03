// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/stablekernel/crucible/source"
)

func TestSentinels_AreDistinct(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		source.ErrDrained,
		source.ErrNoCodec,
		source.ErrPoison,
		source.ErrRetryable,
		source.ErrInvalidForState,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i != j && errors.Is(a, b) {
				t.Fatalf("sentinel %d matches sentinel %d; want distinct", i, j)
			}
		}
	}
}

func TestDecodeError_IsPoison(t *testing.T) {
	t.Parallel()
	underlying := errors.New("boom")
	de := &source.DecodeError{ContentType: "application/json", Subject: "orders", Err: underlying}

	if !errors.Is(de, source.ErrPoison) {
		t.Error("DecodeError should match ErrPoison via Is")
	}
	if errors.Is(de, source.ErrInvalidForState) {
		t.Error("DecodeError should not match ErrInvalidForState")
	}
	if !errors.Is(de, underlying) {
		t.Error("DecodeError should unwrap to its underlying error")
	}
	if !strings.Contains(de.Error(), "application/json") || !strings.Contains(de.Error(), "orders") {
		t.Errorf("DecodeError message %q missing content type or subject", de.Error())
	}
}

func TestDecodeError_NoContentType(t *testing.T) {
	t.Parallel()
	de := &source.DecodeError{Subject: "orders", Err: source.ErrNoCodec}
	if strings.Contains(de.Error(), "content-type") {
		t.Errorf("message %q should omit content-type when empty", de.Error())
	}
	if !errors.Is(de, source.ErrNoCodec) {
		t.Error("DecodeError should unwrap to ErrNoCodec")
	}
}

func TestGuardRejection_IsInvalidForState(t *testing.T) {
	t.Parallel()
	underlying := errors.New("guard failed")
	gr := &source.GuardRejection{Event: "Cancel", State: "delivered", Err: underlying}

	if !errors.Is(gr, source.ErrInvalidForState) {
		t.Error("GuardRejection should match ErrInvalidForState via Is")
	}
	if errors.Is(gr, source.ErrPoison) {
		t.Error("GuardRejection should not match ErrPoison")
	}
	if !errors.Is(gr, underlying) {
		t.Error("GuardRejection should unwrap to its underlying error")
	}
	if !strings.Contains(gr.Error(), "Cancel") || !strings.Contains(gr.Error(), "delivered") {
		t.Errorf("GuardRejection message %q missing event or state", gr.Error())
	}
}

func TestGuardRejection_AsTarget(t *testing.T) {
	t.Parallel()
	gr := &source.GuardRejection{Event: "X", State: "s", Err: errors.New("x")}
	wrapped := errors.Join(errors.New("ctx"), gr)
	var target *source.GuardRejection
	if !errors.As(wrapped, &target) {
		t.Fatal("errors.As should recover the *GuardRejection")
	}
	if target.Event != "X" {
		t.Errorf("recovered event = %q, want X", target.Event)
	}
}
