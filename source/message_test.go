// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"testing"

	"github.com/stablekernel/crucible/source"
)

func TestHeaders_Get(t *testing.T) {
	t.Parallel()
	h := source.Headers{
		{Key: "content-type", Value: "application/json"},
		{Key: "x-trace", Value: "abc"},
		{Key: "content-type", Value: "ignored-duplicate"},
	}
	if v, ok := h.Get("content-type"); !ok || v != "application/json" {
		t.Fatalf("Get(content-type) = %q,%v, want application/json,true", v, ok)
	}
	if v, ok := h.Get("x-trace"); !ok || v != "abc" {
		t.Fatalf("Get(x-trace) = %q,%v", v, ok)
	}
	if _, ok := h.Get("missing"); ok {
		t.Fatal("Get(missing) should report not present")
	}
}

func TestHeaders_Keys(t *testing.T) {
	t.Parallel()
	if keys := source.Headers(nil).Keys(); keys != nil {
		t.Fatalf("Keys() on empty = %v, want nil", keys)
	}
	h := source.Headers{{Key: "a"}, {Key: "b"}, {Key: "a"}}
	keys := h.Keys()
	want := []string{"a", "b", "a"}
	if len(keys) != len(want) {
		t.Fatalf("Keys() = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("Keys() = %v, want %v", keys, want)
		}
	}
}
