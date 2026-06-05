// SPDX-License-Identifier: Apache-2.0

package cdc_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cdc"
)

// row is the concrete shape the test projects before/after images into.
type row struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func TestCodec_Decode_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		payload   string
		wantOp    cdc.Operation
		wantOpStr string
		hasBefore bool
		hasAfter  bool
		wantTable string
		wantSnap  bool
		wantTS    time.Time
	}{
		{
			name: "create has after only",
			payload: `{"op":"c","before":null,
				"after":{"id":1,"name":"ada","email":"ada@x.io"},
				"source":{"connector":"postgresql","db":"shop","schema":"public","table":"users","lsn":42,"txId":"900"},
				"ts_ms":1700000000000}`,
			wantOp:    cdc.OpCreate,
			wantOpStr: "c",
			hasAfter:  true,
			wantTable: "users",
			wantTS:    time.UnixMilli(1700000000000).UTC(),
		},
		{
			name: "snapshot read marks snapshot",
			payload: `{"op":"r",
				"after":{"id":2,"name":"grace","email":"grace@x.io"},
				"source":{"connector":"mysql","db":"shop","table":"users","snapshot":"true"},
				"ts_ms":1700000001000}`,
			wantOp:    cdc.OpRead,
			wantOpStr: "r",
			hasAfter:  true,
			wantTable: "users",
			wantSnap:  true,
			wantTS:    time.UnixMilli(1700000001000).UTC(),
		},
		{
			name: "update has before and after",
			payload: `{"op":"u",
				"before":{"id":1,"name":"ada","email":"ada@x.io"},
				"after":{"id":1,"name":"ada lovelace","email":"ada@x.io"},
				"source":{"connector":"postgresql","db":"shop","schema":"public","table":"users","snapshot":false},
				"ts_ms":1700000002000}`,
			wantOp:    cdc.OpUpdate,
			wantOpStr: "u",
			hasBefore: true,
			hasAfter:  true,
			wantTable: "users",
			wantTS:    time.UnixMilli(1700000002000).UTC(),
		},
		{
			name: "delete has before only",
			payload: `{"op":"d",
				"before":{"id":1,"name":"ada","email":"ada@x.io"},
				"after":null,
				"source":{"connector":"postgresql","table":"users"},
				"ts_ms":1700000003000}`,
			wantOp:    cdc.OpDelete,
			wantOpStr: "d",
			hasBefore: true,
			wantTable: "users",
			wantTS:    time.UnixMilli(1700000003000).UTC(),
		},
		{
			name:      "no ts_ms yields zero time",
			payload:   `{"op":"c","after":{"id":3,"name":"x","email":"x@x.io"}}`,
			wantOp:    cdc.OpCreate,
			wantOpStr: "c",
			hasAfter:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ev := decode(t, []byte(tt.payload))

			if ev.Operation != tt.wantOp {
				t.Fatalf("operation = %v, want %v", ev.Operation, tt.wantOp)
			}
			if got := ev.Operation.String(); got != tt.wantOpStr {
				t.Errorf("operation string = %q, want %q", got, tt.wantOpStr)
			}
			if ev.Before.Present() != tt.hasBefore {
				t.Errorf("before present = %v, want %v", ev.Before.Present(), tt.hasBefore)
			}
			if ev.After.Present() != tt.hasAfter {
				t.Errorf("after present = %v, want %v", ev.After.Present(), tt.hasAfter)
			}
			if ev.Source.Table != tt.wantTable {
				t.Errorf("table = %q, want %q", ev.Source.Table, tt.wantTable)
			}
			if ev.Source.Snapshot != tt.wantSnap {
				t.Errorf("snapshot = %v, want %v", ev.Source.Snapshot, tt.wantSnap)
			}
			if !ev.Timestamp.Equal(tt.wantTS) {
				t.Errorf("timestamp = %v, want %v", ev.Timestamp, tt.wantTS)
			}
		})
	}
}

func TestCodec_Decode_TypedImages(t *testing.T) {
	t.Parallel()

	ev := decode(t, []byte(`{"op":"u",
		"before":{"id":1,"name":"ada","email":"ada@x.io"},
		"after":{"id":1,"name":"ada lovelace","email":"ada@x.io"},
		"source":{"table":"users"}}`))

	before, err := cdc.BeforeAs[row](ev)
	if err != nil {
		t.Fatalf("BeforeAs: %v", err)
	}
	if before.Name != "ada" {
		t.Errorf("before.Name = %q, want %q", before.Name, "ada")
	}

	after, err := cdc.AfterAs[row](ev)
	if err != nil {
		t.Fatalf("AfterAs: %v", err)
	}
	if after.Name != "ada lovelace" {
		t.Errorf("after.Name = %q, want %q", after.Name, "ada lovelace")
	}
}

func TestCodec_Decode_MissingImage(t *testing.T) {
	t.Parallel()

	// A create has no before-image: projecting it reports ErrMissingImage.
	ev := decode(t, []byte(`{"op":"c","after":{"id":1,"name":"ada","email":"ada@x.io"}}`))

	if _, err := cdc.BeforeAs[row](ev); !errors.Is(err, cdc.ErrMissingImage) {
		t.Fatalf("BeforeAs error = %v, want ErrMissingImage", err)
	}
	// A delete has no after-image.
	del := decode(t, []byte(`{"op":"d","before":{"id":1,"name":"ada","email":"ada@x.io"}}`))
	if _, err := cdc.AfterAs[row](del); !errors.Is(err, cdc.ErrMissingImage) {
		t.Fatalf("AfterAs error = %v, want ErrMissingImage", err)
	}
}

func TestCodec_Decode_Tombstone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "empty slice", payload: []byte{}},
		{name: "nil slice", payload: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ev := decode(t, tt.payload)
			if ev.Operation != cdc.OpTombstone {
				t.Fatalf("operation = %v, want OpTombstone", ev.Operation)
			}
			if ev.Before.Present() || ev.After.Present() {
				t.Errorf("tombstone carried an image: before=%v after=%v",
					ev.Before.Present(), ev.After.Present())
			}
			if got := ev.Operation.String(); got != "tombstone" {
				t.Errorf("operation string = %q, want %q", got, "tombstone")
			}
		})
	}
}

func TestCodec_Decode_Malformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		wantErr error
	}{
		{name: "not json", payload: `}{`, wantErr: cdc.ErrMalformedEnvelope},
		{name: "json array", payload: `[1,2,3]`, wantErr: cdc.ErrMalformedEnvelope},
		{name: "unknown op", payload: `{"op":"z","after":{}}`, wantErr: cdc.ErrUnknownOperation},
		{name: "empty op", payload: `{"after":{}}`, wantErr: cdc.ErrUnknownOperation},
		{
			name:    "bad source block",
			payload: `{"op":"c","after":{},"source":"not-an-object"}`,
			wantErr: cdc.ErrMalformedEnvelope,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			codec := cdc.New()
			_, err := codec.Decode([]byte(tt.payload), nil)
			if err == nil {
				t.Fatalf("Decode(%q) = nil error, want %v", tt.payload, tt.wantErr)
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Decode(%q) error = %v, want errors.Is %v", tt.payload, err, tt.wantErr)
			}
		})
	}
}

func TestRegistry_Decode_MalformedIsPoison(t *testing.T) {
	t.Parallel()

	registry := source.NewRegistry().SetDefault(cdc.New())
	msg := changeMessage{value: []byte(`}{`)}

	_, err := registry.Decode(msg)
	if err == nil {
		t.Fatal("Decode = nil error, want *source.DecodeError")
	}
	var decErr *source.DecodeError
	if !errors.As(err, &decErr) {
		t.Fatalf("error = %v, want *source.DecodeError", err)
	}
	if !errors.Is(err, source.ErrPoison) {
		t.Errorf("error does not report ErrPoison: %v", err)
	}
	if !errors.Is(err, cdc.ErrMalformedEnvelope) {
		t.Errorf("error does not unwrap to ErrMalformedEnvelope: %v", err)
	}
}

func TestDecodeEvent_WrongTypeIsPoison(t *testing.T) {
	t.Parallel()

	// A registry default that decodes to a non-ChangeEvent value: DecodeEvent
	// must report poison rather than a panic.
	registry := source.NewRegistry().SetDefault(source.CodecFunc(
		func([]byte, source.Headers) (any, error) { return 42, nil },
	))
	_, err := cdc.DecodeEvent(registry, changeMessage{value: []byte(`{}`)})
	if !errors.Is(err, source.ErrPoison) {
		t.Fatalf("DecodeEvent error = %v, want ErrPoison", err)
	}
}

func TestDecodeEvent_RegistryErrorPropagates(t *testing.T) {
	t.Parallel()

	// A registry whose codec fails to decode: DecodeEvent returns the registry's
	// (poison-classified) decode error rather than masking it.
	registry := source.NewRegistry().SetDefault(source.CodecFunc(
		func([]byte, source.Headers) (any, error) {
			return nil, errors.New("codec exploded")
		},
	))
	_, err := cdc.DecodeEvent(registry, changeMessage{value: []byte(`{"op":"c"}`)})
	if err == nil {
		t.Fatal("DecodeEvent error = nil, want registry decode error")
	}
	if !errors.Is(err, source.ErrPoison) {
		t.Fatalf("DecodeEvent error = %v, want poison-classified decode error", err)
	}
}

func TestRawJSON_As_UnmarshalErrorReported(t *testing.T) {
	t.Parallel()

	// A create carries an after-image; decoding it into an incompatible shape
	// surfaces the json.Unmarshal error (wrapped), not ErrMissingImage.
	ev := decode(t, []byte(`{"op":"c","after":{"id":1,"name":"ada"}}`))
	var dst int // the after-image is an object; an int cannot hold it
	err := ev.After.As(&dst)
	if err == nil {
		t.Fatal("RawJSON.As error = nil, want unmarshal error")
	}
	if errors.Is(err, cdc.ErrMissingImage) {
		t.Fatalf("RawJSON.As error = %v, want unmarshal error not ErrMissingImage", err)
	}
}

func TestSourceHeaders(t *testing.T) {
	t.Parallel()

	ev := decode(t, []byte(`{"op":"r",
		"after":{"id":1,"name":"ada","email":"ada@x.io"},
		"source":{"connector":"postgresql","db":"shop","schema":"public","table":"users","snapshot":"last","lsn":42,"txId":"900"}}`))

	h := cdc.SourceHeaders(ev)

	want := map[string]string{
		cdc.OperationHeader: "r",
		cdc.ConnectorHeader: "postgresql",
		cdc.DatabaseHeader:  "shop",
		cdc.SchemaHeader:    "public",
		cdc.TableHeader:     "users",
		cdc.SnapshotHeader:  "true",
		cdc.LSNHeader:       "42",
		cdc.TxIDHeader:      "900",
	}
	for key, wantVal := range want {
		got, ok := h.Get(key)
		if !ok {
			t.Errorf("header %q absent", key)
			continue
		}
		if got != wantVal {
			t.Errorf("header %q = %q, want %q", key, got, wantVal)
		}
	}
}

func TestSourceHeaders_OmitsEmpty(t *testing.T) {
	t.Parallel()

	// A non-snapshot create with a sparse source block: only operation and the
	// present fields appear; no blank snapshot header.
	ev := decode(t, []byte(`{"op":"c","after":{"id":1},"source":{"table":"users"}}`))
	h := cdc.SourceHeaders(ev)

	if _, ok := h.Get(cdc.SnapshotHeader); ok {
		t.Error("snapshot header present for a non-snapshot record")
	}
	if _, ok := h.Get(cdc.ConnectorHeader); ok {
		t.Error("connector header present for an absent connector")
	}
	if got, _ := h.Get(cdc.TableHeader); got != "users" {
		t.Errorf("table header = %q, want %q", got, "users")
	}
}

func TestOperation_String_Unknown(t *testing.T) {
	t.Parallel()

	if got := cdc.OpUnknown.String(); got != "unknown" {
		t.Errorf("OpUnknown.String() = %q, want %q", got, "unknown")
	}
}

// decode is a helper that runs the codec and fails the test on error.
func decode(t *testing.T, payload []byte) cdc.ChangeEvent {
	t.Helper()
	v, err := cdc.New().Decode(payload, nil)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	ev, ok := cdc.EventOf(v)
	if !ok {
		t.Fatalf("decoded value is not a ChangeEvent: %T", v)
	}
	return ev
}

// changeMessage is a minimal source.Message for registry-level tests.
type changeMessage struct {
	value []byte
}

func (m changeMessage) Key() []byte             { return nil }
func (m changeMessage) Value() []byte           { return m.value }
func (m changeMessage) Headers() source.Headers { return nil }
func (m changeMessage) Subject() string         { return "shop.public.users" }
func (m changeMessage) PartitionKey() string    { return "" }
func (m changeMessage) Cursor() source.Cursor   { return nil }
func (m changeMessage) As(any) bool             { return false }
