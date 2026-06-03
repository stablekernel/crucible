// SPDX-License-Identifier: Apache-2.0

package source_test

import (
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source"
)

type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

func TestRegistry_Decode(t *testing.T) {
	t.Parallel()
	jsonCodec := source.NewJSONCodec[order]()

	tests := []struct {
		name    string
		setup   func(*source.Registry)
		msg     source.Message
		wantErr error // errors.Is target, nil for success
		wantID  string
	}{
		{
			name:  "by content type",
			setup: func(r *source.Registry) { r.Register("application/json", jsonCodec) },
			msg: testMsg{
				value:   []byte(`{"id":"A-1","qty":2}`),
				headers: source.Headers{{Key: "content-type", Value: "application/json"}},
				subject: "orders",
			},
			wantID: "A-1",
		},
		{
			name:   "by default codec, no header",
			setup:  func(r *source.Registry) { r.SetDefault(jsonCodec) },
			msg:    testMsg{value: []byte(`{"id":"B-2","qty":1}`), subject: "orders"},
			wantID: "B-2",
		},
		{
			name:   "unknown content type falls back to default",
			setup:  func(r *source.Registry) { r.SetDefault(jsonCodec) },
			msg:    testMsg{value: []byte(`{"id":"C-3"}`), headers: source.Headers{{Key: "content-type", Value: "x"}}, subject: "orders"},
			wantID: "C-3",
		},
		{
			name:    "no codec resolves to ErrNoCodec",
			setup:   func(*source.Registry) {},
			msg:     testMsg{value: []byte(`{}`), subject: "orders"},
			wantErr: source.ErrNoCodec,
		},
		{
			name:    "malformed payload is poison",
			setup:   func(r *source.Registry) { r.SetDefault(jsonCodec) },
			msg:     testMsg{value: []byte(`{not json`), subject: "orders"},
			wantErr: source.ErrPoison,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := source.NewRegistry()
			tt.setup(r)

			v, err := r.Decode(tt.msg)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("Decode err = %v, want errors.Is %v", err, tt.wantErr)
				}
				var de *source.DecodeError
				if !errors.As(err, &de) {
					t.Fatalf("Decode err = %v, want a *DecodeError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Decode unexpected err = %v", err)
			}
			o, ok := v.(order)
			if !ok {
				t.Fatalf("decoded value type = %T, want order", v)
			}
			if o.ID != tt.wantID {
				t.Fatalf("decoded ID = %q, want %q", o.ID, tt.wantID)
			}
		})
	}
}

func TestDecodeTyped(t *testing.T) {
	t.Parallel()
	r := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())

	o, err := source.DecodeTyped[order](r, testMsg{value: []byte(`{"id":"Z","qty":9}`)})
	if err != nil {
		t.Fatalf("DecodeTyped err = %v", err)
	}
	if o.ID != "Z" || o.Qty != 9 {
		t.Fatalf("DecodeTyped = %+v, want {Z 9}", o)
	}
}

func TestDecodeTyped_WrongType(t *testing.T) {
	t.Parallel()
	// Registry decodes to order, but the caller asks for a string.
	r := source.NewRegistry().SetDefault(source.NewJSONCodec[order]())
	_, err := source.DecodeTyped[string](r, testMsg{value: []byte(`{"id":"Z"}`)})
	if !errors.Is(err, source.ErrPoison) {
		t.Fatalf("DecodeTyped type mismatch err = %v, want ErrPoison", err)
	}
}

func TestCodecFunc(t *testing.T) {
	t.Parallel()
	c := source.CodecFunc(func(data []byte, _ source.Headers) (any, error) {
		return string(data), nil
	})
	v, err := c.Decode([]byte("hi"), nil)
	if err != nil || v.(string) != "hi" {
		t.Fatalf("CodecFunc.Decode = %v, %v", v, err)
	}
}

func TestRegistry_DecodePropagatesCodecError(t *testing.T) {
	t.Parallel()
	want := errors.New("codec down")
	r := source.NewRegistry().SetDefault(source.CodecFunc(func([]byte, source.Headers) (any, error) {
		return nil, want
	}))
	_, err := r.Decode(testMsg{subject: "s"})
	if !errors.Is(err, want) {
		t.Fatalf("err = %v, want wraps %v", err, want)
	}
	if !errors.Is(err, source.ErrPoison) {
		t.Fatalf("err = %v, want poison-classified", err)
	}
}
