// SPDX-License-Identifier: Apache-2.0

package cloudevents_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	cesdk "github.com/cloudevents/sdk-go/v2/event"
	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/cloudevents"
)

// order is the typed data payload used across the tests.
type order struct {
	ID  string `json:"id"`
	Qty int    `json:"qty"`
}

// fakeMessage is a minimal source.Message for driving the codec through a
// source.Registry. Only the fields the codec reads are populated.
type fakeMessage struct {
	value   []byte
	headers source.Headers
	subject string
}

func (m fakeMessage) Key() []byte             { return nil }
func (m fakeMessage) Value() []byte           { return m.value }
func (m fakeMessage) Headers() source.Headers { return m.headers }
func (m fakeMessage) Subject() string         { return m.subject }
func (m fakeMessage) PartitionKey() string    { return "" }
func (m fakeMessage) Cursor() source.Cursor   { return nil }
func (m fakeMessage) As(any) bool             { return false }

// newStructuredEvent builds a valid CloudEvent and returns its structured-mode
// JSON encoding alongside the source event.
func newStructuredEvent(t *testing.T) ([]byte, cesdk.Event) {
	t.Helper()
	e := cesdk.New(cesdk.CloudEventsVersionV1)
	e.SetID("evt-1")
	e.SetSource("/crucible/test")
	e.SetType("com.example.order.created")
	e.SetSubject("orders/42")
	e.SetTime(time.Date(2026, 6, 3, 10, 0, 0, 0, time.UTC))
	e.SetExtension("traceparent", "00-abc-def-01")
	if err := e.SetData(cesdk.ApplicationJSON, order{ID: "o-42", Qty: 3}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return b, e
}

func TestDetect(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		contentType string
		want        cloudevents.Mode
	}{
		{"structured json", "application/cloudevents+json", cloudevents.Structured},
		{"structured with charset", "application/cloudevents+json; charset=utf-8", cloudevents.Structured},
		{"structured batch prefix", "application/cloudevents-batch+json", cloudevents.Structured},
		{"structured mixed case", "Application/CloudEvents+JSON", cloudevents.Structured},
		{"binary plain json", "application/json", cloudevents.Binary},
		{"binary empty", "", cloudevents.Binary},
		{"binary octet", "application/octet-stream", cloudevents.Binary},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := cloudevents.Detect(tc.contentType); got != tc.want {
				t.Fatalf("Detect(%q) = %v, want %v", tc.contentType, got, tc.want)
			}
		})
	}
}

func TestModeString(t *testing.T) {
	t.Parallel()
	if got := cloudevents.Structured.String(); got != "structured" {
		t.Fatalf("Structured.String() = %q", got)
	}
	if got := cloudevents.Binary.String(); got != "binary" {
		t.Fatalf("Binary.String() = %q", got)
	}
	if got := cloudevents.Mode(99).String(); !strings.Contains(got, "99") {
		t.Fatalf("unknown Mode.String() = %q", got)
	}
}

func TestCodec_Decode_RoundTrip(t *testing.T) {
	t.Parallel()
	structured, want := newStructuredEvent(t)

	tests := []struct {
		name    string
		message source.Message
	}{
		{
			name: "structured mode",
			message: fakeMessage{
				subject: "orders",
				value:   structured,
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType},
				},
			},
		},
		{
			name: "binary mode",
			message: fakeMessage{
				subject: "orders",
				value:   mustJSON(t, order{ID: "o-42", Qty: 3}),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: "application/json"},
					{Key: "ce-specversion", Value: "1.0"},
					{Key: "ce-id", Value: "evt-1"},
					{Key: "ce-source", Value: "/crucible/test"},
					{Key: "ce-type", Value: "com.example.order.created"},
					{Key: "ce-subject", Value: "orders/42"},
					{Key: "ce-time", Value: "2026-06-03T10:00:00Z"},
					{Key: "ce-traceparent", Value: "00-abc-def-01"},
				},
			},
		},
		{
			name: "binary mode underscore separator and mixed case",
			message: fakeMessage{
				subject: "orders",
				value:   mustJSON(t, order{ID: "o-42", Qty: 3}),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: "application/json"},
					{Key: "Ce_specversion", Value: "1.0"},
					{Key: "Ce_id", Value: "evt-1"},
					{Key: "Ce_source", Value: "/crucible/test"},
					{Key: "Ce_type", Value: "com.example.order.created"},
					{Key: "Ce_subject", Value: "orders/42"},
					{Key: "Ce_time", Value: "2026-06-03T10:00:00Z"},
					{Key: "Ce_traceparent", Value: "00-abc-def-01"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()

			gotEvent, data, err := cloudevents.DecodeData[order](r, tc.message)
			if err != nil {
				t.Fatalf("DecodeData: %v", err)
			}
			assertSameEvent(t, want, gotEvent)
			if data != (order{ID: "o-42", Qty: 3}) {
				t.Fatalf("data = %+v, want {o-42 3}", data)
			}
		})
	}
}

func TestCodec_Decode_Malformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		message source.Message
	}{
		{
			name: "structured invalid json",
			message: fakeMessage{
				subject: "orders",
				value:   []byte("{not json"),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType},
				},
			},
		},
		{
			name: "structured missing required attributes",
			message: fakeMessage{
				subject: "orders",
				value:   []byte(`{"specversion":"1.0","id":"x"}`),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType},
				},
			},
		},
		{
			name: "binary missing required attributes",
			message: fakeMessage{
				subject: "orders",
				value:   mustJSON(t, order{ID: "o-1", Qty: 1}),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: "application/json"},
					{Key: "ce-id", Value: "evt-1"},
				},
			},
		},
		{
			name: "binary bad time attribute",
			message: fakeMessage{
				subject: "orders",
				value:   mustJSON(t, order{ID: "o-1", Qty: 1}),
				headers: source.Headers{
					{Key: source.ContentTypeHeader, Value: "application/json"},
					{Key: "ce-specversion", Value: "1.0"},
					{Key: "ce-id", Value: "evt-1"},
					{Key: "ce-source", Value: "/x"},
					{Key: "ce-type", Value: "t"},
					{Key: "ce-time", Value: "not-a-time"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newRegistry()

			_, err := r.Decode(tc.message)
			if err == nil {
				t.Fatal("expected decode error, got nil")
			}
			var de *source.DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("error is not *source.DecodeError: %T (%v)", err, err)
			}
			if !errors.Is(err, source.ErrPoison) {
				t.Fatalf("decode error does not report ErrPoison: %v", err)
			}
			if de.Subject != "orders" {
				t.Fatalf("DecodeError.Subject = %q, want orders", de.Subject)
			}
		})
	}
}

func TestCodec_Decode_BinaryDataContentTypeAndSchema(t *testing.T) {
	t.Parallel()
	// datacontenttype carried as a bare header (no ce- prefix), plus a
	// dataschema attribute and an empty header that must be ignored.
	msg := fakeMessage{
		subject: "orders",
		value:   mustJSON(t, order{ID: "o-9", Qty: 9}),
		headers: source.Headers{
			{Key: "datacontenttype", Value: "application/json"},
			{Key: "ce-specversion", Value: "1.0"},
			{Key: "ce-id", Value: "evt-9"},
			{Key: "ce-source", Value: "/x"},
			{Key: "ce-type", Value: "t"},
			{Key: "ce-dataschema", Value: "https://example.com/schema/order.json"},
			{Key: "ce-subject", Value: ""}, // blank: ignored
			{Key: "x-not-cloudevents", Value: "ignored"},
			{Key: "c", Value: "too-short-for-prefix"}, // shorter than "ce-"
			{Key: "ce", Value: "no-separator"},        // no separator char
			{Key: "xy-id", Value: "not-ce-prefix"},    // right length, wrong prefix
		},
	}
	r := newRegistry()
	e, data, err := cloudevents.DecodeData[order](r, msg)
	if err != nil {
		t.Fatalf("DecodeData: %v", err)
	}
	if e.DataSchema() != "https://example.com/schema/order.json" {
		t.Fatalf("dataschema = %q", e.DataSchema())
	}
	if e.Subject() != "" {
		t.Fatalf("blank subject header should be ignored, got %q", e.Subject())
	}
	if data != (order{ID: "o-9", Qty: 9}) {
		t.Fatalf("data = %+v", data)
	}
}

func TestCodec_Decode_BinaryDataContentTypeViaCePrefix(t *testing.T) {
	t.Parallel()
	// datacontenttype carried as a ce-prefixed header (ce-datacontenttype).
	msg := fakeMessage{
		subject: "orders",
		value:   mustJSON(t, order{ID: "o-3", Qty: 1}),
		headers: source.Headers{
			{Key: "ce-datacontenttype", Value: "application/json"},
			{Key: "ce-specversion", Value: "1.0"},
			{Key: "ce-id", Value: "evt-3"},
			{Key: "ce-source", Value: "/x"},
			{Key: "ce-type", Value: "t"},
		},
	}
	r := newRegistry()
	if _, _, err := cloudevents.DecodeData[order](r, msg); err != nil {
		t.Fatalf("DecodeData: %v", err)
	}
}

func TestDecodeData_EventDecodeFailure(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	msg := fakeMessage{
		subject: "orders",
		value:   []byte("{not json"),
		headers: source.Headers{{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType}},
	}
	_, _, err := cloudevents.DecodeData[order](r, msg)
	if err == nil || !errors.Is(err, source.ErrPoison) {
		t.Fatalf("expected poison decode error, got %v", err)
	}
}

func TestDecodeData_DataShapeFailure(t *testing.T) {
	t.Parallel()
	r := newRegistry()
	structured, _ := newStructuredEvent(t)
	msg := fakeMessage{
		subject: "orders",
		value:   structured,
		headers: source.Headers{{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType}},
	}
	// Target type cannot hold the object data: data decode fails.
	_, _, err := cloudevents.DecodeData[string](r, msg)
	if err == nil {
		t.Fatal("expected data-shape decode error, got nil")
	}
}

func TestExtensions(t *testing.T) {
	t.Parallel()
	e := cesdk.New(cesdk.CloudEventsVersionV1)
	e.SetID("evt-1")
	e.SetSource("/x")
	e.SetType("t")
	e.SetExtension("traceparent", "00-abc")
	e.SetExtension("partitionkey", "k-7")

	headers := cloudevents.Extensions(e)

	got := map[string]string{}
	for _, h := range headers {
		got[h.Key] = h.Value
	}
	if v := got[cloudevents.ExtensionHeaderPrefix+"traceparent"]; v != "00-abc" {
		t.Fatalf("traceparent extension = %q", v)
	}
	if v := got[cloudevents.ExtensionHeaderPrefix+"partitionkey"]; v != "k-7" {
		t.Fatalf("partitionkey extension = %q", v)
	}

	// Sorted by attribute name: partitionkey before traceparent.
	if len(headers) != 2 ||
		headers[0].Key != cloudevents.ExtensionHeaderPrefix+"partitionkey" ||
		headers[1].Key != cloudevents.ExtensionHeaderPrefix+"traceparent" {
		t.Fatalf("extensions not in sorted order: %+v", headers)
	}

	if got := cloudevents.Extensions(cesdk.New(cesdk.CloudEventsVersionV1)); got != nil {
		t.Fatalf("no-extension event should yield nil headers, got %+v", got)
	}
}

func TestExtensions_SurfacedThroughDecode(t *testing.T) {
	t.Parallel()
	structured, _ := newStructuredEvent(t)
	r := newRegistry()
	msg := fakeMessage{
		subject: "orders",
		value:   structured,
		headers: source.Headers{
			{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType},
		},
	}

	e, err := cloudevents.DecodeEvent(r, msg)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	tp, ok := cloudevents.Extensions(e).Get(cloudevents.ExtensionHeaderPrefix + "traceparent")
	if !ok || tp != "00-abc-def-01" {
		t.Fatalf("traceparent through Headers.Get = %q, ok=%v", tp, ok)
	}
}

func TestEventOf_WrongType(t *testing.T) {
	t.Parallel()
	if _, ok := cloudevents.EventOf("not an event"); ok {
		t.Fatal("EventOf should reject a non-event value")
	}
}

func TestDecodeEvent_WrongCodecResult(t *testing.T) {
	t.Parallel()
	// A registry whose default produces a non-CloudEvent value: DecodeEvent must
	// classify the type mismatch as poison.
	r := source.NewRegistry()
	r.SetDefault(source.CodecFunc(func([]byte, source.Headers) (any, error) {
		return order{ID: "x"}, nil
	}))
	_, err := cloudevents.DecodeEvent(r, fakeMessage{subject: "orders", value: []byte("{}")})
	if err == nil || !errors.Is(err, source.ErrPoison) {
		t.Fatalf("expected poison error for wrong-shape result, got %v", err)
	}
}

func TestDataAs_Mismatch(t *testing.T) {
	t.Parallel()
	structured, _ := newStructuredEvent(t)
	r := newRegistry()
	msg := fakeMessage{
		subject: "orders",
		value:   structured,
		headers: source.Headers{{Key: source.ContentTypeHeader, Value: cloudevents.StructuredContentType}},
	}
	e, err := cloudevents.DecodeEvent(r, msg)
	if err != nil {
		t.Fatalf("DecodeEvent: %v", err)
	}
	// Decoding JSON object data into a string target fails.
	var s string
	if err := cloudevents.DataAs(e, &s); err == nil {
		t.Fatal("expected DataAs mismatch error, got nil")
	}
}

// newRegistry returns a registry that decodes both structured and binary
// CloudEvents through a single instance-scoped codec.
func newRegistry() *source.Registry {
	codec := cloudevents.New()
	return source.NewRegistry().
		Register(cloudevents.StructuredContentType, codec).
		SetDefault(codec)
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// assertSameEvent compares the load-bearing attributes of two events.
func assertSameEvent(t *testing.T, want, got cesdk.Event) {
	t.Helper()
	if got.ID() != want.ID() {
		t.Errorf("id = %q, want %q", got.ID(), want.ID())
	}
	if got.Source() != want.Source() {
		t.Errorf("source = %q, want %q", got.Source(), want.Source())
	}
	if got.Type() != want.Type() {
		t.Errorf("type = %q, want %q", got.Type(), want.Type())
	}
	if got.Subject() != want.Subject() {
		t.Errorf("subject = %q, want %q", got.Subject(), want.Subject())
	}
	if !got.Time().Equal(want.Time()) {
		t.Errorf("time = %v, want %v", got.Time(), want.Time())
	}
}
