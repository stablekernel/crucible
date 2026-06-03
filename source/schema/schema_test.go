// SPDX-License-Identifier: Apache-2.0

package schema_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stablekernel/crucible/source"
	"github.com/stablekernel/crucible/source/schema"
)

// stubMsg is a minimal [source.Message] for middleware tests.
type stubMsg struct {
	value   []byte
	headers source.Headers
	subject string
}

func (m stubMsg) Key() []byte             { return nil }
func (m stubMsg) Value() []byte           { return m.value }
func (m stubMsg) Headers() source.Headers { return m.headers }
func (m stubMsg) Subject() string         { return m.subject }
func (m stubMsg) PartitionKey() string    { return "" }
func (m stubMsg) Cursor() source.Cursor   { return stubCursor{} }
func (m stubMsg) As(any) bool             { return false }

type stubCursor struct{}

func (stubCursor) String() string { return "stub" }

func TestMiddleware_ValidPasses(t *testing.T) {
	t.Parallel()
	var ran bool
	v := schema.ValidatorFunc(func(_ context.Context, _ source.Message) error { return nil })
	h := schema.Middleware(v)(func(_ context.Context, _ source.Message) source.Result {
		ran = true
		return source.Ack()
	})
	got := h(context.Background(), stubMsg{subject: "orders"})
	if !ran {
		t.Error("handler did not run for valid message")
	}
	if got.Action != source.ActionAck {
		t.Errorf("action = %v, want ack", got.Action)
	}
}

func TestMiddleware_InvalidTerminatesAsPoison(t *testing.T) {
	t.Parallel()
	cause := errors.New("field x missing")
	var ran bool
	v := schema.ValidatorFunc(func(_ context.Context, _ source.Message) error { return cause })
	h := schema.Middleware(v)(func(_ context.Context, _ source.Message) source.Result {
		ran = true
		return source.Ack()
	})
	got := h(context.Background(), stubMsg{subject: "orders"})

	if ran {
		t.Error("handler ran for invalid message, want skipped")
	}
	if got.Action != source.ActionTerm {
		t.Errorf("action = %v, want term", got.Action)
	}
	if got.Class != source.Poison {
		t.Errorf("class = %v, want poison", got.Class)
	}
	if !errors.Is(got.Err, source.ErrPoison) {
		t.Errorf("err not ErrPoison: %v", got.Err)
	}
	var se *schema.SchemaError
	if !errors.As(got.Err, &se) {
		t.Fatalf("err not *SchemaError: %v", got.Err)
	}
	if se.Subject != "orders" {
		t.Errorf("SchemaError.Subject = %q, want orders", se.Subject)
	}
	if !errors.Is(se, cause) {
		t.Errorf("SchemaError does not unwrap to cause: %v", se)
	}
}

func TestMiddleware_NilValidatorPassthrough(t *testing.T) {
	t.Parallel()
	var ran bool
	h := schema.Middleware(nil)(func(_ context.Context, _ source.Message) source.Result {
		ran = true
		return source.Ack()
	})
	h(context.Background(), stubMsg{})
	if !ran {
		t.Error("handler did not run with nil validator")
	}
}

func TestContentTypeValidator(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		v       schema.ContentTypeValidator
		msg     stubMsg
		wantErr bool
	}{
		{
			name:    "present and allowed",
			v:       schema.ContentTypeValidator{Allowed: []string{"application/json"}},
			msg:     stubMsg{headers: source.Headers{{Key: "content-type", Value: "application/json"}}},
			wantErr: false,
		},
		{
			name:    "present but not allowed",
			v:       schema.ContentTypeValidator{Allowed: []string{"application/json"}},
			msg:     stubMsg{headers: source.Headers{{Key: "content-type", Value: "text/plain"}}},
			wantErr: true,
		},
		{
			name:    "missing header",
			v:       schema.ContentTypeValidator{},
			msg:     stubMsg{},
			wantErr: true,
		},
		{
			name:    "any allowed when allow-list empty",
			v:       schema.ContentTypeValidator{},
			msg:     stubMsg{headers: source.Headers{{Key: "content-type", Value: "anything"}}},
			wantErr: false,
		},
		{
			name:    "custom header key",
			v:       schema.ContentTypeValidator{Header: "ce-type"},
			msg:     stubMsg{headers: source.Headers{{Key: "ce-type", Value: "x"}}},
			wantErr: false,
		},
		{
			name:    "require value rejects empty payload",
			v:       schema.ContentTypeValidator{RequireValue: true},
			msg:     stubMsg{headers: source.Headers{{Key: "content-type", Value: "x"}}},
			wantErr: true,
		},
		{
			name:    "require value passes with payload",
			v:       schema.ContentTypeValidator{RequireValue: true},
			msg:     stubMsg{value: []byte("p"), headers: source.Headers{{Key: "content-type", Value: "x"}}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.v.Validate(context.Background(), tt.msg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaError(t *testing.T) {
	t.Parallel()
	inner := errors.New("bad")
	se := &schema.SchemaError{Subject: "s", Err: inner}
	if !errors.Is(se, source.ErrPoison) {
		t.Error("SchemaError should match ErrPoison")
	}
	if !errors.Is(se, inner) {
		t.Error("SchemaError should unwrap to inner")
	}
	if se.Error() == "" {
		t.Error("empty Error()")
	}
}
