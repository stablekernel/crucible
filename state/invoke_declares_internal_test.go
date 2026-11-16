package state

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// TestDeclaresOnError_NoPanicOnAnUncomparableEvent pins that declaresOnError, for an
// invocation loaded from JSON (no builder flag), doesn't panic when its event type is an
// interface holding an uncomparable value, as a JSON array decodes to, and reads such
// an onError as declared and the zero event as not.
func TestDeclaresOnError_NoPanicOnAnUncomparableEvent(t *testing.T) {
	loaded := Invocation[string, any, int]{OnError: []any{1.0}}
	if !loaded.declaresOnError() {
		t.Fatal("an onError holding a JSON array reads as not declared")
	}
	if zero := (Invocation[string, any, int]{}); zero.declaresOnError() {
		t.Fatal("a nil onError reads as declared")
	}
	if typed := (Invocation[string, int, int]{OnError: 0}); typed.declaresOnError() {
		t.Fatal("a loaded int onError of 0 reads as declared")
	}
	if built := (Invocation[string, int, int]{OnError: 0, hasOnError: true}); !built.declaresOnError() {
		t.Fatal("a built onError of 0 reads as not declared")
	}
}

// TestInvocation_UndeclaredOnErrorKeepsEveryOtherField pins that an invocation with no
// onError declared, which MarshalJSON writes without the onError key, still writes every
// other field: each exported field of Invocation, found by reflection, so a field added
// later is covered too.
func TestInvocation_UndeclaredOnErrorKeepsEveryOtherField(t *testing.T) {
	inv := Invocation[string, string, int]{}
	v := reflect.ValueOf(&inv).Elem()
	typ := v.Type()
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() || f.Name == "OnError" {
			continue
		}
		fv := v.Field(i)
		switch fv.Kind() {
		case reflect.String:
			fv.SetString("x")
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fv.SetInt(1)
		case reflect.Map:
			fv.Set(reflect.MakeMapWithSize(fv.Type(), 1))
			fv.SetMapIndex(reflect.ValueOf("k"), reflect.ValueOf(any("v")))
		case reflect.Struct:
			if f.Type == reflect.TypeOf(Ref{}) {
				fv.Set(reflect.ValueOf(Ref{Name: "x"}))
			}
		default:
			t.Fatalf("field %s has kind %s: teach this test to fill it", f.Name, fv.Kind())
		}
	}
	raw, err := json.Marshal(inv)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal(raw, &keys); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if !f.IsExported() {
			continue
		}
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		_, present := keys[name]
		if f.Name == "OnError" {
			if present {
				t.Fatal("an undeclared onError was written")
			}
			continue
		}
		if !present {
			t.Fatalf("field %s (%q) is missing from the JSON of an invocation with no onError declared: %s", f.Name, name, raw)
		}
	}
}
