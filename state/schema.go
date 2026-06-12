package state

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"
)

// This file ships the ContextSchema — a serializable description of a machine's
// context data model. It is the data contract that lets an expression layer
// type-check guards and assigns against the context, lets a polyglot binding
// agree on context shape across languages, and lets a studio render
// context-update and guard forms from a known set of fields.
//
// The schema is metadata only: the kernel never inspects it and Fire never reads
// it. It attaches to the IR through the additive envelope slot (IR.Context),
// round-trips losslessly via the same preserve-unknown machinery as the rest of
// the envelope, and is opt-in — a machine with no schema is valid and simply
// limits later type-checking. SchemaOf derives one from a Go type by reflection;
// WithContextSchema attaches it to a builder.
//
// Everything here is stdlib-only and JSON-serializable, since the schema is meant
// to travel over a builder API and across language boundaries.

// SchemaKind names the type category of a context field. The scalar kinds reuse
// the ParamType vocabulary verbatim (string/int/float/bool/duration, plus the
// time scalar and enum); the composite kinds — object, list, map — describe
// structured shapes that ParamType does not cover. It serializes as its lowercase
// string for a stable, language-neutral wire form.
type SchemaKind string

// The schema kinds. Scalars share their wire string with the matching ParamType
// so a single vocabulary spans both the param schema and the context schema.
const (
	// SchemaString is a free-form string.
	SchemaString SchemaKind = "string"
	// SchemaInt is an integer.
	SchemaInt SchemaKind = "int"
	// SchemaFloat is a floating-point number.
	SchemaFloat SchemaKind = "float"
	// SchemaBool is a boolean.
	SchemaBool SchemaKind = "bool"
	// SchemaDuration is a time.Duration, conventionally carried as a Go duration
	// string (e.g. "1500ms").
	SchemaDuration SchemaKind = "duration"
	// SchemaTime is a time.Time, conventionally carried as an RFC 3339 string.
	SchemaTime SchemaKind = "time"
	// SchemaObject is a nested object with named fields, carried on Fields.
	SchemaObject SchemaKind = "object"
	// SchemaList is an ordered list whose element type is carried on Elem.
	SchemaList SchemaKind = "list"
	// SchemaMap is a keyed map whose key and value types are carried on Key and
	// Elem.
	SchemaMap SchemaKind = "map"
	// SchemaEnum is a string constrained to an enumerated set carried on Enum.
	SchemaEnum SchemaKind = "enum"
	// SchemaAny is the honest kind for a Go type that cannot be reflected to a
	// narrower schema — an interface, func, channel, or complex value. It signals
	// "shape unknown" rather than asserting a concrete kind, so a freeze-time
	// type-check is not given false confidence by a coerced-to-string field.
	SchemaAny SchemaKind = "any"
)

// ContextSchema is a serializable description of a machine's context type: an
// object whose named fields each carry a SchemaField type. It is the root of the
// data model attached to the IR and reuses the closed-enum extension policy used
// across the envelope, so an unknown field kind a newer producer emitted survives
// a load -> save cycle verbatim.
type ContextSchema struct {
	// Fields are the context's named top-level fields, in declaration order for
	// objects derived by SchemaOf (struct field order) and as authored otherwise.
	Fields []SchemaField `json:"fields,omitempty"`

	// Meta is the reserved per-schema extension namespace, round-tripped verbatim
	// like every other Meta in the IR. The kernel never inspects it.
	Meta map[string]any `json:"meta,omitempty"`

	// extra preserves unknown top-level JSON keys a newer producer emitted so they
	// survive a load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// contextSchemaKnownKeys is the set of JSON keys ContextSchema models; anything
// else is captured into extra and preserved verbatim on round-trip.
var contextSchemaKnownKeys = map[string]struct{}{"fields": {}, "meta": {}}

// MarshalJSON encodes a ContextSchema, merging its preserved unknown keys back in
// with stable key ordering.
func (s ContextSchema) MarshalJSON() ([]byte, error) {
	type alias ContextSchema
	return marshalWithExtra(alias(s), s.extra)
}

// UnmarshalJSON decodes a ContextSchema and captures any unknown top-level keys
// into extra so they survive re-serialization.
func (s *ContextSchema) UnmarshalJSON(data []byte) error {
	type alias ContextSchema
	var a alias
	extra, err := captureExtra(data, &a, contextSchemaKnownKeys)
	if err != nil {
		return err
	}
	*s = ContextSchema(a)
	s.extra = extra
	return nil
}

// SchemaField is one named field of a context object: its name, its type kind,
// whether it is nullable (a Go pointer or other nilable type), and the
// kind-specific shape carried on Fields (object), Elem (list element, map value),
// Key (map key), and Enum (enum values).
type SchemaField struct {
	// Name is the field's wire name — the JSON-tag name for a SchemaOf-derived
	// struct field, the Go field name when no JSON tag is present.
	Name string `json:"name"`
	// Kind is the field's type category.
	Kind SchemaKind `json:"kind"`
	// Nullable reports whether the field may be absent/nil (a Go pointer, or a
	// natively nilable map/slice). It is informational metadata; the kernel never
	// enforces it.
	Nullable bool `json:"nullable,omitempty"`

	// Fields carries the nested named fields when Kind is SchemaObject.
	Fields []SchemaField `json:"fields,omitempty"`
	// Elem carries the element type when Kind is SchemaList, or the value type when
	// Kind is SchemaMap.
	Elem *SchemaField `json:"elem,omitempty"`
	// Key carries the key type when Kind is SchemaMap.
	Key *SchemaField `json:"key,omitempty"`
	// Enum lists the allowed values when Kind is SchemaEnum; it is empty otherwise.
	Enum []string `json:"enum,omitempty"`

	// extra preserves unknown JSON keys a newer producer emitted so they survive a
	// load -> save cycle (forward-compat). Never inspected by the kernel.
	extra map[string]json.RawMessage
}

// schemaFieldKnownKeys is the set of JSON keys SchemaField models; anything else
// is captured into extra and preserved verbatim on round-trip. This is what makes
// an unknown field kind's sidecar data survive a load -> save cycle.
var schemaFieldKnownKeys = map[string]struct{}{
	"name": {}, "kind": {}, "nullable": {}, "fields": {}, "elem": {}, "key": {}, "enum": {},
}

// MarshalJSON encodes a SchemaField, merging its preserved unknown keys back in
// with stable key ordering.
func (f SchemaField) MarshalJSON() ([]byte, error) {
	type alias SchemaField
	return marshalWithExtra(alias(f), f.extra)
}

// UnmarshalJSON decodes a SchemaField and captures any unknown keys into extra so
// they survive re-serialization.
func (f *SchemaField) UnmarshalJSON(data []byte) error {
	type alias SchemaField
	var a alias
	extra, err := captureExtra(data, &a, schemaFieldKnownKeys)
	if err != nil {
		return err
	}
	*f = SchemaField(a)
	f.extra = extra
	return nil
}

// FieldAt resolves the SchemaField at a dotted field path, descending object
// fields and unwrapping list/map element types when a path segment names a
// collection's element. It returns the resolved field and true, or the zero field
// and false when any segment does not resolve. The lookup is the type-side helper
// an expression layer uses to type a guard/assign reference like "order.total".
//
// Path semantics: each segment names a field of the current object; to step into
// a list or map element, the segment names the list/map field and the next
// segment continues into its element type (lists and maps both descend through
// their Elem). An empty path returns the schema's root object as an unnamed
// object field.
func (s ContextSchema) FieldAt(path string) (SchemaField, bool) {
	root := SchemaField{Kind: SchemaObject, Fields: s.Fields}
	if path == "" {
		return root, true
	}
	cur := root
	for _, seg := range strings.Split(path, ".") {
		next, ok := childField(cur, seg)
		if !ok {
			return SchemaField{}, false
		}
		cur = next
	}
	return cur, true
}

// childField resolves a single path segment against a field, looking up a named
// field on an object and descending into the element type of a list or map.
func childField(f SchemaField, seg string) (SchemaField, bool) {
	switch f.Kind {
	case SchemaObject:
		for _, fld := range f.Fields {
			if fld.Name == seg {
				return fld, true
			}
		}
		return SchemaField{}, false
	case SchemaList, SchemaMap:
		if f.Elem == nil {
			return SchemaField{}, false
		}
		return childField(*f.Elem, seg)
	default:
		return SchemaField{}, false
	}
}

// SchemaOf derives a ContextSchema from the Go type C by reflection. It is the
// opt-in helper a host pairs with WithContextSchema to attach a context's shape to
// a machine; deriving is never automatic at Forge, so an absent schema stays
// valid.
//
// The reflection mapping is:
//
//   - struct                      -> object; one field per exported field, named by
//     its json tag (falling back to the Go field name), in declaration order.
//     A field tagged `json:"-"` is skipped; an embedded (anonymous) struct is
//     flattened, mirroring encoding/json.
//   - string                      -> string
//   - all integer kinds           -> int
//   - float32/float64             -> float
//   - bool                        -> bool
//   - time.Time                   -> time
//   - time.Duration               -> duration
//   - slice / array               -> list, with the element type derived recursively
//   - map                         -> map, with key and value types derived recursively
//   - pointer                     -> the pointee's type, marked Nullable
//   - interface{} / other kinds   -> string (the conservative fallback; the kind
//     cannot be reflected to anything narrower)
//
// Enums cannot be reflected reliably: a Go enum is typically a named integer or
// string type whose allowed values live in package-level constants the reflect
// package cannot enumerate. SchemaOf therefore maps such a type to its underlying
// scalar (int or string); declare the allowed values explicitly with a SchemaField
// of Kind SchemaEnum to override the scalar — for example, by authoring the
// ContextSchema directly rather than deriving it.
func SchemaOf[C any]() ContextSchema {
	t := reflect.TypeOf((*C)(nil)).Elem()
	// Unwrap a top-level pointer so SchemaOf[*Order] and SchemaOf[Order] derive the
	// same object shape; a pointer context is a common Go convention.
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct && !isTimeType(t) {
		return ContextSchema{Fields: structFields(t)}
	}
	// A non-struct context (e.g. a map or a named scalar) is described as a
	// single-field object would be unnatural; instead the root carries no fields and
	// the whole shape is the derived field's — but a ContextSchema is an object by
	// contract, so a non-struct root yields an empty field set. Authors of
	// non-struct contexts attach a hand-authored schema.
	return ContextSchema{}
}

// structFields derives the SchemaField slice for a struct type, honoring json
// tags, skipping unexported and json:"-" fields, and flattening embedded structs
// to mirror encoding/json's field promotion.
func structFields(t reflect.Type) []SchemaField {
	var out []SchemaField
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" && !sf.Anonymous {
			// Unexported, non-embedded field: invisible to encoding/json, omit it.
			continue
		}
		name, skip := jsonFieldName(sf)
		if skip {
			continue
		}
		// Flatten an embedded struct with no json tag, mirroring encoding/json's
		// promotion of anonymous struct fields into the parent object.
		if sf.Anonymous && name == sf.Name {
			ft := sf.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if ft.Kind() == reflect.Struct && !isTimeType(ft) {
				out = append(out, structFields(ft)...)
				continue
			}
		}
		field := fieldOf(sf.Type)
		field.Name = name
		out = append(out, field)
	}
	return out
}

// fieldOf derives an unnamed SchemaField for a Go type, recursing through
// pointers (marking Nullable), structs, slices/arrays, and maps. The caller sets
// Name.
func fieldOf(t reflect.Type) SchemaField {
	nullable := false
	for t.Kind() == reflect.Pointer {
		nullable = true
		t = t.Elem()
	}
	f := kindOf(t)
	if nullable {
		f.Nullable = true
	}
	return f
}

// kindOf maps a dereferenced Go type to its SchemaField, dispatching scalars, the
// time/duration special cases, structs, slices, arrays, and maps.
func kindOf(t reflect.Type) SchemaField {
	switch {
	case isTimeType(t):
		return SchemaField{Kind: SchemaTime}
	case isDurationType(t):
		return SchemaField{Kind: SchemaDuration}
	}
	switch t.Kind() {
	case reflect.String:
		return SchemaField{Kind: SchemaString}
	case reflect.Bool:
		return SchemaField{Kind: SchemaBool}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return SchemaField{Kind: SchemaInt}
	case reflect.Float32, reflect.Float64:
		return SchemaField{Kind: SchemaFloat}
	case reflect.Struct:
		return SchemaField{Kind: SchemaObject, Fields: structFields(t)}
	case reflect.Slice, reflect.Array:
		elem := fieldOf(t.Elem())
		f := SchemaField{Kind: SchemaList, Elem: &elem}
		if t.Kind() == reflect.Slice {
			f.Nullable = true
		}
		return f
	case reflect.Map:
		key := fieldOf(t.Key())
		val := fieldOf(t.Elem())
		return SchemaField{Kind: SchemaMap, Key: &key, Elem: &val, Nullable: true}
	default:
		// Interface, channel, func, complex, and any other kind cannot be reflected
		// to a narrower schema; report SchemaAny so the field still appears without
		// dishonestly asserting it is a string (which would mislead a type-check).
		return SchemaField{Kind: SchemaAny}
	}
}

// jsonFieldName returns the wire name for a struct field per its json tag, and
// whether the field is skipped (json:"-"). A field with no json tag uses its Go
// field name.
func jsonFieldName(sf reflect.StructField) (name string, skip bool) {
	tag, ok := sf.Tag.Lookup("json")
	if !ok {
		return sf.Name, false
	}
	first := tag
	if i := strings.IndexByte(tag, ','); i >= 0 {
		first = tag[:i]
	}
	if first == "-" && !strings.Contains(tag, ",") {
		return "", true
	}
	if first == "" {
		return sf.Name, false
	}
	return first, false
}

// timeType and durationType are the reflect.Type identities of the two time
// special cases SchemaOf recognizes, resolved once at init.
var (
	timeType     = reflect.TypeOf(time.Time{})
	durationType = reflect.TypeOf(time.Duration(0))
)

// isTimeType reports whether t is exactly time.Time.
func isTimeType(t reflect.Type) bool { return t == timeType }

// isDurationType reports whether t is exactly time.Duration. It is checked before
// the integer fallback so a duration maps to SchemaDuration, not SchemaInt.
func isDurationType(t reflect.Type) bool { return t == durationType }

// cloneContextSchema deep-copies a ContextSchema so a copied IR node never aliases
// the source's fields or preserved extras. A nil input yields nil.
func cloneContextSchema(in *ContextSchema) *ContextSchema {
	if in == nil {
		return nil
	}
	out := ContextSchema{
		Fields: cloneSchemaFields(in.Fields),
		Meta:   cloneMeta(in.Meta),
		extra:  cloneRawExtra(in.extra),
	}
	return &out
}

// cloneSchemaFields deep-copies a SchemaField slice, recursing through nested
// object fields and list/map element and key types.
func cloneSchemaFields(in []SchemaField) []SchemaField {
	if in == nil {
		return nil
	}
	out := make([]SchemaField, len(in))
	for i := range in {
		out[i] = cloneSchemaField(in[i])
	}
	return out
}

// cloneSchemaField deep-copies a single SchemaField, including its nested fields,
// element, key, enum values, and preserved extras.
func cloneSchemaField(in SchemaField) SchemaField {
	out := in
	out.Fields = cloneSchemaFields(in.Fields)
	out.Enum = append([]string(nil), in.Enum...)
	out.extra = cloneRawExtra(in.extra)
	if in.Elem != nil {
		e := cloneSchemaField(*in.Elem)
		out.Elem = &e
	}
	if in.Key != nil {
		k := cloneSchemaField(*in.Key)
		out.Key = &k
	}
	return out
}
