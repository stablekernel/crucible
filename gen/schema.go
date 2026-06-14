package gen

import (
	"bytes"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// writeContextType emits the generated context type from the IR's context schema.
//
// When the schema is absent or declares no fields, a map alias is emitted rather
// than an empty struct or a bare any:
//
//	type Context = map[string]any
//
// The alias is chosen over `type Context any` because the stubs still need to
// index context fields (ctx.Entity["order_total"]) without a type assertion; an
// alias to map[string]any keeps that ergonomic while honestly admitting the shape
// is open. A distinct named struct with no fields would force callers to migrate
// off it once the schema is filled in, whereas the alias is a drop-in.
func writeContextType(buf *bytes.Buffer, schema *state.ContextSchema, ctxName string, imports *importSet) {
	if schema == nil || len(schema.Fields) == 0 {
		fmt.Fprintf(buf, "// %s is the machine context. The IR carried no context schema, so it is an\n", ctxName)
		fmt.Fprintf(buf, "// open map alias: stubs may still index fields by key while the concrete shape\n")
		fmt.Fprintf(buf, "// is unknown.\n")
		fmt.Fprintf(buf, "type %s = map[string]any\n\n", ctxName)
		return
	}

	fmt.Fprintf(buf, "// %s is the machine context, synthesized from the IR's context schema.\n", ctxName)
	fmt.Fprintf(buf, "type %s struct {\n", ctxName)
	for _, f := range schema.Fields {
		goType := schemaFieldGoType(f, imports)
		fmt.Fprintf(buf, "\t%s %s `json:%q`\n", exportedFieldName(f.Name), goType, f.Name)
	}
	buf.WriteString("}\n\n")
}

// schemaFieldGoType maps a SchemaField's kind to a Go type for the generated
// struct field.
//
// Mapping decisions:
//   - Int -> int64: IR JSON numbers have no width; int64 is the widest lossless
//     integer and avoids platform-dependent int sizing in generated code.
//   - Float -> float64, Bool -> bool, String/Enum -> string. Enum is a plain
//     string (the allowed set is not enforced at the Go type level here).
//   - Duration -> time.Duration, Time -> time.Time (both add the time import).
//   - Object -> map[string]any: nested objects are flattened to an open map. This
//     is the simplest deterministic choice and avoids emitting and naming nested
//     struct types; a host that wants nested structs can refine the generated
//     field.
//   - List -> []any, Map -> map[string]any: element/key/value shapes are not
//     expanded, again favoring a deterministic, dependency-free rendering.
//   - Any / unknown kinds -> any.
//
// Nullable is intentionally not reflected (no pointer wrapping): map/slice/any
// fields are already nilable, and pointer-wrapping scalars would complicate the
// stubs for little gain. Nullability is documented in the schema, not the Go type.
func schemaFieldGoType(f state.SchemaField, imports *importSet) string {
	switch f.Kind {
	case state.SchemaString, state.SchemaEnum:
		return "string"
	case state.SchemaInt:
		return "int64"
	case state.SchemaFloat:
		return "float64"
	case state.SchemaBool:
		return "bool"
	case state.SchemaDuration:
		imports.add("time")
		return "time.Duration"
	case state.SchemaTime:
		imports.add("time")
		return "time.Time"
	case state.SchemaObject, state.SchemaMap:
		return "map[string]any"
	case state.SchemaList:
		return "[]any"
	case state.SchemaAny:
		return "any"
	default:
		return "any"
	}
}

// exportedFieldName turns a schema field's wire name into an exported Go struct
// field identifier. It reuses the identifier sanitizer (camel-casing across
// separators) so order_total becomes OrderTotal; an unnameable field falls back to
// a stable placeholder.
func exportedFieldName(name string) string {
	id := sanitizeIdent(name)
	if id == "" {
		return "Field"
	}
	return id
}
