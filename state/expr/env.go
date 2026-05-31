package expr

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"

	"github.com/stablekernel/crucible/state"
)

// newEnv builds a deterministic CEL environment whose variables are derived from a
// ContextSchema. It starts from the CEL standard library — which carries no
// ambient or nondeterministic builtin (verified against the pinned cel-go: there is
// no now, no random, and the timestamp/duration constructors are pure of their
// arguments) — and adds no extension library and no host function, so a program
// compiled in this env is a pure function of its context.
//
// Each top-level schema field becomes a typed CEL variable named by its wire
// (JSON) name, so the activation map produced by marshalActivation lines up with
// the declared variables. A field whose kind cannot be expressed as a CEL variable
// (a non-string/int/bool map key) is a build error rather than a silent demotion.
func newEnv(schema state.ContextSchema) (*cel.Env, error) {
	opts := []cel.EnvOption{cel.StdLib()}
	for _, f := range schema.Fields {
		ct, err := celType(f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		opts = append(opts, cel.Variable(f.Name, ct))
	}
	env, err := cel.NewCustomEnv(opts...)
	if err != nil {
		return nil, fmt.Errorf("build cel env: %w", err)
	}
	return env, nil
}

// celType maps a SchemaField to its CEL type. Scalars map one-to-one; an enum is a
// constrained string (the allowed-set constraint is a schema concern CEL's type
// system does not model, matching Core's "enum is a constrained string" stance); a
// nested object degrades to map<string, dyn> at the v1 floor; a list and a map
// recurse into their element (and key) types. A map with a non-string/int/bool key
// has no CEL counterpart and is reported as an error.
func celType(f state.SchemaField) (*cel.Type, error) {
	switch f.Kind {
	case state.SchemaString, state.SchemaEnum:
		return cel.StringType, nil
	case state.SchemaInt:
		return cel.IntType, nil
	case state.SchemaFloat:
		return cel.DoubleType, nil
	case state.SchemaBool:
		return cel.BoolType, nil
	case state.SchemaDuration:
		return cel.DurationType, nil
	case state.SchemaTime:
		return cel.TimestampType, nil
	case state.SchemaObject:
		// The v1 floor models a nested object as an open map; per-field typing of
		// nested objects is an additive improvement, not a v1 requirement.
		return cel.MapType(cel.StringType, cel.DynType), nil
	case state.SchemaList:
		elem := cel.DynType
		if f.Elem != nil {
			et, err := celType(*f.Elem)
			if err != nil {
				return nil, err
			}
			elem = et
		}
		return cel.ListType(elem), nil
	case state.SchemaMap:
		key := cel.StringType
		if f.Key != nil {
			kt, err := mapKeyType(*f.Key)
			if err != nil {
				return nil, err
			}
			key = kt
		}
		val := cel.DynType
		if f.Elem != nil {
			vt, err := celType(*f.Elem)
			if err != nil {
				return nil, err
			}
			val = vt
		}
		return cel.MapType(key, val), nil
	default:
		// An unknown (newer-producer) kind cannot be typed; fall back to dyn so the
		// variable still exists and the source can reference it, deferring type
		// safety to runtime rather than failing the whole env build.
		return cel.DynType, nil
	}
}

// mapKeyType maps a map-key SchemaField to a CEL map-key type. CEL restricts map
// keys to string, int, uint, and bool; any other key kind (notably float, a
// composite, or a duration) is reported as an error so the gap is loud at env
// build rather than a silent miscompile.
func mapKeyType(f state.SchemaField) (*cel.Type, error) {
	switch f.Kind {
	case state.SchemaString, state.SchemaEnum:
		return cel.StringType, nil
	case state.SchemaInt:
		return cel.IntType, nil
	case state.SchemaBool:
		return cel.BoolType, nil
	default:
		return nil, fmt.Errorf("map key kind %q is not a valid CEL map key (string, int, or bool)", f.Kind)
	}
}

// marshalActivation projects a context value to the map[string]any activation a
// compiled program evaluates against. It marshals the entity to JSON and back so
// field names follow the same JSON-tag convention the ContextSchema and the kernel
// reflective path reader use, keeping the activation language-neutral and exactly
// aligned with the declared variables. Schema-typed fields are coerced to the CEL
// runtime representation a typed variable expects: integers to int64, durations to
// a CEL Duration, and timestamps to a CEL Timestamp.
func marshalActivation(entity any, schema state.ContextSchema) (map[string]any, error) {
	b, err := json.Marshal(entity)
	if err != nil {
		return nil, fmt.Errorf("marshal context: %w", err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("project context to map: %w", err)
	}
	for _, f := range schema.Fields {
		v, ok := raw[f.Name]
		if !ok || v == nil {
			continue
		}
		coerced, err := coerceField(f, v)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		raw[f.Name] = coerced
	}
	return raw, nil
}

// coerceField coerces a JSON-decoded value to the CEL runtime representation its
// declared variable type expects. JSON decodes every number as float64, so an int
// field is narrowed to int64; a duration field (a Go duration string) becomes a CEL
// Duration; a time field (an RFC 3339 string) becomes a CEL Timestamp. Other kinds
// pass through — CEL's default type adapter handles strings, bools, floats, lists,
// and maps directly.
func coerceField(f state.SchemaField, v any) (any, error) {
	switch f.Kind {
	case state.SchemaInt:
		n, ok := v.(float64)
		if !ok {
			return v, nil
		}
		return int64(n), nil
	case state.SchemaDuration:
		// A duration reaches the activation either as a Go duration string (when the
		// context marshals it as text) or as a number of nanoseconds (the default
		// encoding/json form of a time.Duration, which is an int64). Honor both.
		switch d := v.(type) {
		case string:
			parsed, err := parseGoDuration(d)
			if err != nil {
				return nil, err
			}
			return types.Duration{Duration: parsed}, nil
		case float64:
			return types.Duration{Duration: time.Duration(int64(d))}, nil
		default:
			return v, nil
		}
	case state.SchemaTime:
		s, ok := v.(string)
		if !ok {
			return v, nil
		}
		ts, err := parseRFC3339(s)
		if err != nil {
			return nil, err
		}
		return types.Timestamp{Time: ts}, nil
	default:
		return v, nil
	}
}
