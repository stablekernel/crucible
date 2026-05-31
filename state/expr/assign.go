package expr

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"

	"github.com/stablekernel/crucible/state"
)

// Assign compiles a CEL expression from source that evaluates to a map of context
// field updates, and registers it under name in reg as a CEL-backed assign reducer
// the kernel folds inside Fire. Reference it from a transition with the Assign verb
// (or a state with OnEntryAssign / OnExitAssign) exactly like a Go reducer.
//
// Compilation and type-checking happen once, here, at authoring time — never inside
// Fire. The expression is checked against the schema-derived environment, where the
// context's fields are bound as top-level variables by their JSON name (the same
// projection rich guards read). The result type must be a map keyed by string: each
// entry names a context field and supplies its new value, and authoring fails loudly
// otherwise. For example, over an order context:
//
//	expr.Assign(reg, "applyDiscount", `{"total": total * 0.9, "status": "discounted"}`, schema)
//
// At run time the reducer evaluates the expression against the prior context, then
// merges the resulting field updates onto a copy of that context (a shallow,
// top-level overlay) and returns the next context. The merge goes through the same
// JSON projection as the read path, so it is symmetric and language-neutral.
// Like every assign the reducer is total and pure: it emits no effect and cannot
// fail the step. A rare runtime evaluation error (the expression is already
// type-checked, so this is unusual) leaves the context unchanged.
//
// The expression reads only the context, mirroring the rich-guard environment;
// reading the triggering event is a later, additive capability. With a Catalog
// option the type-checked AST is collected for tooling and polyglot transport, the
// same as for guards. The context type parameter C is the registry's context type.
func Assign[C any](reg *state.Registry[C], name, source string, schema state.ContextSchema, opts ...Option) error {
	cfg := config{costLimit: defaultCostLimit}
	for _, o := range opts {
		o(&cfg)
	}

	env, err := newEnv(schema)
	if err != nil {
		return fmt.Errorf("assign %q: %w", name, err)
	}
	ast, iss := env.Compile(source)
	if iss != nil && iss.Err() != nil {
		return fmt.Errorf("assign %q: compile: %w", name, iss.Err())
	}
	if ast.OutputType().Kind() != types.MapKind {
		return fmt.Errorf("assign %q: result type is %s, want a map of field updates", name, ast.OutputType())
	}

	program, err := env.Program(ast, cel.CostLimit(cfg.costLimit), cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return fmt.Errorf("assign %q: build program: %w", name, err)
	}

	reg.Assign(name, celAssign[C](program, schema))

	if cfg.catalog != nil {
		astBytes, err := checkedASTBytes(ast)
		if err != nil {
			return fmt.Errorf("assign %q: %w", name, err)
		}
		if err := cfg.catalog.add(name, RichEntry{
			Source:     source,
			Dialect:    Dialect,
			CheckedAST: astBytes,
		}); err != nil {
			return fmt.Errorf("assign %q: %w", name, err)
		}
	}
	return nil
}

// celAssign builds the AssignFn that evaluates the compiled program against the
// prior context and merges the resulting field-update map back into it. Any failure
// to build the activation, evaluate, read the map, or round-trip the merge leaves
// the context unchanged — the reducer is total and cannot surface an error.
func celAssign[C any](program cel.Program, schema state.ContextSchema) state.AssignFn[C] {
	return func(in state.AssignCtx[C]) C {
		activation, err := marshalActivation(in.Entity, schema)
		if err != nil {
			return in.Entity
		}
		out, _, err := program.Eval(activation)
		if err != nil {
			return in.Entity
		}
		native, err := out.ConvertToNative(reflect.TypeOf(map[string]any{}))
		if err != nil {
			return in.Entity
		}
		// ConvertToNative to the map[string]any target type yields that type or an
		// error, so the assertion is safe; an empty update set is a no-op.
		updates := native.(map[string]any)
		if len(updates) == 0 {
			return in.Entity
		}
		return mergeUpdates(in.Entity, updates)
	}
}

// mergeUpdates overlays the field updates onto a copy of entity through the JSON
// projection: the prior context is marshaled to a map, the updates replace the named
// keys, and the result is unmarshaled back into a fresh context value. A marshaling
// failure leaves the entity unchanged.
func mergeUpdates[C any](entity C, updates map[string]any) C {
	base, err := json.Marshal(entity)
	if err != nil {
		return entity
	}
	var m map[string]any
	if err = json.Unmarshal(base, &m); err != nil {
		return entity
	}
	for k, v := range updates {
		m[k] = v
	}
	merged, err := json.Marshal(m)
	if err != nil {
		return entity
	}
	var next C
	if err = json.Unmarshal(merged, &next); err != nil {
		return entity
	}
	return next
}
