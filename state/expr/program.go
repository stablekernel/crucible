package expr

import (
	"context"
	"fmt"

	celpb "cel.dev/expr"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types/ref"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
	"google.golang.org/protobuf/proto"

	"github.com/stablekernel/crucible/state"
)

// defaultCostLimit bounds a guard program's evaluation cost so a pathological
// expression cannot run unbounded. CEL is already non-Turing-complete; the cost
// limit is a second, quantitative guardrail. It is generous for the boolean
// predicates a guard expresses and is configurable per guard.
const defaultCostLimit uint64 = 1_000_000

// celGuard is the CEL-backed state.GuardBinding. It holds a compiled program and
// the schema needed to project a context value into the program's activation. Its
// EvalGuard runs synchronously, so the kernel can call it inside the pure Fire step
// exactly like a Go-func guard. It is generic over the registry's context type C
// only to satisfy state.GuardBinding[C]; evaluation reads the context through the
// type-erased view, so the program never needs the concrete C.
type celGuard[C any] struct {
	program cel.Program
	schema  state.ContextSchema
}

// EvalGuard projects the request's context to a CEL activation, evaluates the
// compiled program, and returns its boolean verdict. A program whose output is not
// a bool, or an activation that cannot be built, is an error — surfaced to the
// kernel, which treats a guard error as a non-transitioning false, matching how a
// Go guard that cannot decide does not enable a transition.
func (g celGuard[C]) EvalGuard(_ context.Context, req state.GuardRequest[C]) (state.GuardResult, error) {
	activation, err := marshalActivation(req.Context.Raw(), g.schema)
	if err != nil {
		return state.GuardResult{}, fmt.Errorf("guard %q: %w", req.Name, err)
	}
	out, _, err := g.program.Eval(activation)
	if err != nil {
		return state.GuardResult{}, fmt.Errorf("guard %q: eval: %w", req.Name, err)
	}
	ok, err := boolVal(out)
	if err != nil {
		return state.GuardResult{}, fmt.Errorf("guard %q: %w", req.Name, err)
	}
	return state.GuardResult{OK: ok}, nil
}

// boolVal extracts a Go bool from a CEL result value, rejecting a non-bool result
// (a guard must be a predicate).
func boolVal(v ref.Val) (bool, error) {
	b, ok := v.Value().(bool)
	if !ok {
		return false, fmt.Errorf("guard result is %T, want bool", v.Value())
	}
	return b, nil
}

// checkedASTBytes serializes a type-checked AST to the canonical cel.dev/expr
// CheckedExpr wire form — the persistence form a polyglot evaluator (the browser
// CEL implementation) consumes. cel-go's public converter emits the legacy
// v1alpha1 CheckedExpr, which is wire-identical to the canonical proto, so the
// bytes are obtained by marshaling the v1alpha1 message and re-encoding them
// through the cel.dev/expr message to land canonical, stable bytes.
func checkedASTBytes(ast *cel.Ast) ([]byte, error) {
	alpha, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, fmt.Errorf("ast to checked expr: %w", err)
	}
	wire, err := proto.Marshal(alpha)
	if err != nil {
		return nil, fmt.Errorf("marshal checked expr: %w", err)
	}
	var canonical celpb.CheckedExpr
	if uerr := proto.Unmarshal(wire, &canonical); uerr != nil {
		return nil, fmt.Errorf("decode canonical checked expr: %w", uerr)
	}
	canonicalBytes, err := proto.Marshal(&canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical checked expr: %w", err)
	}
	return canonicalBytes, nil
}

// EvalCheckedAST rebuilds a program from stored canonical cel.dev/expr CheckedExpr
// bytes and evaluates it against a context, returning the boolean verdict. It is the
// proof that a stored rich AST is not an opaque blob but a working program — the
// same path a tooling or polyglot consumer reconstructs the guard through — and is
// the in-Go counterpart of the browser CEL evaluator that consumes the same bytes.
//
// The AST is rebound against the schema-derived env so its variables resolve; an env
// whose variables do not match the AST's declarations surfaces as an eval error.
func EvalCheckedAST(checkedAST []byte, schema state.ContextSchema, entity any) (bool, error) {
	compiled, err := CompileChecked(checkedAST, schema)
	if err != nil {
		return false, err
	}
	return compiled.Eval(entity)
}

// CompiledChecked is a rich AST that has been rebuilt and bound to its
// schema-derived environment exactly once, ready to evaluate against many context
// values. EvalCheckedAST rebuilds the env and program on every call, which is fine
// for a one-shot tooling probe but wasteful when the same stored AST is replayed
// repeatedly; CompileChecked pays that cost once and Eval reuses the program. The
// type is immutable after construction and its Eval is safe for the same
// synchronous, single-evaluator use as a celGuard.
type CompiledChecked struct {
	program cel.Program
	schema  state.ContextSchema
}

// CompileChecked rebuilds a program from stored canonical cel.dev/expr CheckedExpr
// bytes and binds it to the schema-derived environment once, returning a reusable
// CompiledChecked. It performs the same env-rebuild and program-build EvalCheckedAST
// does, so a malformed AST or an env whose variables do not match the AST's
// declarations surfaces here rather than per evaluation.
func CompileChecked(checkedAST []byte, schema state.ContextSchema) (*CompiledChecked, error) {
	ast, err := astFromCheckedBytes(checkedAST)
	if err != nil {
		return nil, err
	}
	env, err := newEnv(schema)
	if err != nil {
		return nil, fmt.Errorf("rebuild env: %w", err)
	}
	program, err := env.Program(ast, cel.CostLimit(defaultCostLimit))
	if err != nil {
		return nil, fmt.Errorf("rebuild program: %w", err)
	}
	return &CompiledChecked{program: program, schema: schema}, nil
}

// Eval projects the entity into the bound environment and evaluates the compiled
// program, returning its boolean verdict. It reuses the program built by
// CompileChecked, so repeated evaluations skip the env/AST/program rebuild.
func (c *CompiledChecked) Eval(entity any) (bool, error) {
	activation, err := marshalActivation(entity, c.schema)
	if err != nil {
		return false, err
	}
	out, _, err := c.program.Eval(activation)
	if err != nil {
		return false, fmt.Errorf("eval: %w", err)
	}
	return boolVal(out)
}

// astFromCheckedBytes rebuilds a CEL AST from canonical cel.dev/expr CheckedExpr
// bytes — the inverse of checkedASTBytes. The canonical bytes are wire-compatible
// with the legacy v1alpha1 message cel-go's loader consumes, so they decode into a
// v1alpha1 CheckedExpr and rehydrate through the public converter.
func astFromCheckedBytes(b []byte) (*cel.Ast, error) {
	var alpha exprpb.CheckedExpr
	if err := proto.Unmarshal(b, &alpha); err != nil {
		return nil, fmt.Errorf("decode checked expr: %w", err)
	}
	return cel.CheckedExprToAst(&alpha), nil
}
