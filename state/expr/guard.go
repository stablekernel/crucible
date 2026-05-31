package expr

import (
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/stablekernel/crucible/state"
)

// Option configures a Guard authoring call. Options follow the functional-options
// pattern so the rich tier gains capabilities additively without changing the Guard
// signature: required arguments stay positional, everything optional arrives as an
// Option.
type Option func(*config)

// config holds the resolved Guard options.
type config struct {
	catalog   *Catalog
	costLimit uint64
}

// WithCatalog records the authored guard's source and type-checked AST into cat,
// the name-keyed sidecar a host later attaches to the IR's Meta with Catalog.Meta.
// Without it, the guard is still compiled and registered for evaluation, but its AST
// is not collected for tooling or polyglot transport.
func WithCatalog(cat *Catalog) Option {
	return func(c *config) { c.catalog = cat }
}

// WithCostLimit overrides the per-guard CEL evaluation cost ceiling. The default is
// generous for boolean predicates; lower it to tighten the bound on an untrusted or
// expensive expression.
func WithCostLimit(limit uint64) Option {
	return func(c *config) { c.costLimit = limit }
}

// Guard compiles a CEL guard from source against schema, registers it under name in
// reg as a CEL-backed guard binding the kernel evaluates inside Fire, and returns
// the rich IR node (a named-ref leaf tagged Kind "rich") that references it.
//
// Compilation happens once, here, at authoring time — never inside Fire. The source
// is parsed and type-checked against the schema-derived environment; a type error
// (an unknown field, a comparison CEL rejects, a non-bool result) fails authoring
// loudly rather than at evaluation. The single compiled program feeds both the
// registered binding (what the kernel calls) and, when a Catalog option is supplied,
// the stored type-checked AST (what tooling reads), so the evaluated guard and the
// stored AST cannot drift.
//
// The returned node is an ordinary named-ref guard leaf as far as the kernel is
// concerned: drop it into a transition with WhenExpr, compose it with And/Or/Not, or
// reference it by name from a JSON-authored machine that Provides reg. The state
// type parameter S is the machine's state type the returned node composes over; the
// context type parameter C is the registry's context type.
func Guard[S comparable, C any](
	reg *state.Registry[C], name, source string, schema state.ContextSchema, opts ...Option,
) (state.GuardNode[S], error) {
	cfg := config{costLimit: defaultCostLimit}
	for _, o := range opts {
		o(&cfg)
	}

	env, err := newEnv(schema)
	if err != nil {
		return state.GuardNode[S]{}, fmt.Errorf("guard %q: %w", name, err)
	}
	ast, iss := env.Compile(source)
	if iss != nil && iss.Err() != nil {
		return state.GuardNode[S]{}, fmt.Errorf("guard %q: compile: %w", name, iss.Err())
	}
	if ast.OutputType() != cel.BoolType {
		return state.GuardNode[S]{}, fmt.Errorf("guard %q: result type is %s, want bool", name, ast.OutputType())
	}

	program, err := env.Program(ast, cel.CostLimit(cfg.costLimit), cel.EvalOptions(cel.OptOptimize))
	if err != nil {
		return state.GuardNode[S]{}, fmt.Errorf("guard %q: build program: %w", name, err)
	}

	reg.BindGuard(name, celGuard[C]{program: program, schema: schema})

	if cfg.catalog != nil {
		astBytes, err := checkedASTBytes(ast)
		if err != nil {
			return state.GuardNode[S]{}, fmt.Errorf("guard %q: %w", name, err)
		}
		if err := cfg.catalog.add(name, RichEntry{
			Source:     source,
			Dialect:    Dialect,
			CheckedAST: astBytes,
		}); err != nil {
			return state.GuardNode[S]{}, fmt.Errorf("guard %q: %w", name, err)
		}
	}

	return richNode[S](name), nil
}

// richNode builds the rich IR node for a registered rich guard: a named-ref guard
// leaf tagged Kind "rich". It is structurally identical to a Core named-ref leaf —
// the kernel resolves and evaluates it by name — but the Kind discriminant marks it
// as rich so analysis and tooling know its truth lives in the sidecar AST rather
// than in the kernel's structured tree.
func richNode[S comparable](name string) state.GuardNode[S] {
	node := state.Guard[S](name)
	node.Kind = state.GuardKindRich
	return node
}
