package state

import "encoding/json"

// IR is the serializable definition produced and consumed by the data
// front-end. It is the canonical machine: pure, lossless data. Behavior lives
// in a host registry and is referenced by name (via Ref), never embedded, so
// the IR round-trips to and from JSON without losing structure or bindings'
// identity.
//
// Non-serializable concerns — CurrentStateFn, requirement predicates, and
// middleware — are pure-runtime and are intentionally absent from the IR; a
// machine rehydrated from JSON is Cast from an explicit state and bound to a
// registry via Provide.
type IR[S comparable, E comparable, C any] struct {
	Name       string           `json:"name"`
	States     []State[S, E, C] `json:"states,omitempty"`
	Initial    S                `json:"initial"`
	HasInitial bool             `json:"hasInitial"`
}

// ToJSON serializes the machine's IR losslessly.
func (m *Machine[S, E, C]) ToJSON(opts ...ToJSONOption) ([]byte, error) {
	cfg := toJSONConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	ir := IR[S, E, C]{
		Name:       m.name,
		States:     append([]State[S, E, C](nil), m.states...),
		Initial:    m.initial,
		HasInitial: m.hasInitial,
	}
	return json.Marshal(ir)
}

// LoadFromJSON rehydrates an IR from JSON.
func LoadFromJSON[S comparable, E comparable, C any](b []byte, opts ...LoadOption) (*IR[S, E, C], error) {
	cfg := loadConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	var ir IR[S, E, C]
	if err := json.Unmarshal(b, &ir); err != nil {
		return nil, err
	}
	return &ir, nil
}

// Provide binds every Ref in the IR against the host registry and returns a
// Builder ready to Quench. Refs that do not resolve are surfaced at Quench as
// the typed *ErrUnboundRef (the same failure the DSL raises for an unregistered
// ref), so a UI/JSON-authored machine and a DSL-authored machine fail
// identically.
func (ir *IR[S, E, C]) Provide(reg *Registry[C], opts ...ProvideOption) *Builder[S, E, C] {
	cfg := provideConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	b := Forge[S, E, C](ir.Name)
	// Adopt the host registry wholesale; ref resolution happens at Quench.
	for name, fn := range reg.guards {
		b.reg.guards[name] = fn
	}
	for name, fn := range reg.actions {
		b.reg.actions[name] = fn
	}

	// The IR carries its hierarchy already nested. Register every state
	// (top-level and nested) in the flat builder index so lint and indexing see
	// them, and mark the builder prebuilt so Quench keeps the nested structure
	// verbatim rather than re-assembling it.
	b.prebuilt = true
	for i := range ir.States {
		s := &ir.States[i]
		sd := &stateDef[S, E, C]{state: *s}
		b.states = append(b.states, sd)
		b.stateIndex[s.Name] = sd
		indexNestedIR(b, s)
	}

	if ir.HasInitial {
		b.initial = ir.Initial
		b.hasInitial = true
	}

	return b
}

// indexNestedIR registers a state's nested children and region states in the
// builder's flat index so transition-target lints resolve them. It does not add
// them to b.states: only top-level states are emitted by Quench when prebuilt.
func indexNestedIR[S comparable, E comparable, C any](b *Builder[S, E, C], s *State[S, E, C]) {
	for i := range s.Children {
		c := &s.Children[i]
		b.stateIndex[c.Name] = &stateDef[S, E, C]{state: *c}
		indexNestedIR(b, c)
	}
	for ri := range s.Regions {
		for i := range s.Regions[ri].States {
			c := &s.Regions[ri].States[i]
			b.stateIndex[c.Name] = &stateDef[S, E, C]{state: *c}
			indexNestedIR(b, c)
		}
	}
}
