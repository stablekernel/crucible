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
	states := deepCopyStates(m.states)
	if cfg.withoutSrcPos {
		for i := range states {
			stripSrcPos(&states[i])
		}
	}
	ir := IR[S, E, C]{
		Name:       m.name,
		States:     states,
		Initial:    m.initial,
		HasInitial: m.hasInitial,
	}
	return json.Marshal(ir)
}

// deepCopyStates clones a state slice deeply enough that mutating source
// positions on the copy never touches the live machine — children, regions,
// and the transition slices each get their own backing arrays.
func deepCopyStates[S comparable, E comparable, C any](in []State[S, E, C]) []State[S, E, C] {
	if in == nil {
		return nil
	}
	out := make([]State[S, E, C], len(in))
	for i := range in {
		out[i] = in[i]
		out[i].Transitions = append([]Transition[S, E, C](nil), in[i].Transitions...)
		for ti := range out[i].Transitions {
			out[i].Transitions[ti].GuardExpr = cloneGuardNode(in[i].Transitions[ti].GuardExpr)
		}
		out[i].Children = deepCopyStates(in[i].Children)
		if in[i].Regions != nil {
			out[i].Regions = make([]Region[S, E, C], len(in[i].Regions))
			for r := range in[i].Regions {
				out[i].Regions[r] = in[i].Regions[r]
				out[i].Regions[r].States = deepCopyStates(in[i].Regions[r].States)
			}
		}
	}
	return out
}

// stripSrcPos clears the diagnostic source-position fields on a state's
// transitions and recurses through its hierarchy, so a WithoutSrcPos
// serialization carries no absolute filesystem paths.
func stripSrcPos[S comparable, E comparable, C any](s *State[S, E, C]) {
	for i := range s.Transitions {
		s.Transitions[i].SrcFile = ""
		s.Transitions[i].SrcLine = 0
	}
	for i := range s.Children {
		stripSrcPos(&s.Children[i])
	}
	for r := range s.Regions {
		for i := range s.Regions[r].States {
			stripSrcPos(&s.Regions[r].States[i])
		}
	}
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
	for name, fn := range reg.services {
		b.reg.services[name] = fn
	}
	// Carry the palette metadata over too, so a Provide'd machine surfaces the
	// same Palette as a DSL-authored one.
	for key, d := range reg.descriptors {
		b.reg.descriptors[key] = d
	}
	b.reg.actorDescs = append(b.reg.actorDescs, reg.actorDescs...)

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
