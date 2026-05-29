package state

// PlanPath returns the shortest event sequence that drives an instance from the
// `from` state to the `to` state, found by breadth-first search over the static
// transition graph. Guards are honored against the supplied entity, so the
// returned path is one the entity can actually traverse. The entity is never
// mutated. ErrNoPath is returned when no sequence connects from->to.
func (m *Machine[S, E, C]) PlanPath(from, to S, entity C, opts ...PlanOption) ([]E, error) {
	cfg := planConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if from == to {
		return []E{}, nil
	}

	type node struct {
		state S
		path  []E
	}

	visited := map[S]bool{from: true}
	queue := []node{{state: from, path: nil}}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		src, ok := m.stateByName(cur.state)
		if !ok {
			continue
		}

		for ti := range src.Transitions {
			t := &src.Transitions[ti]
			if t.EventLess || t.Internal {
				continue
			}
			if visited[t.To] {
				continue
			}
			if !m.guardsPass(t, entity) {
				continue
			}

			nextPath := make([]E, len(cur.path)+1)
			copy(nextPath, cur.path)
			nextPath[len(cur.path)] = t.On

			if t.To == to {
				return nextPath, nil
			}

			visited[t.To] = true
			queue = append(queue, node{state: t.To, path: nextPath})
		}
	}

	return nil, &ErrNoPath{From: fmtState(from), To: fmtState(to)}
}

// guardsPass reports whether every guard on a transition passes for the entity.
// A guard panic is treated as a failed guard for planning purposes (the path is
// not traversable), keeping PlanPath pure and non-panicking.
func (m *Machine[S, E, C]) guardsPass(t *Transition[S, E, C], entity C) bool {
	for _, g := range t.Guards {
		ok, err := m.evalGuard(g, entity)
		if err != nil || !ok {
			return false
		}
	}
	return true
}
