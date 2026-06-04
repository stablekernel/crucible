package state

import "fmt"

// Requirements returns the declarative requirements for a state, or nil if
// the state declares none (or is undeclared).
func (m *Machine[S, E, C]) Requirements(s S) []Requirement[C] {
	reqs := m.requirements[s]
	if len(reqs) == 0 {
		return nil
	}
	return append([]Requirement[C](nil), reqs...)
}

// Verify checks that an externally-constructed entity legally satisfies a
// state's declarative requirements, without firing. The default mode is
// fail-fast (the returned *VerifyError carries the first failure); Aggregate
// collects every failure in one pass. The error type is uniform across modes.
func (m *Machine[S, E, C]) Verify(s S, entity C, opts ...VerifyOption) error {
	cfg := verifyConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	if _, ok := m.stateByName(s); !ok {
		return &ErrUndeclaredState{State: fmtState(s)}
	}

	var failures []RequirementFailure
	for _, req := range m.requirements[s] {
		if req.Predicate == nil {
			continue
		}
		if !req.Predicate(entity) {
			failures = append(failures, RequirementFailure{
				Name:   req.Name,
				Reason: fmt.Sprintf("requirement %q not satisfied", req.Name),
			})
			if !cfg.aggregate {
				break
			}
		}
	}

	if len(failures) > 0 {
		return &VerifyError{Failures: failures}
	}
	return nil
}
