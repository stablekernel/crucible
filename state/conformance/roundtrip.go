package conformance

import (
	"bytes"
	"fmt"

	"github.com/stablekernel/crucible/state"
)

// RoundTripIdentity proves the config/implementation split honest: a machine
// authored in Go and the same machine after ToJSON -> LoadFromJSON -> Provide ->
// Quench are the same machine, on both structure and behavior.
//
// It performs two checks:
//
//  1. Structural — the IR is byte-stable under a round-trip (serialize, reload,
//     reserialize, compare).
//  2. Behavioral — every scenario produces an identical result against the
//     code-built machine and the JSON-loaded machine: same final state, same
//     effects, same trace. Because behavior is rebound by name from the same
//     registry, identity here is exact, not approximate.
//
// The caller supplies the registry the JSON-loaded machine binds against (the
// same host palette the DSL registered), a fresh entity per run, and the start
// state. A divergence means the IR is lossy or the registry binding drifted, and
// is returned as an *ErrConformance.
func RoundTripIdentity[S comparable, E comparable, C any](
	forged *state.Machine[S, E, C],
	reg *state.Registry[C],
	scenarios []Scenario,
	codec EventCodec[E],
	startState S,
	newEntity freshEntity[C],
	opts ...CompareOption,
) error {
	data, err := forged.ToJSON()
	if err != nil {
		return fmt.Errorf("conformance: round-trip: ToJSON: %w", err)
	}
	ir, err := state.LoadFromJSON[S, E, C](data)
	if err != nil {
		return fmt.Errorf("conformance: round-trip: LoadFromJSON: %w", err)
	}
	loaded := ir.Provide(reg).Quench()

	data2, err := loaded.ToJSON()
	if err != nil {
		return fmt.Errorf("conformance: round-trip: reserialize: %w", err)
	}

	var mismatches []Mismatch
	if !bytes.Equal(data, data2) {
		mismatches = append(mismatches, Mismatch{
			Scenario:  "round-trip",
			Field:     "ir.bytes",
			Reference: fmt.Sprintf("%d bytes", len(data)),
			Subject:   fmt.Sprintf("%d bytes", len(data2)),
		})
	}

	cfg := compareConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	for _, sc := range scenarios {
		forgedRes := RunAgainst(forged, sc, newEntity(), codec, startState)
		loadedRes := RunAgainst(loaded, sc, newEntity(), codec, startState)
		mismatches = append(mismatches, diffResults(sc.Name, forgedRes, loadedRes, cfg)...)
	}

	if len(mismatches) == 0 {
		return nil
	}
	return &ErrConformance{Mismatches: mismatches}
}
