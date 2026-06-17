package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/stablekernel/crucible/state"
)

// readInput returns the bytes of the named IR file, or of stdin when path is
// "-". It is the single entry point every subcommand uses to read an IR
// argument, so "-" means stdin uniformly.
func readInput(path string, stdin io.Reader) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(stdin)
	}
	return os.ReadFile(path)
}

// loadIR reads and decodes an IR from the named path (or stdin for "-"). The
// machine's state, event, and context types are fixed to string, string, any —
// the headless tool never instantiates the context, so any is sufficient to load
// and inspect the structure.
func loadIR(path string, stdin io.Reader) (*state.IR[string, string, any], error) {
	b, err := readInput(path, stdin)
	if err != nil {
		return nil, err
	}
	ir, err := state.LoadFromJSON[string, string, any](b)
	if err != nil {
		return nil, fmt.Errorf("load IR: %w", err)
	}
	return ir, nil
}

// quench binds an IR against a stub registry and assembles a *Machine. Quench
// panics on a structural defect the lint rejects (an undeclared transition
// target, for example), so the panic is recovered and returned as an error
// rather than crashing the tool.
func quench(ir *state.IR[string, string, any]) (m *state.Machine[string, string, any], err error) {
	return quenchWith(ir, stubRegistry(ir))
}

// quenchWith binds an IR against a caller-supplied registry and assembles a
// *Machine. Like quench, it recovers the panic Quench raises on a structural
// defect and returns it as an error. simulate uses this to bind a registry whose
// guards return seeded verdicts rather than the always-false stubs.
func quenchWith(ir *state.IR[string, string, any], reg *state.Registry[any]) (m *state.Machine[string, string, any], err error) {
	defer func() {
		if r := recover(); r != nil {
			m = nil
			err = fmt.Errorf("quench: %v", r)
		}
	}()
	return ir.Provide(reg).Quench(), nil
}

// simulateRegistry walks an IR to enumerate every referenced behavior name, then
// registers behaviors suitable for a structural simulation. Guards return the
// seeded verdict from verdicts (defaulting to false for an unseeded guard), so a
// caller can drive a machine down a chosen path without real implementations.
// Actions, reducers, and services remain total no-ops, matching stubRegistry.
func simulateRegistry(ir *state.IR[string, string, any], verdicts map[string]bool) *state.Registry[any] {
	var b behaviorNames
	for i := range ir.States {
		b.walkState(&ir.States[i])
	}
	reg := state.NewRegistry[any]()
	for name := range b.guards {
		reg.Guard(name, func(state.GuardCtx[any]) bool { return verdicts[name] })
	}
	for name := range b.actions {
		reg.Action(name, func(state.ActionCtx[any]) (state.Effect, error) { return nil, nil })
	}
	for name := range b.reducers {
		reg.Reducer(name, func(in state.AssignCtx[any]) any { return in.Entity })
	}
	for name := range b.services {
		reg.Service(name, func(context.Context, state.ServiceCtx[any]) (any, error) { return nil, nil })
	}
	return reg
}
