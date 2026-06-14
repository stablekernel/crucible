package main

import (
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
	defer func() {
		if r := recover(); r != nil {
			m = nil
			err = fmt.Errorf("quench: %v", r)
		}
	}()
	reg := stubRegistry(ir)
	return ir.Provide(reg).Quench(), nil
}
