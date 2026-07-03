module github.com/stablekernel/crucible/wasm

go 1.25.11

toolchain go1.26.4

replace github.com/stablekernel/crucible/state => ../state

require (
	github.com/stablekernel/crucible/state v0.0.0-00010101000000-000000000000
	github.com/tetratelabs/wazero v1.12.0
)

require golang.org/x/sys v0.45.0 // indirect
